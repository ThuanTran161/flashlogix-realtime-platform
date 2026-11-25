"""FlashLogix Command Center — Interactive Control Plane & Real-Time Lakehouse UI."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pandas as pd
import streamlit as st
from sqlalchemy import text

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.core.config import settings
from app.core.database import engine
from app.core.storage import get_s3_client
from app.services.inventory_service import inventory_service
from lakehouse.batch_gold_aggregation import run_gold_etl

st.set_page_config(
    page_title="FlashLogix — Command Center",
    page_icon=":material/bolt:",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container { padding-top: 1.4rem; padding-bottom: 2rem; }
    .status-pill {
        display: inline-flex; align-items: center; gap: 0.4rem;
        padding: 0.28rem 0.75rem; border-radius: 999px;
        font-size: 0.78rem; font-weight: 600; margin-right: 0.45rem; margin-bottom: 0.35rem;
    }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
    .pill-ok { background: rgba(16, 185, 129, 0.14); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.38); }
    .pill-ok .status-dot { background: #10b981; }
    .pill-err { background: rgba(239, 68, 68, 0.14); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.38); }
    .pill-err .status-dot { background: #ef4444; }
    .pill-warn { background: rgba(245, 158, 11, 0.14); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.38); }
    .pill-warn .status-dot { background: #f59e0b; }
    </style>
    """,
    unsafe_allow_html=True,
)


def api_request(
    base_url: str,
    method: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 5.0,
) -> Tuple[int, Dict[str, Any], Dict[str, str], float]:
    """Executes HTTP JSON request against FastAPI using stdlib urllib."""
    url = f"{base_url.rstrip('/')}{path}"
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    data_bytes = None
    if payload is not None:
        data_bytes = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data_bytes, headers=req_headers, method=method.upper())
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            body = json.loads(resp.read().decode("utf-8"))
            resp_headers = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, body, resp_headers, elapsed_ms
    except urllib.error.HTTPError as exc:
        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except Exception:
            body = {"detail": raw, "error_code": f"HTTP_{exc.code}"}
        resp_headers = {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
        return exc.code, body, resp_headers, elapsed_ms
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        return 0, {"detail": str(exc), "error_code": "CONNECTION_ERROR"}, {}, elapsed_ms


def load_oltp_metrics() -> Tuple[pd.DataFrame, pd.DataFrame, Tuple[int, float, float]]:
    with engine.connect() as conn:
        products_df = pd.read_sql(
            text("SELECT id, name, price, stock, version FROM products ORDER BY id"),
            conn,
        )
        orders_df = pd.read_sql(
            text(
                "SELECT id, user_id, product_id, quantity, total_amount, latency_ms, status, created_at "
                "FROM orders ORDER BY id DESC LIMIT 200"
            ),
            conn,
        )
        agg_row = conn.execute(
            text("SELECT COUNT(*), COALESCE(SUM(total_amount), 0), COALESCE(AVG(latency_ms), 0) FROM orders")
        ).fetchone()
    return products_df, orders_df, (int(agg_row[0]), float(agg_row[1]), float(agg_row[2]))


def load_outbox_metrics() -> Tuple[Dict[str, int], pd.DataFrame]:
    with engine.connect() as conn:
        counts_rows = conn.execute(
            text("SELECT status, COUNT(*) FROM outbox_events GROUP BY status")
        ).fetchall()
        outbox_df = pd.read_sql(
            text(
                "SELECT id, aggregate_id, status, attempts, last_error, created_at, published_at "
                "FROM outbox_events ORDER BY id DESC LIMIT 50"
            ),
            conn,
        )
    counts = {"PUBLISHED": 0, "PENDING": 0, "FAILED": 0}
    for status_name, cnt in counts_rows:
        counts[str(status_name)] = int(cnt)
    return counts, outbox_df


def load_gold_metrics() -> Tuple[Tuple[int, float, int], pd.DataFrame]:
    with engine.connect() as conn:
        fact_row = conn.execute(
            text(
                "SELECT COUNT(*), COALESCE(SUM(total_amount), 0), "
                "COALESCE(SUM(CASE WHEN is_fraudulent THEN 1 ELSE 0 END), 0) "
                "FROM Fact_Flash_Sale_Orders"
            )
        ).fetchone()
        tiers_df = pd.read_sql(
            text(
                "SELECT membership_tier, COUNT(*) AS user_count "
                "FROM Dim_Users WHERE is_current = TRUE GROUP BY membership_tier"
            ),
            conn,
        )
    return (int(fact_row[0]), float(fact_row[1]), int(fact_row[2])), tiers_df


def load_data_quality_report() -> Dict[str, Any]:
    s3 = get_s3_client()
    resp = s3.get_object(
        Bucket=settings.MINIO_BUCKET,
        Key="gold/data_quality_report/latest.json",
    )
    return json.loads(resp["Body"].read().decode("utf-8"))


def count_lakehouse_objects() -> Dict[str, int]:
    s3 = get_s3_client()
    counts = {"bronze": 0, "silver": 0, "gold": 0, "quarantine": 0}
    paginator = s3.get_paginator("list_objects_v2")
    for layer in counts:
        total = 0
        for page in paginator.paginate(Bucket=settings.MINIO_BUCKET, Prefix=f"{layer}/"):
            for obj in page.get("Contents", []):
                if obj.get("Size", 0) > 0:
                    total += 1
        counts[layer] = total
    return counts


def list_quarantine_keys(limit: int = 20) -> List[str]:
    s3 = get_s3_client()
    resp = s3.list_objects_v2(Bucket=settings.MINIO_BUCKET, Prefix="quarantine/")
    keys = [
        o["Key"]
        for o in sorted(resp.get("Contents", []), key=lambda x: x["LastModified"], reverse=True)
        if o.get("Size", 0) > 0
    ]
    return keys[:limit]


def render_pill(label: str, healthy: Optional[bool]) -> str:
    if healthy is True:
        return f"<span class='status-pill pill-ok'><span class='status-dot'></span>{label}: ONLINE</span>"
    if healthy is False:
        return f"<span class='status-pill pill-err'><span class='status-dot'></span>{label}: OFFLINE</span>"
    return f"<span class='status-pill pill-warn'><span class='status-dot'></span>{label}: CHECKING</span>"


# ----------------------------------------------------------------------
# Sidebar Control Center
# ----------------------------------------------------------------------
with st.sidebar:
    st.header(":material/tune: Command Center")
    api_base_url = st.text_input("FastAPI Endpoint", value="http://127.0.0.1:8000")
    if st.button(
        "Làm mới dữ liệu",
        icon=":material/refresh:",
        width="stretch",
        type="primary",
    ):
        st.rerun()

    st.divider()
    st.subheader(":material/link: Truy cập nhanh")
    st.markdown(
        f"- [Swagger API Docs]({api_base_url.rstrip('/')}/docs)\n"
        f"- [Telemetry JSON]({api_base_url.rstrip('/')}/metrics)\n"
        "- [MinIO Console](http://localhost:9001)"
    )
    st.caption(
        "Mẹo: Dùng `.\\run.ps1` để bật toàn bộ hệ thống chỉ với 1 dòng lệnh, "
        "và `.\\run.ps1 stop` để tắt gọn."
    )


# ----------------------------------------------------------------------
# Header & Live Infrastructure Status Strip
# ----------------------------------------------------------------------
st.title(":material/bolt: FlashLogix — Transaction Engine & Lakehouse")
st.caption(
    "Điều khiển tập trung API hiệu năng cao (Redis Lua + Lazy DB + Outbox), "
    "Kafka DLQ, PySpark Streaming, Gold Star Schema và Data Quality SLA."
)

health_code, health_body, _, health_ms = api_request(api_base_url, "GET", "/health", timeout=2.0)
api_online = health_code == 200
svc_map = health_body.get("services", {}) if api_online else {}

pills_html = "".join(
    [
        render_pill(f"FastAPI ({health_ms:.0f}ms)" if api_online else "FastAPI", api_online),
        render_pill("PostgreSQL", svc_map.get("database") == "HEALTHY" if api_online else None),
        render_pill("Redis", svc_map.get("redis") == "HEALTHY" if api_online else None),
        render_pill("Kafka", svc_map.get("kafka") == "HEALTHY" if api_online else None),
        render_pill("MinIO", svc_map.get("minio") == "HEALTHY" if api_online else None),
    ]
)
st.markdown(pills_html, unsafe_allow_html=True)

if not api_online:
    st.warning(
        f"Chưa kết nối được FastAPI tại `{api_base_url}`. "
        "Hãy chạy `.\\run.ps1` (hoặc `uvicorn app.main:app --port 8000 --workers 4`) để bật đầy đủ nút điều khiển API."
    )

# Load core database & telemetry snapshots safely
try:
    products_df, orders_df, (total_orders, total_revenue, avg_latency) = load_oltp_metrics()
    outbox_counts, outbox_df = load_outbox_metrics()
except Exception as exc:
    st.error(f"Không thể kết nối PostgreSQL FlashLogix (port 5433): {exc}")
    st.stop()

redis_stock_1 = inventory_service.get_stock(1)
pending_or_failed = outbox_counts.get("PENDING", 0) + outbox_counts.get("FAILED", 0)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Confirmed Orders (OLTP)", f"{total_orders:,}")
k2.metric("Gross Revenue", f"${total_revenue:,.2f}")
k3.metric("Avg OLTP Latency", f"{avg_latency:.1f} ms")
k4.metric("Product #1 Redis Stock", str(redis_stock_1) if redis_stock_1 is not None else "N/A")
k5.metric(
    "Outbox Pending / Failed",
    str(pending_or_failed),
    delta="All Published" if pending_or_failed == 0 else "Needs Replay",
    delta_color="normal" if pending_or_failed == 0 else "inverse",
)

# ----------------------------------------------------------------------
# 5 Interactive Control Tabs (Material Symbols Icons)
# ----------------------------------------------------------------------
tab_ctrl, tab_metrics, tab_outbox, tab_lake, tab_dq = st.tabs(
    [
        ":material/play_circle: Điều Khiển & Test",
        ":material/monitoring: Telemetry & OLTP",
        ":material/mark_email_read: Outbox & Kafka DLQ",
        ":material/database: Lakehouse & Gold ETL",
        ":material/verified_user: Data Quality SLA",
    ]
)

# === TAB 1: INTERACTIVE PLAYGROUND & SYSTEM CONTROLS ===
with tab_ctrl:
    col_inv, col_ord = st.columns(2)

    with col_inv:
        with st.container(border=True):
            st.subheader(":material/inventory_2: Quản lý Kho & Sản phẩm")
            if not products_df.empty:
                prod_options = {
                    f"#{int(r.id)} — {r.name} (DB={int(r.stock)}, Redis={inventory_service.get_stock(int(r.id))})": int(
                        r.id
                    )
                    for r in products_df.itertuples(index=False)
                }
                selected_label = st.selectbox("Chọn sản phẩm", list(prod_options.keys()))
                selected_pid = prod_options[selected_label]
            else:
                selected_pid = st.number_input("Product ID", min_value=1, value=1, step=1)

            reset_stock_val = st.number_input("Số lượng tồn kho mới", min_value=0, value=100, step=10)
            if st.button(
                "Nạp / Reset Kho (DB + Redis)",
                icon=":material/sync:",
                width="stretch",
                disabled=not api_online,
            ):
                code, body, _, ms = api_request(
                    api_base_url,
                    "POST",
                    f"/api/v1/products/{int(selected_pid)}/init-stock",
                    payload={"initial_stock": int(reset_stock_val)},
                )
                if code == 200:
                    st.success(f"Đã nạp kho Product #{selected_pid} = {reset_stock_val} ({ms} ms).")
                    st.rerun()
                else:
                    st.error(f"Lỗi ({code}): {body}")

            with st.expander("Tạo sản phẩm mới", icon=":material/add_circle:"):
                new_p_name = st.text_input("Tên sản phẩm", value="FlashLogix Gaming Mouse X1")
                c_price, c_stock = st.columns(2)
                new_p_price = c_price.number_input("Giá ($)", min_value=1.0, value=79.99, step=5.0)
                new_p_stock = c_stock.number_input("Tồn kho ban đầu", min_value=0, value=50, step=10)
                if st.button("Tạo sản phẩm", icon=":material/add_circle:", width="stretch", disabled=not api_online):
                    code, body, _, _ = api_request(
                        api_base_url,
                        "POST",
                        "/api/v1/products",
                        payload={
                            "name": new_p_name,
                            "price": float(new_p_price),
                            "stock": int(new_p_stock),
                        },
                    )
                    if code == 201:
                        st.success(f"Đã tạo sản phẩm ID #{body['data']['id']}!")
                        st.rerun()
                    else:
                        st.error(f"Lỗi ({code}): {body}")

    with col_ord:
        with st.container(border=True):
            st.subheader(":material/shopping_cart: Đặt Đơn Trực Tiếp & Test Idempotency")
            if "ui_idem_key" not in st.session_state:
                st.session_state["ui_idem_key"] = f"ui_{uuid.uuid4().hex[:12]}"

            o_col1, o_col2 = st.columns(2)
            order_pid = o_col1.number_input(
                "Product ID đặt mua", min_value=1, value=int(selected_pid), step=1
            )
            order_qty = o_col2.number_input("Số lượng (1-10)", min_value=1, max_value=10, value=1)
            order_uid = st.text_input("User ID", value="demo_shopper_01")

            ik1, ik2 = st.columns([3, 1])
            idem_val = ik1.text_input("Idempotency-Key", key="ui_idem_key")
            with ik2:
                st.write("")
                if st.button("Key mới", icon=":material/autorenew:", width="stretch"):
                    st.session_state["ui_idem_key"] = f"ui_{uuid.uuid4().hex[:12]}"
                    st.rerun()

            st.caption("Giữ nguyên Idempotency-Key và bấm Đặt Đơn 2 lần để kiểm chứng chống trừ kho kép.")
            if st.button(
                "Gửi Đơn Hàng (POST /orders)",
                icon=":material/send:",
                width="stretch",
                type="primary",
                disabled=not api_online,
            ):
                code, body, hdrs, ms = api_request(
                    api_base_url,
                    "POST",
                    "/api/v1/orders",
                    payload={
                        "user_id": order_uid,
                        "product_id": int(order_pid),
                        "quantity": int(order_qty),
                    },
                    headers={"Idempotency-Key": idem_val} if idem_val else None,
                )
                req_id = hdrs.get("x-request-id", "N/A")
                proc_ms = hdrs.get("x-process-time-ms", str(ms))
                if code == 201:
                    st.success(
                        f"HTTP {code} Created | Order #{body['data']['id']} | "
                        f"Total=${body['data']['total_amount']:.2f} | {proc_ms} ms | Request-ID={req_id}"
                    )
                else:
                    st.warning(f"HTTP {code} | {body.get('error_code')} | {body.get('detail')} | {proc_ms} ms")
                st.json(body)

    with st.container(border=True):
        st.subheader(":material/speed: Giả Lập Tải Cao & Bot Fraud")
        sim1, sim2 = st.columns(2)

        with sim1:
            st.markdown("**1) Burst Test (Nhiều người cùng tranh mua)**")
            b_col1, b_col2 = st.columns(2)
            burst_total = b_col1.slider("Tổng số requests", min_value=10, max_value=120, value=40, step=10)
            burst_workers = b_col2.slider("Số luồng đồng thời", min_value=2, max_value=30, value=15, step=1)
            if st.button(
                "Bắn Burst Flash Sale",
                icon=":material/flash_on:",
                width="stretch",
                disabled=not api_online,
            ):
                def _fire_once(idx: int) -> Tuple[int, float]:
                    c, _, _, m = api_request(
                        api_base_url,
                        "POST",
                        "/api/v1/orders",
                        payload={
                            "user_id": f"burst_user_{idx}",
                            "product_id": int(selected_pid),
                            "quantity": 1,
                        },
                        headers={"Idempotency-Key": f"burst_{uuid.uuid4().hex[:12]}"},
                    )
                    return c, m

                t_start = time.perf_counter()
                codes: List[int] = []
                latencies: List[float] = []
                with ThreadPoolExecutor(max_workers=burst_workers) as pool:
                    futs = [pool.submit(_fire_once, i) for i in range(burst_total)]
                    for f in as_completed(futs):
                        sc, lm = f.result()
                        codes.append(sc)
                        latencies.append(lm)
                wall_sec = max(0.01, time.perf_counter() - t_start)
                ok_201 = sum(1 for c in codes if c == 201)
                oos_400 = sum(1 for c in codes if c == 400)
                other_err = sum(1 for c in codes if c not in (201, 400, 409))
                st.info(
                    f"Hoàn thành {burst_total} requests trong {wall_sec:.2f}s "
                    f"(~{burst_total / wall_sec:.1f} req/s): "
                    f"**201 Thành công = {ok_201}** | **400 Hết hàng = {oos_400}** | **Lỗi khác = {other_err}** | "
                    f"Avg Latency = {sum(latencies)/len(latencies):.1f} ms"
                )

        with sim2:
            st.markdown("**2) Bot Velocity Fraud Test (>= 5 đơn/phút)**")
            bot_uid = st.text_input("Bot User ID giả lập", value="bot_attacker_999")
            if st.button(
                "Bắn 6 Đơn Bot Liên Tiếp",
                icon=":material/security:",
                width="stretch",
                disabled=not api_online,
            ):
                bot_codes = []
                for idx in range(6):
                    sc, _, _, _ = api_request(
                        api_base_url,
                        "POST",
                        "/api/v1/orders",
                        payload={
                            "user_id": bot_uid,
                            "product_id": int(selected_pid),
                            "quantity": 1,
                        },
                        headers={"Idempotency-Key": f"bot_{uuid.uuid4().hex[:10]}_{idx}"},
                    )
                    bot_codes.append(sc)
                st.warning(
                    f"Đã gửi 6 đơn từ `{bot_uid}` -> HTTP codes: {bot_codes}. "
                    "Hãy qua tab **Lakehouse & Gold ETL** chạy Gold ETL để thấy `Fraud Flagged` tăng."
                )


# === TAB 2: MULTI-WORKER TELEMETRY & OLTP ===
with tab_metrics:
    m_code, m_body, _, _ = api_request(api_base_url, "GET", "/metrics", timeout=2.5)
    if m_code == 200:
        counters = m_body.get("counters", {})
        pool_info = m_body.get("db_pool", {})
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("HTTP Requests", f"{counters.get('http_requests_total', 0):,}")
        c2.metric("Product Cache Hits", f"{counters.get('product_cache_hits_total', 0):,}")
        c3.metric("Lua Out-of-Stock Rejects", f"{counters.get('redis_gate_rejected_out_of_stock_total', 0):,}")
        c4.metric("Kafka Published", f"{counters.get('kafka_events_published_total', 0):,}")
        c5.metric("Kafka Failures", f"{counters.get('kafka_publish_failures_total', 0):,}")

        ch1, ch2 = st.columns(2)
        with ch1:
            st.markdown("**Phân bổ HTTP Status (Multi-Worker Redis Aggregated)**")
            status_df = pd.DataFrame(
                [
                    {"Status": "200 OK", "Count": counters.get("http_status_200_total", 0)},
                    {"Status": "201 Created", "Count": counters.get("http_status_201_total", 0)},
                    {"Status": "400 OutOfStock", "Count": counters.get("http_status_400_total", 0)},
                    {"Status": "409 Conflict", "Count": counters.get("http_status_409_total", 0)},
                    {"Status": "500 Error", "Count": counters.get("http_status_500_total", 0)},
                ]
            ).set_index("Status")
            st.bar_chart(status_df, width="stretch")

        with ch2:
            st.markdown("**Hiệu quả Redis Gate & DB Pool**")
            gate_df = pd.DataFrame(
                [
                    {"Metric": "Cache Hits", "Count": counters.get("product_cache_hits_total", 0)},
                    {"Metric": "Cache Misses", "Count": counters.get("product_cache_misses_total", 0)},
                    {"Metric": "Lua Rejects", "Count": counters.get("redis_gate_rejected_out_of_stock_total", 0)},
                    {"Metric": "DB Committed", "Count": counters.get("orders_committed_total", 0)},
                ]
            ).set_index("Metric")
            st.bar_chart(gate_df, width="stretch")
            st.caption(
                f"Worker DB Pool: size={pool_info.get('pool_size')} | "
                f"checked_in={pool_info.get('checked_in')} | checked_out={pool_info.get('checked_out')}"
            )
    else:
        st.info("Bật FastAPI server để xem biểu đồ `/metrics` trực tiếp từ 4 workers.")

    st.divider()
    st.subheader(":material/warehouse: Tồn kho Realtime (PostgreSQL vs Redis RAM)")
    if not products_df.empty:
        inv_view = products_df.copy()
        inv_view["redis_ram_stock"] = inv_view["id"].apply(inventory_service.get_stock)
        inv_view["in_sync"] = inv_view["stock"] == inv_view["redis_ram_stock"]
        st.dataframe(inv_view, width="stretch")
    else:
        st.info("Chưa có sản phẩm nào trong DB.")

    st.subheader(":material/receipt_long: Lịch sử Đơn hàng gần đây (OLTP)")
    f1, f2 = st.columns(2)
    filter_uid = f1.text_input("Lọc theo user_id (để trống = tất cả)", value="")
    filter_pid = f2.number_input("Lọc theo product_id (0 = tất cả)", min_value=0, value=0, step=1)
    filtered_orders = orders_df.copy()
    if filter_uid.strip():
        filtered_orders = filtered_orders[
            filtered_orders["user_id"].astype(str).str.contains(filter_uid.strip(), case=False, na=False)
        ]
    if filter_pid > 0:
        filtered_orders = filtered_orders[filtered_orders["product_id"] == int(filter_pid)]
    st.dataframe(filtered_orders.head(50), width="stretch")


# === TAB 3: TRANSACTIONAL OUTBOX & KAFKA DLQ ===
with tab_outbox:
    ob1, ob2, ob3, ob4 = st.columns(4)
    ob1.metric("PUBLISHED", f"{outbox_counts.get('PUBLISHED', 0):,}")
    ob2.metric("PENDING", f"{outbox_counts.get('PENDING', 0):,}")
    ob3.metric("FAILED", f"{outbox_counts.get('FAILED', 0):,}")
    with ob4:
        st.write("")
        if st.button(
            "Replay Outbox Events",
            icon=":material/replay:",
            width="stretch",
            disabled=not api_online,
        ):
            code, body, _, _ = api_request(
                api_base_url, "POST", "/api/v1/orders/outbox/replay?limit=200"
            )
            if code == 200:
                st.success(f"Replay xong: {body.get('data')}")
                st.rerun()
            else:
                st.error(f"Replay thất bại ({code}): {body}")

    st.dataframe(outbox_df, width="stretch")

    st.divider()
    st.subheader(":material/report_problem: Poison-Pill Quarantine / Dead-Letter Queue (MinIO)")
    try:
        q_keys = list_quarantine_keys(limit=20)
        if q_keys:
            selected_q = st.selectbox("Chọn bản ghi cách ly để kiểm tra", q_keys)
            if selected_q.endswith(".json"):
                s3 = get_s3_client()
                raw_q = s3.get_object(Bucket=settings.MINIO_BUCKET, Key=selected_q)["Body"].read().decode("utf-8")
                st.json(json.loads(raw_q))
            else:
                st.info(f"File Parquet Quarantine: `s3://{settings.MINIO_BUCKET}/{selected_q}`")
        else:
            st.success("Chưa có poison-pill nào trong `quarantine/`.")
    except Exception as exc:
        st.warning(f"Không truy cập được MinIO quarantine: {exc}")


# === TAB 4: LAKEHOUSE & GOLD STAR SCHEMA ETL ===
with tab_lake:
    st.subheader(":material/account_tree: Điều khiển Pipeline Lakehouse (Silver & Gold)")
    etl_c1, etl_c2, etl_c3 = st.columns([1.2, 1.0, 1.2])

    with etl_c1:
        st.write("")
        if st.button(
            "Chạy PySpark Silver (--available-now)",
            icon=":material/stream:",
            width="stretch",
        ):
            with st.status("Đang chạy PySpark Structured Streaming (Kafka -> Silver UTC)...", expanded=True) as status_box:
                t0 = time.perf_counter()
                proc = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "streaming.spark_stream_orders",
                        "--starting-offsets",
                        "earliest",
                        "--available-now",
                    ],
                    cwd=ROOT_DIR,
                    capture_output=True,
                    text=True,
                )
                dur = round(time.perf_counter() - t0, 2)
                if proc.returncode == 0:
                    status_box.update(label=f"PySpark Silver hoàn tất sau {dur}s!", state="complete")
                    st.code("\n".join(proc.stdout.splitlines()[-15:]))
                else:
                    status_box.update(label="PySpark Silver gặp lỗi!", state="error")
                    st.code(proc.stderr[-1500:])

    default_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prune_date = etl_c2.text_input(
        "Ngày UTC cho Gold Pruning (để trống = ALL)",
        value="",
        placeholder=default_utc,
    )
    with etl_c3:
        st.write("")
        if st.button(
            "Chạy Gold Star Schema ETL",
            icon=":material/layers:",
            width="stretch",
            type="primary",
        ):
            target = prune_date.strip() or None
            with st.spinner(f"Đang chạy Gold ETL (target_date={target or 'ALL'})..."):
                res = run_gold_etl(target_date=target)
            dq_res = res.get("data_quality", {})
            st.success(
                f"Gold ETL xong trong {dq_res.get('etl_duration_sec', 0)}s | "
                f"Source={dq_res.get('source_layer')} | Processed={res.get('processed_orders')} | "
                f"Parity={dq_res.get('order_parity_pct')}% | Delta=${dq_res.get('revenue_delta')}"
            )

    st.divider()
    lake_col, gold_col = st.columns(2)
    with lake_col:
        st.subheader(":material/storage: Tầng Dữ Liệu MinIO Medallion")
        try:
            lake_counts = count_lakehouse_objects()
            lake_df = pd.DataFrame(
                [{"Layer": k.upper(), "Files": v} for k, v in lake_counts.items()]
            ).set_index("Layer")
            st.bar_chart(lake_df, width="stretch")
        except Exception as exc:
            st.warning(f"MinIO chưa sẵn sàng: {exc}")

    with gold_col:
        st.subheader(":material/schema: Gold Star Schema & SCD Type 2")
        try:
            (gold_orders, gold_rev, fraud_cnt), tiers_df = load_gold_metrics()
            g1, g2, g3 = st.columns(3)
            g1.metric("Gold Fact Orders", f"{gold_orders:,}")
            g2.metric("Gold Revenue", f"${gold_rev:,.2f}")
            g3.metric("Fraud Flagged Orders", f"{fraud_cnt:,}")
            if not tiers_df.empty:
                st.bar_chart(tiers_df.set_index("membership_tier"), width="stretch")
        except Exception as exc:
            st.info(f"Chưa có bảng Gold Star Schema — hãy bấm 'Chạy Gold Star Schema ETL' ({exc}).")


# === TAB 5: DATA QUALITY SLA AUDIT ===
with tab_dq:
    st.subheader(":material/verified_user: Data Quality & Lakehouse Reconciliation SLA")
    try:
        dq = load_data_quality_report()
        d1, d2, d3, d4, d5 = st.columns(5)
        d1.metric("OLTP ↔ Gold Parity", f"{dq.get('order_parity_pct', 0):.2f}%")
        d2.metric("Revenue Delta", f"${dq.get('revenue_delta', 0):.2f}")
        d3.metric("Duplicates Filtered", f"{dq.get('duplicates_filtered', 0):,}")
        d4.metric("Quarantined / DLQ", f"{dq.get('quarantined_objects', 0):,}")
        d5.metric(
            "PII Compliance",
            "PASS (0 violations)" if dq.get("pii_compliance_passed") else "FAIL",
        )
        st.json(dq)
    except Exception:
        st.info("Chưa có báo cáo Data Quality. Hãy qua tab **Lakehouse & Gold ETL** và bấm **Chạy Gold Star Schema ETL**.")
