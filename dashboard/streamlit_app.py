"""FlashLogix Real-Time OLTP & Lakehouse Analytics Dashboard."""
import json
import pandas as pd
import streamlit as st
from sqlalchemy import text

from app.core.config import settings
from app.core.database import engine
from app.core.storage import get_s3_client
from app.services.inventory_service import inventory_service

st.set_page_config(
    page_title="FlashLogix — Real-Time Command Center",
    page_icon="⚡",
    layout="wide",
)

st.title("⚡ FlashLogix — High-Concurrency Transaction & Lakehouse Monitor")
st.caption(
    "Unified monitoring across FastAPI/Redis (OLTP), Kafka + MinIO Medallion Lakehouse, "
    "and PostgreSQL Kimball Star Schema (Gold Layer)."
)


def load_oltp_metrics():
    with engine.connect() as conn:
        products_df = pd.read_sql(
            text("SELECT id, name, price, stock, version FROM products ORDER BY id"),
            conn,
        )
        orders_df = pd.read_sql(
            text(
                "SELECT id, user_id, product_id, quantity, total_amount, latency_ms, status, created_at "
                "FROM orders ORDER BY created_at DESC LIMIT 500"
            ),
            conn,
        )
        agg_row = conn.execute(
            text(
                "SELECT COUNT(*), COALESCE(SUM(total_amount), 0), COALESCE(AVG(latency_ms), 0) FROM orders"
            )
        ).fetchone()
    return products_df, orders_df, agg_row


def load_gold_metrics():
    with engine.connect() as conn:
        fact_count = conn.execute(
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
    return fact_count, tiers_df


def load_data_quality_report():
    s3 = get_s3_client()
    resp = s3.get_object(
        Bucket=settings.MINIO_BUCKET,
        Key="gold/data_quality_report/latest.json",
    )
    return json.loads(resp["Body"].read().decode("utf-8"))


def count_lakehouse_objects():
    s3 = get_s3_client()
    counts = {"bronze": 0, "silver": 0, "gold": 0}
    for layer in counts:
        resp = s3.list_objects_v2(Bucket=settings.MINIO_BUCKET, Prefix=f"{layer}/")
        counts[layer] = len(resp.get("Contents", []))
    return counts


if st.button("🔄 Refresh Telemetry"):
    st.rerun()

try:
    products_df, orders_df, agg_row = load_oltp_metrics()
    total_orders, total_revenue, avg_latency = int(agg_row[0]), float(agg_row[1]), float(agg_row[2])

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Confirmed Orders (OLTP)", f"{total_orders:,}")
    col2.metric("Gross Merchandise Value", f"${total_revenue:,.2f}")
    col3.metric("Avg OLTP Latency", f"{avg_latency:.1f} ms")

    redis_stock_1 = inventory_service.get_stock(1)
    col4.metric("Product #1 Stock (Redis RAM)", str(redis_stock_1) if redis_stock_1 is not None else "N/A")

    st.divider()
    left, right = st.columns(2)

    with left:
        st.subheader("📦 Real-Time Inventory (PostgreSQL vs Redis)")
        if not products_df.empty:
            products_df["redis_ram_stock"] = products_df["id"].apply(inventory_service.get_stock)
            st.dataframe(products_df, use_container_width=True)
        else:
            st.info("No products created yet.")

    with right:
        st.subheader("🗄️ MinIO Medallion Lakehouse Layers")
        try:
            lake_counts = count_lakehouse_objects()
            lake_df = pd.DataFrame(
                [{"Layer": k.upper(), "Parquet Files": v} for k, v in lake_counts.items()]
            )
            st.bar_chart(lake_df.set_index("Layer"))
        except Exception as exc:
            st.warning(f"MinIO telemetry unavailable: {exc}")

    st.divider()
    st.subheader("⭐ Gold Layer — Kimball Star Schema & SCD Type 2")
    try:
        gold_row, tiers_df = load_gold_metrics()
        g1, g2, g3 = st.columns(3)
        g1.metric("Gold Fact Orders", f"{int(gold_row[0]):,}")
        g2.metric("Gold Verified Revenue", f"${float(gold_row[1]):,.2f}")
        g3.metric("Fraud / Bot Flagged Orders", f"{int(gold_row[2]):,}")
        if not tiers_df.empty:
            st.bar_chart(tiers_df.set_index("membership_tier"))
    except Exception as exc:
        st.info(f"Gold Star Schema awaiting initial ETL run ({exc}).")

    st.divider()
    st.subheader("🛡️ Data Quality & Lakehouse Reconciliation SLA")
    try:
        dq = load_data_quality_report()
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("OLTP ↔ Gold Parity", f"{dq.get('order_parity_pct', 0):.2f}%")
        d2.metric("Revenue Delta", f"${dq.get('revenue_delta', 0):.2f}")
        d3.metric("Duplicates Filtered", f"{dq.get('duplicates_filtered', 0):,}")
        d4.metric(
            "PII Compliance",
            "PASS (0 violations)" if dq.get("pii_compliance_passed") else "FAIL",
        )
    except Exception:
        st.info("Run `python -m lakehouse.batch_gold_aggregation` to generate Data Quality SLA metrics.")

    st.subheader("🧾 Recent Confirmed Orders")
    if not orders_df.empty:
        st.dataframe(orders_df.head(50), use_container_width=True)

except Exception as exc:
    st.error(f"Unable to connect to FlashLogix databases: {exc}")
