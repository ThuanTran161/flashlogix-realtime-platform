"""Controlled Before-vs-After Load Benchmark & End-to-End Lakehouse SLA Verifier.

Usage:
    .venv\\Scripts\\python.exe scripts/benchmark_compare.py --reset-clean --verify-lakehouse
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.core.config import settings
from app.core.database import engine, ensure_oltp_schema_and_constraints
from app.core.storage import ensure_lakehouse_bucket, get_s3_client
from app.services.inventory_service import redis_client
from app.services.kafka_producer import EventPublisher
from lakehouse.batch_gold_aggregation import run_gold_etl
from streaming.audit_consumer import OrderAuditConsumer


def wait_for_health(base_url: str, timeout_sec: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/health", timeout=2.0) as resp:
                if resp.status == 200:
                    # Allow all multi-worker Uvicorn processes to finish binding on Windows
                    time.sleep(2.5)
                    return
        except Exception:
            pass
        time.sleep(0.4)
    raise RuntimeError(f"Server at {base_url} failed health check within {timeout_sec}s.")


def _stop_server_process(proc: subprocess.Popen) -> None:
    """Terminates Uvicorn supervisor and all child worker processes cleanly."""
    if proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
            text=True,
        )
    else:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    time.sleep(1.5)


def reset_clean_environment(initial_stock: int = 100) -> None:
    """Resets OLTP, Gold Star Schema, Redis, MinIO prefixes, and Kafka topics cleanly."""
    logger.info("[RESET] Cleaning PostgreSQL OLTP/Gold, Redis, MinIO, and Kafka topics...")
    ensure_oltp_schema_and_constraints()
    with engine.begin() as conn:
        for table in [
            "Fact_Flash_Sale_Orders",
            "Dim_Users",
            "Dim_Products",
            "Dim_Date",
            "orders",
            "products",
        ]:
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
        conn.execute(
            text(
                "INSERT INTO products (name, price, stock, version) "
                "VALUES ('FlashLogix Mechanical Keyboard Pro', 199.99, :stock, 1)"
            ),
            {"stock": initial_stock},
        )

    redis_client.flushdb()
    redis_client.set("stock:product:1", initial_stock)

    ensure_lakehouse_bucket(settings.MINIO_BUCKET)
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.MINIO_BUCKET):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=settings.MINIO_BUCKET, Delete={"Objects": objs})

    admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    topics = [settings.KAFKA_TOPIC_ORDERS, settings.KAFKA_TOPIC_ORDERS_DLQ]
    try:
        fs = admin.delete_topics(topics, operation_timeout=10.0)
        for fut in fs.values():
            try:
                fut.result()
            except Exception:
                pass
        time.sleep(1.5)
    except Exception:
        pass

    fs_create = admin.create_topics(
        [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics]
    )
    for fut in fs_create.values():
        try:
            fut.result()
        except Exception:
            pass
    logger.info(f"[RESET] Clean baseline ready (Product #1 stock={initial_stock}).")


def init_product_stock_via_api(base_url: str, stock: int = 100) -> None:
    payload = json.dumps({"initial_stock": stock}).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/api/v1/products/1/init-stock",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Failed to init stock: {resp.status}")


def run_single_benchmark(
    mode: str,
    port: int,
    users: int,
    spawn_rate: int,
    duration: str,
    stock: int,
    csv_prefix: Path,
) -> Dict[str, Any]:
    """Starts 4 Uvicorn workers in `mode`, runs headless Locust, and returns metrics."""
    base_url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env["CONCURRENCY_MODE"] = mode
    env["LOG_LEVEL"] = "WARNING"

    logger.info(
        f"[BENCHMARK] Starting Uvicorn (mode={mode}, port={port}, workers=4, "
        f"users={users}, duration={duration})..."
    )
    server_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--workers",
            "4",
            "--backlog",
            "4096",
            "--log-level",
            "warning",
        ],
        cwd=str(ROOT_DIR),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        wait_for_health(base_url, timeout_sec=30.0)
        init_product_stock_via_api(base_url, stock=stock)

        # Warm product cache briefly before burst for redis_async_lazy
        with urllib.request.urlopen(f"{base_url}/api/v1/products/1", timeout=3.0):
            pass

        with engine.connect() as conn:
            orders_before = int(conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() or 0)

        locust_cmd = [
            sys.executable,
            "-m",
            "locust",
            "-f",
            "tests/locustfile.py",
            "--headless",
            "-u",
            str(users),
            "-r",
            str(spawn_rate),
            "-t",
            duration,
            "--host",
            base_url,
            "--csv",
            str(csv_prefix),
            "--exit-code-on-error",
            "0",
            "--only-summary",
        ]
        locust_proc = subprocess.run(
            locust_cmd,
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
        )
        if locust_proc.returncode != 0:
            logger.error(f"[LOCUST_STDERR] {locust_proc.stderr}")
            raise RuntimeError(f"Locust failed with exit code {locust_proc.returncode}")

        # Wait briefly for all BackgroundTasks to complete Kafka delivery flush
        time.sleep(2.5)

        with engine.connect() as conn:
            orders_after = int(conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() or 0)
            db_stock = int(
                conn.execute(text("SELECT stock FROM products WHERE id = 1")).scalar() or 0
            )
    finally:
        _stop_server_process(server_proc)

    stats_file = Path(f"{csv_prefix}_stats.csv")
    agg_row: Dict[str, Any] = {}
    with open(stats_file, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("Name") == "Aggregated":
                agg_row = row
                break

    failures_file = Path(f"{csv_prefix}_failures.csv")
    if failures_file.exists():
        content = failures_file.read_text(encoding="utf-8").strip()
        if len(content.splitlines()) > 1:
            logger.warning(f"[LOCUST_FAILURES ({mode})]\n{content}")

    total_reqs = int(agg_row.get("Request Count", 0))
    failures = int(agg_row.get("Failure Count", 0))
    rps = round(float(agg_row.get("Requests/s", 0.0)), 2)
    p50 = float(agg_row.get("50%", 0.0))
    p95 = float(agg_row.get("95%", 0.0))
    p99 = float(agg_row.get("99%", 0.0))
    avg_ms = round(float(agg_row.get("Average Response Time", 0.0)), 2)
    min_ms = round(float(agg_row.get("Min Response Time", 0.0)), 2)
    max_ms = round(float(agg_row.get("Max Response Time", 0.0)), 2)

    orders_created = orders_after - orders_before
    result = {
        "mode": mode,
        "total_requests": total_reqs,
        "failures": failures,
        "failure_rate_pct": round((failures / total_reqs) * 100.0, 2) if total_reqs else 0.0,
        "rps": rps,
        "avg_ms": avg_ms,
        "min_ms": min_ms,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "max_ms": max_ms,
        "orders_created": orders_created,
        "oversold_count": max(0, orders_created - stock),
        "remaining_db_stock": db_stock,
    }
    logger.info(
        f"[RESULT] mode={mode} | RPS={rps} | reqs={total_reqs} | fails={failures} | "
        f"P50={p50}ms | P95={p95}ms | P99={p99}ms | orders_created={orders_created}/{stock}"
    )
    return result


def verify_lakehouse_pipeline(duplicate_inject_count: int = 20) -> Dict[str, Any]:
    """Runs duplicate + poison-pill injection, Bronze/DLQ, PySpark Silver/Quarantine, and Gold ETL."""
    logger.info(
        f"[LAKEHOUSE_E2E] Injecting {duplicate_inject_count} duplicate Kafka events + 1 poison-pill "
        f"payload to test deduplication, DLQ quarantine, and 100% parity..."
    )
    with engine.connect() as conn:
        sample_orders = conn.execute(
            text(
                "SELECT id, user_id, product_id, quantity, total_amount, latency_ms, created_at "
                "FROM orders ORDER BY id ASC LIMIT :lim"
            ),
            {"lim": duplicate_inject_count},
        ).fetchall()

    publisher = EventPublisher()
    for row in sample_orders:
        unit_price = round(float(row[4]) / max(1, int(row[3])), 2)
        dup_event = EventPublisher.build_order_event(
            order_id=int(row[0]),
            user_id=str(row[1]),
            product_id=int(row[2]),
            quantity=int(row[3]),
            unit_price=unit_price,
            latency_ms=int(row[5]),
            created_at=row[6],
        )
        dup_event["event_id"] = f"dup_{uuid.uuid4().hex[:8]}"
        publisher.publish_order_event(dup_event)
    publisher.flush(timeout=5.0)

    # Inject 1 malformed JSON poison-pill record into Kafka to prove DLQ resilience
    raw_producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    raw_producer.produce(
        topic=settings.KAFKA_TOPIC_ORDERS,
        value=b"{corrupt_poison_pill_payload",
    )
    raw_producer.flush(timeout=5.0)

    # Consume all Kafka events into MinIO Bronze Parquet (routing poison-pill to DLQ + quarantine)
    consumer = OrderAuditConsumer(group_id=f"bench_audit_{uuid.uuid4().hex[:6]}")
    bronze_consumed = consumer.run(batch_size=100, flush_interval_sec=1.5, idle_timeout_sec=4.0)
    logger.info(
        f"[LAKEHOUSE_E2E] AuditConsumer archived {bronze_consumed} valid Bronze events, "
        f"quarantined={consumer.quarantined_count} poison-pill events."
    )

    # Run PySpark Structured Streaming in --available-now mode (Kafka -> Silver + Quarantine)
    spark_start = time.perf_counter()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "streaming.spark_stream_orders",
            "--starting-offsets",
            "earliest",
            "--available-now",
        ],
        cwd=str(ROOT_DIR),
        check=True,
    )
    spark_duration_sec = round(time.perf_counter() - spark_start, 2)

    # Run Gold Star Schema ETL & Data Quality Audit
    gold_result = run_gold_etl()
    dq = gold_result["data_quality"]
    dq["spark_microbatch_duration_sec"] = spark_duration_sec
    dq["bronze_consumed_events"] = bronze_consumed
    dq["dlq_quarantined_events"] = consumer.quarantined_count
    return dq


def main() -> None:
    parser = argparse.ArgumentParser(description="FlashLogix Benchmark & Lakehouse SLA Verifier")
    parser.add_argument("--users", type=int, default=300)
    parser.add_argument("--spawn-rate", type=int, default=100)
    parser.add_argument("--duration", default="10s")
    parser.add_argument("--stock", type=int, default=100)
    parser.add_argument("--reset-clean", action="store_true")
    parser.add_argument("--verify-lakehouse", action="store_true")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    if args.reset_clean:
        reset_clean_environment(initial_stock=args.stock)

    results: List[Dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="flashlogix_bench_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        res_db = run_single_benchmark(
            mode="db_only",
            port=8011,
            users=args.users,
            spawn_rate=args.spawn_rate,
            duration=args.duration,
            stock=args.stock,
            csv_prefix=tmp_path / "db_only",
        )
        results.append(res_db)

        res_async = run_single_benchmark(
            mode="redis_async_lazy",
            port=8012,
            users=args.users,
            spawn_rate=args.spawn_rate,
            duration=args.duration,
            stock=args.stock,
            csv_prefix=tmp_path / "redis_async_lazy",
        )
        results.append(res_async)

    dq_report: Dict[str, Any] = {}
    if args.verify_lakehouse:
        dq_report = verify_lakehouse_pipeline(duplicate_inject_count=20)

    summary = {"benchmarks": results, "data_quality": dq_report}
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        logger.info(f"[SUMMARY] Saved report to {out_path}")
    logger.info(f"[SUMMARY]\n{json.dumps(summary, indent=2)}")


if __name__ == "__main__":
    main()
