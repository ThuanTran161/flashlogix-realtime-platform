"""Gold Layer Batch Aggregation & Kimball Star Schema ETL.

Transforms Lakehouse Bronze/Silver Parquet records from MinIO into the
PostgreSQL analytical Star Schema (`Dim_Date`, `Dim_Users` with SCD Type 2,
`Dim_Products`, and `Fact_Flash_Sale_Orders`), enforcing PII masking,
idempotent fact loading, and automated Data Quality & Reconciliation auditing.
"""
import hashlib
import io
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.core.config import settings
from app.core.database import engine
from app.core.storage import ensure_lakehouse_bucket, get_s3_client

SHA256_HEX_RE = re.compile(r"^[a-f0-9]{64}$")


def mask_pii(raw_user_id: str, salt: Optional[str] = None) -> str:
    """Applies deterministic salted SHA-256 hashing matching PySpark's sha2."""
    effective_salt = salt if salt is not None else settings.PII_SALT
    return hashlib.sha256(f"{raw_user_id}{effective_salt}".encode("utf-8")).hexdigest()


def infer_membership_tier(total_spent: float) -> str:
    """Derives customer membership tier for SCD Type 2 tracking."""
    if total_spent >= 3000.0:
        return "Platinum"
    if total_spent >= 1500.0:
        return "VIP"
    return "Standard"


def read_parquet_prefix_from_minio(prefix: str) -> pd.DataFrame:
    """Reads and concatenates all Parquet objects under a MinIO S3 prefix."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    frames: List[pd.DataFrame] = []

    for page in paginator.paginate(Bucket=settings.MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            resp = s3.get_object(Bucket=settings.MINIO_BUCKET, Key=key)
            table = pq.read_table(io.BytesIO(resp["Body"].read()))
            frames.append(table.to_pandas())

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def ensure_star_schema_tables(conn: Connection) -> None:
    """Executes DDL statements from streaming/star_schema.sql idempotently."""
    with open("streaming/star_schema.sql", "r", encoding="utf-8") as f:
        ddl_sql = f.read()
    for stmt in [s.strip() for s in ddl_sql.split(";") if s.strip()]:
        conn.execute(text(stmt))


def upsert_dim_date(conn: Connection, dt: datetime) -> int:
    """Idempotently inserts a date row and returns its YYYYMMDD surrogate key."""
    date_key = int(dt.strftime("%Y%m%d"))
    conn.execute(
        text(
            """
            INSERT INTO Dim_Date (date_key, full_date, day_of_week, month, quarter, year, is_weekend)
            VALUES (:date_key, :full_date, :day_of_week, :month, :quarter, :year, :is_weekend)
            ON CONFLICT (date_key) DO NOTHING
            """
        ),
        {
            "date_key": date_key,
            "full_date": dt.date(),
            "day_of_week": dt.strftime("%A"),
            "month": dt.month,
            "quarter": (dt.month - 1) // 3 + 1,
            "year": dt.year,
            "is_weekend": dt.weekday() >= 5,
        },
    )
    return date_key


def upsert_dim_product(
    conn: Connection,
    product_id: int,
    product_name: str,
    base_price: float,
    category: str = "FlashSale",
) -> int:
    """Upserts product dimension and returns surrogate key `product_sk`."""
    row = conn.execute(
        text("SELECT product_sk FROM Dim_Products WHERE product_id = :pid LIMIT 1"),
        {"pid": product_id},
    ).fetchone()
    if row:
        return int(row[0])

    inserted = conn.execute(
        text(
            """
            INSERT INTO Dim_Products (product_id, product_name, category, base_price)
            VALUES (:pid, :name, :cat, :price)
            RETURNING product_sk
            """
        ),
        {"pid": product_id, "name": product_name, "cat": category, "price": base_price},
    ).fetchone()
    return int(inserted[0])


def upsert_dim_user_scd2(
    conn: Connection, masked_user_id: str, new_tier: str
) -> int:
    """Executes SCD Type 2 Upsert on Dim_Users (Skill 4).

    1. Expires active row (`is_current = FALSE, effective_to = NOW()`) if tier changed.
    2. Inserts a new active row if no current row matches `(masked_user_id, new_tier)`.
    3. Returns the active `user_sk` surrogate key.
    """
    conn.execute(
        text(
            """
            UPDATE Dim_Users
            SET effective_to = NOW(),
                is_current = FALSE
            WHERE masked_user_id = :masked_user_id
              AND is_current = TRUE
              AND membership_tier != :new_tier
            """
        ),
        {"masked_user_id": masked_user_id, "new_tier": new_tier},
    )

    conn.execute(
        text(
            """
            INSERT INTO Dim_Users (masked_user_id, membership_tier, effective_from, effective_to, is_current)
            SELECT CAST(:masked_user_id AS VARCHAR(64)), CAST(:new_tier AS VARCHAR(20)), NOW(), NULL, TRUE
            WHERE NOT EXISTS (
                SELECT 1 FROM Dim_Users
                WHERE masked_user_id = :masked_user_id
                  AND membership_tier = :new_tier
                  AND is_current = TRUE
            )
            """
        ),
        {"masked_user_id": masked_user_id, "new_tier": new_tier},
    )

    row = conn.execute(
        text(
            """
            SELECT user_sk FROM Dim_Users
            WHERE masked_user_id = :masked_user_id AND is_current = TRUE
            ORDER BY user_sk DESC LIMIT 1
            """
        ),
        {"masked_user_id": masked_user_id},
    ).fetchone()
    return int(row[0])


def compute_pii_compliance(df: pd.DataFrame) -> Dict[str, Any]:
    """Verifies zero raw PII exposure and strict 64-hex SHA-256 masking compliance."""
    raw_pii_column_present = "user_id" in df.columns
    invalid_hashes = 0
    if "masked_user_id" in df.columns:
        invalid_hashes = int(
            (~df["masked_user_id"].astype(str).str.match(SHA256_HEX_RE)).sum()
        )
    else:
        invalid_hashes = len(df)

    violations = invalid_hashes + (len(df) if raw_pii_column_present else 0)
    return {
        "raw_pii_column_present": raw_pii_column_present,
        "invalid_hash_count": invalid_hashes,
        "pii_violations": violations,
        "pii_compliance_passed": violations == 0,
    }


def run_gold_etl() -> Dict[str, Any]:
    """Runs Gold Lakehouse ETL and emits Data Quality & Reconciliation SLA metrics."""
    start_ts = time.perf_counter()
    ensure_lakehouse_bucket(settings.MINIO_BUCKET)

    # Prefer Silver Parquet; fall back to Bronze Parquet (and apply PII masking) if Silver is empty
    source_layer = "silver"
    df = read_parquet_prefix_from_minio("silver/orders/")
    bronze_df = read_parquet_prefix_from_minio("bronze/orders/")
    if df.empty:
        source_layer = "bronze"
        df = bronze_df

    if df.empty:
        logger.info("[GOLD_ETL] No Parquet events found in MinIO Silver or Bronze layers yet.")
        return {"processed_orders": 0, "inserted_facts": 0, "data_quality": {}}

    source_raw_records = int(len(df))
    bronze_raw_records = int(len(bronze_df)) if not bronze_df.empty else source_raw_records
    raw_lakehouse_records = max(source_raw_records, bronze_raw_records)

    null_order_ids = int(df["order_id"].isna().sum()) if "order_id" in df.columns else source_raw_records
    df = df.dropna(subset=["order_id"]).copy()

    ts_sort_col = "order_time" if "order_time" in df.columns else "timestamp"
    if ts_sort_col in df.columns:
        df = df.sort_values(ts_sort_col)
    df = df.drop_duplicates(subset=["order_id"], keep="last").copy()

    deduplicated_orders = int(len(df))
    duplicates_filtered = max(0, raw_lakehouse_records - deduplicated_orders)

    # Enforce PII Masking if sourced from Bronze
    if "masked_user_id" not in df.columns and "user_id" in df.columns:
        df["masked_user_id"] = df["user_id"].astype(str).apply(mask_pii)
        df.drop(columns=["user_id"], inplace=True)

    if "is_fraudulent" not in df.columns:
        df["is_fraudulent"] = False

    pii_audit = compute_pii_compliance(df)
    invalid_amount_count = (
        int((df["total_amount"].fillna(-1) <= 0).sum())
        if "total_amount" in df.columns
        else deduplicated_orders
    )

    # Normalize timestamp column
    ts_col = "order_time" if "order_time" in df.columns else "timestamp"
    df["parsed_ts"] = pd.to_datetime(df[ts_col], utc=True).dt.tz_convert(None)

    # Compute cumulative spend per masked_user_id for SCD Type 2 tier classification
    user_spend = df.groupby("masked_user_id")["total_amount"].sum().to_dict()

    with engine.begin() as conn:
        ensure_star_schema_tables(conn)
        prod_rows = conn.execute(text("SELECT id, name, price FROM products")).fetchall()
        product_catalog = {int(r[0]): (str(r[1]), float(r[2])) for r in prod_rows}

        inserted_facts = 0
        for row in df.itertuples(index=False):
            order_id = int(row.order_id)
            product_id = int(row.product_id)
            masked_uid = str(row.masked_user_id)
            qty = int(row.quantity)
            total_amt = float(getattr(row, "total_amount", 0.0) or 0.0)
            latency_ms = int(getattr(row, "latency_ms", 0) or 0)
            is_fraud = bool(getattr(row, "is_fraudulent", False))
            created_dt = row.parsed_ts.to_pydatetime()

            prod_name, base_price = product_catalog.get(
                product_id,
                (f"Flash Product #{product_id}", float(getattr(row, "unit_price", 0.0) or 0.0)),
            )
            tier = infer_membership_tier(float(user_spend.get(masked_uid, total_amt)))

            date_key = upsert_dim_date(conn, created_dt)
            product_sk = upsert_dim_product(conn, product_id, prod_name, base_price)
            user_sk = upsert_dim_user_scd2(conn, masked_uid, tier)

            res = conn.execute(
                text(
                    """
                    INSERT INTO Fact_Flash_Sale_Orders (
                        order_id, user_sk, product_sk, date_key,
                        quantity, total_amount, latency_ms, is_fraudulent, created_at
                    )
                    VALUES (
                        :order_id, :user_sk, :product_sk, :date_key,
                        :quantity, :total_amount, :latency_ms, :is_fraudulent, :created_at
                    )
                    ON CONFLICT (order_id) DO NOTHING
                    """
                ),
                {
                    "order_id": order_id,
                    "user_sk": user_sk,
                    "product_sk": product_sk,
                    "date_key": date_key,
                    "quantity": qty,
                    "total_amount": total_amt,
                    "latency_ms": latency_ms,
                    "is_fraudulent": is_fraud,
                    "created_at": created_dt,
                },
            )
            inserted_facts += res.rowcount or 0

        # OLTP-to-Gold Reconciliation Audit
        oltp_row = conn.execute(
            text("SELECT COUNT(*), COALESCE(SUM(total_amount), 0) FROM orders")
        ).fetchone()
        gold_row = conn.execute(
            text(
                "SELECT COUNT(*), COALESCE(SUM(total_amount), 0), "
                "COALESCE(SUM(CASE WHEN is_fraudulent THEN 1 ELSE 0 END), 0) "
                "FROM Fact_Flash_Sale_Orders"
            )
        ).fetchone()

    oltp_orders_count = int(oltp_row[0])
    oltp_revenue = round(float(oltp_row[1]), 2)
    gold_fact_count = int(gold_row[0])
    gold_revenue = round(float(gold_row[1]), 2)
    fraud_flagged_count = int(gold_row[2])

    order_parity_pct = (
        round((gold_fact_count / oltp_orders_count) * 100.0, 2)
        if oltp_orders_count > 0
        else 100.0
    )
    revenue_delta = round(abs(oltp_revenue - gold_revenue), 2)

    # Materialize Gold summary Parquet snapshot to MinIO
    gold_summary = (
        df.groupby("product_id", as_index=False)
        .agg(
            total_orders=("order_id", "count"),
            total_units=("quantity", "sum"),
            total_revenue=("total_amount", "sum"),
            avg_latency_ms=("latency_ms", "mean"),
        )
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    gold_summary["generated_at"] = generated_at
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(gold_summary), buf, compression="snappy")
    buf.seek(0)
    s3 = get_s3_client()
    s3.put_object(
        Bucket=settings.MINIO_BUCKET,
        Key="gold/daily_product_revenue/latest.parquet",
        Body=buf.getvalue(),
    )

    etl_duration_sec = round(time.perf_counter() - start_ts, 3)
    data_quality_report = {
        "generated_at": generated_at,
        "source_layer": source_layer,
        "etl_duration_sec": etl_duration_sec,
        "bronze_raw_records": bronze_raw_records,
        "raw_lakehouse_records": raw_lakehouse_records,
        "deduplicated_orders": deduplicated_orders,
        "duplicates_filtered": duplicates_filtered,
        "dedup_filter_rate_pct": (
            round((duplicates_filtered / raw_lakehouse_records) * 100.0, 2)
            if raw_lakehouse_records > 0
            else 0.0
        ),
        "null_order_id_count": null_order_ids,
        "invalid_amount_count": invalid_amount_count,
        **pii_audit,
        "oltp_orders_count": oltp_orders_count,
        "gold_fact_count": gold_fact_count,
        "oltp_revenue": oltp_revenue,
        "gold_revenue": gold_revenue,
        "order_parity_pct": order_parity_pct,
        "revenue_delta": revenue_delta,
        "fraud_flagged_count": fraud_flagged_count,
    }
    s3.put_object(
        Bucket=settings.MINIO_BUCKET,
        Key="gold/data_quality_report/latest.json",
        Body=json.dumps(data_quality_report, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        f"[GOLD_ETL] Completed from '{source_layer}' in {etl_duration_sec}s | "
        f"raw={raw_lakehouse_records} -> dedup={deduplicated_orders} "
        f"(filtered_dups={duplicates_filtered}) | inserted_facts={inserted_facts} | "
        f"parity={order_parity_pct}% (delta=${revenue_delta}) | "
        f"pii_violations={pii_audit['pii_violations']}"
    )
    return {
        "processed_orders": deduplicated_orders,
        "inserted_facts": inserted_facts,
        "data_quality": data_quality_report,
    }


if __name__ == "__main__":
    run_gold_etl()
