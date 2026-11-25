"""Gold Layer Batch Aggregation & Kimball Star Schema ETL.

Transforms Lakehouse Bronze/Silver Parquet records from MinIO into the
PostgreSQL analytical Star Schema (`Dim_Date`, `Dim_Users` with SCD Type 2,
`Dim_Products`, and `Fact_Flash_Sale_Orders`), using date-partition pruning
and set-based batch upserts, enforcing PII masking, unified velocity-based
fraud flagging, idempotent fact loading, and automated Data Quality auditing.
"""
import argparse
import hashlib
import io
import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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


def build_partitioned_prefix(prefix: str, target_date: Optional[str] = None) -> str:
    """Builds an S3 prefix with optional `order_date=YYYY-MM-DD/` partition pruning."""
    normalized = prefix.rstrip("/") + "/"
    if target_date:
        return f"{normalized}order_date={target_date}/"
    return normalized


def read_parquet_prefix_from_minio(
    prefix: str,
    target_date: Optional[str] = None,
) -> pd.DataFrame:
    """Reads and concatenates Parquet objects under a MinIO S3 prefix (with optional date pruning)."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    effective_prefix = build_partitioned_prefix(prefix, target_date=target_date)
    frames: List[pd.DataFrame] = []

    for page in paginator.paginate(Bucket=settings.MINIO_BUCKET, Prefix=effective_prefix):
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


def count_quarantine_objects() -> int:
    """Counts poison-pill records isolated under MinIO `quarantine/` prefixes."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=settings.MINIO_BUCKET, Prefix="quarantine/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") or (key.endswith(".parquet") and obj.get("Size", 0) > 0):
                count += 1
    return count


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
    """Executes SCD Type 2 Upsert on Dim_Users for a single masked user."""
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


def _batch_upsert_dim_dates(conn: Connection, unique_dts: List[datetime]) -> Dict[int, int]:
    """Batch-inserts unique Dim_Date rows and returns `{date_key: date_key}`."""
    date_map: Dict[int, int] = {}
    params: List[Dict[str, Any]] = []
    for dt in unique_dts:
        dk = int(dt.strftime("%Y%m%d"))
        if dk in date_map:
            continue
        date_map[dk] = dk
        params.append(
            {
                "date_key": dk,
                "full_date": dt.date(),
                "day_of_week": dt.strftime("%A"),
                "month": dt.month,
                "quarter": (dt.month - 1) // 3 + 1,
                "year": dt.year,
                "is_weekend": dt.weekday() >= 5,
            }
        )
    if params:
        conn.execute(
            text(
                """
                INSERT INTO Dim_Date (date_key, full_date, day_of_week, month, quarter, year, is_weekend)
                VALUES (:date_key, :full_date, :day_of_week, :month, :quarter, :year, :is_weekend)
                ON CONFLICT (date_key) DO NOTHING
                """
            ),
            params,
        )
    return date_map


def _batch_upsert_dim_products(
    conn: Connection,
    products_needed: Dict[int, Tuple[str, float]],
) -> Dict[int, int]:
    """Batch-upserts Dim_Products rows and returns `{product_id: product_sk}`."""
    existing_rows = conn.execute(
        text("SELECT product_id, product_sk FROM Dim_Products")
    ).fetchall()
    product_sk_map = {int(r[0]): int(r[1]) for r in existing_rows}

    to_insert = [
        {"pid": pid, "name": name, "cat": "FlashSale", "price": price}
        for pid, (name, price) in products_needed.items()
        if pid not in product_sk_map
    ]
    if to_insert:
        conn.execute(
            text(
                """
                INSERT INTO Dim_Products (product_id, product_name, category, base_price)
                VALUES (:pid, :name, :cat, :price)
                ON CONFLICT (product_id) DO NOTHING
                """
            ),
            to_insert,
        )
        refreshed = conn.execute(
            text("SELECT product_id, product_sk FROM Dim_Products")
        ).fetchall()
        product_sk_map = {int(r[0]): int(r[1]) for r in refreshed}
    return product_sk_map


def _batch_upsert_dim_users_scd2(
    conn: Connection,
    user_tiers: Dict[str, str],
) -> Dict[str, int]:
    """Executes set-based SCD Type 2 batch upsert on Dim_Users."""
    if not user_tiers:
        return {}

    params = [
        {"masked_user_id": masked_uid, "new_tier": tier}
        for masked_uid, tier in user_tiers.items()
    ]
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
        params,
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
        params,
    )

    rows = conn.execute(
        text(
            """
            SELECT masked_user_id, user_sk
            FROM Dim_Users
            WHERE is_current = TRUE
            ORDER BY user_sk ASC
            """
        )
    ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows if str(r[0]) in user_tiers}


def _batch_upsert_fact_orders(conn: Connection, fact_rows: List[Dict[str, Any]]) -> int:
    """Batch-upserts Fact_Flash_Sale_Orders using SQLAlchemy executemany."""
    if not fact_rows:
        return 0
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
            ON CONFLICT (order_id)
            DO UPDATE SET is_fraudulent = Fact_Flash_Sale_Orders.is_fraudulent OR EXCLUDED.is_fraudulent
            """
        ),
        fact_rows,
    )
    return int(res.rowcount or len(fact_rows))


def apply_unified_velocity_fraud_flags(
    df: pd.DataFrame,
    bronze_df: Optional[pd.DataFrame] = None,
    threshold_per_min: Optional[int] = None,
) -> pd.DataFrame:
    """Computes unified fraud flags from 1-minute velocity windows + Bronze alerts."""
    result = df.copy()
    threshold = (
        threshold_per_min
        if threshold_per_min is not None
        else settings.FRAUD_VELOCITY_THRESHOLD_PER_MIN
    )

    existing_flag = (
        result["is_fraudulent"].fillna(False).astype(bool)
        if "is_fraudulent" in result.columns
        else pd.Series(False, index=result.index)
    )

    # Compute 1-minute tumbling window velocity per masked_user_id
    window_bucket = result["parsed_ts"].dt.floor("1min")
    orders_in_window = (
        result.assign(_win=window_bucket)
        .groupby(["masked_user_id", "_win"])["order_id"]
        .transform("count")
    )
    velocity_flag = orders_in_window >= threshold

    # Merge any Bronze real-time velocity flags recorded by OrderAuditConsumer
    bronze_flag = pd.Series(False, index=result.index)
    if bronze_df is not None and not bronze_df.empty and "is_fraudulent" in bronze_df.columns:
        flagged_order_ids = set(
            bronze_df.loc[bronze_df["is_fraudulent"].fillna(False).astype(bool), "order_id"]
            .dropna()
            .astype(int)
            .tolist()
        )
        if flagged_order_ids:
            bronze_flag = result["order_id"].astype(int).isin(flagged_order_ids)

    result["is_fraudulent"] = existing_flag | velocity_flag | bronze_flag
    return result


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


def run_gold_etl(target_date: Optional[str] = None) -> Dict[str, Any]:
    """Runs Gold Lakehouse ETL with optional date-partition pruning & bulk Star Schema upserts."""
    start_ts = time.perf_counter()
    ensure_lakehouse_bucket(settings.MINIO_BUCKET)

    source_layer = "silver"
    df = read_parquet_prefix_from_minio("silver/orders/", target_date=target_date)
    bronze_df = read_parquet_prefix_from_minio("bronze/orders/", target_date=target_date)
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

    if "masked_user_id" not in df.columns and "user_id" in df.columns:
        df["masked_user_id"] = df["user_id"].astype(str).apply(mask_pii)
        df.drop(columns=["user_id"], inplace=True)

    pii_audit = compute_pii_compliance(df)
    invalid_amount_count = (
        int((df["total_amount"].fillna(-1) <= 0).sum())
        if "total_amount" in df.columns
        else deduplicated_orders
    )

    ts_col = "order_time" if "order_time" in df.columns else "timestamp"
    df["parsed_ts"] = pd.to_datetime(df[ts_col], utc=True).dt.tz_convert(None)
    df = apply_unified_velocity_fraud_flags(df, bronze_df=bronze_df)

    # Compute cumulative spend & tier per distinct masked_user_id for batch SCD Type 2
    user_spend = df.groupby("masked_user_id")["total_amount"].sum().to_dict()
    user_tiers = {
        str(uid): infer_membership_tier(float(spent))
        for uid, spent in user_spend.items()
    }

    with engine.begin() as conn:
        ensure_star_schema_tables(conn)
        prod_rows = conn.execute(text("SELECT id, name, price FROM products")).fetchall()
        product_catalog = {int(r[0]): (str(r[1]), float(r[2])) for r in prod_rows}

        products_needed: Dict[int, Tuple[str, float]] = {}
        unique_dts: List[datetime] = []
        seen_date_keys = set()

        prepared_rows: List[Dict[str, Any]] = []
        for row in df.itertuples(index=False):
            product_id = int(row.product_id)
            created_dt = row.parsed_ts.to_pydatetime()
            dk = int(created_dt.strftime("%Y%m%d"))
            if dk not in seen_date_keys:
                seen_date_keys.add(dk)
                unique_dts.append(created_dt)
            if product_id not in products_needed:
                products_needed[product_id] = product_catalog.get(
                    product_id,
                    (f"Flash Product #{product_id}", float(getattr(row, "unit_price", 0.0) or 0.0)),
                )
            prepared_rows.append(
                {
                    "order_id": int(row.order_id),
                    "product_id": product_id,
                    "masked_uid": str(row.masked_user_id),
                    "date_key": dk,
                    "quantity": int(row.quantity),
                    "total_amount": float(getattr(row, "total_amount", 0.0) or 0.0),
                    "latency_ms": int(getattr(row, "latency_ms", 0) or 0),
                    "is_fraudulent": bool(getattr(row, "is_fraudulent", False)),
                    "created_at": created_dt,
                }
            )

        _batch_upsert_dim_dates(conn, unique_dts)
        product_sk_map = _batch_upsert_dim_products(conn, products_needed)
        user_sk_map = _batch_upsert_dim_users_scd2(conn, user_tiers)

        fact_rows = [
            {
                "order_id": r["order_id"],
                "user_sk": user_sk_map[r["masked_uid"]],
                "product_sk": product_sk_map[r["product_id"]],
                "date_key": r["date_key"],
                "quantity": r["quantity"],
                "total_amount": r["total_amount"],
                "latency_ms": r["latency_ms"],
                "is_fraudulent": r["is_fraudulent"],
                "created_at": r["created_at"],
            }
            for r in prepared_rows
        ]
        inserted_facts = _batch_upsert_fact_orders(conn, fact_rows)

        # OLTP-to-Gold Reconciliation Audit (full or target_date slice)
        if target_date:
            target_dt_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
            target_date_key = int(target_dt_obj.strftime("%Y%m%d"))
            oltp_row = conn.execute(
                text(
                    "SELECT COUNT(*), COALESCE(SUM(total_amount), 0) "
                    "FROM orders WHERE DATE(created_at) = :td"
                ),
                {"td": target_dt_obj},
            ).fetchone()
            gold_row = conn.execute(
                text(
                    "SELECT COUNT(*), COALESCE(SUM(total_amount), 0), "
                    "COALESCE(SUM(CASE WHEN is_fraudulent THEN 1 ELSE 0 END), 0) "
                    "FROM Fact_Flash_Sale_Orders WHERE date_key = :dk"
                ),
                {"dk": target_date_key},
            ).fetchone()
        else:
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
    quarantined_objects = count_quarantine_objects()

    order_parity_pct = (
        round((gold_fact_count / oltp_orders_count) * 100.0, 2)
        if oltp_orders_count > 0
        else 100.0
    )
    revenue_delta = round(abs(oltp_revenue - gold_revenue), 2)

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
        "target_date": target_date,
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
        "quarantined_objects": quarantined_objects,
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
        f"[GOLD_ETL] Completed from '{source_layer}' (date={target_date or 'ALL'}) "
        f"in {etl_duration_sec}s | raw={raw_lakehouse_records} -> dedup={deduplicated_orders} "
        f"(filtered_dups={duplicates_filtered}, quarantine_objs={quarantined_objects}) | "
        f"fraud_flagged={fraud_flagged_count} | parity={order_parity_pct}% (delta=${revenue_delta}) | "
        f"pii_violations={pii_audit['pii_violations']}"
    )
    return {
        "processed_orders": deduplicated_orders,
        "inserted_facts": inserted_facts,
        "data_quality": data_quality_report,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FlashLogix Gold Star Schema Batch ETL")
    parser.add_argument(
        "--date",
        dest="target_date",
        default=None,
        help="Optional UTC partition date (YYYY-MM-DD) for incremental partition pruning.",
    )
    args = parser.parse_args()
    run_gold_etl(target_date=args.target_date)
