import hashlib
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
import json
import uuid
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from sqlalchemy import text

from app.core.database import engine
from lakehouse.batch_gold_aggregation import (
    compute_pii_compliance,
    ensure_star_schema_tables,
    mask_pii,
    upsert_dim_user_scd2,
)
from streaming.transformations import (
    apply_pii_masking_and_clean,
    compute_windowed_sales,
    parse_kafka_order_events,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("FlashLogix-Unit-Tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_pyspark_pii_masking_and_deduplication(spark):
    """Tests Kafka JSON schema parsing, SHA-256 PII masking, and order_id deduplication."""
    salt = "TEST_SALT"
    events = [
        json.dumps(
            {
                "event_id": "evt_1",
                "event_type": "ORDER_CREATED",
                "order_id": 101,
                "user_id": "alice_secret_id",
                "product_id": 1,
                "quantity": 2,
                "unit_price": 50.0,
                "total_amount": 100.0,
                "latency_ms": 12,
                "timestamp": "2026-11-25T10:00:05+00:00",
            }
        ),
        # Duplicate order_id=101 simulating Kafka at-least-once redelivery
        json.dumps(
            {
                "event_id": "evt_1_dup",
                "event_type": "ORDER_CREATED",
                "order_id": 101,
                "user_id": "alice_secret_id",
                "product_id": 1,
                "quantity": 2,
                "unit_price": 50.0,
                "total_amount": 100.0,
                "latency_ms": 12,
                "timestamp": "2026-11-25T10:00:05+00:00",
            }
        ),
        json.dumps(
            {
                "event_id": "evt_2",
                "event_type": "ORDER_CREATED",
                "order_id": 102,
                "user_id": "bob_secret_id",
                "product_id": 1,
                "quantity": 1,
                "unit_price": 50.0,
                "total_amount": 50.0,
                "latency_ms": 18,
                "timestamp": "2026-11-25T10:00:30+00:00",
            }
        ),
    ]

    raw_df = spark.createDataFrame([(e,) for e in events], ["value"])
    parsed_df = parse_kafka_order_events(raw_df)
    silver_df = apply_pii_masking_and_clean(parsed_df, salt=salt)

    rows = silver_df.collect()
    assert len(rows) == 2
    assert "user_id" not in silver_df.columns
    assert "masked_user_id" in silver_df.columns

    expected_alice_hash = hashlib.sha256(f"alice_secret_id{salt}".encode("utf-8")).hexdigest()
    alice_row = [r for r in rows if r["order_id"] == 101][0]
    assert alice_row["masked_user_id"] == expected_alice_hash

    # Verify 1-minute tumbling window aggregation
    win_rows = compute_windowed_sales(silver_df).collect()
    assert len(win_rows) == 1
    assert win_rows[0]["order_count"] == 2
    assert win_rows[0]["total_units"] == 3
    assert win_rows[0]["window_revenue"] == 150.0


def test_pii_compliance_audit_check():
    """Verifies Gold ETL Data Quality check flags raw PII and validates SHA-256 hex format."""
    compliant_df = pd.DataFrame([{"order_id": 1, "masked_user_id": mask_pii("u1")}])
    res_ok = compute_pii_compliance(compliant_df)
    assert res_ok["pii_compliance_passed"] is True
    assert res_ok["pii_violations"] == 0

    non_compliant_df = pd.DataFrame([{"order_id": 1, "user_id": "u1", "masked_user_id": "bad_hash"}])
    res_fail = compute_pii_compliance(non_compliant_df)
    assert res_fail["pii_compliance_passed"] is False
    assert res_fail["pii_violations"] > 0


def test_scd_type_2_dim_users_upsert():
    """Tests SCD Type 2 historical tracking in PostgreSQL Dim_Users."""
    test_masked_id = mask_pii(f"scd2_user_{uuid.uuid4().hex}")

    with engine.begin() as conn:
        ensure_star_schema_tables(conn)

        # Initial insert -> Standard tier
        sk1 = upsert_dim_user_scd2(conn, test_masked_id, "Standard")

        # Idempotent re-run with same tier -> returns same surrogate key
        sk1_again = upsert_dim_user_scd2(conn, test_masked_id, "Standard")
        assert sk1_again == sk1

        # Upgrade tier -> expires old record, creates new active record
        sk2 = upsert_dim_user_scd2(conn, test_masked_id, "VIP")
        assert sk2 != sk1

        history = conn.execute(
            text(
                "SELECT user_sk, membership_tier, is_current, effective_to "
                "FROM Dim_Users WHERE masked_user_id = :mid ORDER BY user_sk ASC"
            ),
            {"mid": test_masked_id},
        ).fetchall()

        assert len(history) == 2
        assert history[0][1] == "Standard"
        assert history[0][2] is False
        assert history[0][3] is not None
        assert history[1][1] == "VIP"
        assert history[1][2] is True
        assert history[1][3] is None
