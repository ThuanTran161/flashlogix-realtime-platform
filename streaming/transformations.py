"""PySpark Data Transformations for FlashLogix Streaming Pipeline.

Implements:
1. Explicit StructType schema enforcement (no inferSchema on streams).
2. PII Data Masking using salted SHA-256 hashing (`masked_user_id`).
3. UTC timestamp normalization and event deduplication.
4. Tumbling window aggregations (1-minute revenue & velocity-based bot flags).
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    concat,
    count,
    from_json,
    lit,
    round as spark_round,
    sha2,
    sum as spark_sum,
    to_date,
    to_utc_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Explicit Kafka JSON Event Schema (nullable=True protects against partial payloads)
ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("latency_ms", IntegerType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


def parse_kafka_order_events(raw_kafka_df: DataFrame) -> DataFrame:
    """Parses raw Kafka binary `value` payloads using explicit ORDER_EVENT_SCHEMA."""
    return (
        raw_kafka_df.select(
            from_json(col("value").cast("string"), ORDER_EVENT_SCHEMA).alias("data")
        )
        .select("data.*")
        .filter(col("order_id").isNotNull() & col("product_id").isNotNull())
    )


def apply_pii_masking_and_clean(df: DataFrame, salt: str) -> DataFrame:
    """Masks customer PII via SHA-256 + salt, normalizes UTC timestamps, and deduplicates."""
    cleaned = (
        df.withColumn("masked_user_id", sha2(concat(col("user_id"), lit(salt)), 256))
        .withColumn("order_time", to_utc_timestamp(col("timestamp"), "UTC"))
        .withColumn("order_date", to_date(col("order_time")))
        .withColumn(
            "total_amount",
            when(col("total_amount").isNotNull(), col("total_amount")).otherwise(
                spark_round(col("unit_price") * col("quantity"), 2)
            ),
        )
        .drop("user_id")
    )
    return cleaned.dropDuplicates(["order_id"])


def compute_windowed_sales(clean_df: DataFrame) -> DataFrame:
    """Computes 1-minute tumbling window order counts, revenue, and average latency."""
    return (
        clean_df.withWatermark("order_time", "2 minutes")
        .groupBy(
            window(col("order_time"), "1 minute"),
            col("product_id"),
        )
        .agg(
            count("order_id").alias("order_count"),
            spark_sum("quantity").alias("total_units"),
            spark_round(spark_sum("total_amount"), 2).alias("window_revenue"),
            spark_round(avg("latency_ms"), 2).alias("avg_latency_ms"),
        )
    )


def detect_bot_activity_window(clean_df: DataFrame, threshold: int = 5) -> DataFrame:
    """Flags masked users exceeding order velocity threshold within a 1-minute window."""
    return (
        clean_df.withWatermark("order_time", "2 minutes")
        .groupBy(
            window(col("order_time"), "1 minute"),
            col("masked_user_id"),
        )
        .agg(count("order_id").alias("orders_in_window"))
        .withColumn("is_fraudulent", col("orders_in_window") >= lit(threshold))
    )
