"""PySpark Structured Streaming Pipeline for FlashLogix Lakehouse.

Consumes order events from Kafka (`flashsale.orders`), enforces explicit
StructType schema, applies SHA-256 salted PII masking, persists clean Silver
Parquet records to MinIO/S3 with isolated checkpointing, and computes 1-minute
tumbling window aggregations.
"""
import argparse
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pyspark
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from app.core.config import settings
from app.core.storage import ensure_lakehouse_bucket
from streaming.transformations import (
    apply_pii_masking_and_clean,
    compute_windowed_sales,
    parse_kafka_order_events,
)


def create_spark_session(app_name: str = "FlashLogix-Realtime-Analytics") -> SparkSession:
    """Initializes SparkSession with Kafka and MinIO S3A connectors."""
    packages = (
        f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__},"
        "org.apache.hadoop:hadoop-aws:3.5.0"
    )
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.packages", packages)
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_streaming_pipeline(
    starting_offsets: str = "latest",
    available_now: bool = False,
) -> None:
    """Starts the Kafka -> Silver Parquet & Windowed Aggregation streaming queries."""
    ensure_lakehouse_bucket(settings.MINIO_BUCKET)
    spark = create_spark_session()

    base_s3_path = f"s3a://{settings.MINIO_BUCKET}"
    silver_path = f"{base_s3_path}/silver/orders"
    checkpoint_path = f"{base_s3_path}/checkpoints/silver_orders_chk"
    window_checkpoint_path = f"{base_s3_path}/checkpoints/window_sales_chk"

    logger.info(
        f"[SPARK_STREAM] Connecting to Kafka '{settings.KAFKA_BOOTSTRAP_SERVERS}' "
        f"topic='{settings.KAFKA_TOPIC_ORDERS}' offsets='{starting_offsets}'"
    )

    raw_kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", settings.KAFKA_TOPIC_ORDERS)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_orders = parse_kafka_order_events(raw_kafka_stream)
    secure_silver_orders = apply_pii_masking_and_clean(
        parsed_orders, salt=settings.PII_SALT
    ).select(
        col("event_id"),
        col("event_type"),
        col("order_id"),
        col("masked_user_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        col("total_amount"),
        col("latency_ms"),
        col("order_time"),
        col("order_date"),
    )

    silver_writer = (
        secure_silver_orders.writeStream.format("parquet")
        .partitionBy("order_date")
        .option("path", silver_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
    )
    if available_now:
        silver_writer = silver_writer.trigger(availableNow=True)

    silver_query = silver_writer.start()
    logger.info(
        f"[SPARK_STREAM] Silver sink started -> path='{silver_path}' "
        f"checkpoint='{checkpoint_path}'"
    )

    windowed_sales = compute_windowed_sales(secure_silver_orders)
    console_writer = (
        windowed_sales.writeStream.format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("checkpointLocation", window_checkpoint_path)
    )
    if available_now:
        console_writer = console_writer.trigger(availableNow=True)

    console_query = console_writer.start()

    if available_now:
        silver_query.awaitTermination()
        console_query.awaitTermination()
        logger.info("[SPARK_STREAM] AvailableNow micro-batch completed cleanly.")
        spark.stop()
    else:
        logger.info("[SPARK_STREAM] Continuous streaming queries active. Awaiting termination...")
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FlashLogix PySpark Structured Streaming")
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["earliest", "latest"],
        help="Kafka offset strategy on initial run.",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process all currently available Kafka messages once and exit cleanly.",
    )
    args = parser.parse_args()
    run_streaming_pipeline(
        starting_offsets=args.starting_offsets,
        available_now=args.available_now,
    )
