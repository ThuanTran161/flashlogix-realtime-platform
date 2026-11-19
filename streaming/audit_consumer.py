"""Background Kafka Audit & Fraud Velocity Consumer (OLAP Ingestion).

Consumes real-time order events from Apache Kafka (`flashsale.orders`),
performs real-time velocity-based bot/fraud detection using Redis counters,
and archives immutable raw event batches to the MinIO Bronze Lakehouse layer.
"""
import io
import json
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from loguru import logger

from app.core.config import settings
from app.core.storage import ensure_lakehouse_bucket, get_s3_client
from app.services.inventory_service import redis_client


class OrderAuditConsumer:
    """Consumes Kafka order events for real-time audit, fraud checks, and Bronze archival."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        fraud_threshold_per_min: int = 5,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or settings.KAFKA_TOPIC_ORDERS
        self.group_id = group_id or settings.KAFKA_CONSUMER_GROUP
        self.fraud_threshold_per_min = fraud_threshold_per_min
        self._running = True
        self._consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._s3 = get_s3_client()
        ensure_lakehouse_bucket(settings.MINIO_BUCKET)

    def stop(self, *args) -> None:
        logger.info("[AUDIT_CONSUMER] Shutdown signal received. Flushing batch...")
        self._running = False

    def detect_bot_velocity(self, user_id: str) -> bool:
        """Sliding 60-second velocity check via Redis INCR + EXPIRE."""
        key = f"fraud:velocity:{user_id}"
        try:
            count = redis_client.incr(key)
            if count == 1:
                redis_client.expire(key, 60)
            return int(count) > self.fraud_threshold_per_min
        except Exception as exc:
            logger.warning(f"[AUDIT_CONSUMER] Redis velocity check skipped: {exc}")
            return False

    def flush_bronze_batch(self, records: List[Dict]) -> Optional[str]:
        """Writes an immutable raw event batch as Parquet to MinIO Bronze layer."""
        if not records:
            return None

        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        ts_str = now.strftime("%Y%m%dT%H%M%S_%f")
        object_key = f"bronze/orders/order_date={date_str}/events_{ts_str}.parquet"

        table = pa.Table.from_pylist(records)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        self._s3.put_object(
            Bucket=settings.MINIO_BUCKET,
            Key=object_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.info(
            f"[BRONZE_LAKE] Flushed {len(records)} raw events to "
            f"s3://{settings.MINIO_BUCKET}/{object_key}"
        )
        return object_key

    def run(
        self,
        batch_size: int = 50,
        flush_interval_sec: float = 5.0,
        max_messages: Optional[int] = None,
        idle_timeout_sec: Optional[float] = None,
    ) -> int:
        """Runs the consumer loop until stopped or limits are reached."""
        self._consumer.subscribe([self.topic])
        logger.info(
            f"[AUDIT_CONSUMER] Subscribed to topic='{self.topic}' "
            f"group='{self.group_id}' brokers='{self.bootstrap_servers}'"
        )

        buffer: List[Dict] = []
        last_flush = time.monotonic()
        last_msg_ts = time.monotonic()
        processed_count = 0

        try:
            while self._running:
                if max_messages is not None and processed_count >= max_messages:
                    break

                if (
                    idle_timeout_sec is not None
                    and (time.monotonic() - last_msg_ts) >= idle_timeout_sec
                ):
                    logger.info("[AUDIT_CONSUMER] Idle timeout reached. Exiting loop cleanly.")
                    break

                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    if buffer and (time.monotonic() - last_flush) >= flush_interval_sec:
                        self.flush_bronze_batch(buffer)
                        self._consumer.commit(asynchronous=False)
                        buffer.clear()
                        last_flush = time.monotonic()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                last_msg_ts = time.monotonic()
                raw_payload = msg.value().decode("utf-8")
                event = json.loads(raw_payload)

                is_bot = self.detect_bot_velocity(str(event.get("user_id", "")))
                event["is_fraudulent"] = is_bot
                event["ingested_at"] = datetime.now(timezone.utc).isoformat()

                if is_bot:
                    logger.warning(
                        f"[FRAUD_ALERT] order_id={event.get('order_id')} "
                        f"user_id={event.get('user_id')} exceeded velocity threshold!"
                    )

                buffer.append(event)
                processed_count += 1

                if len(buffer) >= batch_size or (time.monotonic() - last_flush) >= flush_interval_sec:
                    self.flush_bronze_batch(buffer)
                    self._consumer.commit(asynchronous=False)
                    buffer.clear()
                    last_flush = time.monotonic()

        finally:
            if buffer:
                self.flush_bronze_batch(buffer)
                try:
                    self._consumer.commit(asynchronous=False)
                except Exception:
                    pass
            self._consumer.close()
            logger.info(f"[AUDIT_CONSUMER] Stopped cleanly. Total processed={processed_count}")

        return processed_count


if __name__ == "__main__":
    consumer = OrderAuditConsumer()
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    consumer.run()
