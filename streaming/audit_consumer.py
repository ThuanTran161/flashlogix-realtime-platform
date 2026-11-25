"""Background Kafka Audit, DLQ & Fraud Velocity Consumer (OLAP Ingestion).

Consumes real-time order events from Apache Kafka (`flashsale.orders`),
performs real-time velocity-based bot/fraud detection using Redis counters,
routes malformed "poison-pill" payloads safely to a Kafka Dead-Letter Queue
(`flashsale.orders.dlq`) + MinIO `quarantine/orders/`, and archives immutable
raw event batches to the MinIO Bronze Lakehouse layer.
"""
import io
import json
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from loguru import logger

from app.core.config import settings
from app.core.storage import ensure_lakehouse_bucket, get_s3_client
from app.services.inventory_service import redis_client

REQUIRED_EVENT_FIELDS = ("order_id", "user_id", "product_id", "quantity")


class OrderAuditConsumer:
    """Consumes Kafka order events for real-time audit, fraud checks, DLQ, and Bronze archival."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        dlq_topic: Optional[str] = None,
        group_id: Optional[str] = None,
        fraud_threshold_per_min: Optional[int] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or settings.KAFKA_TOPIC_ORDERS
        self.dlq_topic = dlq_topic or settings.KAFKA_TOPIC_ORDERS_DLQ
        self.group_id = group_id or settings.KAFKA_CONSUMER_GROUP
        self.fraud_threshold_per_min = (
            fraud_threshold_per_min
            if fraud_threshold_per_min is not None
            else settings.FRAUD_VELOCITY_THRESHOLD_PER_MIN
        )
        self._running = True
        self._consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._dlq_producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "all",
                "retries": 3,
            }
        )
        self._s3 = get_s3_client()
        ensure_lakehouse_bucket(settings.MINIO_BUCKET)
        self.quarantined_count = 0

    def stop(self, *args) -> None:
        logger.info("[AUDIT_CONSUMER] Shutdown signal received. Flushing batch...")
        self._running = False

    def _safe_commit(self, message=None) -> None:
        """Commits Kafka offsets safely, ignoring benign _NO_OFFSET states."""
        try:
            if message is not None:
                self._consumer.commit(message=message, asynchronous=False)
            else:
                self._consumer.commit(asynchronous=False)
        except KafkaException as exc:
            err = exc.args[0] if exc.args else None
            if hasattr(err, "code") and err.code() == KafkaError._NO_OFFSET:
                logger.debug("[AUDIT_CONSUMER] No pending offsets to commit.")
            else:
                raise

    def detect_bot_velocity(self, user_id: str) -> bool:
        """Sliding 60-second velocity check via Redis INCR + EXPIRE."""
        key = f"fraud:velocity:{user_id}"
        try:
            count = redis_client.incr(key)
            if count == 1:
                redis_client.expire(key, 60)
            return int(count) >= self.fraud_threshold_per_min
        except Exception as exc:
            logger.warning(f"[AUDIT_CONSUMER] Redis velocity check skipped: {exc}")
            return False

    @staticmethod
    def validate_and_parse_message(raw_bytes: bytes) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Validates raw Kafka bytes as schema-compliant JSON order event."""
        try:
            raw_text = raw_bytes.decode("utf-8")
            event = json.loads(raw_text)
        except Exception as exc:
            return None, f"INVALID_JSON: {exc}"

        if not isinstance(event, dict):
            return None, "INVALID_PAYLOAD_TYPE: expected JSON object"

        missing = [f for f in REQUIRED_EVENT_FIELDS if event.get(f) is None]
        if missing:
            return None, f"MISSING_REQUIRED_FIELDS: {','.join(missing)}"

        try:
            int(event["order_id"])
            int(event["product_id"])
            qty = int(event["quantity"])
            if qty <= 0:
                return None, f"INVALID_QUANTITY: {qty}"
        except (TypeError, ValueError) as exc:
            return None, f"INVALID_NUMERIC_FIELD: {exc}"

        return event, None

    def route_to_dlq(self, raw_bytes: bytes, reason: str, partition: int, offset: int) -> None:
        """Routes poison-pill records to Kafka DLQ topic and MinIO quarantine prefix."""
        now = datetime.now(timezone.utc)
        raw_preview = raw_bytes.decode("utf-8", errors="replace")[:2048]
        dlq_payload = {
            "source_topic": self.topic,
            "partition": partition,
            "offset": offset,
            "reason": reason,
            "raw_payload": raw_preview,
            "quarantined_at": now.isoformat(),
        }

        try:
            self._dlq_producer.produce(
                topic=self.dlq_topic,
                value=json.dumps(dlq_payload).encode("utf-8"),
            )
            self._dlq_producer.flush(timeout=2.0)
        except Exception as exc:
            logger.error(f"[DLQ_KAFKA_ERROR] Failed to publish to '{self.dlq_topic}': {exc}")

        try:
            date_str = now.strftime("%Y-%m-%d")
            ts_str = now.strftime("%Y%m%dT%H%M%S_%f")
            q_key = f"quarantine/orders/order_date={date_str}/dlq_{partition}_{offset}_{ts_str}.json"
            self._s3.put_object(
                Bucket=settings.MINIO_BUCKET,
                Key=q_key,
                Body=json.dumps(dlq_payload, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as exc:
            logger.error(f"[DLQ_S3_ERROR] Failed to archive quarantined record: {exc}")

        self.quarantined_count += 1
        logger.warning(
            f"[POISON_PILL_QUARANTINED] topic={self.topic} partition={partition} "
            f"offset={offset} reason='{reason}' dlq='{self.dlq_topic}'"
        )

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
            f"[AUDIT_CONSUMER] Subscribed to topic='{self.topic}' dlq='{self.dlq_topic}' "
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
                        buffer.clear()
                        self._safe_commit()
                        last_flush = time.monotonic()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                last_msg_ts = time.monotonic()
                raw_bytes = msg.value() or b""
                event, error_reason = self.validate_and_parse_message(raw_bytes)
                if event is None:
                    if buffer:
                        self.flush_bronze_batch(buffer)
                        buffer.clear()
                    self.route_to_dlq(
                        raw_bytes=raw_bytes,
                        reason=error_reason or "UNKNOWN_VALIDATION_ERROR",
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                    self._safe_commit(message=msg)
                    last_flush = time.monotonic()
                    continue

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
                    buffer.clear()
                    self._safe_commit()
                    last_flush = time.monotonic()

        finally:
            if buffer:
                self.flush_bronze_batch(buffer)
                buffer.clear()
                try:
                    self._safe_commit()
                except Exception:
                    pass
            self._dlq_producer.flush(timeout=2.0)
            self._consumer.close()
            logger.info(
                f"[AUDIT_CONSUMER] Stopped cleanly. processed={processed_count}, "
                f"quarantined={self.quarantined_count}"
            )

        return processed_count


if __name__ == "__main__":
    consumer = OrderAuditConsumer()
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    consumer.run()
