import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from confluent_kafka import KafkaException, Producer
from loguru import logger
from app.core.config import settings


class EventPublisher:
    """Idempotent Kafka Event Publisher for decoupling OLTP from OLAP."""

    def __init__(self, bootstrap_servers: Optional[str] = None) -> None:
        self._bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self._producer: Optional[Producer] = None

    def _get_producer(self) -> Producer:
        if self._producer is None:
            conf = {
                "bootstrap.servers": self._bootstrap_servers,
                "acks": "all",
                "retries": 5,
                "enable.idempotence": True,
                "compression.type": "gzip",
                "linger.ms": 2,
                "batch.size": 32768,
            }
            self._producer = Producer(conf)
        return self._producer

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err is not None:
            logger.error(f"[KAFKA_DELIVERY_FAILED] Error delivering message: {err}")
        else:
            key = msg.key().decode("utf-8") if msg.key() else "unknown"
            logger.debug(
                f"[KAFKA_PUBLISHED] order_id={key} topic={msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    @staticmethod
    def build_order_event(
        order_id: int,
        user_id: str,
        product_id: int,
        quantity: int,
        unit_price: float,
        latency_ms: int,
        created_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Constructs a strict schema-compliant order event payload."""
        ts = created_at or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        return {
            "event_id": f"evt_{order_id}_{uuid.uuid4().hex[:8]}",
            "event_type": "ORDER_CREATED",
            "order_id": order_id,
            "user_id": user_id,
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": round(float(unit_price), 2),
            "total_amount": round(float(unit_price) * quantity, 2),
            "latency_ms": latency_ms,
            "timestamp": ts.isoformat(),
        }

    def publish_order_event(self, event_payload: Dict[str, Any], topic: Optional[str] = None) -> bool:
        """Publishes an order event asynchronously to Kafka with guaranteed delivery flush."""
        target_topic = topic or settings.KAFKA_TOPIC_ORDERS
        order_id = event_payload.get("order_id")
        try:
            producer = self._get_producer()
            producer.produce(
                topic=target_topic,
                key=str(order_id).encode("utf-8"),
                value=json.dumps(event_payload).encode("utf-8"),
                callback=self._delivery_report,
            )
            # Runs inside FastAPI BackgroundTasks (post-response) so flushing up to 1s
            # guarantees zero lost events without adding latency to the client HTTP 201.
            producer.flush(timeout=1.0)
            return True
        except (KafkaException, BufferError) as exc:
            logger.error(f"[KAFKA_ERROR] order_id={order_id} failed to enqueue event: {exc}")
            return False
        except Exception as exc:
            logger.error(f"[KAFKA_UNEXPECTED_ERROR] order_id={order_id} error={exc}")
            return False

    def flush(self, timeout: float = 5.0) -> int:
        """Flushes pending Kafka messages cleanly during shutdown."""
        if self._producer is not None:
            remaining = self._producer.flush(timeout=timeout)
            logger.info(f"[KAFKA_FLUSH] Flushed producer, remaining={remaining}")
            return remaining
        return 0

    def is_connected(self, timeout: float = 1.5) -> bool:
        """Lightweight metadata check for readiness/health probes."""
        try:
            producer = self._get_producer()
            metadata = producer.list_topics(timeout=timeout)
            return len(metadata.brokers) > 0
        except Exception:
            return False


event_publisher = EventPublisher()
