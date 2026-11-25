from datetime import datetime, timezone
import json
from typing import Any, Dict, Optional
from loguru import logger
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.core.telemetry import telemetry
from app.models.outbox import OutboxEvent
from app.services.kafka_producer import event_publisher


def deliver_order_event_and_update_outbox(
    event_payload: Dict[str, Any],
    outbox_id: Optional[int] = None,
) -> bool:
    """Publishes an order event to Kafka and updates its Transactional Outbox status."""
    published = event_publisher.publish_order_event(event_payload)
    if outbox_id is None:
        return published

    db: Session = SessionLocal()
    try:
        record = db.query(OutboxEvent).filter(OutboxEvent.id == outbox_id).first()
        if record is not None:
            record.attempts += 1
            if published:
                record.status = "PUBLISHED"
                record.published_at = datetime.now(timezone.utc).replace(tzinfo=None)
                record.last_error = None
            else:
                record.status = "FAILED"
                record.last_error = "Kafka broker delivery failed; queued for outbox replay."
            db.commit()
    except Exception as exc:
        db.rollback()
        logger.warning(f"[OUTBOX_UPDATE_ERROR] outbox_id={outbox_id} err={exc}")
    finally:
        db.close()

    return published


def replay_pending_outbox_events(limit: int = 100) -> Dict[str, int]:
    """Replays PENDING or FAILED Transactional Outbox events to Kafka."""
    db: Session = SessionLocal()
    checked = 0
    published = 0
    failed = 0
    try:
        rows = (
            db.query(OutboxEvent)
            .filter(OutboxEvent.status.in_(["PENDING", "FAILED"]))
            .order_by(OutboxEvent.id.asc())
            .limit(limit)
            .all()
        )
        for rec in rows:
            checked += 1
            try:
                payload = json.loads(rec.payload)
            except Exception as exc:
                rec.status = "FAILED"
                rec.attempts += 1
                rec.last_error = f"CORRUPT_OUTBOX_JSON: {exc}"
                failed += 1
                continue

            ok = event_publisher.publish_order_event(payload, topic=rec.topic)
            rec.attempts += 1
            if ok:
                rec.status = "PUBLISHED"
                rec.published_at = datetime.now(timezone.utc).replace(tzinfo=None)
                rec.last_error = None
                published += 1
                telemetry.inc("outbox_replayed_published_total")
            else:
                rec.status = "FAILED"
                rec.last_error = "Replay delivery to Kafka failed."
                failed += 1
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error(f"[OUTBOX_REPLAY_ERROR] {exc}")
        raise
    finally:
        db.close()

    return {"checked": checked, "published": published, "failed": failed}
