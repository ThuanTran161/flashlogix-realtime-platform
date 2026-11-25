from datetime import datetime, timezone
from sqlalchemy import Column, DateTime, Index, Integer, String, Text
from app.core.database import Base


class OutboxEvent(Base):
    """Transactional Outbox table guaranteeing at-least-once Kafka event delivery."""

    __tablename__ = "outbox_events"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    aggregate_type = Column(String(50), nullable=False, default="ORDER")
    aggregate_id = Column(String(64), nullable=False, index=True)
    topic = Column(String(128), nullable=False)
    payload = Column(Text, nullable=False)
    status = Column(String(20), nullable=False, default="PENDING", index=True)
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    created_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    published_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_outbox_status_created", "status", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<OutboxEvent(id={self.id}, aggregate_id='{self.aggregate_id}', "
            f"status='{self.status}', attempts={self.attempts})>"
        )
