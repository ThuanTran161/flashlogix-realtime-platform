from datetime import datetime, timezone
from sqlalchemy import CheckConstraint, Column, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship
from app.core.database import Base


class Order(Base):
    """Order transaction record in OLTP database."""

    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(String(100), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False, index=True)
    quantity = Column(Integer, nullable=False, default=1)
    total_amount = Column(Float, nullable=False, default=0.0)
    latency_ms = Column(Integer, nullable=True)
    status = Column(String(50), nullable=False, default="CONFIRMED")
    idempotency_key = Column(String(128), unique=True, nullable=True, index=True)
    created_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    # Use lazy="select" to avoid unnecessary LEFT OUTER JOIN on hot order queries
    product = relationship("Product", backref="orders", lazy="select")

    __table_args__ = (
        Index("idx_orders_user_created", "user_id", "created_at"),
        CheckConstraint("quantity > 0", name="ck_orders_quantity_positive"),
        CheckConstraint("total_amount >= 0", name="ck_orders_total_non_negative"),
    )

    def __repr__(self) -> str:
        return (
            f"<Order(id={self.id}, user_id='{self.user_id}', "
            f"product_id={self.product_id}, total_amount={self.total_amount}, status='{self.status}')>"
        )
