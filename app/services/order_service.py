import time
from typing import Any, Dict, Optional, Tuple
from fastapi import HTTPException, status
from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from app.models.order import Order
from app.models.product import Product
from app.schemas.order import OrderCreate
from app.services.inventory_service import inventory_service
from app.services.kafka_producer import EventPublisher


class OrderService:
    """Orchestrates order transactions, concurrency safety, and compensatory rollbacks."""

    @staticmethod
    def process_order(
        payload: OrderCreate,
        db: Session,
        idempotency_key: Optional[str] = None,
    ) -> Tuple[Order, Optional[Dict[str, Any]]]:
        """Processes an order with zero-overselling guarantee and idempotency enforcement.

        Returns:
            (Order, Optional[event_payload]): event_payload is None on idempotent replay.
        """
        start_ts = time.perf_counter()

        # Step 1: Idempotency check
        if idempotency_key:
            existing_order = (
                db.query(Order)
                .filter(Order.idempotency_key == idempotency_key)
                .first()
            )
            if existing_order:
                logger.info(
                    f"[IDEMPOTENT_RETRY] order_id={existing_order.id} "
                    f"idempotency_key='{idempotency_key}'"
                )
                return existing_order, None

        product_id = payload.product_id
        quantity = payload.quantity

        # Step 2: Atomic decrement on Redis RAM (< 2ms)
        is_deducted, remaining_stock = inventory_service.deduct_stock_atomic(
            product_id, quantity
        )
        if not is_deducted:
            if remaining_stock == -1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "detail": "Flash sale item is sold out or has insufficient quantity.",
                        "error_code": "INSUFFICIENT_STOCK",
                    },
                )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "detail": f"Product {product_id} is not initialized for flash sale in cache.",
                    "error_code": "PRODUCT_NOT_INITIALIZED",
                },
            )

        # Step 3: Persist in PostgreSQL OLTP within a guarded transaction
        try:
            db_product = (
                db.query(Product)
                .filter(Product.id == product_id)
                .with_for_update()
                .first()
            )
            if not db_product:
                raise ValueError(f"Product {product_id} does not exist in relational database.")
            if db_product.stock < quantity:
                raise ValueError(
                    f"Database stock mismatch for product {product_id}: "
                    f"db_stock={db_product.stock}, requested={quantity}"
                )

            unit_price = float(db_product.price)
            total_amount = round(unit_price * quantity, 2)
            db_product.stock -= quantity
            db_product.version += 1

            elapsed_ms = max(1, int((time.perf_counter() - start_ts) * 1000))

            new_order = Order(
                user_id=payload.user_id,
                product_id=product_id,
                quantity=quantity,
                total_amount=total_amount,
                latency_ms=elapsed_ms,
                status="CONFIRMED",
                idempotency_key=idempotency_key,
            )
            db.add(new_order)
            db.commit()
            db.refresh(new_order)

            event_payload = EventPublisher.build_order_event(
                order_id=new_order.id,
                user_id=new_order.user_id,
                product_id=product_id,
                quantity=quantity,
                unit_price=unit_price,
                latency_ms=elapsed_ms,
                created_at=new_order.created_at,
            )

            logger.info(
                f"[ORDER_COMMITTED] order_id={new_order.id} user_id={new_order.user_id} "
                f"product_id={product_id} qty={quantity} total={total_amount} "
                f"latency_ms={elapsed_ms} remaining_ram={remaining_stock}"
            )
            return new_order, event_payload

        except IntegrityError as ie:
            db.rollback()
            inventory_service.compensatory_increment(product_id, quantity)
            logger.error(f"[INTEGRITY_VIOLATION] Rolled back DB & restored Redis stock: {ie}")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "detail": "Duplicate transaction or constraint violation occurred.",
                    "error_code": "DUPLICATE_ORDER",
                },
            )
        except Exception as exc:
            db.rollback()
            restored = inventory_service.compensatory_increment(product_id, quantity)
            logger.critical(
                f"[COMPENSATORY_ROLLBACK] DB transaction failed: {exc}. "
                f"Restored {quantity} items to Redis (current RAM stock: {restored})."
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "detail": "Database transaction failed. Inventory has been safely restored.",
                    "error_code": "DATABASE_ERROR",
                },
            )


order_service = OrderService()
