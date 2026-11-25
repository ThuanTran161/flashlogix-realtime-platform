import asyncio
from datetime import datetime, timezone
import json
import time
from typing import Any, Dict, Optional, Tuple
from fastapi import HTTPException, status
from fastapi.concurrency import run_in_threadpool
from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.database import SessionLocal
from app.core.telemetry import telemetry
from app.models.order import Order
from app.models.outbox import OutboxEvent
from app.models.product import Product
from app.schemas.order import OrderCreate, OrderResponse
from app.services.inventory_service import inventory_service
from app.services.kafka_producer import EventPublisher


class OrderService:
    """Orchestrates order transactions, async Redis concurrency gates, outbox, and rollbacks."""

    @staticmethod
    def _persist_order_sync(
        payload: OrderCreate,
        idempotency_key: Optional[str],
        remaining_stock: int,
        start_ts: float,
    ) -> Tuple[OrderResponse, Optional[Dict[str, Any]], Optional[int]]:
        """Lazily acquires a PostgreSQL session ONLY after Redis Lua approves the order."""
        db: Session = SessionLocal()
        product_id = payload.product_id
        quantity = payload.quantity
        try:
            if idempotency_key:
                existing_order = (
                    db.query(Order)
                    .filter(Order.idempotency_key == idempotency_key)
                    .first()
                )
                if existing_order:
                    # Restore the stock unit deducted before discovering DB idempotency hit
                    inventory_service.compensatory_increment(product_id, quantity)
                    telemetry.inc("idempotency_db_hits_total")
                    logger.info(
                        f"[IDEMPOTENT_RETRY_DB] order_id={existing_order.id} "
                        f"idempotency_key='{idempotency_key}'"
                    )
                    return OrderResponse.model_validate(existing_order), None, None

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
            created_now = datetime.now(timezone.utc).replace(tzinfo=None)

            new_order = Order(
                user_id=payload.user_id,
                product_id=product_id,
                quantity=quantity,
                total_amount=total_amount,
                latency_ms=elapsed_ms,
                status="CONFIRMED",
                idempotency_key=idempotency_key,
                created_at=created_now,
            )
            db.add(new_order)
            db.flush()

            event_payload = EventPublisher.build_order_event(
                order_id=new_order.id,
                user_id=new_order.user_id,
                product_id=product_id,
                quantity=quantity,
                unit_price=unit_price,
                latency_ms=elapsed_ms,
                created_at=new_order.created_at,
            )
            outbox_record = OutboxEvent(
                aggregate_type="ORDER",
                aggregate_id=str(new_order.id),
                topic=settings.KAFKA_TOPIC_ORDERS,
                payload=json.dumps(event_payload),
                status="PENDING",
                attempts=0,
                created_at=created_now,
            )
            db.add(outbox_record)
            db.commit()
            db.refresh(new_order)

            telemetry.inc("orders_committed_total")
            logger.info(
                f"[ORDER_COMMITTED] order_id={new_order.id} user_id={new_order.user_id} "
                f"product_id={product_id} qty={quantity} total={total_amount} "
                f"latency_ms={elapsed_ms} remaining_ram={remaining_stock}"
            )
            return OrderResponse.model_validate(new_order), event_payload, outbox_record.id

        except IntegrityError as ie:
            db.rollback()
            inventory_service.compensatory_increment(product_id, quantity)
            telemetry.inc("compensatory_rollbacks_total")
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
            telemetry.inc("compensatory_rollbacks_total")
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
        finally:
            db.close()

    @staticmethod
    def _process_order_db_only_sync(
        payload: OrderCreate,
        idempotency_key: Optional[str],
        start_ts: float,
    ) -> Tuple[OrderResponse, Optional[Dict[str, Any]], Optional[int]]:
        """Pure PostgreSQL row-lock baseline (used only when CONCURRENCY_MODE='db_only')."""
        db: Session = SessionLocal()
        product_id = payload.product_id
        quantity = payload.quantity
        try:
            if idempotency_key:
                existing_order = (
                    db.query(Order)
                    .filter(Order.idempotency_key == idempotency_key)
                    .first()
                )
                if existing_order:
                    return OrderResponse.model_validate(existing_order), None, None

            db_product = (
                db.query(Product)
                .filter(Product.id == product_id)
                .with_for_update()
                .first()
            )
            if not db_product:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "detail": f"Product {product_id} not found.",
                        "error_code": "PRODUCT_NOT_FOUND",
                    },
                )
            if db_product.stock < quantity:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "detail": "Flash sale item is sold out or has insufficient quantity.",
                        "error_code": "INSUFFICIENT_STOCK",
                    },
                )

            unit_price = float(db_product.price)
            total_amount = round(unit_price * quantity, 2)
            db_product.stock -= quantity
            db_product.version += 1
            elapsed_ms = max(1, int((time.perf_counter() - start_ts) * 1000))
            created_now = datetime.now(timezone.utc).replace(tzinfo=None)

            new_order = Order(
                user_id=payload.user_id,
                product_id=product_id,
                quantity=quantity,
                total_amount=total_amount,
                latency_ms=elapsed_ms,
                status="CONFIRMED",
                idempotency_key=idempotency_key,
                created_at=created_now,
            )
            db.add(new_order)
            db.flush()

            event_payload = EventPublisher.build_order_event(
                order_id=new_order.id,
                user_id=new_order.user_id,
                product_id=product_id,
                quantity=quantity,
                unit_price=unit_price,
                latency_ms=elapsed_ms,
                created_at=new_order.created_at,
            )
            outbox_record = OutboxEvent(
                aggregate_type="ORDER",
                aggregate_id=str(new_order.id),
                topic=settings.KAFKA_TOPIC_ORDERS,
                payload=json.dumps(event_payload),
                status="PENDING",
                attempts=0,
                created_at=created_now,
            )
            db.add(outbox_record)
            db.commit()
            db.refresh(new_order)

            telemetry.inc("orders_committed_total")
            return OrderResponse.model_validate(new_order), event_payload, outbox_record.id
        except HTTPException:
            db.rollback()
            raise
        except IntegrityError:
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "detail": "Duplicate transaction or constraint violation occurred.",
                    "error_code": "DUPLICATE_ORDER",
                },
            )
        except Exception as exc:
            db.rollback()
            logger.error(f"[DB_ONLY_ERROR] {exc}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "detail": "Database transaction failed.",
                    "error_code": "DATABASE_ERROR",
                },
            )
        finally:
            db.close()

    @staticmethod
    async def process_order_with_outbox_async(
        payload: OrderCreate,
        idempotency_key: Optional[str] = None,
    ) -> Tuple[OrderResponse, Optional[Dict[str, Any]], Optional[int]]:
        """Non-blocking flash sale order pipeline returning order, event, and outbox ID."""
        start_ts = time.perf_counter()

        if settings.CONCURRENCY_MODE == "db_only":
            return await run_in_threadpool(
                OrderService._process_order_db_only_sync,
                payload,
                idempotency_key,
                start_ts,
            )

        lock_acquired = False
        try:
            # Step 1: Non-blocking Redis Idempotency Check (zero DB connections)
            if idempotency_key:
                cached_order = await inventory_service.get_idempotency_record_async(
                    idempotency_key
                )
                if cached_order is not None:
                    telemetry.inc("idempotency_redis_hits_total")
                    logger.debug(
                        f"[IDEMPOTENT_RETRY_REDIS] order_id={cached_order.get('id')} "
                        f"idempotency_key='{idempotency_key}'"
                    )
                    return OrderResponse(**cached_order), None, None

                lock_acquired = await inventory_service.acquire_idempotency_lock_async(
                    idempotency_key
                )
                if not lock_acquired:
                    for _ in range(8):
                        await asyncio.sleep(0.025)
                        cached_order = await inventory_service.get_idempotency_record_async(
                            idempotency_key
                        )
                        if cached_order is not None:
                            telemetry.inc("idempotency_redis_hits_total")
                            return OrderResponse(**cached_order), None, None

            # Step 2: Non-blocking Redis Lua Atomic Stock Gate (zero DB connections)
            is_deducted, remaining_stock = await inventory_service.deduct_stock_atomic_async(
                payload.product_id, payload.quantity
            )
            if not is_deducted:
                if remaining_stock == -1:
                    telemetry.inc("redis_gate_rejected_out_of_stock_total")
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "detail": "Flash sale item is sold out or has insufficient quantity.",
                            "error_code": "INSUFFICIENT_STOCK",
                        },
                    )
                telemetry.inc("redis_gate_cache_miss_total")
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "detail": (
                            f"Product {payload.product_id} is not initialized for flash sale in cache."
                        ),
                        "error_code": "PRODUCT_NOT_INITIALIZED",
                    },
                )

            # Step 3: Lazy PostgreSQL Session Acquisition (only winners touch the DB pool)
            order_resp, event_payload, outbox_id = await run_in_threadpool(
                OrderService._persist_order_sync,
                payload,
                idempotency_key,
                remaining_stock,
                start_ts,
            )

            # Step 4: Cache idempotent result in Redis for instant subsequent retries
            if idempotency_key:
                await inventory_service.save_idempotency_record_async(
                    idempotency_key, order_resp.model_dump(mode="json")
                )

            return order_resp, event_payload, outbox_id
        finally:
            if idempotency_key and lock_acquired:
                try:
                    await inventory_service.release_idempotency_lock_async(idempotency_key)
                except Exception:
                    pass

    @staticmethod
    async def process_order_async(
        payload: OrderCreate,
        idempotency_key: Optional[str] = None,
    ) -> Tuple[OrderResponse, Optional[Dict[str, Any]]]:
        """Backward-compatible wrapper returning (OrderResponse, event_payload)."""
        order_resp, event_payload, _ = await OrderService.process_order_with_outbox_async(
            payload=payload,
            idempotency_key=idempotency_key,
        )
        return order_resp, event_payload

    @staticmethod
    def process_order(
        payload: OrderCreate,
        db: Session,
        idempotency_key: Optional[str] = None,
    ) -> Tuple[Order, Optional[Dict[str, Any]]]:
        """Synchronous helper preserved for direct unit testing or sync callers."""
        start_ts = time.perf_counter()

        if idempotency_key:
            existing_order = (
                db.query(Order)
                .filter(Order.idempotency_key == idempotency_key)
                .first()
            )
            if existing_order:
                return existing_order, None

        is_deducted, remaining_stock = inventory_service.deduct_stock_atomic(
            payload.product_id, payload.quantity
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
                    "detail": (
                        f"Product {payload.product_id} is not initialized for flash sale in cache."
                    ),
                    "error_code": "PRODUCT_NOT_INITIALIZED",
                },
            )

        try:
            db_product = (
                db.query(Product)
                .filter(Product.id == payload.product_id)
                .with_for_update()
                .first()
            )
            if not db_product or db_product.stock < payload.quantity:
                raise ValueError("Invalid product or insufficient DB stock.")

            unit_price = float(db_product.price)
            total_amount = round(unit_price * payload.quantity, 2)
            db_product.stock -= payload.quantity
            db_product.version += 1
            elapsed_ms = max(1, int((time.perf_counter() - start_ts) * 1000))

            new_order = Order(
                user_id=payload.user_id,
                product_id=payload.product_id,
                quantity=payload.quantity,
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
                product_id=payload.product_id,
                quantity=payload.quantity,
                unit_price=unit_price,
                latency_ms=elapsed_ms,
                created_at=new_order.created_at,
            )
            return new_order, event_payload
        except Exception as exc:
            db.rollback()
            inventory_service.compensatory_increment(payload.product_id, payload.quantity)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "detail": f"Database transaction failed: {exc}",
                    "error_code": "DATABASE_ERROR",
                },
            )


order_service = OrderService()
