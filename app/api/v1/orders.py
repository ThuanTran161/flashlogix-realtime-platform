from typing import List, Optional
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.order import Order
from app.schemas.common import StandardResponse
from app.schemas.order import OrderCreate, OrderResponse
from app.services.order_service import order_service
from app.services.outbox_service import (
    deliver_order_event_and_update_outbox,
    replay_pending_outbox_events,
)

router = APIRouter(prefix="/orders", tags=["Orders"])


def _list_orders_sync(
    db: Session,
    user_id: Optional[str],
    product_id: Optional[int],
    limit: int,
    offset: int,
) -> List[OrderResponse]:
    query = db.query(Order)
    if user_id is not None:
        query = query.filter(Order.user_id == user_id)
    if product_id is not None:
        query = query.filter(Order.product_id == product_id)
    rows = query.order_by(Order.id.desc()).offset(offset).limit(limit).all()
    return [OrderResponse.model_validate(r) for r in rows]


def _get_order_sync(order_id: int, db: Session) -> OrderResponse:
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "detail": f"Order {order_id} not found.",
                "error_code": "ORDER_NOT_FOUND",
            },
        )
    return OrderResponse.model_validate(order)


@router.get(
    "",
    response_model=StandardResponse[List[OrderResponse]],
    summary="List Orders with Optional Filters & Pagination",
)
async def list_orders(
    user_id: Optional[str] = Query(None, min_length=1, max_length=100),
    product_id: Optional[int] = Query(None, gt=0),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    items = await run_in_threadpool(_list_orders_sync, db, user_id, product_id, limit, offset)
    return StandardResponse(
        success=True,
        message=f"Retrieved {len(items)} orders.",
        data=items,
    )


@router.post(
    "/outbox/replay",
    response_model=StandardResponse[dict],
    summary="Replay Pending or Failed Transactional Outbox Events to Kafka",
)
async def replay_outbox(limit: int = Query(100, ge=1, le=1000)):
    result = await run_in_threadpool(replay_pending_outbox_events, limit)
    return StandardResponse(
        success=True,
        message="Outbox replay cycle completed.",
        data=result,
    )


@router.get(
    "/{order_id}",
    response_model=StandardResponse[OrderResponse],
    summary="Get Order by ID",
)
async def get_order(order_id: int, db: Session = Depends(get_db)):
    order_resp = await run_in_threadpool(_get_order_sync, order_id, db)
    return StandardResponse(
        success=True,
        message="Order retrieved successfully.",
        data=order_resp,
    )


@router.post(
    "",
    response_model=StandardResponse[OrderResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Create Flash Sale Order",
    description=(
        "Non-blocking Redis Idempotency & Lua atomic stock gate prior to lazy "
        "PostgreSQL session acquisition, row-level lock, atomic Transactional Outbox write, "
        "and async Kafka emission."
    ),
)
async def create_order(
    payload: OrderCreate,
    background_tasks: BackgroundTasks,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    order_resp, event_payload, outbox_id = await order_service.process_order_with_outbox_async(
        payload=payload,
        idempotency_key=idempotency_key,
    )

    if event_payload is not None:
        background_tasks.add_task(
            deliver_order_event_and_update_outbox,
            event_payload,
            outbox_id,
        )

    return StandardResponse(
        success=True,
        message="Flash sale order placed successfully.",
        data=order_resp,
    )
