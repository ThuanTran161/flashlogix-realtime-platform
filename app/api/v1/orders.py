from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.order import Order
from app.schemas.common import StandardResponse
from app.schemas.order import OrderCreate, OrderResponse
from app.services.kafka_producer import event_publisher
from app.services.order_service import order_service

router = APIRouter(prefix="/orders", tags=["Orders"])


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
        "PostgreSQL session acquisition, row-level lock, and async Kafka emission."
    ),
)
async def create_order(
    payload: OrderCreate,
    background_tasks: BackgroundTasks,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    order_resp, event_payload = await order_service.process_order_async(
        payload=payload,
        idempotency_key=idempotency_key,
    )

    if event_payload is not None:
        background_tasks.add_task(event_publisher.publish_order_event, event_payload)

    return StandardResponse(
        success=True,
        message="Flash sale order placed successfully.",
        data=order_resp,
    )
