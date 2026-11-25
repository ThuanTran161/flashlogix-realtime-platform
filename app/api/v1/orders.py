from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Header, status
from app.schemas.common import StandardResponse
from app.schemas.order import OrderCreate, OrderResponse
from app.services.kafka_producer import event_publisher
from app.services.order_service import order_service

router = APIRouter(prefix="/orders", tags=["Orders"])


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
