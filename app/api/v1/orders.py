from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Depends, Header, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from app.core.database import get_db
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
        "Atomically deducts stock from Redis via Lua script, persists order to "
        "PostgreSQL with row-level lock, and asynchronously emits Kafka event."
    ),
)
async def create_order(
    payload: OrderCreate,
    background_tasks: BackgroundTasks,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    db: Session = Depends(get_db),
):
    order, event_payload = await run_in_threadpool(
        order_service.process_order,
        payload=payload,
        db=db,
        idempotency_key=idempotency_key,
    )

    if event_payload is not None:
        background_tasks.add_task(event_publisher.publish_order_event, event_payload)

    return StandardResponse(
        success=True,
        message="Flash sale order placed successfully.",
        data=OrderResponse.model_validate(order),
    )
