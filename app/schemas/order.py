from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, field_serializer


class OrderCreate(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=100, description="Identifier of ordering customer")
    product_id: int = Field(..., gt=0, description="Target product ID")
    quantity: int = Field(default=1, ge=1, le=10, description="Purchase quantity per transaction")


class OrderResponse(BaseModel):
    id: int
    user_id: str
    product_id: int
    quantity: int
    total_amount: float = 0.0
    latency_ms: Optional[int] = None
    status: str
    created_at: datetime

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()

    model_config = ConfigDict(from_attributes=True)
