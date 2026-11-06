from typing import Generic, Optional, TypeVar
from pydantic import BaseModel, Field

T = TypeVar("T")


class StandardResponse(BaseModel, Generic[T]):
    """Standardized API response wrapper."""
    success: bool = True
    message: str = "Operation completed successfully"
    data: Optional[T] = None


class ErrorEnvelope(BaseModel):
    """Standardized API error envelope conforming to Agent Rules."""
    detail: str = Field(..., description="Human-readable error description")
    error_code: str = Field(..., description="Machine-readable unique error code")
