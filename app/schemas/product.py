from pydantic import BaseModel, ConfigDict, Field


class ProductBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="Product title")
    price: float = Field(..., gt=0.0, description="Unit price in USD or VND")
    stock: int = Field(..., ge=0, description="Available inventory count")


class ProductCreate(ProductBase):
    pass


class ProductResponse(ProductBase):
    id: int
    version: int

    model_config = ConfigDict(from_attributes=True)


class ProductStockInit(BaseModel):
    initial_stock: int = Field(..., ge=0, description="Stock to initialize in Redis and DB")
