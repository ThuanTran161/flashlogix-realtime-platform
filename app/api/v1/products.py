from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models.product import Product
from app.schemas.common import StandardResponse
from app.schemas.product import ProductCreate, ProductResponse, ProductStockInit
from app.services.inventory_service import inventory_service

router = APIRouter(prefix="/products", tags=["Products"])


def _create_product_sync(payload: ProductCreate, db: Session) -> Product:
    product = Product(
        name=payload.name,
        price=payload.price,
        stock=payload.stock,
    )
    db.add(product)
    db.commit()
    db.refresh(product)
    inventory_service.initialize_stock(product.id, payload.stock)
    return product


def _get_product_sync(product_id: int, db: Session) -> Product:
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "detail": f"Product {product_id} not found.",
                "error_code": "PRODUCT_NOT_FOUND",
            },
        )
    cached_stock = inventory_service.get_stock(product.id)
    if cached_stock is not None:
        product.stock = cached_stock
    return product


def _init_stock_sync(product_id: int, initial_stock: int, db: Session) -> dict:
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "detail": f"Product {product_id} not found in DB.",
                "error_code": "PRODUCT_NOT_FOUND",
            },
        )
    product.stock = initial_stock
    db.commit()
    inventory_service.initialize_stock(product.id, initial_stock)
    return {"product_id": product_id, "stock": initial_stock}


@router.post(
    "",
    response_model=StandardResponse[ProductResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Create Product",
)
async def create_product(payload: ProductCreate, db: Session = Depends(get_db)):
    product = await run_in_threadpool(_create_product_sync, payload, db)
    return StandardResponse(
        success=True,
        message=f"Product '{product.name}' created and stock synchronized to Redis.",
        data=ProductResponse.model_validate(product),
    )


@router.get(
    "/{product_id}",
    response_model=StandardResponse[ProductResponse],
    summary="Get Product with Cache Stock",
)
async def get_product(product_id: int, db: Session = Depends(get_db)):
    product = await run_in_threadpool(_get_product_sync, product_id, db)
    return StandardResponse(
        success=True,
        message="Product retrieved successfully.",
        data=ProductResponse.model_validate(product),
    )


@router.post(
    "/{product_id}/init-stock",
    response_model=StandardResponse[dict],
    summary="Initialize or Reset Flash Sale Stock in Redis",
)
async def init_flash_sale_stock(
    product_id: int,
    payload: ProductStockInit,
    db: Session = Depends(get_db),
):
    data = await run_in_threadpool(
        _init_stock_sync, product_id, payload.initial_stock, db
    )
    return StandardResponse(
        success=True,
        message=(
            f"Stock for product {product_id} initialized to "
            f"{payload.initial_stock} in both DB and Redis."
        ),
        data=data,
    )
