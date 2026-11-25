from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.database import SessionLocal, get_db
from app.models.product import Product
from app.schemas.common import StandardResponse
from app.schemas.product import ProductCreate, ProductResponse, ProductStockInit
from app.services.inventory_service import inventory_service

router = APIRouter(prefix="/products", tags=["Products"])


def _create_product_sync(payload: ProductCreate, db: Session) -> ProductResponse:
    product = Product(
        name=payload.name,
        price=payload.price,
        stock=payload.stock,
    )
    db.add(product)
    db.commit()
    db.refresh(product)
    resp = ProductResponse.model_validate(product)
    inventory_service.initialize_stock(product.id, payload.stock)
    inventory_service.set_product_meta(product.id, resp.model_dump(mode="json"))
    return resp


def _get_product_sync(product_id: int) -> ProductResponse:
    db: Session = SessionLocal()
    try:
        product = db.query(Product).filter(Product.id == product_id).first()
        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "detail": f"Product {product_id} not found.",
                    "error_code": "PRODUCT_NOT_FOUND",
                },
            )
        if settings.CONCURRENCY_MODE != "db_only":
            cached_stock = inventory_service.get_stock(product.id)
            if cached_stock is not None:
                product.stock = cached_stock
        resp = ProductResponse.model_validate(product)
        if settings.CONCURRENCY_MODE != "db_only":
            inventory_service.set_product_meta(product.id, resp.model_dump(mode="json"))
        return resp
    finally:
        db.close()


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
    db.refresh(product)
    resp = ProductResponse.model_validate(product)
    inventory_service.initialize_stock(product.id, initial_stock)
    inventory_service.set_product_meta(product.id, resp.model_dump(mode="json"))
    return {"product_id": product_id, "stock": initial_stock}


@router.post(
    "",
    response_model=StandardResponse[ProductResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Create Product",
)
async def create_product(payload: ProductCreate, db: Session = Depends(get_db)):
    product_resp = await run_in_threadpool(_create_product_sync, payload, db)
    return StandardResponse(
        success=True,
        message=f"Product '{product_resp.name}' created and stock synchronized to Redis.",
        data=product_resp,
    )


@router.get(
    "/{product_id}",
    response_model=StandardResponse[ProductResponse],
    summary="Get Product with Redis Read-Through Cache",
)
async def get_product(product_id: int):
    if settings.CONCURRENCY_MODE != "db_only":
        meta, cached_stock = await inventory_service.get_product_meta_and_stock_async(
            product_id
        )
        if meta is not None:
            if cached_stock is not None:
                meta["stock"] = cached_stock
            return StandardResponse(
                success=True,
                message="Product retrieved from Redis cache.",
                data=ProductResponse(**meta),
            )

    product_resp = await run_in_threadpool(_get_product_sync, product_id)
    return StandardResponse(
        success=True,
        message="Product retrieved successfully.",
        data=product_resp,
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
