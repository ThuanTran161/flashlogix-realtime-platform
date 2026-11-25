from contextlib import asynccontextmanager
import sys
from typing import AsyncGenerator
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy import text

from app.api.v1.orders import router as direct_orders_router
from app.api.v1.router import api_v1_router
from app.core.config import settings
from app.core.database import Base, engine
from app.core.storage import ensure_lakehouse_bucket
from app.services.inventory_service import inventory_service, redis_client
from app.services.kafka_producer import event_publisher


def _configure_logging() -> None:
    logger.remove()
    logger.add(sys.stderr, level=settings.LOG_LEVEL.upper())


_configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manages application startup and graceful shutdown lifecycle."""
    logger.info(
        f"Initializing FlashLogix Transaction Engine (mode={settings.CONCURRENCY_MODE}, "
        f"log_level={settings.LOG_LEVEL})..."
    )

    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database schemas verified and synchronized.")
    except Exception as exc:
        logger.warning(f"Database unavailable on startup: {exc}")

    try:
        redis_client.ping()
        logger.info("Redis cache connection verified.")
    except Exception as exc:
        logger.warning(f"Redis unavailable on startup: {exc}")

    try:
        ensure_lakehouse_bucket()
    except Exception as exc:
        logger.warning(f"MinIO Lakehouse unavailable on startup: {exc}")

    yield

    logger.info("Shutting down FlashLogix Transaction Engine...")
    event_publisher.flush(timeout=5.0)
    await inventory_service.close_async()
    redis_client.close()


app = FastAPI(
    title=settings.APP_NAME,
    description="High-Concurrency Flash Sale Transaction Engine & Real-time Event Analytics Platform",
    version="1.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    """Enforces standard HTTP error envelope: {"detail": "...", "error_code": "..."}."""
    if isinstance(exc.detail, dict) and "error_code" in exc.detail:
        payload = exc.detail
    else:
        payload = {
            "detail": str(exc.detail),
            "error_code": f"HTTP_{exc.status_code}_ERROR",
        }
    return JSONResponse(status_code=exc.status_code, content=payload)


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Catches unhandled server errors and prevents internal stack trace leakage."""
    logger.exception(f"Unhandled server error on path {request.url.path}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "An unexpected internal server error occurred.",
            "error_code": "INTERNAL_SERVER_ERROR",
        },
    )


def _check_dependencies_health() -> dict:
    redis_status = "DISCONNECTED"
    try:
        redis_status = "HEALTHY" if redis_client.ping() else "UNHEALTHY"
    except Exception:
        pass

    db_status = "DISCONNECTED"
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            db_status = "HEALTHY"
    except Exception:
        pass

    kafka_status = "HEALTHY" if event_publisher.is_connected(timeout=1.0) else "DISCONNECTED"
    minio_status = "HEALTHY" if ensure_lakehouse_bucket() else "DISCONNECTED"

    return {
        "redis": redis_status,
        "database": db_status,
        "kafka": kafka_status,
        "minio": minio_status,
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """System liveness and readiness probe for container orchestrators."""
    services = await run_in_threadpool(_check_dependencies_health)
    return {
        "status": "UP",
        "app": settings.APP_NAME,
        "environment": settings.APP_ENV,
        "concurrency_mode": settings.CONCURRENCY_MODE,
        "services": services,
    }


app.include_router(api_v1_router)
app.include_router(direct_orders_router)
