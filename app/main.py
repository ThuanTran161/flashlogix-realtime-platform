from contextlib import asynccontextmanager
import sys
import time
from typing import AsyncGenerator
import uuid
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from loguru import logger
from sqlalchemy import text

from app.api.v1.orders import router as direct_orders_router
from app.api.v1.router import api_v1_router
from app.core.config import settings
from app.core.database import engine, ensure_oltp_schema_and_constraints
from app.core.storage import ensure_lakehouse_bucket
from app.core.telemetry import telemetry
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
        ensure_oltp_schema_and_constraints()
        logger.info("Database schemas, Outbox, and CHECK constraints verified and synchronized.")
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
    telemetry.flush_to_redis()
    event_publisher.flush(timeout=5.0)
    await inventory_service.close_async()
    redis_client.close()


app = FastAPI(
    title=settings.APP_NAME,
    description="High-Concurrency Flash Sale Transaction Engine & Real-time Event Analytics Platform",
    version="1.3.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_tracing_and_telemetry_middleware(request: Request, call_next):
    """Attaches X-Request-ID and X-Process-Time-Ms headers and records request telemetry."""
    request_id = request.headers.get("X-Request-ID") or f"req_{uuid.uuid4().hex[:12]}"
    start_ts = time.perf_counter()
    telemetry.inc("http_requests_total")

    response = await call_next(request)

    elapsed_ms = round((time.perf_counter() - start_ts) * 1000.0, 2)
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time-Ms"] = str(elapsed_ms)
    telemetry.inc(f"http_status_{response.status_code}_total")
    return response


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


def _collect_system_metrics() -> dict:
    snap = telemetry.snapshot()
    pool = engine.pool
    db_pool = {
        "pool_size": getattr(pool, "size", lambda: settings.DB_POOL_SIZE)(),
        "checked_in": getattr(pool, "checkedin", lambda: 0)(),
        "checked_out": getattr(pool, "checkedout", lambda: 0)(),
        "overflow": getattr(pool, "overflow", lambda: 0)(),
    }
    oltp_orders = 0
    outbox_pending_or_failed = 0
    try:
        with engine.connect() as conn:
            oltp_orders = int(conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() or 0)
            outbox_pending_or_failed = int(
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM outbox_events WHERE status IN ('PENDING', 'FAILED')"
                    )
                ).scalar()
                or 0
            )
    except Exception:
        pass

    redis_stock_p1 = None
    try:
        redis_stock_p1 = inventory_service.get_stock(1)
    except Exception:
        pass

    return {
        "app": settings.APP_NAME,
        "concurrency_mode": settings.CONCURRENCY_MODE,
        "uptime_seconds": snap["uptime_seconds"],
        "counters": snap["counters"],
        "db_pool": db_pool,
        "oltp_orders_total": oltp_orders,
        "outbox_pending_or_failed": outbox_pending_or_failed,
        "product_1_redis_stock": redis_stock_p1,
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


@app.get("/metrics", tags=["Observability"])
async def metrics_endpoint(format: str = "json"):
    """Exposes runtime telemetry in JSON or Prometheus text exposition format (`?format=prometheus`)."""
    data = await run_in_threadpool(_collect_system_metrics)
    if format.lower() == "prometheus":
        lines = [
            f"flashlogix_uptime_seconds {data['uptime_seconds']}",
            f"flashlogix_oltp_orders_total {data['oltp_orders_total']}",
            f"flashlogix_outbox_pending_or_failed {data['outbox_pending_or_failed']}",
            f"flashlogix_db_pool_checked_out {data['db_pool']['checked_out']}",
            f"flashlogix_db_pool_checked_in {data['db_pool']['checked_in']}",
        ]
        if data["product_1_redis_stock"] is not None:
            lines.append(f"flashlogix_product_1_redis_stock {data['product_1_redis_stock']}")
        for k, v in sorted(data["counters"].items()):
            lines.append(f"flashlogix_{k} {v}")
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
    return data


app.include_router(api_v1_router)
app.include_router(direct_orders_router)
