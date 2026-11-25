"""Operational & Lakehouse Control Plane endpoints for FlashLogix UI."""
from datetime import datetime, timezone
import json
import os
import re
import subprocess
import sys
import threading
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.core.config import settings
from app.core.database import engine
from app.core.storage import get_s3_client
from app.schemas.common import StandardResponse
from app.services.inventory_service import inventory_service, redis_client
from lakehouse.batch_gold_aggregation import run_gold_etl

router = APIRouter(prefix="/ops", tags=["Operations"])

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SILVER_JOB_REDIS_KEY = "flashlogix:ops:silver_job"
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class GoldEtlRequest(BaseModel):
    target_date: Optional[str] = Field(default=None, description="Optional UTC partition date YYYY-MM-DD")


def _get_overview_sync() -> Dict[str, Any]:
    with engine.connect() as conn:
        agg_row = conn.execute(
            text("SELECT COUNT(*), COALESCE(SUM(total_amount), 0), COALESCE(AVG(latency_ms), 0) FROM orders")
        ).fetchone()

        prod_rows = conn.execute(
            text("SELECT id, name, price, stock, version FROM products ORDER BY id")
        ).fetchall()

        outbox_rows = conn.execute(
            text("SELECT status, COUNT(*) FROM outbox_events GROUP BY status")
        ).fetchall()

        recent_outbox = conn.execute(
            text(
                "SELECT id, aggregate_id, status, attempts, last_error, created_at, published_at "
                "FROM outbox_events ORDER BY id DESC LIMIT 50"
            )
        ).fetchall()

        gold_summary: Optional[Dict[str, Any]] = None
        try:
            fact_row = conn.execute(
                text(
                    "SELECT COUNT(*), COALESCE(SUM(total_amount), 0), "
                    "COALESCE(SUM(CASE WHEN is_fraudulent THEN 1 ELSE 0 END), 0) "
                    "FROM Fact_Flash_Sale_Orders"
                )
            ).fetchone()
            tier_rows = conn.execute(
                text(
                    "SELECT membership_tier, COUNT(*) AS user_count "
                    "FROM Dim_Users WHERE is_current = TRUE GROUP BY membership_tier"
                )
            ).fetchall()
            gold_summary = {
                "fact_orders": int(fact_row[0]),
                "total_revenue": round(float(fact_row[1]), 2),
                "fraud_flagged": int(fact_row[2]),
                "tiers": [
                    {"membership_tier": str(r[0]), "user_count": int(r[1])} for r in tier_rows
                ],
            }
        except Exception:
            gold_summary = None

    inventory_items = []
    for r in prod_rows:
        pid = int(r[0])
        db_stock = int(r[3])
        ram_stock = inventory_service.get_stock(pid)
        inventory_items.append(
            {
                "id": pid,
                "name": str(r[1]),
                "price": float(r[2]),
                "db_stock": db_stock,
                "redis_stock": ram_stock,
                "in_sync": ram_stock == db_stock,
                "version": int(r[4]),
            }
        )

    outbox_counts = {"PUBLISHED": 0, "PENDING": 0, "FAILED": 0}
    for st_name, cnt in outbox_rows:
        outbox_counts[str(st_name)] = int(cnt)

    outbox_items = [
        {
            "id": int(r[0]),
            "aggregate_id": str(r[1]),
            "status": str(r[2]),
            "attempts": int(r[3]),
            "last_error": r[4],
            "created_at": (r[5].replace(tzinfo=timezone.utc) if r[5].tzinfo is None else r[5]).isoformat() if r[5] else None,
            "published_at": (r[6].replace(tzinfo=timezone.utc) if r[6].tzinfo is None else r[6]).isoformat() if r[6] else None,
        }
        for r in recent_outbox
    ]

    lakehouse_counts: Optional[Dict[str, int]] = None
    try:
        s3 = get_s3_client()
        counts = {"bronze": 0, "silver": 0, "gold": 0, "quarantine": 0}
        paginator = s3.get_paginator("list_objects_v2")
        for layer in counts:
            total = 0
            for page in paginator.paginate(Bucket=settings.MINIO_BUCKET, Prefix=f"{layer}/"):
                for obj in page.get("Contents", []):
                    if obj.get("Size", 0) > 0:
                        total += 1
            counts[layer] = total
        lakehouse_counts = counts
    except Exception:
        lakehouse_counts = None

    return {
        "oltp": {
            "total_orders": int(agg_row[0]),
            "total_revenue": round(float(agg_row[1]), 2),
            "avg_latency_ms": round(float(agg_row[2]), 2),
        },
        "inventory": inventory_items,
        "outbox": {
            "counts": outbox_counts,
            "recent": outbox_items,
        },
        "gold": gold_summary,
        "lakehouse": lakehouse_counts,
    }


def _get_quarantine_sync(selected_key: Optional[str], limit: int) -> Dict[str, Any]:
    s3 = get_s3_client()
    resp = s3.list_objects_v2(Bucket=settings.MINIO_BUCKET, Prefix="quarantine/")
    objects = [
        {
            "key": str(o["Key"]),
            "size": int(o.get("Size", 0)),
            "last_modified": o["LastModified"].isoformat() if o["LastModified"] else None,
        }
        for o in sorted(resp.get("Contents", []), key=lambda x: x["LastModified"], reverse=True)
        if o.get("Size", 0) > 0
    ][:limit]

    preview: Any = None
    if selected_key:
        if not selected_key.startswith("quarantine/"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"detail": "Key must stay under quarantine/.", "error_code": "INVALID_QUARANTINE_KEY"},
            )
        if selected_key.endswith(".json"):
            raw = s3.get_object(Bucket=settings.MINIO_BUCKET, Key=selected_key)["Body"].read().decode("utf-8")
            preview = json.loads(raw)
        else:
            preview = {"uri": f"s3://{settings.MINIO_BUCKET}/{selected_key}", "format": "parquet"}

    return {"objects": objects, "preview": preview}


def _get_data_quality_sync() -> Dict[str, Any]:
    s3 = get_s3_client()
    try:
        resp = s3.get_object(
            Bucket=settings.MINIO_BUCKET,
            Key="gold/data_quality_report/latest.json",
        )
        report = json.loads(resp["Body"].read().decode("utf-8"))
        return {"available": True, "report": report}
    except Exception:
        return {"available": False, "report": None}


def _read_silver_job_state() -> Dict[str, Any]:
    try:
        raw = redis_client.get(SILVER_JOB_REDIS_KEY)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return {
        "status": "IDLE",
        "started_at": None,
        "finished_at": None,
        "duration_sec": None,
        "log_tail": [],
    }


def _write_silver_job_state(state: Dict[str, Any]) -> None:
    try:
        redis_client.set(SILVER_JOB_REDIS_KEY, json.dumps(state), ex=3600)
    except Exception:
        pass


def _run_silver_job_background() -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "streaming.spark_stream_orders",
                "--starting-offsets",
                "earliest",
                "--available-now",
            ],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
        )
        duration = round(time.perf_counter() - t0, 2)
        if proc.returncode == 0:
            lines = proc.stdout.splitlines()[-15:]
            _write_silver_job_state(
                {
                    "status": "COMPLETED",
                    "started_at": started_at,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "duration_sec": duration,
                    "log_tail": lines,
                }
            )
        else:
            err_lines = (proc.stderr or proc.stdout).splitlines()[-20:]
            _write_silver_job_state(
                {
                    "status": "FAILED",
                    "started_at": started_at,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "duration_sec": duration,
                    "log_tail": err_lines,
                }
            )
    except Exception as exc:
        _write_silver_job_state(
            {
                "status": "FAILED",
                "started_at": started_at,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "duration_sec": round(time.perf_counter() - t0, 2),
                "log_tail": [str(exc)],
            }
        )


@router.get("/overview", response_model=StandardResponse[dict], summary="Control Plane Overview Snapshot")
async def get_ops_overview():
    data = await run_in_threadpool(_get_overview_sync)
    return StandardResponse(success=True, message="Control plane snapshot retrieved.", data=data)


@router.get("/quarantine", response_model=StandardResponse[dict], summary="Inspect MinIO Quarantine / DLQ")
async def get_quarantine_objects(
    key: Optional[str] = Query(None, description="Optional JSON object key under quarantine/"),
    limit: int = Query(20, ge=1, le=100),
):
    data = await run_in_threadpool(_get_quarantine_sync, key, limit)
    return StandardResponse(success=True, message="Quarantine objects retrieved.", data=data)


@router.get("/data-quality", response_model=StandardResponse[dict], summary="Latest Data Quality SLA Report")
async def get_data_quality_report():
    data = await run_in_threadpool(_get_data_quality_sync)
    return StandardResponse(success=True, message="Data quality report status retrieved.", data=data)


@router.post("/jobs/gold-etl", response_model=StandardResponse[dict], summary="Trigger Gold Star Schema ETL")
async def trigger_gold_etl(payload: Optional[GoldEtlRequest] = None):
    target_date = payload.target_date.strip() if payload and payload.target_date else None
    if target_date and not _DATE_RE.match(target_date):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"detail": "target_date must follow YYYY-MM-DD.", "error_code": "INVALID_DATE_FORMAT"},
        )
    res = await run_in_threadpool(run_gold_etl, target_date)
    return StandardResponse(success=True, message="Gold Star Schema ETL completed.", data=res)


@router.get("/jobs/silver-stream", response_model=StandardResponse[dict], summary="Poll PySpark Silver Job Status")
async def get_silver_job_status():
    state = await run_in_threadpool(_read_silver_job_state)
    return StandardResponse(success=True, message="Silver streaming job status retrieved.", data=state)


@router.post(
    "/jobs/silver-stream",
    response_model=StandardResponse[dict],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger PySpark Silver Streaming (--available-now)",
)
async def trigger_silver_job():
    current = await run_in_threadpool(_read_silver_job_state)
    if current.get("status") == "RUNNING":
        return StandardResponse(
            success=True,
            message="PySpark Silver job is already running.",
            data=current,
        )

    initial_state: Dict[str, Any] = {
        "status": "RUNNING",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "duration_sec": None,
        "log_tail": ["Launching PySpark Structured Streaming (--available-now)..."],
    }
    await run_in_threadpool(_write_silver_job_state, initial_state)
    threading.Thread(target=_run_silver_job_background, daemon=True).start()
    return StandardResponse(
        success=True,
        message="PySpark Silver streaming job started in background.",
        data=initial_state,
    )
