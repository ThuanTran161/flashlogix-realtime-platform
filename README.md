# FlashLogix -- High-Concurrency Transaction Engine & Real-Time Lakehouse Platform

FlashLogix is an end-to-end backend and data engineering platform built for flash-sale workloads. It pairs an asynchronous, Redis-gated **OLTP transaction engine** with an event-driven **Medallion Lakehouse** (`Bronze / Silver / Gold`) and a **Kimball Star Schema** analytical warehouse, backed by deterministic load benchmarks, automated data quality reconciliation, and an internal operations console.

---

## 1. System Architecture

```text
[ Locust Load Suite (300 VUs) ]                [ Ops & Lakehouse Console (React / TS) ]
               |                                                  | (Served at http://localhost:8000)
               v                                                  v
     [ FastAPI Async API (4 Uvicorn Workers) ] ---> Tracing Middleware (X-Request-ID, Process-Time)
        |      |                                    Multi-Worker Telemetry (/metrics JSON & Prometheus)
        |      |
        |      +--> [ Async Redis Gate ]            (redis.asyncio BlockingConnectionPool)
        |      |      |-- Idempotency-Key Cache & Short Lock (idem:order:{key})
        |      |      |-- Product Read-Through Cache (Single-RTT MGET)
        |      |      +-- Lua Atomic DECRBY (Rejects out-of-stock in RAM; zero overselling)
        |      |
        |      +--> [ Lazy PostgreSQL OLTP ]        (Session acquired ONLY for Redis Lua winners)
        |      |      +-- Single TX: orders + outbox_events (PENDING) + CHECK constraints
        |      |          + Automatic Redis INCRBY compensatory rollback on DB failure
        |      |
        |      +--> [ BackgroundTasks / Outbox Replay ] --> [ Apache Kafka 4.3.1 (KRaft) ]
        |                                                       |
        |                       +-------------------------------+-------------------------------+
        |                       |                                                               |
        |                       v                                                               v
        |      [ Kafka Audit, DLQ & Velocity Consumer ]                        [ PySpark Structured Streaming ]
        |      - Real-time 60s Redis fraud velocity check                      - Explicit StructType schema enforcement
        |      - Poison-pill routing -> DLQ + quarantine/                      - UTC partitioning + Salted SHA-256 PII masking
        |      - Immutable Snappy Parquet archival -> bronze/                  - Deduplication + 1-min tumbling windows
        |                       |                                                               |
        |                       +-------------------------------+-------------------------------+
        |                                                       v
        |                                        [ MinIO S3-Compatible Lakehouse ]
        |                                          |-- bronze/orders/order_date=YYYY-MM-DD/
        |                                          |-- silver/orders/order_date=YYYY-MM-DD/
        |                                          |-- quarantine/   (DLQ JSON & Spark bad records)
        |                                          +-- gold/         (Revenue Parquet & DQ SLA JSON)
        |                                                       |
        |                                                       v
        +--------------------------------------> [ PostgreSQL Kimball Star Schema ]
                                                   |-- Dim_Users (SCD Type 2)
                                                   |-- Dim_Products / Dim_Date
                                                   +-- Fact_Flash_Sale_Orders (Unified Fraud Flags)
```

---

## 2. Core Engineering Design

### Backend Concurrency & OLTP Reliability
* **Async Redis Gate & Lazy DB Acquisition:** `OrderService` performs `Idempotency-Key` checks, in-flight lock retries, and `LUA_ATOMIC_DECREMENT` (`app/services/inventory_service.py`) over `redis.asyncio` before opening any SQLAlchemy session. Under flash-sale contention where >98% of requests fail stock checks or hit idempotency cache, PostgreSQL pool exhaustion is avoided (`DB_POOL_SIZE=12`, `DB_MAX_OVERFLOW=8` per worker; 80 max across 4 workers).
* **Transactional Outbox & Replay:** Every committed `Order` writes an `OutboxEvent(status="PENDING")` within the same database transaction (`app/models/outbox.py`, `app/services/outbox_service.py`). Background delivery transitions records to `PUBLISHED` or `FAILED`, and `POST /api/v1/orders/outbox/replay` recovers any undelivered events to Kafka.
* **Defense-in-Depth & Compensatory Rollback:** PostgreSQL enforces `CHECK` constraints (`stock >= 0`, `price > 0`, `quantity > 0`, `total_amount >= 0`) synchronized at startup via `ensure_oltp_schema_and_constraints()`. Any database failure after Redis stock deduction triggers an automatic `INCRBY` rollback on Redis.
* **Multi-Worker Shared Telemetry:** `TelemetryCollector` (`app/core/telemetry.py`) records O(1) in-memory increments on the hot path and flushes batches asynchronously (and within 250 ms for quiet tails) via Redis pipeline `HINCRBY`, ensuring `/metrics` aggregates accurately across all 4 Uvicorn worker processes.

### Data Engineering, Resilience & Lakehouse Governance
* **Dead-Letter Queue (DLQ) & Poison-Pill Quarantine:** `OrderAuditConsumer` (`streaming/audit_consumer.py`) validates raw Kafka payloads and isolates malformed JSON or invalid schemas to Kafka `flashsale.orders.dlq` and MinIO `quarantine/orders/`. PySpark Structured Streaming (`streaming/spark_stream_orders.py`) independently routes schema-invalid records into `quarantine/orders_stream/` without halting stream processing.
* **Unified Velocity Fraud Detection:** Real-time Redis sliding 60-second counters in `OrderAuditConsumer` and 1-minute tumbling window checks in `lakehouse/batch_gold_aggregation.py` (`apply_unified_velocity_fraud_flags`) propagate `is_fraudulent` into `Fact_Flash_Sale_Orders` when an account exceeds `FRAUD_VELOCITY_THRESHOLD_PER_MIN` (default: 5 orders/min).
* **PySpark Streaming & PII Masking:** Enforces strict `StructType` schemas and `UTC` session partitioning (`order_date=YYYY-MM-DD/`), hashes raw `user_id` values into 64-char salted SHA-256 `masked_user_id`, deduplicates by `order_id`, and writes Snappy Parquet to MinIO Silver.
* **Partition-Pruned Star Schema ETL & Reconciliation SLA:** `run_gold_etl()` supports incremental `--date YYYY-MM-DD` partition pruning and set-based `executemany` upserts into `Dim_Date`, `Dim_Products`, SCD Type 2 `Dim_Users`, and `Fact_Flash_Sale_Orders`, writing an automated audit report (`gold/data_quality_report/latest.json`) verifying 100% OLTP-to-Gold parity and zero PII leakage.

---

## 3. Verified Benchmarks & Data Quality SLA

All numbers below were measured locally using `scripts/benchmark_compare.py --reset-clean --verify-lakehouse` against isolated Docker services (`PostgreSQL 15`, `Redis 7.2`, `Apache Kafka 4.3.1`, `MinIO`) under identical load (**300 Concurrent VUs, spawn rate 100/s, 10 s burst, 4 Uvicorn workers, Product #1 stock = 100 at $199.99**).

### A. OLTP Throughput & Latency (Baseline vs. Optimized)

| Metric | Baseline: `SELECT ... FOR UPDATE` (`db_only`) | Optimized: Async Redis Gate + Lazy DB (`redis_async_lazy`) | Delta |
| :--- | :--- | :--- | :--- |
| **Total Requests (10s)** | 4,607 | **6,690** | **+45.2%** |
| **Failed Requests** | 0 (0.00%) | **0 (0.00%)** | 100% Reliable |
| **Throughput (RPS)** | 493.75 req/s | **684.56 req/s** | **+38.6%** |
| **Average Latency** | 355.27 ms | **184.00 ms** | **-48.2%** |
| **Median Latency (P50)** | 330.0 ms | **130.0 ms** | **-60.6%** |
| **Tail Latency (P95 / P99)** | 560.0 ms / 1,200.0 ms | **500.0 ms / 1,200.0 ms** | -10.7% / Matched |
| **Min Latency** | 6.54 ms | **2.51 ms** | **-61.6%** |
| **Oversold Units** | 0 (100 / 100) | **0 (100 / 100)** | Zero Overselling |
| **DB Sessions Bypassed** | 0% | **98.51%** (6,590 / 6,690 in Redis RAM) | Pool contention eliminated |
| **Transactional Outbox** | 0 pending / 0 failed | **0 pending / 0 failed (200 / 200 PUBLISHED)** | 100% Event Delivery |

### B. Lakehouse Pipeline & Data Quality SLA

Measured across both benchmark waves (`200` confirmed orders), **20 deliberate duplicate Kafka events**, and **1 malformed JSON poison-pill payload**:

| Pipeline SLA Metric | Measured Result |
| :--- | :--- |
| **Bronze Raw Events Archived (`OrderAuditConsumer`)** | **220** valid events (200 unique + 20 injected duplicates) |
| **Duplicates Filtered (`Bronze` -> `Silver/Gold`)** | **20 / 220 (9.09% filtered)** -> 200 unique orders |
| **Poison-Pill DLQ & MinIO Quarantine** | **1** Kafka DLQ record (`flashsale.orders.dlq`) + **2** MinIO quarantine artifacts (`orders/` + `orders_stream/`) |
| **OLTP vs. Gold Order Parity** | **200 / 200 (100.00% Parity)** |
| **OLTP vs. Gold Revenue Delta** | **$39,998.00 vs. $39,998.00 ($0.00 Delta)** |
| **PII Compliance Violations** | **0 violations** (`user_id` dropped; 100% salted 64-hex SHA-256 `masked_user_id`) |
| **Gold Star Schema ETL Runtime** | **0.475 s** full / **0.354 s** `--date` pruned (PySpark `--available-now`: **51.22 s** incl. JVM startup) |

---

## 4. API & Operations Endpoints

| Endpoint | Method | Purpose |
| :--- | :--- | :--- |
| `/` | `GET` | React/TypeScript Ops & Lakehouse Console (served from `frontend/dist`) |
| `/docs` | `GET` | Interactive OpenAPI / Swagger documentation |
| `/health` | `GET` | Dependency health checks (PostgreSQL, Redis, Kafka, MinIO) |
| `/metrics` | `GET` | Shared multi-worker counters and DB pool metrics (`?format=prometheus` supported) |
| `/api/v1/products`, `/api/v1/products/{id}` | `GET`, `POST` | Catalog creation and Redis read-through cache lookup |
| `/api/v1/products/{id}/init-stock` | `POST` | Synchronize product stock between PostgreSQL and Redis |
| `/api/v1/orders`, `/api/v1/orders/{id}` | `GET`, `POST` | Order placement (with `Idempotency-Key`) and paginated history |
| `/api/v1/orders/outbox/replay` | `POST` | Replay `PENDING` or `FAILED` Outbox events to Kafka |
| `/api/v1/ops/overview` | `GET` | Consolidated OLTP, inventory sync, Outbox, MinIO, and Gold Star Schema snapshot |
| `/api/v1/ops/jobs/silver-stream` | `GET`, `POST` | Trigger and poll PySpark Silver streaming (`--available-now`) |
| `/api/v1/ops/jobs/gold-etl` | `POST` | Trigger Gold Star Schema batch ETL (optional UTC `target_date` pruning) |
| `/api/v1/ops/quarantine`, `/api/v1/ops/data-quality` | `GET` | Inspect MinIO DLQ artifacts and latest Data Quality SLA report |

---

## 5. Repository Structure

```text
Flashlogix_Platform/
├── .github/workflows/ci.yml        # GitHub Actions CI: Ruff lint + Pytest (Postgres 15, Redis 7.2, Java 17)
├── app/
│   ├── api/v1/                     # FastAPI routes (products, orders, outbox replay, ops control plane)
│   ├── core/                       # Config, SQLAlchemy pool & CHECK constraints, MinIO S3, Redis telemetry
│   ├── models/                     # SQLAlchemy ORM models (Product, Order, OutboxEvent)
│   ├── schemas/                    # Pydantic v2 request/response schemas and UTC serializers
│   ├── services/                   # Async/Sync Redis Lua inventory, Order, Outbox, Kafka producer
│   └── main.py                     # FastAPI entrypoint, tracing middleware, /health, /metrics, SPA serving
├── streaming/
│   ├── audit_consumer.py           # Kafka consumer: velocity fraud check, DLQ + MinIO Bronze/Quarantine
│   ├── transformations.py          # PySpark schema enforcement, quarantine split, PII masking, windows
│   ├── spark_stream_orders.py      # PySpark Structured Streaming job (UTC, Kafka -> Silver + Quarantine)
│   └── star_schema.sql             # Indexed PostgreSQL Kimball Star Schema DDL (SCD Type 2)
├── lakehouse/
│   └── batch_gold_aggregation.py   # Partition-pruned batch bulk Gold Star Schema ETL & SLA audit
├── frontend/                       # React + TypeScript + Tailwind Ops Console (built into frontend/dist)
├── scripts/
│   └── benchmark_compare.py        # Automated Before/After Locust benchmark & Lakehouse E2E verifier
├── tests/
│   ├── test_backend.py             # Concurrency, Idempotency, Lazy DB, Outbox, Ops & CHECK constraint tests
│   ├── test_streaming_and_gold.py  # PySpark quarantine/PII, DLQ, velocity fraud, SCD2 & batch upsert tests
│   └── locustfile.py               # Locust high-concurrency load suite
├── run.ps1                         # All-in-one Windows PowerShell launcher & lifecycle CLI
├── docker-compose.yml              # Isolated Postgres (5433), Redis (6380), Kafka (9094), MinIO (9000/9001)
├── Dockerfile                      # Non-root production container with HEALTHCHECK
├── pyproject.toml                  # Ruff & Pytest configuration
└── requirements.txt
```

---

## 6. Quickstart & Operations

### Prerequisites
* Docker Desktop (running)
* Python 3.12+ (`.venv` recommended) and Java 17 (required by PySpark)
* Node.js 18+ (only needed when rebuilding `frontend/dist`)

### Option A: All-in-One Launcher (Windows PowerShell)

```powershell
# Start isolated Docker stack, sync schema/stock, build UI if needed,
# launch 4-worker FastAPI (:8000) and AuditConsumer, then open http://localhost:8000
.\run.ps1
```

| Command | Action |
| :--- | :--- |
| `.\run.ps1` (`start`) | Starts `flashlogix_*` containers, initializes DB/Redis stock, launches API (`:8000`) and `AuditConsumer` |
| `.\run.ps1 status` | Displays container health and active background process IDs |
| `.\run.ps1 stop` | Stops FastAPI and `AuditConsumer` while keeping Docker services and persisted volumes running |
| `.\run.ps1 down` | Stops app processes and tears down the FlashLogix Docker Compose stack |
| `.\run.ps1 test` | Runs `ruff check` and the full `pytest tests/ -v` test suite |
| `.\run.ps1 bench` | Executes the 300-VU Before/After Locust benchmark and Lakehouse E2E verification |

### Option B: Manual Execution

```powershell
# 1. Environment, infrastructure, UI build, and CI checks
Copy-Item .env.example .env
docker compose up -d
cd frontend; npm.cmd install; npm.cmd run build; cd ..
.\.venv\Scripts\python.exe -m ruff check app streaming lakehouse scripts tests
.\.venv\Scripts\python.exe -m pytest tests/ -v

# 2. Start API server and Kafka AuditConsumer
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
.\.venv\Scripts\python.exe -m streaming.audit_consumer

# 3. Trigger PySpark Silver streaming, Gold Star Schema ETL, or full benchmark
.\.venv\Scripts\python.exe -m streaming.spark_stream_orders --starting-offsets earliest --available-now
.\.venv\Scripts\python.exe -m lakehouse.batch_gold_aggregation
.\.venv\Scripts\python.exe scripts/benchmark_compare.py --reset-clean --verify-lakehouse
```

### Port Isolation
FlashLogix runs inside the dedicated `flashlogix_net` Docker bridge network on non-default host ports to avoid colliding with local database or broker installations:

| Service | Host Port |
| :--- | :--- |
| FastAPI & Ops Console | `8000` |
| PostgreSQL 15 | `5433` |
| Redis 7.2 | `6380` |
| Apache Kafka 4.3.1 (KRaft) | `9094` |
| MinIO S3 API / Web Console | `9000` / `9001` |
