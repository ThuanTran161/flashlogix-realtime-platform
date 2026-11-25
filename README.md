# ⚡ FlashLogix — High-Concurrency Transaction Engine & Real-Time Lakehouse Platform

> **An end-to-end intersection of High-Concurrency Backend Engineering (OLTP) and Event-Driven Data Engineering (OLAP).**  
> Built with **FastAPI, Async Redis (Lua Atomic + Read-Through Cache + Multi-Worker Telemetry), PostgreSQL (Transactional Outbox + Star Schema), Apache Kafka (KRaft), PySpark Structured Streaming, MinIO Lakehouse, Kimball Star Schema (SCD Type 2), Locust, GitHub Actions CI, and a React/TypeScript Ops Console.**

---

## 1. System Architecture

```text
[ Locust Load Test (300+ Concurrent Users) ]       [ Internal Ops Console (React + TypeScript) ]
                   │                                                      │ (Served via FastAPI :8000)
                   ▼                                                      ▼
       [ FastAPI Async API ] ──► 1. Middleware, Ops API (/api/v1/ops/*) & Multi-Worker Telemetry (/metrics)
          │           │             (O(1) local buffer + async Redis HINCRBY across 4 Uvicorn workers)
          │           │
          │           ├────────► 2. Async Redis Gate (redis.asyncio, Zero DB Pool Contention)
          │           │              ├── Idempotency-Key Cache & In-Flight Lock (idem:order:{key})
          │           │              ├── Product Read-Through Cache (MGET product:meta + stock)
          │           │              └── Lua Atomic DECRBY (Zero Overselling; Rejects in RAM)
          │           │
          │           ├────────► 3. Lazy PostgreSQL OLTP (SessionLocal opened ONLY for Lua winners)
          │           │              └── Atomic TX: orders + outbox_events (PENDING) + CHECK Constraints
          │           │                            + Compensatory INCRBY on rollback
          │           │
          │           └────────► 4. BackgroundTasks / Outbox Replay ──► [ Apache Kafka 4.x (KRaft) ]
                                                                             │
                                        ┌────────────────────────────────────┴────────────────────────────────────┐
                                        ▼                                                                         ▼
                        [ Audit, DLQ & Fraud Consumer ]                                           [ PySpark Structured Streaming ]
                        (Velocity Bot Check + Bronze + DLQ)                                       (UTC + Schema + SHA-256 + Quarantine)
                                        │                                                                         │
                                        └────────────────────────────────────┬────────────────────────────────────┘
                                                                             ▼
                                                              [ MinIO S3 Medallion Lakehouse ]
                                                              ├── bronze/orders/order_date=YYYY-MM-DD/
                                                              ├── silver/orders/order_date=YYYY-MM-DD/
                                                              ├── quarantine/        (Poison-Pill JSON & Parquet)
                                                              └── gold/              (Summary Parquet & DQ Audit JSON)
                                                                             │
                                                                             ▼
                                                       [ PostgreSQL Kimball Star Schema (Batch Bulk Upserts) ]
                                                       ├── Dim_Users (SCD Type 2 + Indexes)
                                                       ├── Dim_Products / Dim_Date
                                                       └── Fact_Flash_Sale_Orders (Unified Fraud Flags)
```

---

## 2. Core Engineering Highlights

### Backend, Concurrency & Observability (OLTP)
* **Non-Blocking Async Redis Gate & Lazy DB Acquisition:** `OrderService` executes `Idempotency-Key` lookups, short in-flight retry locks, and `LUA_ATOMIC_DECREMENT` (`app/services/inventory_service.py`) asynchronously via `redis.asyncio` (`BlockingConnectionPool`) *before* acquiring a PostgreSQL `Session`. During flash-sale spikes where >98% of requests fail stock checks or hit cache, PostgreSQL connection pool contention is bypassed cleanly (`DB_POOL_SIZE=12`, `DB_MAX_OVERFLOW=8` per worker -> `80` max across `4` workers).
* **Transactional Outbox & Replay API:** Every committed `Order` writes an `OutboxEvent(status="PENDING")` inside the exact same PostgreSQL commit (`app/models/outbox.py`, `app/services/outbox_service.py`). Background delivery updates `status` to `PUBLISHED` or `FAILED`, while `POST /api/v1/orders/outbox/replay` safely replays any failed/pending events to Kafka.
* **Redis Read-Through Cache & Paginated/Filtered Query APIs:** `GET /api/v1/products?limit=&offset=`, `GET /api/v1/products/{product_id}` (single `MGET` round-trip), `GET /api/v1/orders?user_id=&product_id=&limit=&offset=`, and `GET /api/v1/orders/{order_id}` provide full OLTP read/write lifecycle visibility.
* **Defense-in-Depth Constraints & Compensatory Rollback:** `Product` and `Order` enforce PostgreSQL `CHECK` constraints (`stock >= 0`, `price > 0`, `quantity > 0`, `total_amount >= 0`) synchronized on startup via `ensure_oltp_schema_and_constraints()`. If PostgreSQL fails after Redis deduction, `OrderService` automatically executes a compensating `INCRBY` on Redis.
* **Multi-Worker Shared Redis Telemetry & Tracing:** Every request receives `X-Request-ID` and `X-Process-Time-Ms` headers. `TelemetryCollector` (`app/core/telemetry.py`) buffers increments in O(1) RAM and flushes batches asynchronously (and within 250ms for tail updates) via Redis pipeline `HINCRBY` (`flashlogix:telemetry:counters`) so `/metrics` (`?format=prometheus` or JSON) aggregates accurately across all 4 Uvicorn workers alongside `outbox_pending_or_failed` and Kafka publish/failure counters.

### Data Engineering, Resilience & Lakehouse (OLAP)
* **Dead-Letter Queue (DLQ) & Poison-Pill Quarantine:** `OrderAuditConsumer` validates incoming Kafka events and routes malformed payloads to Kafka `flashsale.orders.dlq` and MinIO `quarantine/orders/`, while PySpark Structured Streaming isolates schema-invalid records into `quarantine/orders_stream/` instead of crashing the stream.
* **Unified Velocity Fraud Detection (Consumer -> Silver -> Gold):** Both `OrderAuditConsumer` (real-time Redis sliding 60s window) and `batch_gold_aggregation.py` (`apply_unified_velocity_fraud_flags`, >= `FRAUD_VELOCITY_THRESHOLD_PER_MIN` per minute) propagate `is_fraudulent` into `Fact_Flash_Sale_Orders` via idempotent upsert.
* **PySpark Structured Streaming & PII Governance:** Enforces explicit `StructType` schemas, strict `UTC` session partitioning (`order_date=YYYY-MM-DD/`), hashes customer PII (`user_id` -> `masked_user_id`) using salted SHA-256, deduplicates by `order_id`, and computes 1-minute tumbling window aggregations with watermarking.
* **Partition-Pruned Batch Bulk Gold Star Schema & DQ Audit:** `lakehouse/batch_gold_aggregation.py` supports incremental date-partition pruning (`--date YYYY-MM-DD`) and set-based `executemany` bulk upserts into indexed `Dim_Date`, `Dim_Products`, SCD Type 2 `Dim_Users`, and `Fact_Flash_Sale_Orders`, exporting an automated reconciliation & PII audit report (`gold/data_quality_report/latest.json`).
* **1-Command All-in-One Launcher & Internal Ops Console:** `run.ps1` orchestrates the isolated Docker stack, schema/seed sync, 4-worker Uvicorn API (serving both REST APIs and the compiled React Ops Console at `http://localhost:8000`), and Kafka `OrderAuditConsumer`.
* **Hardened Container & CI/CD:** Non-root `Dockerfile` (`appuser`) with `HEALTHCHECK` and multi-worker `uvicorn`, paired with `.github/workflows/ci.yml` (`ruff check` + `pytest tests/ -v`).

---

## 3. Verified Benchmarks & Data Quality SLA Metrics

All metrics below were measured locally via `scripts/benchmark_compare.py --reset-clean --verify-lakehouse` against isolated Docker Compose (`PostgreSQL 15`, `Redis 7.2`, `Apache Kafka 4.3.1`, `MinIO`) under identical load (**300 Concurrent VUs, spawn rate 100/s, 10s burst, 4 Uvicorn workers, Product #1 stock = 100 @ \$199.99**).

### A. OLTP Performance: Controlled Baseline vs. Optimized Architecture

| Metric | Baseline: PostgreSQL `SELECT ... FOR UPDATE` (`db_only`) | Optimized: Async Redis Gate + Lazy DB + Read Cache (`redis_async_lazy`) | Delta (Optimized vs. Baseline) |
| :--- | :--- | :--- | :--- |
| **Total Requests (10s)** | 4,607 | **6,690** | **+45.2%** |
| **Failed Requests** | 0 (`0.00%`) | **0 (`0.00%`)** | **100% Reliable** |
| **Throughput (RPS)** | 493.75 req/s | **684.56 req/s** | **+38.6%** |
| **Average Latency** | 355.27 ms | **184.00 ms** | **-48.2%** |
| **Median Latency (P50)** | 330.0 ms | **130.0 ms** | **-60.6%** |
| **Tail Latency (P95)** | 560.0 ms | **500.0 ms** | **-10.7%** |
| **Tail Latency (P99)** | 1,200.0 ms | **1,200.0 ms** | **Matched** |
| **Min / Max Latency** | 6.54 ms / 1,567.28 ms | **2.51 ms / 2,228.41 ms** | **-61.6% Min** |
| **Oversold Units** | 0 (`100 / 100`) | **0 (`100 / 100`)** | **Zero Overselling** |
| **DB Sessions Bypassed** | 0% (all hit DB) | **98.51%** (`6,590 / 6,690` handled in Redis RAM) | **Pool contention eliminated** |
| **Transactional Outbox State** | 0 pending / 0 failed | **0 pending / 0 failed (`200 / 200 PUBLISHED`)** | **100% Event Delivery** |

### B. Data Engineering, DLQ Resilience & Medallion Lakehouse SLA

Verified end-to-end across both benchmark waves (`200` confirmed orders) plus **20 deliberate duplicate Kafka events** and **1 malformed JSON poison-pill payload**:

| Pipeline SLA Metric | Measured Result |
| :--- | :--- |
| **Bronze Raw Events Archived (`OrderAuditConsumer`)** | **220** valid events (`200` unique + `20` injected duplicates) |
| **Duplicate Noise Filtered (Bronze -> Silver/Gold)** | **20 / 220 (9.09% filtered)** -> **200 unique orders** |
| **Poison-Pill DLQ & MinIO Quarantine** | **1** Kafka DLQ event (`flashsale.orders.dlq`) + **2** MinIO quarantine artifacts (`orders/` + `orders_stream/`) |
| **OLTP ↔ Gold Star Schema Order Parity** | **200 / 200 (100.00% Parity)** |
| **OLTP ↔ Gold Revenue Reconciliation Delta** | **\$39,998.00 vs. \$39,998.00 (\$0.00 Delta)** |
| **PII Compliance Violations** | **0 violations** (`user_id` dropped; 100% salted 64-hex SHA-256 `masked_user_id`) |
| **Gold Star Schema Batch ETL Runtime** | **0.475 s** full / **0.354 s** `--date` partition-pruned (PySpark `--available-now`: **51.22 s** incl. JVM startup) |

---

## 4. Repository Structure

```text
Flashlogix_Platform/
├── .github/workflows/
│   └── ci.yml                  # Automated Ruff lint + Pytest CI (Postgres 15, Redis 7.2, Java 17)
├── app/
│   ├── api/v1/                 # FastAPI async routes (products, orders, outbox replay, ops control plane)
│   ├── core/                   # Settings, SQLAlchemy pool & CHECK constraints, MinIO S3, Shared Redis Telemetry
│   ├── models/                 # SQLAlchemy ORM models (Product, Order, OutboxEvent)
│   ├── schemas/                # Pydantic v2 validation & error envelopes
│   ├── services/               # Async/Sync Redis Lua inventory, Order, Outbox, and Kafka producer services
│   └── main.py                 # FastAPI entrypoint, tracing middleware, /health, /metrics, SPA serving
├── streaming/
│   ├── audit_consumer.py       # Kafka consumer: velocity fraud check, DLQ + MinIO Bronze/Quarantine
│   ├── transformations.py      # PySpark schema enforcement, quarantine split, PII masking, windows
│   ├── spark_stream_orders.py  # PySpark Structured Streaming job (UTC, Kafka -> Silver + Quarantine)
│   └── star_schema.sql         # Indexed PostgreSQL Kimball Star Schema DDL (SCD Type 2)
├── lakehouse/
│   └── batch_gold_aggregation.py # Partition-pruned batch bulk Gold Star Schema ETL + DQ Audit
├── frontend/                   # React + TypeScript + Tailwind Ops Console (served via FastAPI :8000)
├── scripts/
│   └── benchmark_compare.py    # Automated Before/After Locust benchmark & Lakehouse E2E verifier
├── tests/
│   ├── test_backend.py         # Observability, query APIs, Ops API, Outbox replay, Idempotency & CHECK tests
│   ├── test_streaming_and_gold.py # PySpark quarantine/PII, DLQ validation, unified fraud, SCD2 & batch upsert tests
│   └── locustfile.py           # High-concurrency Locust load testing suite
├── run.ps1                     # 1-command All-in-One launcher (start / stop / down / status / test / bench)
├── docker-compose.yml          # Isolated Postgres (5433), Redis (6380), Kafka (9094), MinIO (9000)
├── Dockerfile                  # Non-root production FastAPI container with HEALTHCHECK
├── pyproject.toml              # Pytest & Ruff configuration
└── requirements.txt
```

---

## 5. Quickstart Guide

### Option A: 1-Command All-in-One Launcher (Recommended)

Start the entire FlashLogix stack (**isolated Docker containers + DB/Redis seed + React build + 4-worker FastAPI + Kafka AuditConsumer**) with a single command:

```powershell
.\run.ps1
```

Once ready, your browser opens **`http://localhost:8000`** (internal Ops & Lakehouse Console served directly by FastAPI, with Swagger at `/docs` and telemetry at `/metrics`).

| Command | Action |
| :--- | :--- |
| `.\run.ps1` (or `.\run.ps1 start`) | Starts Docker (`flashlogix_*`), builds UI if needed, syncs schema/stock, launches 4-worker API (`:8000`) & `AuditConsumer` |
| `.\run.ps1 status` | Checks `flashlogix_*` container health and running app PIDs |
| `.\run.ps1 stop` | Cleanly stops FastAPI and `AuditConsumer` (keeps Docker & persisted data running) |
| `.\run.ps1 down` | Stops app processes and runs `docker compose down` for FlashLogix only |
| `.\run.ps1 test` | Runs `ruff check` + full `pytest tests/ -v` suite |
| `.\run.ps1 bench` | Runs the automated 300-VU Locust Before/After benchmark + Lakehouse E2E verification |

> **Port Isolation:** Services run on `5433` (Postgres), `6380` (Redis), `9094` (Kafka), and `9000/9001` (MinIO) inside the dedicated `flashlogix_net` bridge network.

### Option B: Manual Component Execution (Advanced)

```powershell
# 1. Start infrastructure, build UI & run test suite
Copy-Item .env.example .env
docker compose up -d
cd frontend; npm install; npm run build; cd ..
.\.venv\Scripts\python.exe -m ruff check app streaming lakehouse scripts tests
.\.venv\Scripts\python.exe -m pytest tests/ -v

# 2. Run API + Ops Console (http://localhost:8000, /docs, /health, /metrics) & AuditConsumer
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
.\.venv\Scripts\python.exe -m streaming.audit_consumer

# 3. Run PySpark Silver Streaming, Gold Star Schema ETL, or Benchmark
.\.venv\Scripts\python.exe -m streaming.spark_stream_orders --starting-offsets earliest
.\.venv\Scripts\python.exe -m lakehouse.batch_gold_aggregation
.\.venv\Scripts\python.exe scripts/benchmark_compare.py --reset-clean --verify-lakehouse
```
