# ⚡ FlashLogix — High-Concurrency Transaction Engine & Real-Time Lakehouse Platform

> **An end-to-end intersection of High-Concurrency Backend Engineering (OLTP) and Event-Driven Data Engineering (OLAP).**  
> Built with **FastAPI, Async Redis (Lua Atomic + Read-Through Cache), PostgreSQL, Apache Kafka (KRaft), PySpark Structured Streaming, MinIO Lakehouse, Kimball Star Schema (SCD Type 2), Locust, Streamlit, and GitHub Actions CI.**

---

## 1. System Architecture

```text
[ Locust Load Test (300+ Concurrent Users) ]
                   │
                   ▼
       [ FastAPI Async API ] ──► 1. Middleware & Observability (/metrics JSON & Prometheus, X-Request-ID)
          │           │
          │           ├────────► 2. Async Redis Gate (redis.asyncio, Zero DB Pool Contention)
          │           │              ├── Idempotency-Key Cache & In-Flight Lock (idem:order:{key})
          │           │              ├── Product Read-Through Cache (MGET product:meta + stock)
          │           │              └── Lua Atomic DECRBY (Zero Overselling; Rejects in RAM)
          │           │
          │           ├────────► 3. Lazy PostgreSQL OLTP (SessionLocal opened ONLY for Lua winners)
          │           │              (SELECT ... FOR UPDATE + CHECK Constraints + Compensatory INCRBY)
          │           │
          │           └────────► 4. FastAPI BackgroundTasks ──► [ Apache Kafka 4.x (KRaft) ]
                                                                   │
                                        ┌──────────────────────────┴──────────────────────────┐
                                        ▼                                                     ▼
                        [ Audit, DLQ & Fraud Consumer ]                   [ PySpark Structured Streaming ]
                        (Velocity Bot Check + Bronze + DLQ)               (Explicit Schema + SHA-256 + Quarantine)
                                        │                                                     │
                                        └──────────────────────────┬──────────────────────────┘
                                                                   ▼
                                                      [ MinIO S3 Medallion Lakehouse ]
                                                      ├── bronze/orders/     (Raw Parquet)
                                                      ├── silver/orders/     (PII-Masked, Deduped Parquet)
                                                      ├── quarantine/        (Poison-Pill JSON & Parquet)
                                                      └── gold/              (Summary Parquet & DQ Audit JSON)
                                                                   │
                                                                   ▼
                                                 [ PostgreSQL Kimball Star Schema ]
                                                 ├── Dim_Users (SCD Type 2 + Indexes)
                                                 ├── Dim_Products / Dim_Date
                                                 └── Fact_Flash_Sale_Orders (Unified Fraud Flags)
                                                                   │
                                                                   ▼
                                                    [ Streamlit Command Center ]
```

---

## 2. Core Engineering Highlights

### Backend, Concurrency & Observability (OLTP)
* **Non-Blocking Async Redis Gate & Lazy DB Acquisition:** `OrderService` executes `Idempotency-Key` lookups, short in-flight retry locks, and `LUA_ATOMIC_DECREMENT` (`app/services/inventory_service.py`) asynchronously via `redis.asyncio` (`BlockingConnectionPool`) *before* acquiring a PostgreSQL `Session`. During flash-sale spikes where >98% of requests fail stock checks or hit cache, PostgreSQL connection pool contention is bypassed cleanly (`DB_POOL_SIZE=12`, `DB_MAX_OVERFLOW=8` per worker -> `80` max across `4` workers).
* **Redis Read-Through Cache & Query Endpoints:** `GET /api/v1/products`, `GET /api/v1/products/{product_id}` (single `MGET` round-trip), and `GET /api/v1/orders/{order_id}` provide full OLTP read/write lifecycle visibility.
* **Defense-in-Depth Constraints & Compensatory Rollback:** `Product` and `Order` enforce PostgreSQL `CHECK` constraints (`stock >= 0`, `price > 0`, `quantity > 0`, `total_amount >= 0`) synchronized on startup via `ensure_oltp_schema_and_constraints()`. If PostgreSQL fails after Redis deduction, `OrderService` automatically executes a compensating `INCRBY` on Redis.
* **Request Tracing & Prometheus/JSON Telemetry:** Every request receives `X-Request-ID` and `X-Process-Time-Ms` headers, and `/metrics` (`?format=prometheus` or JSON) exposes live HTTP, cache hit/miss, idempotency, DB pool, and rollback counters.

### Data Engineering, Resilience & Lakehouse (OLAP)
* **Dead-Letter Queue (DLQ) & Poison-Pill Quarantine:** `OrderAuditConsumer` validates incoming Kafka events and routes malformed payloads to Kafka `flashsale.orders.dlq` and MinIO `quarantine/orders/`, while PySpark Structured Streaming isolates schema-invalid records into `quarantine/orders_stream/` instead of crashing the stream.
* **Unified Velocity Fraud Detection (Consumer -> Silver -> Gold):** Both `OrderAuditConsumer` (real-time Redis sliding 60s window) and `batch_gold_aggregation.py` (`apply_unified_velocity_fraud_flags`, >= `FRAUD_VELOCITY_THRESHOLD_PER_MIN` per minute) propagate `is_fraudulent` into `Fact_Flash_Sale_Orders` via idempotent upsert.
* **PySpark Structured Streaming & PII Governance:** Enforces explicit `StructType` schemas, hashes customer PII (`user_id` -> `masked_user_id`) using salted SHA-256, deduplicates by `order_id`, and computes 1-minute tumbling window aggregations with watermarking.
* **Indexed Kimball Star Schema & Data Quality Audit:** `streaming/star_schema.sql` and `lakehouse/batch_gold_aggregation.py` populate indexed `Dim_Date`, `Dim_Products`, `Fact_Flash_Sale_Orders`, and SCD Type 2 `Dim_Users`, exporting an automated reconciliation & PII audit report (`gold/data_quality_report/latest.json`).
* **Hardened Container & CI/CD:** Non-root `Dockerfile` (`appuser`) with `HEALTHCHECK` and multi-worker `uvicorn`, paired with `.github/workflows/ci.yml` (`ruff check` + `pytest tests/ -v`).

---

## 3. Verified Benchmarks & Data Quality SLA Metrics

All metrics below were measured locally via `scripts/benchmark_compare.py --reset-clean --verify-lakehouse` against isolated Docker Compose (`PostgreSQL 15`, `Redis 7.2`, `Apache Kafka 4.3.1`, `MinIO`) under identical load (**300 Concurrent VUs, spawn rate 100/s, 10s burst, 4 Uvicorn workers, Product #1 stock = 100 @ \$199.99**).

### A. OLTP Performance: Controlled Baseline vs. Optimized Architecture

| Metric | Baseline: PostgreSQL `SELECT ... FOR UPDATE` (`db_only`) | Optimized: Async Redis Gate + Lazy DB + Read Cache (`redis_async_lazy`) | Delta (Optimized vs. Baseline) |
| :--- | :--- | :--- | :--- |
| **Total Requests (10s)** | 4,936 | **8,211** | **+66.3%** |
| **Failed Requests** | 0 (`0.00%`) | **0 (`0.00%`)** | **100% Reliable** |
| **Throughput (RPS)** | 542.19 req/s | **857.36 req/s** | **+58.1%** |
| **Average Latency** | 334.76 ms | **124.59 ms** | **-62.8%** |
| **Median Latency (P50)** | 350.0 ms | **110.0 ms** | **-68.6%** |
| **Tail Latency (P95)** | 490.0 ms | **210.0 ms** | **-57.1%** |
| **Tail Latency (P99)** | 890.0 ms | **720.0 ms** | **-19.1%** |
| **Min / Max Latency** | 5.81 ms / 1,130.94 ms | **3.09 ms / 1,285.93 ms** | **-46.8% Min** |
| **Oversold Units** | 0 (`100 / 100`) | **0 (`100 / 100`)** | **Zero Overselling** |
| **DB Sessions Bypassed** | 0% (all hit DB) | **98.78%** (`8,111 / 8,211` handled in Redis RAM) | **Pool contention eliminated** |

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
| **Gold Star Schema Batch ETL Runtime** | **0.823 s** (PySpark `--available-now` micro-batch: **29.19 s** incl. JVM startup) |

---

## 4. Repository Structure

```text
Flashlogix_Platform/
├── .github/workflows/
│   └── ci.yml                  # Automated Ruff lint + Pytest CI (Postgres 15, Redis 7.2, Java 17)
├── app/
│   ├── api/v1/                 # FastAPI async routes (products list/detail, orders create/detail)
│   ├── core/                   # Settings, SQLAlchemy pool & CHECK constraints, MinIO S3, Telemetry
│   ├── models/                 # SQLAlchemy ORM models (Product, Order with CHECK constraints)
│   ├── schemas/                # Pydantic v2 validation & error envelopes
│   ├── services/               # Async/Sync Redis Lua inventory, Order service, Kafka producer
│   └── main.py                 # FastAPI entrypoint, tracing middleware, /health, /metrics
├── streaming/
│   ├── audit_consumer.py       # Kafka consumer: velocity fraud check, DLQ + MinIO Bronze/Quarantine
│   ├── transformations.py      # PySpark schema enforcement, quarantine split, PII masking, windows
│   ├── spark_stream_orders.py  # PySpark Structured Streaming job (Kafka -> Silver + Quarantine)
│   └── star_schema.sql         # Indexed PostgreSQL Kimball Star Schema DDL (SCD Type 2)
├── lakehouse/
│   └── batch_gold_aggregation.py # MinIO Silver/Bronze -> Gold Star Schema ETL + DQ Audit
├── dashboard/
│   └── streamlit_app.py        # Real-time OLTP, Lakehouse, Quarantine/DLQ & DQ SLA monitor
├── scripts/
│   └── benchmark_compare.py    # Automated Before/After Locust benchmark & Lakehouse E2E verifier
├── tests/
│   ├── test_backend.py         # Observability, query APIs, Idempotency, Rollback, Lazy DB & CHECK tests
│   ├── test_streaming_and_gold.py # PySpark quarantine/PII, DLQ validation, unified fraud & SCD2 tests
│   └── locustfile.py           # High-concurrency Locust load testing suite
├── docker-compose.yml          # Isolated Postgres (5433), Redis (6380), Kafka (9094), MinIO (9000)
├── Dockerfile                  # Non-root production FastAPI container with HEALTHCHECK
├── pyproject.toml              # Pytest & Ruff configuration
└── requirements.txt
```

---

## 5. Quickstart Guide

### Step 1: Configure Environment & Start Infrastructure
```powershell
Copy-Item .env.example .env
docker compose up -d
```
> **Port Isolation:** Services run on `5433` (Postgres), `6380` (Redis), `9094` (Kafka), and `9000/9001` (MinIO) inside the dedicated `flashlogix_net` bridge network.

### Step 2: Run Automated Linter & Test Suite (12/12 Tests)
```powershell
.\.venv\Scripts\python.exe -m ruff check app streaming lakehouse scripts tests
.\.venv\Scripts\python.exe -m pytest tests/ -v
```

### Step 3: Start Backend API & Inspect Observability
```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```
* Swagger UI: `http://localhost:8000/docs`
* Health Probe: `http://localhost:8000/health`
* Telemetry Metrics: `http://localhost:8000/metrics` and `http://localhost:8000/metrics?format=prometheus`

### Step 4: Run Automated Before-vs-After Benchmark & E2E Lakehouse Verification
```powershell
.\.venv\Scripts\python.exe scripts/benchmark_compare.py --reset-clean --verify-lakehouse
```

### Step 5: Run Streaming Consumers, Gold Star Schema ETL & Dashboard
```powershell
# Kafka Audit, DLQ & Bronze Parquet Consumer
.\.venv\Scripts\python.exe -m streaming.audit_consumer

# PySpark Structured Streaming (Kafka -> Silver + Quarantine Parquet on MinIO)
.\.venv\Scripts\python.exe -m streaming.spark_stream_orders --starting-offsets earliest

# Gold Layer Kimball Star Schema, SCD Type 2 & Data Quality Audit
.\.venv\Scripts\python.exe -m lakehouse.batch_gold_aggregation

# Real-Time Monitoring & Data Quality Dashboard
.\.venv\Scripts\python.exe -m streamlit run dashboard/streamlit_app.py
```
