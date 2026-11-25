# ⚡ FlashLogix — High-Concurrency Transaction Engine & Real-Time Lakehouse Platform

> **An end-to-end intersection of High-Concurrency Backend Engineering (OLTP) and Event-Driven Data Engineering (OLAP).**  
> Built with **FastAPI, Async Redis (Lua Atomic + Read-Through Cache), PostgreSQL, Apache Kafka (KRaft), PySpark Structured Streaming, MinIO Lakehouse, Kimball Star Schema (SCD Type 2), Locust, and Streamlit.**

---

## 1. System Architecture

```text
[ Locust Load Test (300+ Concurrent Users) ]
                   │
                   ▼
       [ FastAPI Async API ] ──► 1. Async Redis Gate (redis.asyncio, Zero DB Pool Contention)
          │           │              ├── Idempotency-Key Cache (idem:order:{key})
          │           │              ├── Product Read-Through Cache (MGET product:meta + stock)
          │           │              └── Lua Atomic DECRBY (Zero Overselling; Rejects in RAM)
          │           │
          │           ├──► 2. Lazy PostgreSQL OLTP (SessionLocal opened ONLY for Lua winners)
          │           │       (SELECT ... FOR UPDATE + Compensatory Redis INCRBY Rollback)
          │           │
          │           └──► 3. FastAPI BackgroundTasks ──► [ Apache Kafka 4.x (KRaft) ]
                                                                   │
                                        ┌──────────────────────────┴──────────────────────────┐
                                        ▼                                                     ▼
                        [ Audit & Fraud Consumer ]                        [ PySpark Structured Streaming ]
                        (Velocity Bot Check + Bronze)                     (Explicit Schema + SHA-256 PII Mask)
                                        │                                                     │
                                        └──────────────────────────┬──────────────────────────┘
                                                                   ▼
                                                      [ MinIO S3 Medallion Lakehouse ]
                                                      ├── bronze/orders/ (Raw Parquet)
                                                      ├── silver/orders/ (PII-Masked, Deduped Parquet)
                                                      └── gold/          (Summary Parquet & DQ Audit JSON)
                                                                   │
                                                                   ▼
                                                 [ PostgreSQL Kimball Star Schema ]
                                                 ├── Dim_Users (SCD Type 2 History)
                                                 ├── Dim_Products / Dim_Date
                                                 └── Fact_Flash_Sale_Orders
                                                                   │
                                                                   ▼
                                                    [ Streamlit Command Center ]
```

---

## 2. Core Engineering Highlights

### Backend & Concurrency (OLTP)
* **Non-Blocking Async Redis Gate & Lazy DB Acquisition:** `OrderService` executes `Idempotency-Key` lookups and `LUA_ATOMIC_DECREMENT` (`app/services/inventory_service.py`) asynchronously via `redis.asyncio` (`BlockingConnectionPool`) *before* acquiring a PostgreSQL `Session`. During flash-sale spikes where >95% of requests fail stock checks or hit cache, PostgreSQL connection pool contention is completely bypassed.
* **Redis Read-Through Cache:** `GET /api/v1/products/{product_id}` retrieves product metadata and live RAM stock in a single `MGET` round-trip, avoiding relational DB reads on hot product pages.
* **Zero Overselling & Compensatory Rollback:** If PostgreSQL fails after Redis deduction, `OrderService` automatically executes a compensating `INCRBY` on Redis to guarantee cache-database consistency.

### Data Engineering & Lakehouse (OLAP)
* **Idempotent Event Streaming:** `EventPublisher` produces strict-schema JSON events (`event_id`, `event_type`, `timestamp` UTC) with `acks="all"`, `enable.idempotence=True`, and `gzip` compression.
* **PySpark Structured Streaming & Governance:** Enforces explicit `StructType` schemas, hashes customer PII (`user_id` -> `masked_user_id`) using salted SHA-256, deduplicates by `order_id`, and computes 1-minute tumbling window aggregations with watermarking.
* **Kimball Star Schema & Automated Data Quality Audit:** `lakehouse/batch_gold_aggregation.py` populates `Dim_Date`, `Dim_Products`, `Fact_Flash_Sale_Orders`, tracks customer tier transitions in `Dim_Users` via SCD Type 2 (`effective_from`, `effective_to`, `is_current`), and exports a reconciliation & PII audit report (`gold/data_quality_report/latest.json`).

---

## 3. Verified Benchmarks & Data Quality SLA Metrics

All metrics below were measured locally via `scripts/benchmark_compare.py` against Docker Compose (`PostgreSQL 15`, `Redis 7.2`, `Apache Kafka 4.3.1`, `MinIO`) under identical load (**300 Concurrent VUs, spawn rate 100/s, 10s burst, 4 Uvicorn workers, Product #1 stock = 100 @ \$199.99**).

### A. OLTP Performance: Before vs. After Optimization

| Metric | Phase 0: PostgreSQL `SELECT ... FOR UPDATE` (DB-Only) | Phase 1: Sync Redis + Eager `Depends(get_db)` | Phase 2: Async Redis Gate + Lazy DB + Read Cache | Delta (Phase 2 vs. Phase 0) |
| :--- | :--- | :--- | :--- | :--- |
| **Total Requests (10s)** | 2,889 | 2,829 | **3,723** | **+28.9%** |
| **Throughput (RPS)** | 266.73 req/s | 257.99 req/s | **382.94 req/s** | **+43.6%** (`+48.4%` vs. Phase 1) |
| **Average Latency** | 619.49 ms | 572.47 ms (OLTP) | **382.02 ms** | **-38.3%** |
| **Median Latency (P50)** | 480 ms | 380 ms | **270 ms** | **-43.8%** |
| **Tail Latency (P95)** | 1,300 ms | 1,500 ms | **990 ms** | **-23.8%** (`-34.0%` vs. Phase 1) |
| **Tail Latency (P99)** | 4,400 ms | 2,000 ms | **2,200 ms** | **-50.0%** |
| **Min / Max Latency** | 7.39 ms / 6,462.69 ms | 8.00 ms / — | **2.84 ms / 4,709.70 ms** | **-61.6% Min** |
| **Oversold Units** | 0 (`100 / 100`) | 0 (`100 / 100`) | **0 (`100 / 100`)** | **Zero Overselling** |
| **DB Sessions Saved** | 0% (100% hit DB) | 0% (eager `Depends`) | **~97.3%** (`3,623 / 3,723` served/rejected in RAM) | **Pool contention eliminated** |

> *Note: Under isolated 10s load, Phase 2 achieved `330.76 req/s` across `3,480` requests with `0 (0.00%)` failures (`P95 = 1,100 ms`); under back-to-back multi-worker bursts at `382.94 req/s`, `1.13%` transient Windows loopback socket resets occurred on client ramp.*

### B. Data Engineering & Medallion Lakehouse SLA

Verified end-to-end across both benchmark waves (`200` confirmed orders) plus **20 deliberate duplicate Kafka events injected** to validate stream deduplication:

| Pipeline SLA Metric | Measured Result |
| :--- | :--- |
| **Bronze Raw Events Archived (`OrderAuditConsumer`)** | **220** events (`200` unique + `20` injected duplicates) |
| **Duplicate Noise Filtered (Bronze -> Silver/Gold)** | **20 / 220 (9.09% filtered)** -> **200 unique orders** |
| **OLTP ↔ Gold Star Schema Order Parity** | **200 / 200 (100.00% Parity)** |
| **OLTP ↔ Gold Revenue Reconciliation Delta** | **\$39,998.00 vs. \$39,998.00 (\$0.00 Delta)** |
| **PII Compliance Violations** | **0 violations** (`user_id` dropped; 100% salted 64-hex SHA-256 `masked_user_id`) |
| **Gold Star Schema Batch ETL Runtime** | **1.585 s** (PySpark `--available-now` micro-batch: `73.36 s` incl. JVM startup) |

---

## 4. Repository Structure

```text
Flashlogix_Platform/
├── app/
│   ├── api/v1/                 # FastAPI async routes (orders, products with Redis MGET cache)
│   ├── core/                   # Pydantic Settings, SQLAlchemy pooling, MinIO S3 helper
│   ├── models/                 # SQLAlchemy ORM models (Product, Order)
│   ├── schemas/                # Pydantic v2 validation & error envelopes
│   ├── services/               # Async/Sync Redis Lua inventory, Order service, Kafka producer
│   └── main.py                 # FastAPI entrypoint, lifespan, Loguru config, exception handlers
├── streaming/
│   ├── audit_consumer.py       # Kafka consumer: bot velocity check & Bronze Parquet sink
│   ├── transformations.py      # PySpark schema parsing, PII masking, window aggregations
│   ├── spark_stream_orders.py  # PySpark Structured Streaming job (Kafka -> Silver MinIO)
│   └── star_schema.sql         # PostgreSQL Kimball Star Schema DDL (SCD Type 2)
├── lakehouse/
│   └── batch_gold_aggregation.py # MinIO Silver/Bronze -> Gold Star Schema ETL + DQ Audit
├── dashboard/
│   └── streamlit_app.py        # Real-time OLTP, Lakehouse & Data Quality SLA monitor
├── scripts/
│   └── benchmark_compare.py    # Automated Before/After Locust benchmark & Lakehouse E2E verifier
├── tests/
│   ├── test_backend.py         # Unit & integration tests (Idempotency, Rollback, Lazy DB skip)
│   ├── test_streaming_and_gold.py # PySpark transformations, PII audit & SCD Type 2 tests
│   └── locustfile.py           # High-concurrency Locust load testing suite
├── docker-compose.yml          # Isolated Postgres (5433), Redis (6380), Kafka (9094), MinIO (9000)
├── Dockerfile                  # Production FastAPI container definition
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

### Step 2: Run Automated Test Suite
```powershell
.\.venv\Scripts\python.exe -m pytest tests/ -v
```

### Step 3: Start Backend API & Seed Product
```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```
Open Swagger UI at `http://localhost:8000/docs` or verify health at `http://localhost:8000/health`.

### Step 4: Run Automated Before-vs-After Benchmark & E2E Lakehouse Verification
```powershell
.\.venv\Scripts\python.exe scripts/benchmark_compare.py --reset-clean --verify-lakehouse
```

### Step 5: Run Streaming Consumers, Gold Star Schema ETL & Dashboard
```powershell
# Kafka Audit & Bronze Parquet Consumer
.\.venv\Scripts\python.exe -m streaming.audit_consumer

# PySpark Structured Streaming (Kafka -> Silver Parquet on MinIO)
.\.venv\Scripts\python.exe -m streaming.spark_stream_orders --starting-offsets earliest

# Gold Layer Kimball Star Schema, SCD Type 2 & Data Quality Audit
.\.venv\Scripts\python.exe -m lakehouse.batch_gold_aggregation

# Real-Time Monitoring & Data Quality Dashboard
.\.venv\Scripts\python.exe -m streamlit run dashboard/streamlit_app.py
```
