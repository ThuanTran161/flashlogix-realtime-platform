# ⚡ FlashLogix — High-Concurrency Transaction Engine & Real-Time Lakehouse Platform

> **An end-to-end intersection of High-Concurrency Backend Engineering (OLTP) and Event-Driven Data Engineering (OLAP).**  
> Built with **FastAPI, Redis (Lua Atomic), PostgreSQL, Apache Kafka (KRaft), PySpark Structured Streaming, MinIO Lakehouse, Kimball Star Schema (SCD Type 2), Locust, and Streamlit.**

---

## 1. System Architecture

```text
[ Locust Load Test (300+ Concurrent Users) ]
                   │
                   ▼
       [ FastAPI Async API ] ──(Idempotency-Key Gate)
          │           │
          │           ├──► 1. Redis In-Memory Lua Script (Atomic DECRBY, Zero Overselling)
          │           │
          │           ├──► 2. PostgreSQL OLTP (SELECT ... FOR UPDATE + Compensatory Rollback)
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
                                                      ├── silver/orders/ (PII-Masked Parquet)
                                                      └── gold/          (Curated Snapshots)
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
* **Zero Overselling Mandate:** Inventory checks and deductions execute atomically inside Redis via a custom Lua script (`LUA_ATOMIC_DECREMENT` in `app/services/inventory_service.py`), eliminating check-then-act race conditions.
* **Compensatory Rollback:** If PostgreSQL fails after Redis deduction, `OrderService` automatically executes a compensating `INCRBY` on Redis to guarantee cache-database consistency.
* **Idempotency & Non-Blocking I/O:** Supports `Idempotency-Key` headers to prevent duplicate charges on retries, executes sync SQLAlchemy/Redis calls inside `run_in_threadpool`, and offloads Kafka publishing to `BackgroundTasks`.

### Data Engineering & Lakehouse (OLAP)
* **Idempotent Event Streaming:** `EventPublisher` produces strict-schema JSON events (`event_id`, `event_type`, `timestamp` UTC) with `acks="all"`, `enable.idempotence=True`, and `gzip` compression.
* **PySpark Structured Streaming & Governance:** Enforces explicit `StructType` schemas, hashes customer PII (`user_id` $\rightarrow$ `masked_user_id`) using salted SHA-256, deduplicates by `order_id`, and computes 1-minute tumbling window aggregations with watermarking.
* **Kimball Star Schema & SCD Type 2:** `lakehouse/batch_gold_aggregation.py` populates `Dim_Date`, `Dim_Products`, `Fact_Flash_Sale_Orders`, and tracks customer tier transitions in `Dim_Users` using Slowly Changing Dimension Type 2 (`effective_from`, `effective_to`, `is_current`).

---

## 3. Verified Load Test & E2E Data Parity Results

Executed locally against Docker Compose (`PostgreSQL 15`, `Redis 7.2`, `Apache Kafka 4.3.1`, `MinIO`) with **Product #1 initial stock = 100** (`$199.99`):

| Metric | Measured Result |
| :--- | :--- |
| **Locust Concurrency** | 300 Virtual Users (spawn rate 100/s, 10s burst) |
| **Total Requests Processed** | **2,829 requests** (`257.99 req/s`, `min = 8 ms`) |
| **Unhandled Failures** | **0 (0.00%)** |
| **OLTP Confirmed Orders (`orders`)** | **100** (exact match to stock — **0 oversold**) |
| **Final Stock (PostgreSQL & Redis)** | **DB = 0**, **Redis = 0** |
| **Gold Star Schema (`Fact_Flash_Sale_Orders`)** | **100 facts**, **$19,999.00** verified revenue |

---

## 4. Repository Structure

```text
Flashlogix_Platform/
├── app/
│   ├── api/v1/                 # FastAPI async routes (orders, products)
│   ├── core/                   # Pydantic Settings, SQLAlchemy pooling, MinIO S3 helper
│   ├── models/                 # SQLAlchemy ORM models (Product, Order)
│   ├── schemas/                # Pydantic v2 validation & error envelopes
│   ├── services/               # Redis Lua inventory, Order transaction, Kafka producer
│   └── main.py                 # FastAPI entrypoint, lifespan, global exception handlers
├── streaming/
│   ├── audit_consumer.py       # Kafka consumer: bot velocity check & Bronze Parquet sink
│   ├── transformations.py      # PySpark schema parsing, PII masking, window aggregations
│   ├── spark_stream_orders.py  # PySpark Structured Streaming job (Kafka -> Silver MinIO)
│   └── star_schema.sql         # PostgreSQL Kimball Star Schema DDL (SCD Type 2)
├── lakehouse/
│   └── batch_gold_aggregation.py # MinIO Bronze/Silver -> Gold Star Schema ETL
├── dashboard/
│   └── streamlit_app.py        # Real-time OLTP & Lakehouse telemetry dashboard
├── tests/
│   ├── test_backend.py         # Unit & integration tests (Idempotency, Rollback, Lua)
│   ├── test_streaming_and_gold.py # PySpark transformations & SCD Type 2 DB tests
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

### Step 4: Run Locust Load Test
```powershell
.\.venv\Scripts\python.exe -m locust -f tests/locustfile.py --host http://127.0.0.1:8000
```
Open Locust UI at `http://localhost:8089`.

### Step 5: Run Streaming Consumers & Gold Star Schema ETL
```powershell
# Kafka Audit & Bronze Parquet Consumer
.\.venv\Scripts\python.exe -m streaming.audit_consumer

# PySpark Structured Streaming (Kafka -> Silver Parquet on MinIO)
.\.venv\Scripts\python.exe -m streaming.spark_stream_orders --starting-offsets earliest

# Gold Layer Kimball Star Schema & SCD Type 2 ETL
.\.venv\Scripts\python.exe -m lakehouse.batch_gold_aggregation
```

### Step 6: Launch Real-Time Monitoring Dashboard
```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard/streamlit_app.py
```
