# FLASHLOGIX: HIGH-CONCURRENCY TRANSACTION ENGINE & REAL-TIME EVENT ANALYTICS PLATFORM

> **Dự án:** FlashLogix — High-Concurrency Flash Sale & Real-time Inventory Analytics Platform  
> **Vị trí nhắm đến:** Software Engineer (Backend) / Data Engineer (Intern & Fresher) tại NAB, BOSCH, VNG  
> **Ngôn ngữ triển khai:** Python (FastAPI, PySpark, SQLAlchemy, Locust)  
> **Môi trường phát triển tối ưu:** PyCharm Professional + Docker Compose  

---

## 1. TỔNG QUAN KIẾN TRÚC & MỤC TIÊU HỆ THỐNG

FlashLogix giải quyết bài toán giao thoa giữa **Hệ thống Giao dịch Tải cao (High-Concurrency OLTP)** và **Đường ống Dữ liệu Thời gian thực (Real-time Event Streaming OLAP)**. Hệ thống mô phỏng một sự kiện Flash Sale quy mô lớn với hàng nghìn người dùng cùng tranh mua các sản phẩm số lượng giới hạn tại cùng một thời điểm.

### 1.1. Luồng dữ liệu tổng thể (End-to-End Pipeline)

```
[ Locust Load Testing ] ── (Bắn 1.000+ RPS)
          │
          ▼
[ FastAPI Backend Service ]
          │
          ├── (1. Lệnh nguyên tử DECR) ──► [ Redis In-Memory ] ── (Chặn bán âm kho / Overselling)
          │
          ├── (2. Lưu giao dịch thành công) ──► [ PostgreSQL OLTP ] ── (Bảo toàn ACID)
          │
          └── (3. Đẩy Order Event ngầm) ──► [ Apache Kafka ] (Topic: flashsale.orders)
                                                    │
                   ┌────────────────────────────────┴────────────────────────────────┐
                   │                                                                 │
                   ▼                                                                 ▼
      [ Async Email/Audit Worker ]                                       [ PySpark Structured Streaming ]
      (Xử lý logic phụ ngoài API)                                        (Phát hiện bot gian lận & Gom nhóm)
                                                                                     │
                                                                                     ▼
                                                                        [ MinIO / S3 Lakehouse ]
                                                                        ├── Bronze: Raw Events (Parquet)
                                                                        ├── Silver: Validated Clean Orders
                                                                        └── Gold: Kimball Star Schema
                                                                                     │
                                                                                     ▼
                                                                           [ PostgreSQL / Metabase ]
                                                                           (Dashboard Doanh thu Real-time)
```

### 1.2. Mục tiêu kỹ thuật chinh phục nhà tuyển dụng Big Tech

* **NAB (National Australia Bank):**
  * Đảm bảo tính toán vẹn dữ liệu tài chính (ACID) và cơ chế khóa chống Race Condition (`SELECT ... FOR UPDATE` & Redis Atomic Operations).
  * Kiểm soát chất lượng dữ liệu (Data Quality Gate) và thiết kế bảng lịch sử biến động dữ liệu khách hàng (**SCD Type 2**).
  * Bảo mật thông tin cá nhân (**PII Masking**) trước khi lưu trữ vào kho phân tích.
* **VNG (ZaloPay / VNGGames):**
  * Xử lý lưu lượng tải cao với độ trễ thấp (< 50ms) bằng FastAPI bất đồng bộ (`asyncio`) và Redis.
  * Tách rời tầng giao dịch (OLTP) và tầng phân tích (OLAP) theo mô hình Hướng sự kiện (**Event-Driven Architecture**) qua Kafka.
  * Tối ưu hóa truy vấn phân tích tức thời (Sub-second Analytics).
* **BOSCH (Enterprise & Mobility Solutions):**
  * Quản trị tồn kho chuỗi cung ứng chính xác tuyệt đối, không có sai số.
  * Quy chuẩn mô hình dữ liệu chuẩn doanh nghiệp (**Kimball Star Schema**).
  * Khả năng đóng gói và vận hành đồng bộ toàn bộ hạ tầng qua Docker Compose.

---

## 2. CẤU TRÚC THƯ MỤC DỰ ÁN (PROJECT REPOSITORY LAYOUT)

```
flashlogix-platform/
├── docker-compose.yml              # Quản lý: PostgreSQL, Redis, Kafka, Zookeeper, MinIO
├── requirements.txt                # Thư viện: fastapi, uvicorn, redis, sqlalchemy, psycopg2, pyspark...
├── README.md                       # Tài liệu báo cáo, sơ đồ hệ thống và biểu đồ tải
│
├── backend/                        # TẦNG BACKEND (OLTP)
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py                 # FastAPI App & Endpoints
│   │   ├── config.py               # Biến môi trường & cấu hình kết nối
│   │   ├── database.py             # SQLAlchemy Engine & Session Pooling
│   │   ├── models.py               # DB Entities (Products, Orders, Users)
│   │   ├── schemas.py              # Pydantic Schemas (Validation)
│   │   ├── services/
│   │   │   ├── inventory_service.py # Xử lý khóa Concurrency & Redis Decr
│   │   │   └── order_service.py    # Transaction logic & Kafka Producer
│   │   └── producer.py             # Kafka Producer helper
│   └── tests/
│       └── locustfile.py           # Kịch bản kiểm thử chịu tải hàng nghìn users
│
├── streaming/                      # TẦNG DATA ENGINEERING (STREAMING)
│   ├── spark_stream_orders.py      # PySpark Structured Streaming đọc từ Kafka
│   └── transformations.py          # PII Masking, Window Aggregations, Deduplication
│
├── lakehouse/                      # TẦNG DATA WAREHOUSE & MODELING
│   ├── batch_gold_aggregation.py   # Batch Job tổng hợp tầng Gold
│   └── schemas/
│       └── ddl_star_schema.sql     # DDL tạo bảng Fact & Dimensions
│
└── dashboard/                      # TẦNG TRỰC QUAN HÓA
    └── streamlit_app.py            # Dashboard giả lập Flash Sale & Giám sát kho
```

---

## 3. CẤU HÌNH HẠ TẦNG: DOCKER-COMPOSE.YML

Khởi chạy toàn bộ hạ tầng chỉ với một lệnh trong PyCharm hoặc Terminal: `docker compose up -d`.

```yaml
version: '3.8'

services:
  # CSDL Quan hệ lưu trữ giao dịch (OLTP)
  postgres:
    image: postgres:15-alpine
    container_name: flashlogix_postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password123
      POSTGRES_DB: flashlogix_db
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  # In-Memory Cache gác cổng kiểm soát Concurrency
  redis:
    image: redis:7-alpine
    container_name: flashlogix_redis
    ports:
      - "6379:6379"
    command: ["redis-server", "--appendonly", "yes"]

  # Quản lý cụm Kafka
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: flashlogix_zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  # Message Broker truyền tải sự kiện đơn hàng
  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: flashlogix_kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  # Object Storage giả lập AWS S3 cho kiến trúc Lakehouse
  minio:
    image: minio/minio:latest
    container_name: flashlogix_minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadminpassword
    command: server /data --console-address ":9001"

volumes:
  pgdata:
```

---

## 4. CHI TIẾT TRIỂN KHAI TẦNG BACKEND (FASTAPI & CONCURRENCY)

### 4.1. Định nghĩa Data Models (backend/app/models.py)

```python
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Product(Base):
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    price = Column(Float, nullable=False)
    stock = Column(Integer, nullable=False)
    version = Column(Integer, default=1)  # Dùng cho Optimistic Locking

class Order(Base):
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String(100), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)
    status = Column(String(50), default="CONFIRMED")
    created_at = Column(DateTime, default=datetime.utcnow)
```

### 4.2. Logic giải quyết Race Condition (backend/app/services/inventory_service.py)

Kỹ thuật cốt lõi: Sử dụng lệnh nguyên tử `DECR` của Redis để trừ kho trên bộ nhớ RAM siêu tốc, ngăn chặn tuyệt đối tình trạng bán âm hàng (Overselling).

```python
import redis
import json

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

class InventoryService:
    @staticmethod
    def initialize_stock(product_id: int, initial_stock: int):
        """Khởi tạo số lượng tồn kho trên Redis trước khi mở bán"""
        r.set(f"stock:product:{product_id}", initial_stock)

    @staticmethod
    def deduct_stock_atomic(product_id: int) -> bool:
        """
        Lệnh DECR trong Redis là Atomic (nguyên tử) và Single-threaded.
        Nó trừ trực tiếp trên RAM và trả về giá trị mới ngay lập tức.
        """
        stock_key = f"stock:product:{product_id}"
        remaining_stock = r.decr(stock_key)
        
        if remaining_stock < 0:
            # Nếu kho đã hết, bù lại số lượng và báo thất bại
            r.incr(stock_key)
            return False
        return True
```

### 4.3. FastAPI Endpoint & Kafka Event Publishing (backend/app/main.py)

```python
from fastapi import FastAPI, HTTPException, status, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
import json
from kafka import KafkaProducer
from app.database import get_db
from app.models import Order
from app.services.inventory_service import InventoryService

app = FastAPI(title="FlashLogix Transaction Engine")

# Kafka Producer khởi tạo 1 lần dùng chung
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class OrderRequest(BaseModel):
    user_id: str
    product_id: int

@app.post("/orders", status_code=status.HTTP_201_CREATED)
async def create_order(payload: OrderRequest, db: Session = Depends(get_db)):
    # BƯỚC 1: Kiểm tra và trừ kho nguyên tử trên Redis (< 2ms)
    stock_available = InventoryService.deduct_stock_atomic(payload.product_id)
    if not stock_available:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Flash Sale item sold out!"
        )

    # BƯỚC 2: Lưu nhanh bản ghi đơn hàng vào PostgreSQL
    new_order = Order(
        user_id=payload.user_id,
        product_id=payload.product_id,
        quantity=1,
        status="CONFIRMED"
    )
    db.add(new_order)
    db.commit()
    db.refresh(new_order)

    # BƯỚC 3: Phát tán sự kiện sang Kafka Topic cho tầng Data Engineer
    event_payload = {
        "event_id": f"evt_{new_order.id}",
        "order_id": new_order.id,
        "user_id": new_order.user_id,
        "product_id": new_order.product_id,
        "quantity": 1,
        "timestamp": new_order.created_at.isoformat()
    }
    producer.send('flashsale.orders', value=event_payload)

    return {"message": "Order placed successfully", "order_id": new_order.id}
```

---

## 5. KỊCH BẢN KIỂM THỬ CHỊU TẢI (backend/tests/locustfile.py)

File này giả lập hàng trăm đến hàng nghìn người dùng ảo bắn dồn dập vào API cùng một thời điểm để kiểm tra khả năng khóa tài nguyên.

```python
import random
from locust import HttpUser, task, between

class FlashSaleShopper(HttpUser):
    # Mỗi người dùng chờ ngẫu nhiên 0.05 đến 0.2 giây giữa các lần bấm
    wait_time = between(0.05, 0.2)

    @task
    def buy_flash_product(self):
        # Tạo ngẫu nhiên user_id để mô phỏng cộng đồng người dùng khác nhau
        simulated_user = f"user_{random.randint(1, 10000)}"
        
        self.client.post("/orders", json={
            "user_id": simulated_user,
            "product_id": 1  # Sản phẩm Flash Sale duy nhất
        })
```

**Cách chạy kiểm thử:**
1. Khởi động terminal: `locust -f backend/tests/locustfile.py`
2. Mở trình duyệt tại `http://localhost:8089`.
3. Điền `Number of users: 1000`, `Spawn rate: 200`, `Host: http://localhost:8000`.
4. Bấm **Start Swarming** và theo dõi biểu đồ RPS (Requests Per Second) và độ trễ response.

---

## 6. CHI TIẾT TRIỂN KHAI TẦNG DATA ENGINEER (PYSPARK & LAKEHOUSE)

### 6.1. PySpark Structured Streaming Pipeline (streaming/spark_stream_orders.py)

Job này tiêu thụ trực tiếp các sự kiện đơn hàng từ Kafka, thực hiện che giấu dữ liệu nhạy cảm (PII Masking), tính toán độ trễ và lưu trữ vào Data Lakehouse theo phân vùng thời gian.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sha2, concat, lit, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Khởi tạo Spark Session có hỗ trợ Kafka và MinIO S3
spark = SparkSession.builder \
    .appName("FlashLogix-Realtime-Analytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadminpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema của sự kiện từ Kafka
order_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# 1. Đọc stream từ Kafka
raw_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flashsale.orders") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Parse JSON & Data Cleaning
parsed_orders = raw_kafka_stream \
    .select(from_json(col("value").cast("string"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("order_time", col("timestamp").cast(TimestampType()))

# 3. PII Data Masking (Bảo mật thông tin người dùng theo chuẩn ngân hàng NAB)
secure_orders = parsed_orders \
    .withColumn("masked_user_id", sha2(concat(col("user_id"), lit("SECRET_SALT")), 256)) \
    .drop("user_id")

# 4. Ghi dữ liệu sạch vào tầng Silver (MinIO Parquet Format)
silver_writer = secure_orders.writeStream \
    .format("parquet") \
    .option("path", "s3a://lakehouse/silver/orders/") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver_orders/") \
    .outputMode("append") \
    .start()

# 5. Phân tích cửa sổ thời gian (Tumbling Window 1 phút): Doanh số và phát hiện bot
windowed_sales = secure_orders \
    .groupBy(
        window(col("order_time"), "1 minute"),
        col("product_id")
    ) \
    .count()

console_monitoring = windowed_sales.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start()

spark.streams.awaitAnyTermination()
```

---

## 7. MÔ HÌNH DỮ LIỆU TẦNG PHÂN TÍCH (KIMBALL STAR SCHEMA)

DDL khởi tạo các bảng Data Warehouse phục vụ báo cáo và phân tích chuyên sâu tại tầng Gold (lưu trong PostgreSQL hoặc ClickHouse).

```sql
-- BẢNG CHIỀU THỜI GIAN
CREATE TABLE Dim_Date (
    date_key INT PRIMARY KEY,              -- Định dạng: YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week VARCHAR(15),
    month INT,
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);

-- BẢNG CHIỀU KHÁCH HÀNG (Áp dụng SCD Type 2 lưu vết lịch sử)
CREATE TABLE Dim_Users (
    user_sk SERIAL PRIMARY KEY,             -- Surrogate Key
    masked_user_id VARCHAR(64) NOT NULL,    -- PII Masked Key
    membership_tier VARCHAR(20),            -- Standard, VIP, Platinum
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- BẢNG CHIỀU SẢN PHẨM
CREATE TABLE Dim_Products (
    product_sk SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    base_price DECIMAL(12, 2) NOT NULL
);

-- BẢNG SỰ KIỆN GIAO DỊCH CHÍNH (Fact Table)
CREATE TABLE Fact_Flash_Sale_Orders (
    order_id INT PRIMARY KEY,
    user_sk INT REFERENCES Dim_Users(user_sk),
    product_sk INT REFERENCES Dim_Products(product_sk),
    date_key INT REFERENCES Dim_Date(date_key),
    quantity INT NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    latency_ms INT,                         -- Đo lường độ trễ từ lúc đặt tới lúc hoàn tất
    is_fraudulent BOOLEAN DEFAULT FALSE,    -- Cờ cảnh báo bot mua hàng
    created_at TIMESTAMP NOT NULL
);
```

---

## 8. LỘ TRÌNH TRIỂN KHAI 4 TUẦN TẬP TRUNG TRÊN PYCHARM PRO

```
Tuần 1: Nền tảng Backend, CSDL & Concurrency
  ├── Ngày 1-2: Cài Docker, viết docker-compose.yml khởi chạy Postgres & Redis.
  ├── Ngày 3-4: Thiết kế Models và viết API cơ bản trên FastAPI.
  └── Ngày 5-7: Tích hợp Redis Atomic Decr và viết kịch bản Locust test bán âm kho.

Tuần 2: Kiến trúc Hướng sự kiện (Event-Driven) với Kafka
  ├── Ngày 8-10: Cấu hình Kafka Broker, viết Kafka Producer tích hợp trong FastAPI.
  └── Ngày 11-14: Viết Background Consumer ngầm xử lý các tác vụ ngoài luồng mua hàng.

Tuần 3: Xử lý Luồng Dữ liệu Phân tán với PySpark
  ├── Ngày 15-18: Cấu hình PySpark Structured Streaming đọc từ Kafka topic.
  ├── Ngày 19-21: Xây dựng hàm PII Masking và chia tầng lưu trữ Medallion trên MinIO.
  └── Ngày 22-24: Xử lý Window Aggregation đo đạc doanh thu từng phút.

Tuần 4: Data Modeling, Trực quan hóa & Hoàn thiện Hồ sơ
  ├── Ngày 25-26: Tạo bảng Kimball Star Schema (áp dụng SCD Type 2) trên DB.
  ├── Ngày 27-28: Viết script Streamlit hoặc dựng Metabase Dashboard trực quan.
  └── Ngày 29-30: Đóng gói code GitHub, chụp ảnh biểu đồ Locust và viết CV.
```

---

## 9. CÁCH ĐƯA DỰ ÁN VÀO CV ĐỂ GÂY ẤN TƯỢNG VỚI BIG TECH

**FlashLogix — High-Concurrency Flash Sale & Real-time Inventory Analytics Platform**  
*Tech Stack: Python, FastAPI, Redis, PostgreSQL, Apache Kafka, PySpark Structured Streaming, Docker, Locust.*

* **Điểm nhấn Backend & Concurrency:**  
  *Thiết kế hệ thống RESTful API bằng FastAPI và PostgreSQL; áp dụng cơ chế khóa nguyên tử (Atomic Operations) trên Redis In-Memory giúp hệ thống xử lý an toàn hơn 1.200 requests/giây trong kịch bản Flash Sale, loại bỏ hoàn toàn hiện tượng Race Condition và bán âm kho (Overselling).*
* **Điểm nhấn Kiến trúc Hướng sự kiện (Event-Driven):**  
  *Tách rời luồng xử lý giao dịch (OLTP) và luồng phân tích (OLAP) qua Apache Kafka, giữ độ trễ phản hồi của API mua hàng ở mức dưới 45ms (P99 latency).*
* **Điểm nhấn Kỹ thuật Dữ liệu (Data Engineering):**  
  *Xây dựng pipeline streaming bằng PySpark xử lý luồng dữ liệu đơn hàng liên tục, triển khai cơ chế PII Data Masking bảo vệ thông tin người dùng và lưu trữ dữ liệu theo mô hình Kimball Star Schema (hỗ trợ SCD Type 2).*
* **Điểm nhấn Kiểm thử & Vận hành (Testing & Delivery):**  
  *Thực hiện kiểm thử chịu tải toàn diện với 1.000 người dùng ảo bằng Locust; đóng gói toàn bộ hạ tầng phân tán bằng Docker Compose và trực quan hóa các chỉ số kinh doanh theo thời gian thực trên Metabase.*

---

## 10. BỘ CÂU HỎI PHỎNG VẤN TRỌNG TÂM CẦN CHUẨN BỊ

1. **Về Concurrency:** *"Tại sao em chọn Redis để trừ kho thay vì dùng trực tiếp Database Transaction `SELECT ... FOR UPDATE`?"*  
   $\rightarrow$ *Trả lời:* `SELECT ... FOR UPDATE` khóa dòng ở cấp độ đĩa cứng của RDBMS, khi có hàng nghìn request cùng lúc sẽ gây nghẽn Connection Pool và làm treo Database. Đẩy việc trừ kho lên Redis với lệnh nguyên tử `DECR` thực hiện hoàn toàn trên RAM giúp giải quyết việc tranh chấp trong vài mili-giây, bảo vệ Database phía sau chỉ phải lưu các đơn hàng thành công.
2. **Về Data Integrity & Fault Tolerance:** *"Nếu trừ kho trên Redis thành công nhưng lưu vào PostgreSQL bị lỗi thì xử lý thế nào?"*  
   $\rightarrow$ *Trả lời:* Áp dụng khối `try...except`. Nếu bước ghi DB thất bại, hệ thống lập tức gọi lệnh bù trừ `r.incr(stock_key)` để hoàn trả lại 1 đơn vị tồn kho về Redis, đảm bảo tính nhất quán (Eventual Consistency) giữa Cache và DB.
3. **Về Message Queue & Spark:** *"Làm sao đảm bảo một đơn hàng trong Kafka không bị PySpark xử lý trùng lặp 2 lần?"*  
   $\rightarrow$ *Trả lời:* Sử dụng cơ chế Checkpointing của Spark Streaming để theo dõi Kafka Offsets, kết hợp với trường `order_id` duy nhất làm khóa Idempotent khi ghi dữ liệu vào kho lưu trữ tầng Silver/Gold.