# AGENT SKILLS: EXECUTION RECIPES & SYSTEM PATTERNS

This guide equips the agent with specific execution recipes, boilerplate templates, and debugging workflows for the FlashLogix platform.

---

## SKILL 1: Redis Lua Script for Atomic Inventory Decrement
Use this pattern whenever writing inventory logic to eliminate race conditions between checking stock and decrementing.

```python
# skills/redis_atomic.py
import redis

# Lua script ensures stock check and decrement happen atomically in a single operation
LUA_DECR_STOCK = """
local current = redis.call('GET', KEYS[1])
if not current then
    return -1 -- Key does not exist
end
if tonumber(current) <= 0 then
    return 0  -- Out of stock
end
return redis.call('DECR', KEYS[1]) -- Successfully decremented, returns remaining
"""

def execute_atomic_decrement(r: redis.Redis, product_id: int) -> int:
    """
    Returns:
       > 0: Remaining stock
         0: Just sold the last item
        -1: Sold out / unavailable
    """
    script = r.register_script(LUA_DECR_STOCK)
    key = f"stock:product:{product_id}"
    return script(keys=[key])
```

---

## SKILL 2: Idempotent Kafka Event Publishing
Use this recipe when emitting order events from FastAPI to Kafka to prevent message loss and duplicates.

```python
# skills/kafka_publisher.py
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger("flashlogix.kafka")

class EventPublisher:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",             # Wait for full in-sync replica acknowledgement
            retries=5,              # Resilient against transient network drops
            enable_idempotence=True # Enforce exactly-once producer semantics
        )

    def publish_order_event(self, topic: str, order_data: dict):
        try:
            future = self.producer.send(topic, key=str(order_data["order_id"]).encode("utf-8"), value=order_data)
            record_metadata = future.get(timeout=2.0)
            logger.info(f"Published order {order_data['order_id']} to {record_metadata.topic} partition {record_metadata.partition}")
        except KafkaError as e:
            logger.error(f"Failed to publish order {order_data.get('order_id')}: {str(e)}")
            raise e
```

---

## SKILL 3: PySpark Streaming PII Masking & Parquet Checkpointing
Use this skill when processing raw streaming messages and persisting into the Lakehouse Silver layer.

```python
# skills/spark_processor.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, lit, to_utc_timestamp

def apply_pii_masking_and_clean(df: DataFrame, salt: str = "SECRET_PEPPER") -> DataFrame:
    """Masks customer identity and normalizes UTC timestamp."""
    return df.select(
        col("order_id"),
        col("product_id"),
        col("quantity"),
        sha2(concat(col("user_id"), lit(salt)), 256).alias("masked_user_id"),
        to_utc_timestamp(col("timestamp"), "UTC").alias("order_timestamp")
    )

def sink_to_silver_lake(stream_df: DataFrame, base_path: str, checkpoint_path: str):
    """Writes streaming data in append mode to MinIO/S3 partitioned by date."""
    return stream_df.writeStream \
        .format("parquet") \
        .partitionBy("product_id") \
        .option("path", f"{base_path}/silver/orders") \
        .option("checkpointLocation", f"{checkpoint_path}/silver_orders_chk") \
        .outputMode("append") \
        .start()
```

---

## SKILL 4: SCD Type 2 Upsert Query in PostgreSQL (Tầng Gold)
Use this SQL workflow to record changing customer attributes without destroying historical truth.

```sql
-- Step 1: Invalidate existing active record if attributes changed
UPDATE Dim_Users
SET 
    effective_to = NOW(),
    is_current = FALSE
WHERE 
    masked_user_id = :masked_user_id 
    AND is_current = TRUE
    AND membership_tier != :new_tier;

-- Step 2: Insert new active record
INSERT INTO Dim_Users (masked_user_id, membership_tier, effective_from, effective_to, is_current)
SELECT 
    :masked_user_id, 
    :new_tier, 
    NOW(), 
    NULL, 
    TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM Dim_Users 
    WHERE masked_user_id = :masked_user_id 
      AND membership_tier = :new_tier 
      AND is_current = TRUE
);
```

---

## SKILL 5: Concurrency Debugging & Failure Playbook

| Triệu chứng (Symptom) | Nguyên nhân gốc (Root Cause) | Quy trình xử lý (Actionable Fix) |
| :--- | :--- | :--- |
| Tồn kho bị âm (`stock < 0`) sau khi chạy Locust | Code dùng `SELECT` sau đó `UPDATE` không có lock | Chuyển toàn bộ logic trừ kho sang Lua script trên Redis (`Skill 1`) hoặc `SELECT ... FOR UPDATE`. |
| API trả về HTTP 500 khi chịu tải cao | Database cạn kiệt Connection Pool | Tăng `pool_size` và `max_overflow` trong SQLAlchemy `create_engine`. Đóng session sau mỗi request. |
| Spark Streaming báo lỗi `SchemaMismatch` | Dữ liệu JSON từ Kafka bị thiếu key hoặc đổi kiểu | Bỏ `inferSchema`, định nghĩa tường minh `StructType` với `nullable=True`. |
| Kafka broker từ chối request (`MessageSizeTooLarge`) | Batch payload gửi lên quá lớn | Cấu hình lại `batch_size` và bật nén dữ liệu (`compression_type='gzip'`) trong Kafka producer. |