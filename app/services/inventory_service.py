from typing import Optional, Tuple
from loguru import logger
import redis
from app.core.config import settings

# Thread-safe Redis connection pool
redis_pool = redis.ConnectionPool(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
    max_connections=settings.REDIS_MAX_CONNECTIONS,
    decode_responses=True
)

redis_client = redis.Redis(connection_pool=redis_pool)

# Lua script ensures stock inspection and decrement execute atomically
# Returns:
#   >= 0 : Remaining stock after deduction (successful purchase)
#     -1 : Out of stock (not enough quantity or zero)
#     -2 : Key does not exist in Redis cache
LUA_ATOMIC_DECREMENT = """
local current = redis.call('GET', KEYS[1])
local qty = tonumber(ARGV[1])

if not current then
    return -2
end

local stock_val = tonumber(current)
if stock_val < qty then
    return -1
end

return redis.call('DECRBY', KEYS[1], qty)
"""


class InventoryService:
    """High-concurrency inventory service utilizing Redis atomic operations."""

    def __init__(self, r: Optional[redis.Redis] = None) -> None:
        self.r: redis.Redis = r or redis_client
        self._script = self.r.register_script(LUA_ATOMIC_DECREMENT)

    @staticmethod
    def _stock_key(product_id: int) -> str:
        return f"stock:product:{product_id}"

    def initialize_stock(self, product_id: int, initial_stock: int) -> None:
        """Sets or resets the stock level for a product in Redis cache."""
        key = self._stock_key(product_id)
        self.r.set(key, initial_stock)
        logger.info(f"Initialized Redis stock for product {product_id} to {initial_stock}")

    def get_stock(self, product_id: int) -> Optional[int]:
        """Retrieves current stock from Redis cache."""
        key = self._stock_key(product_id)
        val = self.r.get(key)
        return int(val) if val is not None else None

    def deduct_stock_atomic(self, product_id: int, quantity: int = 1) -> Tuple[bool, int]:
        """Atomically inspects and decrements stock via Lua script.

        Returns:
            (is_successful: bool, remaining_or_status_code: int)
        """
        key = self._stock_key(product_id)
        result = self._script(keys=[key], args=[quantity])

        if result >= 0:
            logger.info(
                f"[CONCURRENCY] Atomic DECR succeeded for product {product_id}. "
                f"Deducted: {quantity}, Remaining in RAM: {result}"
            )
            return True, int(result)

        if result == -1:
            logger.debug(
                f"[OVERSOLD_PREVENTED] Product {product_id} is out of stock or insufficient quantity."
            )
            return False, -1

        # result == -2: cache miss / key uninitialized
        logger.error(f"[CACHE_MISS] Product {product_id} stock key '{key}' not found in Redis.")
        return False, -2

    def compensatory_increment(self, product_id: int, quantity: int = 1) -> int:
        """Compensatory rollback (INCRBY) when DB transaction fails after Redis decrement.

        Enforces eventual consistency between Redis In-Memory cache and PostgreSQL OLTP.
        """
        key = self._stock_key(product_id)
        restored = self.r.incrby(key, quantity)
        logger.warning(
            f"[ROLLBACK] Compensatory increment for product {product_id} by {quantity}. "
            f"Restored stock in RAM: {restored}"
        )
        return int(restored)


inventory_service = InventoryService()
