import asyncio
import json
from typing import Any, Dict, Optional, Tuple
import weakref
from loguru import logger
import redis
import redis.asyncio as aioredis
from app.core.config import settings

# Thread-safe sync Redis connection pool (used by background consumers, dashboard, sync helpers)
redis_pool = redis.ConnectionPool(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
    max_connections=settings.REDIS_MAX_CONNECTIONS,
    decode_responses=True,
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
    """High-concurrency inventory & cache service utilizing Redis atomic operations."""

    def __init__(self, r: Optional[redis.Redis] = None) -> None:
        self.r: redis.Redis = r or redis_client
        self._script = self.r.register_script(LUA_ATOMIC_DECREMENT)
        # Per-event-loop cache for redis.asyncio clients so both long-lived Uvicorn
        # workers and multi-loop TestClient runs reuse pools cleanly without loop errors.
        self._async_clients: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Tuple[aioredis.Redis, Any]]" = (
            weakref.WeakKeyDictionary()
        )

    def _get_async_client(self) -> Tuple[aioredis.Redis, Any]:
        loop = asyncio.get_running_loop()
        cached = self._async_clients.get(loop)
        if cached is not None:
            return cached

        # BlockingConnectionPool avoids MaxConnectionsError under >100-coroutine flash-sale spikes
        pool = aioredis.BlockingConnectionPool(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
            max_connections=settings.REDIS_MAX_CONNECTIONS,
            timeout=10,
            decode_responses=True,
        )
        client = aioredis.Redis(connection_pool=pool)
        script = client.register_script(LUA_ATOMIC_DECREMENT)
        self._async_clients[loop] = (client, script)
        return client, script

    async def close_async(self) -> None:
        """Closes active asyncio Redis connection pool cleanly."""
        loop = asyncio.get_running_loop()
        cached = self._async_clients.pop(loop, None)
        if cached is not None:
            client, _ = cached
            await client.aclose()

    @staticmethod
    def _stock_key(product_id: int) -> str:
        return f"stock:product:{product_id}"

    @staticmethod
    def _product_meta_key(product_id: int) -> str:
        return f"product:meta:{product_id}"

    @staticmethod
    def _idem_order_key(idempotency_key: str) -> str:
        return f"idem:order:{idempotency_key}"

    @staticmethod
    def _idem_lock_key(idempotency_key: str) -> str:
        return f"idem:lock:{idempotency_key}"

    # ------------------------------------------------------------------
    # Synchronous Inventory & Cache Helpers
    # ------------------------------------------------------------------
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

    def set_product_meta(self, product_id: int, meta: Dict[str, Any]) -> None:
        """Caches static product metadata (name, price, created_at) with TTL."""
        key = self._product_meta_key(product_id)
        self.r.set(key, json.dumps(meta), ex=settings.PRODUCT_CACHE_TTL_SEC)

    def deduct_stock_atomic(self, product_id: int, quantity: int = 1) -> Tuple[bool, int]:
        """Atomically inspects and decrements stock via Lua script."""
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

        logger.error(f"[CACHE_MISS] Product {product_id} stock key '{key}' not found in Redis.")
        return False, -2

    def compensatory_increment(self, product_id: int, quantity: int = 1) -> int:
        """Compensatory rollback (INCRBY) when DB transaction fails after Redis decrement."""
        key = self._stock_key(product_id)
        restored = self.r.incrby(key, quantity)
        logger.warning(
            f"[ROLLBACK] Compensatory increment for product {product_id} by {quantity}. "
            f"Restored stock in RAM: {restored}"
        )
        return int(restored)

    # ------------------------------------------------------------------
    # Asynchronous High-Throughput Redis Gate & Read-Through Cache
    # ------------------------------------------------------------------
    async def get_stock_async(self, product_id: int) -> Optional[int]:
        client, _ = self._get_async_client()
        val = await client.get(self._stock_key(product_id))
        return int(val) if val is not None else None

    async def initialize_stock_async(self, product_id: int, initial_stock: int) -> None:
        client, _ = self._get_async_client()
        await client.set(self._stock_key(product_id), initial_stock)

    async def deduct_stock_atomic_async(
        self, product_id: int, quantity: int = 1
    ) -> Tuple[bool, int]:
        """Non-blocking atomic Lua stock check & decrement executed on event loop."""
        key = self._stock_key(product_id)
        _, script = self._get_async_client()
        result = await script(keys=[key], args=[quantity])

        if result >= 0:
            logger.debug(
                f"[CONCURRENCY_ASYNC] Atomic DECR succeeded for product {product_id}. "
                f"Deducted: {quantity}, Remaining in RAM: {result}"
            )
            return True, int(result)

        if result == -1:
            logger.debug(
                f"[OVERSOLD_PREVENTED] Product {product_id} is out of stock or insufficient quantity."
            )
            return False, -1

        logger.error(f"[CACHE_MISS] Product {product_id} stock key '{key}' not found in Redis.")
        return False, -2

    async def compensatory_increment_async(self, product_id: int, quantity: int = 1) -> int:
        client, _ = self._get_async_client()
        restored = await client.incrby(self._stock_key(product_id), quantity)
        logger.warning(
            f"[ROLLBACK_ASYNC] Compensatory increment for product {product_id} by {quantity}. "
            f"Restored stock in RAM: {restored}"
        )
        return int(restored)

    async def get_product_meta_and_stock_async(
        self, product_id: int
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        """Fetches product metadata and live RAM stock in a single Redis MGET round-trip."""
        client, _ = self._get_async_client()
        meta_raw, stock_raw = await client.mget(
            self._product_meta_key(product_id),
            self._stock_key(product_id),
        )
        meta = json.loads(meta_raw) if meta_raw else None
        stock = int(stock_raw) if stock_raw is not None else None
        return meta, stock

    async def set_product_meta_async(self, product_id: int, meta: Dict[str, Any]) -> None:
        client, _ = self._get_async_client()
        await client.set(
            self._product_meta_key(product_id),
            json.dumps(meta),
            ex=settings.PRODUCT_CACHE_TTL_SEC,
        )

    async def get_idempotency_record_async(
        self, idempotency_key: str
    ) -> Optional[Dict[str, Any]]:
        client, _ = self._get_async_client()
        raw = await client.get(self._idem_order_key(idempotency_key))
        return json.loads(raw) if raw else None

    async def save_idempotency_record_async(
        self, idempotency_key: str, order_data: Dict[str, Any]
    ) -> None:
        client, _ = self._get_async_client()
        await client.set(
            self._idem_order_key(idempotency_key),
            json.dumps(order_data),
            ex=settings.IDEMPOTENCY_TTL_SEC,
        )

    async def acquire_idempotency_lock_async(
        self, idempotency_key: str, ttl_sec: int = 15
    ) -> bool:
        client, _ = self._get_async_client()
        acquired = await client.set(
            self._idem_lock_key(idempotency_key), "1", nx=True, ex=ttl_sec
        )
        return bool(acquired)

    async def release_idempotency_lock_async(self, idempotency_key: str) -> None:
        client, _ = self._get_async_client()
        await client.delete(self._idem_lock_key(idempotency_key))


inventory_service = InventoryService()
