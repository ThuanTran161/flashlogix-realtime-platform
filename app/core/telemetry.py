from collections import defaultdict
import threading
import time
from typing import Any, Dict, Optional
import redis
from app.core.config import settings


class TelemetryCollector:
    """Thread-safe, multi-worker-aware telemetry collector for JSON & Prometheus `/metrics`.

    Hot-path `inc()` calls update in-memory counters in O(1) with zero event-loop I/O.
    Accumulated deltas are flushed asynchronously in batches to Redis (and within 250ms
    for quiet tails) so `/metrics` aggregates accurately across all Uvicorn worker processes.
    """

    REDIS_HASH_KEY = "flashlogix:telemetry:counters"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = time.time()
        self._counters: Dict[str, int] = defaultdict(int)
        self._pending_redis: Dict[str, int] = defaultdict(int)
        self._last_flush_ts = time.monotonic()
        self._flush_in_flight = False
        self._timer_scheduled = False
        self._redis_client: Optional[redis.Redis] = None

    def _get_redis(self) -> redis.Redis:
        if self._redis_client is None:
            self._redis_client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
                socket_connect_timeout=0.5,
                socket_timeout=0.5,
                decode_responses=True,
            )
        return self._redis_client

    def flush_to_redis(self) -> None:
        """Flushes accumulated local deltas to the shared Redis telemetry hash."""
        with self._lock:
            self._timer_scheduled = False
            if not self._pending_redis:
                self._flush_in_flight = False
                return
            batch = dict(self._pending_redis)
            self._pending_redis.clear()
            self._last_flush_ts = time.monotonic()

        try:
            r = self._get_redis()
            pipe = r.pipeline()
            for metric_name, delta in batch.items():
                pipe.hincrby(self.REDIS_HASH_KEY, metric_name, int(delta))
            pipe.execute()
        except Exception:
            # Never allow telemetry sink issues to disrupt request processing
            pass
        finally:
            with self._lock:
                self._flush_in_flight = False

    def inc(self, metric_name: str, amount: int = 1) -> None:
        should_spawn_flush = False
        should_schedule_tail_timer = False
        with self._lock:
            self._counters[metric_name] += amount
            self._pending_redis[metric_name] += amount
            pending_total = sum(self._pending_redis.values())
            elapsed = time.monotonic() - self._last_flush_ts
            if not self._flush_in_flight and (pending_total >= 25 or elapsed >= 0.5):
                self._flush_in_flight = True
                should_spawn_flush = True
            elif not self._flush_in_flight and not self._timer_scheduled:
                self._timer_scheduled = True
                should_schedule_tail_timer = True

        if should_spawn_flush:
            threading.Thread(target=self.flush_to_redis, daemon=True).start()
        elif should_schedule_tail_timer:
            timer = threading.Timer(0.25, self.flush_to_redis)
            timer.daemon = True
            timer.start()

    def snapshot(self) -> Dict[str, Any]:
        self.flush_to_redis()
        with self._lock:
            counts = dict(self._counters)

        try:
            raw_shared = self._get_redis().hgetall(self.REDIS_HASH_KEY)
            if raw_shared:
                shared_counts = {k: int(v) for k, v in raw_shared.items()}
                for k, v in counts.items():
                    shared_counts.setdefault(k, v)
                counts = shared_counts
        except Exception:
            pass

        return {
            "uptime_seconds": round(time.time() - self._started_at, 2),
            "counters": counts,
        }


telemetry = TelemetryCollector()
