from collections import defaultdict
import threading
import time
from typing import Any, Dict


class TelemetryCollector:
    """Thread-safe in-process telemetry collector for Prometheus & JSON `/metrics`."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = time.time()
        self._counters: Dict[str, int] = defaultdict(int)

    def inc(self, metric_name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[metric_name] += amount

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            counts = dict(self._counters)
        return {
            "uptime_seconds": round(time.time() - self._started_at, 2),
            "counters": counts,
        }


telemetry = TelemetryCollector()
