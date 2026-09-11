"""Low-cardinality metrics and platform storage spans."""

from __future__ import annotations

import math
import threading
from collections import defaultdict
from collections import deque
from contextlib import contextmanager

REQUIRED_METRICS = frozenset({
    "gateway.callback_total",
    "gateway.verification_latency_ms",
    "gateway.inbox_latency_ms",
    "gateway.duplicate_total",
    "queue.backlog",
    "queue.oldest_age_seconds",
    "queue.partition_skew",
    "queue.redelivery_total",
    "worker.active_runs",
    "worker.run_latency_ms",
    "worker.timeout_total",
    "worker.lease_lost_total",
    "session.get_latency_ms",
    "session.append_latency_ms",
    "session.revision_conflict_total",
    "session.fencing_reject_total",
    "memory.task_backlog",
    "memory.visibility_lag_seconds",
    "redis.command_latency_ms",
    "redis.pool_usage",
    "sql.transaction_latency_ms",
    "sql.pool_usage",
    "vector.query_latency_ms",
    "vector.index_lag_seconds",
    "object.upload_latency_ms",
    "object.hash_mismatch_total",
    "model.first_token_latency_ms",
    "model.total_latency_ms",
    "model.token_total",
    "tool.call_latency_ms",
    "tool.unknown_outcome_total",
    "outbox.pending",
    "outbox.oldest_age_seconds",
    "migration.copied_total",
    "migration.verified_total",
    "migration.diff_total",
})


_FORBIDDEN_LABELS = {"user", "user_id", "session", "session_id", "request_id", "trace_id"}


class MetricsRegistry:
    def __init__(self, max_samples: int = 2048, max_series: int = 4096) -> None:
        if min(max_samples, max_series) < 1:
            raise ValueError("metric retention limits must be positive")
        self._values = defaultdict(lambda: deque(maxlen=max_samples))
        self._counts = defaultdict(int)
        self._totals = defaultdict(float)
        self._max_series = max_series
        self._lock = threading.Lock()

    def _ensure_series(self, key):
        if key not in self._values and len(self._values) >= self._max_series:
            raise ValueError("metric series limit exceeded")

    @staticmethod
    def _key(name: str, labels: dict) -> str:
        forbidden = _FORBIDDEN_LABELS & labels.keys()
        if forbidden:
            raise ValueError(f"high-cardinality labels are forbidden: {sorted(forbidden)}")
        suffix = ",".join(f"{key}={labels[key]}" for key in sorted(labels))
        return f"{name}{{{suffix}}}" if suffix else name

    def increment(self, name: str, amount: float = 1, **labels) -> None:
        key = self._key(name, labels)
        with self._lock:
            self._ensure_series(key)
            current = self._values[key][-1] if self._values[key] else 0
            self._values[key].clear()
            self._values[key].append(current + amount)
            self._counts[key] += 1
            self._totals[key] = current + amount

    def observe(self, name: str, value: float, **labels) -> None:
        key = self._key(name, labels)
        with self._lock:
            self._ensure_series(key)
            self._values[key].append(float(value))
            self._counts[key] += 1
            self._totals[key] += float(value)

    def snapshot(self) -> dict[str, dict[str, float]]:
        with self._lock:
            values = {key: list(items) for key, items in self._values.items()}
            counts, totals = dict(self._counts), dict(self._totals)
        return {
            key: {
                "count": counts[key],
                "sum": totals[key],
                "retained_samples": len(items),
                "p50": percentile(items, 0.50),
                "p95": percentile(items, 0.95),
                "p99": percentile(items, 0.99),
            } for key, items in values.items()
        }


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


@contextmanager
def storage_span(operation: str, *, tenant_id: str, backend_kind: str, resource_type: str):
    from .runtime import operation as measured_operation
    with measured_operation(f"storage.{operation}") as span:
        span.set_attribute("tenant.id", tenant_id)
        span.set_attribute("storage.backend", backend_kind)
        span.set_attribute("storage.resource", resource_type)
        yield span
