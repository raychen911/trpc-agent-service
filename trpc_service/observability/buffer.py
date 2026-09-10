"""Bounded dual-priority in-memory telemetry buffer (FR-009, NFR-004, DEC-002).

Fail-open staging area between the recorder and the exporter: critical
envelopes get a reserved zone, ordinary envelopes are evicted oldest-first,
retries and TTL are bounded, and every drop is counted by category. The
buffer never touches disk, Redis or PostgreSQL — only safe envelopes are
accepted and the sole failure mode is dropping, never blocking.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from trpc_service.observability.models import (
    CriticalDiagnosticSummary,
    TelemetryEnvelope,
)

DEFAULT_CAPACITY = 10_000
CRITICAL_RESERVE_FRACTION = 0.2
MAX_ATTEMPTS = 3

DROP_CATEGORIES: tuple[str, ...] = (
    "normal_evicted",
    "critical_dropped",
    "retry_exhausted",
    "ttl_expired",
    "invalid_envelope",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PriorityTelemetryBuffer:
    """Bounded, priority-aware, purely in-memory envelope queue."""

    def __init__(
        self,
        *,
        capacity: int = DEFAULT_CAPACITY,
        critical_reserve_fraction: float = CRITICAL_RESERVE_FRACTION,
        max_attempts: int = MAX_ATTEMPTS,
        clock: Any = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if not 0 < critical_reserve_fraction <= 1:
            raise ValueError("critical reserve fraction must be in (0, 1]")
        self.capacity = int(capacity)
        self.critical_capacity = max(1, int(self.capacity * critical_reserve_fraction))
        self._max_attempts = int(max_attempts)
        self._clock = clock or _now
        self._critical: deque[TelemetryEnvelope] = deque()
        self._normal: deque[TelemetryEnvelope] = deque()
        self._drop_counters: dict[str, int] = {category: 0 for category in DROP_CATEGORIES}

    # -- intake -----------------------------------------------------------

    def offer(self, envelope: Any) -> bool:
        """Non-blocking intake. Returns True when the envelope is retained."""
        if not isinstance(envelope, TelemetryEnvelope):
            self._drop_counters["invalid_envelope"] += 1
            raise TypeError(
                "PriorityTelemetryBuffer only accepts TelemetryEnvelope instances"
            )
        if envelope.attempt_count >= self._max_attempts:
            # The envelope already consumed its full retry budget; a re-offer
            # now is a bounded drop, never a block.
            self._drop_counters["retry_exhausted"] += 1
            return False
        now = self._clock()
        if envelope.expires_at is not None and envelope.expires_at <= now:
            self._drop_counters["ttl_expired"] += 1
            return False
        if envelope.priority == "critical":
            return self._offer_critical(envelope)
        return self._offer_normal(envelope)

    def _offer_critical(self, envelope: TelemetryEnvelope) -> bool:
        if len(self._critical) < self.critical_capacity:
            self._critical.append(envelope)
            return True
        # Critical space exhausted: preserve a fixed-size summary of the
        # oldest critical envelope before dropping it (DEC-002 — the summary
        # is minimal and never claims completeness).
        evicted = self._critical.popleft()
        self._critical.append(self._summarize(evicted))
        self._drop_counters["critical_dropped"] += 1
        return True

    def _offer_normal(self, envelope: TelemetryEnvelope) -> bool:
        normal_capacity = self.capacity - self.critical_capacity
        while len(self._normal) >= normal_capacity:
            self._normal.popleft()
            self._drop_counters["normal_evicted"] += 1
        self._normal.append(envelope)
        return True

    def _summarize(self, envelope: TelemetryEnvelope) -> TelemetryEnvelope:
        summary = CriticalDiagnosticSummary(
            trace_digest=envelope.trace_digest or "sha256:unknown",
            scope_digest=envelope.scope_digest,
            component=str(envelope.payload.get("component", "platform")),
            stage=str(envelope.payload.get("stage", "unknown")),
            error_type="telemetry_buffer_exhausted",
            retryable=False,
            configuration_version=envelope.payload.get("configuration_version"),
            occurred_at=self._clock(),
        )
        return TelemetryEnvelope(
            envelope_id=f"summary-{envelope.envelope_id}",
            signal_type="critical_summary",
            priority="critical",
            scope_digest=summary.scope_digest,
            payload={
                "trace_digest": summary.trace_digest,
                "scope_digest": summary.scope_digest,
                "component": summary.component,
                "stage": summary.stage,
                "error_type": summary.error_type,
                "retryable": summary.retryable,
                "configuration_version": summary.configuration_version,
                "occurred_at": summary.occurred_at.isoformat(),
            },
            trace_digest=summary.trace_digest,
        )

    # -- outtake ----------------------------------------------------------

    def drain_critical(self) -> list[TelemetryEnvelope]:
        drained = list(self._critical)
        self._critical.clear()
        return drained

    def drain_normal(self) -> list[TelemetryEnvelope]:
        drained = list(self._normal)
        self._normal.clear()
        return drained

    def size(self) -> int:
        return len(self._critical) + len(self._normal)

    def drop_counters(self) -> dict[str, int]:
        return dict(self._drop_counters)
