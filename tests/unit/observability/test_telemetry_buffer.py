"""T032 RED: bounded dual-priority telemetry buffer (FR-009, NFR-004, DEC-002).

Fail-open in-memory buffer: critical zone reservation, oldest-first eviction
of ordinary data, bounded retries, TTL expiry, categorized drop counters and
a fixed-size critical summary when critical space is exhausted. Never
touches disk, Redis or PostgreSQL.
"""

from __future__ import annotations

import importlib
from datetime import timedelta

from tests.observability_support import FIXED_OBS_UTC, stable_scope_digest, stable_trace_digest


def _load():
    try:
        return importlib.import_module("trpc_service.observability.buffer")
    except ModuleNotFoundError:
        return None


def _envelope_module():
    from trpc_service.observability.models import TelemetryEnvelope

    return TelemetryEnvelope


def _envelope(index: int, *, priority: str = "normal", signal_type: str = "log",
              attempt_count: int = 0, expires_at=None):
    TelemetryEnvelope = _envelope_module()
    return TelemetryEnvelope(
        envelope_id=f"env-{priority}-{index}",
        signal_type=signal_type,
        priority=priority,
        scope_digest=stable_scope_digest("tenant-alpha"),
        payload={"stage": "gateway.accept", "index": index},
        trace_digest=stable_trace_digest(f"trace-{index}"),
        attempt_count=attempt_count,
        created_at=FIXED_OBS_UTC,
        expires_at=expires_at,
    )


def test_offer_is_non_blocking_and_synchronous() -> None:
    buffer_mod = _load()
    assert buffer_mod is not None, "trpc_service.observability.buffer is not implemented"
    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=100)
    import inspect

    assert not inspect.iscoroutinefunction(buffer.offer)
    accepted = buffer.offer(_envelope(1))
    assert accepted is True


def test_default_capacity_is_bounded_at_ten_thousand() -> None:
    buffer_mod = _load()
    assert buffer_mod is not None
    buffer = buffer_mod.PriorityTelemetryBuffer()
    assert buffer.capacity == 10_000


def test_critical_zone_reserves_at_least_twenty_percent() -> None:
    buffer_mod = _load()
    assert buffer_mod is not None
    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=100)
    assert buffer.critical_capacity >= 20


def test_ordinary_overflow_evicts_oldest_and_counts_drops() -> None:
    buffer_mod = _load()
    assert buffer_mod is not None
    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=10)
    for index in range(20):
        buffer.offer(_envelope(index, priority="normal"))
    assert buffer.size() <= 10
    counters = buffer.drop_counters()
    assert counters.get("normal_evicted", 0) >= 10
    # Oldest-first: the surviving ordinary envelopes are the newest ones.
    remaining_ids = {envelope.envelope_id for envelope in buffer.drain_normal()}
    assert f"env-normal-{19}" in remaining_ids
    assert f"env-normal-0" not in remaining_ids


def test_retries_are_bounded_by_three_attempts() -> None:
    buffer_mod = _load()
    assert buffer_mod is not None
    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=100)
    # A re-offer after the final failed attempt (attempt_count == 3) is
    # dropped: at most 3 export attempts ever happen per envelope.
    assert buffer.offer(_envelope(1, attempt_count=2))
    assert not buffer.offer(_envelope(2, attempt_count=3))
    counters = buffer.drop_counters()
    assert counters.get("retry_exhausted", 0) >= 1


def test_expired_envelopes_are_dropped_by_ttl() -> None:
    buffer_mod = _load()
    assert buffer_mod is not None
    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=100)
    expired = _envelope(1, expires_at=FIXED_OBS_UTC - timedelta(seconds=1))
    assert not buffer.offer(expired)
    counters = buffer.drop_counters()
    assert counters.get("ttl_expired", 0) == 1


def test_critical_overflow_generates_fixed_size_summary() -> None:
    buffer_mod = _load()
    assert buffer_mod is not None
    from trpc_service.observability.models import CriticalDiagnosticSummary

    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=10)
    for index in range(30):
        buffer.offer(_envelope(index, priority="critical"))
    summaries = [
        envelope for envelope in buffer.drain_critical()
        if envelope.signal_type == "critical_summary"
    ]
    assert summaries, "critical exhaustion must emit at least one fixed-size summary"
    summary = summaries[-1]
    assert summary.trace_digest is not None
    assert summary.payload["error_type"] == "telemetry_buffer_exhausted"
    # The summary payload stays fixed-size: only the known summary fields.
    assert set(summary.payload) <= {
        "trace_digest", "scope_digest", "component", "stage", "error_type",
        "retryable", "configuration_version", "occurred_at",
    }
    # And it never claims to be a complete trace.
    assert summary.payload.get("complete") is None


def test_buffer_never_writes_to_disk(tmp_path) -> None:
    buffer_mod = _load()
    assert buffer_mod is not None
    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=10)
    for index in range(50):
        buffer.offer(_envelope(index, priority="critical" if index % 2 else "normal"))
    buffer.drain_critical()
    buffer.drain_normal()
    assert list(tmp_path.iterdir()) == []
    # No persistence hooks exist on the buffer at all.
    forbidden = {"redis", "engine", "session", "connection", "client"}
    assert not forbidden & set(vars(buffer))


def test_only_safe_envelopes_are_accepted() -> None:
    buffer_mod = _load()
    assert buffer_mod is not None
    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=10)
    try:
        buffer.offer({"envelope_id": "not-an-envelope"})
    except TypeError:
        pass
    else:
        raise AssertionError("raw dicts must not be accepted by the buffer")
