"""T033 RED: OTLP HTTP exporter adapter contract (FR-009, NFR-004/005, DEC-002).

The adapter accepts only validated safe envelopes, folds every transport
outcome into a stable result (success | retryable_failure |
permanent_failure) with bounded retries, exponential backoff plus jitter and
an explicit timeout. Exceptions never propagate into business code.
"""

from __future__ import annotations

import asyncio
import importlib
from datetime import timedelta

from tests.observability_support import FIXED_OBS_UTC, stable_scope_digest


def _load():
    try:
        return importlib.import_module("trpc_service.observability.exporter")
    except ModuleNotFoundError:
        return None


def _envelope(index: int):
    from trpc_service.observability.models import TelemetryEnvelope

    return TelemetryEnvelope(
        envelope_id=f"env-{index}",
        signal_type="log",
        priority="normal",
        scope_digest=stable_scope_digest("tenant-alpha"),
        payload={"stage": "gateway.accept"},
        created_at=FIXED_OBS_UTC,
    )


class FakeTransport:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0
        self.received = []

    async def __call__(self, envelopes, *, timeout_seconds):
        self.calls += 1
        self.received.append(list(envelopes))
        self.last_timeout = timeout_seconds
        outcome = self.outcomes.pop(0) if self.outcomes else "success"
        if outcome == "success":
            return True
        if outcome == "permanent":
            raise RuntimeError("connection refused: otel collector down")
        raise TimeoutError("otlp sink timeout")


def test_adapter_exports_validated_envelopes_and_reports_success() -> None:
    exporter_mod = _load()
    assert exporter_mod is not None, "trpc_service.observability.exporter is not implemented"
    transport = FakeTransport(["success"])
    adapter = exporter_mod.OtlpHttpExporterAdapter(
        endpoint="https://otel.invalid/v1/traces",
        transport=transport,
        retry_limit=3,
    )
    result = asyncio.run(adapter.export([_envelope(1)]))
    assert result.action == "success"
    assert transport.calls == 1
    assert transport.received[0][0].envelope_id == "env-1"
    assert transport.last_timeout > 0


def test_retryable_failure_retries_with_backoff_and_jitter() -> None:
    exporter_mod = _load()
    assert exporter_mod is not None
    transport = FakeTransport(["timeout", "timeout", "timeout", "timeout"])
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    adapter = exporter_mod.OtlpHttpExporterAdapter(
        endpoint="https://otel.invalid/v1/traces",
        transport=transport,
        retry_limit=3,
        sleep=fake_sleep,
    )
    result = asyncio.run(adapter.export([_envelope(1)]))
    assert result.action == "retryable_failure"
    assert result.attempts == 3
    assert transport.calls == 3
    # Backoff is bounded and non-decreasing; jitter keeps retries > 0.
    assert len(sleeps) == 2
    assert all(delay > 0 for delay in sleeps)
    assert sleeps[1] >= sleeps[0]


def test_permanent_failure_does_not_retry() -> None:
    exporter_mod = _load()
    assert exporter_mod is not None

    class PermanentTransport:
        calls = 0

        async def __call__(self, envelopes, *, timeout_seconds):
            PermanentTransport.calls += 1
            raise exporter_mod.PermanentExportRefusal("otel collector refused")

    adapter = exporter_mod.OtlpHttpExporterAdapter(
        endpoint="https://otel.invalid/v1/traces",
        transport=PermanentTransport(),
        retry_limit=3,
    )
    result = asyncio.run(adapter.export([_envelope(1)]))
    assert result.action == "permanent_failure"
    assert PermanentTransport.calls == 1


def test_unvalidated_payloads_are_rejected_without_transport_call() -> None:
    exporter_mod = _load()
    assert exporter_mod is not None
    transport = FakeTransport(["success"])
    adapter = exporter_mod.OtlpHttpExporterAdapter(
        endpoint="https://otel.invalid/v1/traces",
        transport=transport,
        retry_limit=3,
    )
    result = asyncio.run(adapter.export([{"not": "an envelope"}]))
    assert result.action == "permanent_failure"
    assert result.reason == "invalid_envelope"
    assert transport.calls == 0


def test_transport_exceptions_are_folded_not_raised() -> None:
    exporter_mod = _load()
    assert exporter_mod is not None

    async def exploding(envelopes, *, timeout_seconds):
        raise RuntimeError("boom with SENTINEL detail")

    adapter = exporter_mod.OtlpHttpExporterAdapter(
        endpoint="https://otel.invalid/v1/traces",
        transport=exploding,
        retry_limit=2,
    )
    result = asyncio.run(adapter.export([_envelope(1)]))
    # Folded to a stable result; the raw exception text is not surfaced.
    assert result.action in ("retryable_failure", "permanent_failure")
    assert "boom" not in str(result.reason)
    assert "SENTINEL" not in str(result.reason)


def test_expired_envelopes_are_skipped_before_transport() -> None:
    exporter_mod = _load()
    assert exporter_mod is not None
    from trpc_service.observability.models import TelemetryEnvelope

    fresh = _envelope(1)
    stale = TelemetryEnvelope(
        envelope_id="env-2",
        signal_type="log",
        priority="normal",
        scope_digest=stable_scope_digest("tenant-alpha"),
        payload={"stage": "gateway.accept"},
        created_at=FIXED_OBS_UTC,
        expires_at=FIXED_OBS_UTC - timedelta(seconds=1),
    )
    transport = FakeTransport(["success"])
    adapter = exporter_mod.OtlpHttpExporterAdapter(
        endpoint="https://otel.invalid/v1/traces",
        transport=transport,
        retry_limit=3,
    )
    result = asyncio.run(adapter.export([stale, fresh]))
    assert result.action == "success"
    assert len(transport.received[0]) == 1
    assert transport.received[0][0].envelope_id == "env-1"


def test_actions_are_the_stable_closed_set() -> None:
    exporter_mod = _load()
    assert exporter_mod is not None
    assert exporter_mod.ExportResult.ACTIONS == (
        "success", "retryable_failure", "permanent_failure",
    )
