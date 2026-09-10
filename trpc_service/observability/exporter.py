"""OTLP HTTP exporter adapter (FR-009, NFR-004/005, DEC-002).

Wraps a vendor-neutral OTLP/HTTP transport behind a stable, closed result
set. Only validated safe envelopes are exported; transport failures are
folded into bounded retry semantics with exponential backoff plus jitter
and an explicit timeout. Exceptions never propagate into business code.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, ClassVar, Iterable

from trpc_service.observability.models import TelemetryEnvelope

DEFAULT_TIMEOUT_SECONDS = 2.0
DEFAULT_RETRY_LIMIT = 3
DEFAULT_BASE_BACKOFF_SECONDS = 0.25
DEFAULT_MAX_BACKOFF_SECONDS = 8.0

TransportCallable = Callable[..., Awaitable[bool]]


class PermanentExportRefusal(Exception):
    """Raised by the transport when retrying cannot possibly succeed.

    The adapter folds this into permanent_failure without consuming the
    retry budget; every other transport exception is treated retryable.
    """


@dataclass(frozen=True, slots=True)
class ExportResult:
    """Stable, closed outcome of one export attempt cycle."""

    action: str
    reason: str
    attempts: int

    ACTIONS: ClassVar[tuple[str, ...]] = ("success", "retryable_failure", "permanent_failure")

    def __post_init__(self) -> None:
        if self.action not in self.ACTIONS:
            raise ValueError(f"unknown export action {self.action!r}")


def _is_valid_envelope(envelope: Any) -> bool:
    return isinstance(envelope, TelemetryEnvelope)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class OtlpHttpExporterAdapter:
    """Vendor-neutral OTLP exporter with folded failure semantics."""

    def __init__(
        self,
        *,
        endpoint: str,
        transport: TransportCallable,
        retry_limit: int = DEFAULT_RETRY_LIMIT,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        jitter: Callable[[], float] | None = None,
        clock: Any = None,
    ) -> None:
        if retry_limit < 1:
            raise ValueError("retry_limit must be at least 1")
        self._endpoint = endpoint
        self._transport = transport
        self._retry_limit = int(retry_limit)
        self._timeout_seconds = float(timeout_seconds)
        self._base_backoff = float(base_backoff_seconds)
        self._max_backoff = float(max_backoff_seconds)
        self._sleep = sleep or asyncio.sleep
        self._jitter = jitter or random.random
        self._clock = clock or _now

    async def export(self, envelopes: Iterable[Any]) -> ExportResult:
        """Export one batch; never raises, never blocks business flow."""

        batch = list(envelopes)
        invalid = [item for item in batch if not _is_valid_envelope(item)]
        if invalid:
            # Unvalidated payloads never reach the transport (fail-closed at
            # the boundary, but folded as a stable result for the caller).
            return ExportResult(
                action="permanent_failure", reason="invalid_envelope", attempts=0
            )
        now = self._clock()
        fresh = [
            envelope
            for envelope in batch
            if envelope.expires_at is None or envelope.expires_at > now
        ]
        if not fresh:
            return ExportResult(
                action="success", reason="all_expired_skipped", attempts=0
            )
        attempts = 0
        last_retryable = False
        while attempts < self._retry_limit:
            attempts += 1
            try:
                acknowledged = await self._transport(
                    fresh, timeout_seconds=self._timeout_seconds
                )
            except asyncio.CancelledError:
                raise
            except PermanentExportRefusal:
                # Folded: no retry can succeed, stop immediately.
                return ExportResult(
                    action="permanent_failure", reason="transport_refused",
                    attempts=attempts,
                )
            except Exception:
                # Folded: transport exception types/details never surface.
                last_retryable = True
            else:
                if acknowledged:
                    return ExportResult(
                        action="success", reason="acknowledged", attempts=attempts
                    )
                last_retryable = True
            if attempts < self._retry_limit:
                await self._sleep(self._backoff_seconds(attempts))
        if last_retryable:
            return ExportResult(
                action="retryable_failure", reason="transport_retry_exhausted",
                attempts=attempts,
            )
        return ExportResult(
            action="permanent_failure", reason="transport_permanent_failure",
            attempts=attempts,
        )

    def _backoff_seconds(self, attempt: int) -> float:
        exponential = min(
            self._base_backoff * (2 ** (attempt - 1)), self._max_backoff
        )
        # Bounded jitter keeps retries strictly positive and non-shrinking
        # across consecutive attempts (jitter factor in [0.5, 1.0]).
        return exponential * (0.5 + 0.5 * self._jitter())
