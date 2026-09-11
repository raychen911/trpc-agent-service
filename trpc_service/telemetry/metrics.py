"""Low-cardinality runtime metrics (Stage 6C).

Fixed contract (design §Task 2): metric labels may ONLY be ``service``,
``operation``, ``result`` and ``error_code`` — all bounded enumerations.
``tenant_id``, user/session/message ids and model profiles are unbounded
domains and structurally cannot enter a metric here (they live in SQL);
:meth:`MetricsRecorder.record_counter` / ``observe`` take them only as
keyword-enumerated parameters, and every attribute value passes through a
length/type guard.

Disabled/closed runtimes hand out a :class:`NoopMetricsRecorder` (zero
cost, zero objects created).  All recording paths swallow ANY exception:
a collector outage can never change a business response — the same
failure-safety contract as the 6B1 span exporter.
"""

from __future__ import annotations

from typing import Final

_MAX_ATTR_LEN: Final[int] = 64


class MetricsAttributeError(ValueError):
    """A metric attribute violated the fixed low-cardinality contract."""


def _check_value(name: str, value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > _MAX_ATTR_LEN:
        raise MetricsAttributeError(f"metric attribute {name!r} must be a short fixed string")
    return value


def _attrs(service: str, operation: str, result: str | None, error_code: str | None) -> dict[str, str]:
    attrs = {
        "service": _check_value("service", service),
        "operation": _check_value("operation", operation),
    }
    if result is not None:
        attrs["result"] = _check_value("result", result)
    if error_code is not None:
        attrs["error_code"] = _check_value("error_code", error_code)
    return attrs


class NoopMetricsRecorder:
    """Zero-cost sink used while telemetry is disabled/closed."""

    def record_counter(self, name: str, value: float = 1, **_kw: object) -> None:
        return None

    def observe(self, name: str, value: float, **_kw: object) -> None:
        return None


class MetricsRecorder:
    """Validated facade over one OTel meter (never raises into business code)."""

    def __init__(self, meter: object, service: str) -> None:
        self._meter = meter
        self._service = service
        self._counters: dict[str, object] = {}
        self._histograms: dict[str, object] = {}

    def record_counter(
        self,
        name: str,
        value: float = 1,
        *,
        operation: str,
        result: str | None = None,
        error_code: str | None = None,
    ) -> None:
        try:
            attrs = _attrs(self._service, operation, result, error_code)
            instrument = self._counters.get(name)
            if instrument is None:
                instrument = self._meter.create_counter(name)  # type: ignore[attr-defined]
                self._counters[name] = instrument
            instrument.add(value, attrs)  # type: ignore[attr-defined]
        except Exception:
            return None

    def observe(
        self,
        name: str,
        value: float,
        *,
        operation: str,
        result: str | None = None,
    ) -> None:
        try:
            attrs = _attrs(self._service, operation, result, None)
            instrument = self._histograms.get(name)
            if instrument is None:
                instrument = self._meter.create_histogram(name)  # type: ignore[attr-defined]
                self._histograms[name] = instrument
            instrument.record(value, attrs)  # type: ignore[attr-defined]
        except Exception:
            return None


# Fixed metric names (low-cardinality catalog — acceptance scans for these).
METRIC_REQUESTS: Final[str] = "trpc.requests"
METRIC_TOKENS: Final[str] = "trpc.tokens"
METRIC_COST_MICROUNITS: Final[str] = "trpc.cost.microunits"
METRIC_RATE_REJECTIONS: Final[str] = "trpc.rate_limit.rejections"
METRIC_BUDGET_REJECTIONS: Final[str] = "trpc.budget.rejections"
METRIC_DELIVERY: Final[str] = "trpc.delivery"
METRIC_DURATION_MS: Final[str] = "trpc.operation.duration.ms"

__all__ = [
    "METRIC_BUDGET_REJECTIONS",
    "METRIC_COST_MICROUNITS",
    "METRIC_DELIVERY",
    "METRIC_DURATION_MS",
    "METRIC_RATE_REJECTIONS",
    "METRIC_REQUESTS",
    "METRIC_TOKENS",
    "MetricsAttributeError",
    "MetricsRecorder",
    "NoopMetricsRecorder",
]
