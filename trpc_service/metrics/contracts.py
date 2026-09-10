"""Replaceable metrics recording port."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from trpc_service.metrics.models import MetricSnapshot


class MetricsUnavailable(RuntimeError):
    pass


@runtime_checkable
class MetricsRecorder(Protocol):
    def record(
        self, scope: Any, *, trace_id: UUID, stage: str, outcome: str,
        duration_ms: float, values: dict[str, float] | None = None,
    ) -> None: ...
    def snapshot(self, scope: Any) -> MetricSnapshot: ...
    def reset(self) -> None: ...
