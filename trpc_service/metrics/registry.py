"""Low-cardinality metrics and trace context used by the prototype."""

from __future__ import annotations

import time
import uuid
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Self

_trace_id: ContextVar[str] = ContextVar("trace_id", default="")


def current_trace_id() -> str:
    return _trace_id.get()


@dataclass
class TraceContext:
    trace_id: str
    _token: object | None = None

    def __enter__(self) -> Self:
        self._token = _trace_id.set(self.trace_id)
        return self

    def __exit__(self, *_: object) -> None:
        if self._token is not None:
            _trace_id.reset(self._token)


def new_trace_id() -> str:
    try:
        from opentelemetry import trace

        context = trace.get_current_span().get_span_context()
        if context.is_valid:
            return format(context.trace_id, "032x")
    except (ImportError, RuntimeError):
        pass
    return uuid.uuid4().hex


class MetricsRegistry:
    def __init__(self) -> None:
        self.counters: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()
        self.latencies: list[float] = []

    def inc(self, name: str, **labels: str) -> None:
        self.counters[(name, tuple(sorted(labels.items())))] += 1

    @contextmanager
    def observe(self, name: str, **labels: str) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            self.latencies.append(time.perf_counter() - start)
            self.inc(name, **labels)
