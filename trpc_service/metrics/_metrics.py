# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Low-cardinality enterprise metrics with an OTel and local snapshot sink."""

from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any
from typing import Optional


class EnterpriseMetrics:
    """Record required tenant metrics without user/session high-cardinality labels."""

    def __init__(self, meter: Optional[Any] = None) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._histograms: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, float]] = {}
        if meter is False:
            meter = None
        elif meter is None:
            try:
                from opentelemetry import metrics

                meter = metrics.get_meter("trpc.python.agent.enterprise")
            except ImportError:  # pragma: no cover - OTel optional
                meter = None
        self._meter = meter
        self._otel_counters: dict[str, Any] = {}
        self._otel_histograms: dict[str, Any] = {}

    @staticmethod
    def _labels(attributes: dict[str, Any]) -> tuple[tuple[str, str], ...]:
        return tuple(sorted((key, str(value)) for key, value in attributes.items() if value is not None))

    def increment(self, name: str, value: float = 1, **attributes: Any) -> None:
        labels = self._labels(attributes)
        with self._lock:
            self._counters[(name, labels)] += value
        if self._meter is not None:
            instrument = self._otel_counters.get(name)
            if instrument is None:
                instrument = self._meter.create_counter(name)
                self._otel_counters[name] = instrument
            instrument.add(value, dict(labels))

    def observe(self, name: str, value: float, **attributes: Any) -> None:
        labels = self._labels(attributes)
        with self._lock:
            aggregate = self._histograms.setdefault((name, labels), {"count": 0.0, "sum": 0.0, "max": 0.0})
            aggregate["count"] += 1
            aggregate["sum"] += value
            aggregate["max"] = max(aggregate["max"], value)
        if self._meter is not None:
            instrument = self._otel_histograms.get(name)
            if instrument is None:
                instrument = self._meter.create_histogram(name)
                self._otel_histograms[name] = instrument
            instrument.record(value, dict(labels))

    def snapshot(self, tenant_id: Optional[str] = None) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict[str, Any]]] = {"counters": [], "histograms": []}
        with self._lock:
            counters = list(self._counters.items())
            histograms = list(self._histograms.items())
        for (name, labels), value in counters:
            attributes = dict(labels)
            if tenant_id is None or attributes.get("tenant_id") == tenant_id:
                result["counters"].append({"name": name, "attributes": attributes, "value": value})
        for (name, labels), value in histograms:
            attributes = dict(labels)
            if tenant_id is None or attributes.get("tenant_id") == tenant_id:
                result["histograms"].append({"name": name, "attributes": attributes, **value})
        return result


_DEFAULT_METRICS = EnterpriseMetrics()


def get_enterprise_metrics() -> EnterpriseMetrics:
    return _DEFAULT_METRICS
