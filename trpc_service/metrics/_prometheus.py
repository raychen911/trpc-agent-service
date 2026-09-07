# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Read the cross-process enterprise metric snapshot from Prometheus."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any
from typing import Optional

import httpx

from ._metrics import METRIC_DEFINITIONS

_INFRASTRUCTURE_LABELS = frozenset({
    "instance",
    "job",
    "service_instance_id",
    "service_name",
    "telemetry_sdk_language",
    "telemetry_sdk_name",
    "telemetry_sdk_version",
})


def _business_attributes(labels: dict[str, str]) -> dict[str, str]:
    attributes = {
        key: value
        for key, value in labels.items()
        if key != "__name__" and key not in _INFRASTRUCTURE_LABELS and not key.startswith("otel_scope_")
    }
    tenant_id = attributes.pop("tenant.id", None)
    if tenant_id is not None:
        attributes["tenant_id"] = tenant_id
    return attributes


def _sample_value(series: dict[str, Any]) -> Optional[float]:
    sample = series.get("value")
    if not isinstance(sample, list) or len(sample) != 2:
        return None
    try:
        value = float(sample[1])
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def parse_prometheus_snapshot(series: list[dict[str, Any]], tenant_id: Optional[str] = None) -> dict[str, Any]:
    """Convert one Prometheus instant vector into the Admin metrics schema."""
    counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
    gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    histograms: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}
    histogram_names = tuple(name for name, definition in METRIC_DEFINITIONS.items() if definition.kind == "histogram")

    for item in series:
        labels = item.get("metric") or {}
        metric_name = labels.get("__name__")
        value = _sample_value(item)
        if not metric_name or value is None:
            continue
        attributes = _business_attributes(labels)
        if tenant_id is not None and attributes.get("tenant_id") != tenant_id:
            continue
        label_key = tuple(sorted(attributes.items()))
        definition = METRIC_DEFINITIONS.get(metric_name)
        if definition is not None and definition.kind == "counter":
            counters[(metric_name, label_key)] += value
            continue
        if definition is not None and definition.kind == "gauge":
            key = (metric_name, label_key)
            gauges[key] = max(gauges.get(key, -math.inf), value)
            continue

        for base_name in histogram_names:
            component = None
            if metric_name == f"{base_name}_bucket":
                component = "bucket"
            elif metric_name == f"{base_name}_count":
                component = "count"
            elif metric_name == f"{base_name}_sum":
                component = "sum"
            elif metric_name == f"{base_name}_max":
                component = "max"
            if component is None:
                continue

            upper = attributes.pop("le", None)
            label_key = tuple(sorted(attributes.items()))
            aggregate = histograms.setdefault((base_name, label_key), {
                "count": 0.0,
                "sum": 0.0,
                "max": None,
                "cumulative_buckets": defaultdict(float),
            })
            if component == "bucket" and upper is not None:
                boundary = math.inf if upper in {"+Inf", "Inf"} else float(upper)
                aggregate["cumulative_buckets"][boundary] += value
            elif component == "max":
                aggregate["max"] = value if aggregate["max"] is None else max(aggregate["max"], value)
            else:
                aggregate[component] += value
            break

    result: dict[str, list[dict[str, Any]]] = {"counters": [], "gauges": [], "histograms": []}
    for (name, labels), value in sorted(counters.items()):
        definition = METRIC_DEFINITIONS[name]
        result["counters"].append({
            "name": name,
            "description": definition.description,
            "unit": definition.unit,
            "attributes": dict(labels),
            "value": value,
        })
    for (name, labels), value in sorted(gauges.items()):
        definition = METRIC_DEFINITIONS[name]
        result["gauges"].append({
            "name": name,
            "description": definition.description,
            "unit": definition.unit,
            "attributes": dict(labels),
            "value": value,
        })
    for (name, labels), aggregate in sorted(histograms.items()):
        definition = METRIC_DEFINITIONS[name]
        cumulative = aggregate.pop("cumulative_buckets")
        previous = 0.0
        lower = None
        buckets = []
        for upper, cumulative_count in sorted(cumulative.items()):
            count = max(0.0, cumulative_count - previous)
            buckets.append({
                "lower": lower,
                "upper": None if math.isinf(upper) else upper,
                "count": count,
            })
            previous = cumulative_count
            lower = None if math.isinf(upper) else upper
        if not aggregate["count"] and math.inf in cumulative:
            aggregate["count"] = cumulative[math.inf]
        result["histograms"].append({
            "name": name,
            "description": definition.description,
            "unit": definition.unit,
            "attributes": dict(labels),
            "buckets": buckets,
            **aggregate,
        })
    return result


class PrometheusMetricsReader:
    """Query all exported ``agent_*`` series from a Prometheus HTTP API."""

    def __init__(self, base_url: str, *, transport: Optional[httpx.AsyncBaseTransport] = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._transport = transport

    async def snapshot(self, tenant_id: Optional[str] = None) -> dict[str, Any]:
        async with httpx.AsyncClient(transport=self._transport, timeout=5.0) as client:
            response = await client.get(
                f"{self._base_url}/api/v1/query",
                params={"query": '{__name__=~"agent_.*"}'},
            )
            response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success" or payload.get("data", {}).get("resultType") != "vector":
            raise ValueError("Prometheus returned an invalid instant-query response")
        return parse_prometheus_snapshot(payload["data"].get("result", []), tenant_id)
