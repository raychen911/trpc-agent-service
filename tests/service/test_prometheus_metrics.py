# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Prometheus-backed Admin metric snapshot tests."""

from __future__ import annotations

import httpx

from trpc_service.metrics import PrometheusMetricsReader
from trpc_service.metrics._prometheus import parse_prometheus_snapshot


def _series(name: str, value: float, **labels: str) -> dict:
    return {
        "metric": {
            "__name__": name,
            **labels
        },
        "value": [1_700_000_000, str(value)],
    }


def test_parse_prometheus_snapshot_aggregates_processes_and_histogram_buckets():
    labels = {"tenant_id": "tenant_a", "outcome": "success"}
    series = [
        _series("agent_callback_total", 3, instance="collector-a", service_instance_id="gateway-a", **labels),
        _series("agent_callback_total", 4, instance="collector-b", service_instance_id="gateway-b", **labels),
        _series("agent_budget_tokens_used", 125, service_instance_id="worker-a", tenant_id="tenant_a"),
        _series("agent_budget_tokens_used", 125, service_instance_id="worker-b", tenant_id="tenant_a"),
        _series("agent_callback_duration_ms_bucket", 2, le="10", **labels),
        _series("agent_callback_duration_ms_bucket", 5, le="50", **labels),
        _series("agent_callback_duration_ms_bucket", 6, le="+Inf", **labels),
        _series("agent_callback_duration_ms_count", 6, **labels),
        _series("agent_callback_duration_ms_sum", 90, **labels),
        _series("unrelated_total", 99, tenant_id="tenant_a"),
        _series("agent_callback_total", 8, tenant_id="tenant_b", outcome="success"),
    ]

    snapshot = parse_prometheus_snapshot(series, tenant_id="tenant_a")

    assert snapshot["counters"] == [{
        "name": "agent_callback_total",
        "description": "Number of IM callbacks handled.",
        "unit": "1",
        "attributes": labels,
        "value": 7.0,
    }]
    assert snapshot["gauges"][0]["value"] == 125
    histogram = snapshot["histograms"][0]
    assert histogram["count"] == 6
    assert histogram["sum"] == 90
    assert histogram["max"] is None
    assert histogram["buckets"] == [
        {
            "lower": None,
            "upper": 10.0,
            "count": 2.0,
        },
        {
            "lower": 10.0,
            "upper": 50.0,
            "count": 3.0,
        },
        {
            "lower": 50.0,
            "upper": None,
            "count": 1.0,
        },
    ]


async def test_prometheus_reader_queries_agent_series():
    captured = {}

    def respond(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [_series("agent_requests_total", 2, tenant_id="tenant_a", outcome="success")],
                },
            },
        )

    reader = PrometheusMetricsReader("http://prometheus:9090/", transport=httpx.MockTransport(respond))

    snapshot = await reader.snapshot("tenant_a")

    assert "/api/v1/query" in captured["url"]
    assert snapshot["counters"][0]["name"] == "agent_requests_total"
    assert snapshot["counters"][0]["value"] == 2
