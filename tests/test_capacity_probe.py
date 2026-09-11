"""Contract tests for the deliberately small R3 capacity probe."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "capacity_probe.py"
SPEC = importlib.util.spec_from_file_location("capacity_probe", SCRIPT)
assert SPEC and SPEC.loader
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def test_percentiles_are_bounded_and_never_fabricate_samples():
    assert probe.percentile([], 50) is None
    assert probe.percentile([11], 95) == 11
    assert probe.percentile([1, 2, 100], 50) == 2
    assert probe.percentile([1, 2, 100], 95) == 100


def test_result_json_has_only_the_safe_documented_shape():
    result = probe.build_result(
        concurrent_sessions=2,
        successes=3,
        elapsed_seconds=1.5,
        latencies_ms=[10, 20, 30],
        tokens_before=10,
        tokens_after=16,
        redis_before=2,
        redis_after=7,
        postgres_before=4,
        postgres_after=9,
    )
    encoded = json.dumps(result, sort_keys=True)
    assert set(result) == set(probe.RESULT_KEYS)
    assert result == {
        "concurrent_sessions": 2,
        "successful_requests": 3,
        "ingress_peak_rps": 2.0,
        "latency_ms_p50": 20,
        "latency_ms_p95": 30,
        "average_tokens": 2.0,
        "redis_ops": 5,
        "postgres_transactions": 5,
    }
    for forbidden in ("http", "tenant", "prompt", "reply", "error", "token="):
        assert forbidden not in encoded.lower()


def test_unknown_optional_counters_stay_null_not_zero():
    result = probe.build_result(1, 0, 0.0, [], None, None, None, None, None, None)
    assert result["latency_ms_p50"] is None
    assert result["latency_ms_p95"] is None
    assert result["ingress_peak_rps"] is None
    assert result["average_tokens"] is None
    assert result["redis_ops"] is None
    assert result["postgres_transactions"] is None


def test_daily_usage_uses_the_admin_api_authentication_header(monkeypatch):
    captured = {}

    def fake_http(url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        return 200, {"profiles": [{"input_tokens": 3, "output_tokens": 5}]}

    monkeypatch.setattr(probe, "_http_json", fake_http)

    assert probe._daily_tokens("http://admin", "secret-token", "tenant_a") == 8
    assert captured["headers"] == {"X-TRPC-Admin-Token": "secret-token"}
    assert "tenant_a/usage?day=" in captured["url"]
