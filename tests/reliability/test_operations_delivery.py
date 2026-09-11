"""Release gates and SLO/alert delivery contracts."""

from trpc_service.operations.release import rollout
from trpc_service.operations.slo import report


def test_canary_rolls_back_when_a_probe_fails(monkeypatch):
    calls = []

    def call(*args, **kwargs):
        calls.append(args[4]["percent"])
        return 200, {}

    probes = iter([True, False])
    monkeypatch.setattr("trpc_service.operations.release._call", call)
    monkeypatch.setattr("trpc_service.operations.release._probe", lambda _: next(probes))
    result = rollout("http://gateway", "token", "tenant", 2, 1, (10, 50), ("http://node/health",))
    assert result.rolled_back is True
    assert calls == [10, 50, 0]


def test_slo_report_calculates_targets():
    result = report({
        "gateway.callback_total{outcome=accepted}": {"count": 99, "sum": 99},
        "gateway.callback_total{outcome=failed}": {"count": 1, "sum": 1},
        "gateway.inbox_latency_ms": {"p95": 120},
    }, availability_target=0.98, latency_target_ms=200)
    assert result["availability"] == 0.99
    assert result["availability_met"] is True
    assert result["latency_met"] is True
