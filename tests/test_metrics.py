"""metrics 采集器测试：进程内聚合与快照。"""
import pytest

from trpc_service.metrics.collector import MetricsCollector, metrics_collector


@pytest.fixture()
def collector():
    c = MetricsCollector()
    yield c
    c.reset()


def test_request_counts_and_error_rate(collector):
    collector.inc_request("t1", "web", latency_ms=100)
    collector.inc_request("t1", "web", error=True, latency_ms=300)
    snap = collector.snapshot()["t1"]
    assert snap["requests"] == 2
    assert snap["errors"] == 1
    assert snap["error_rate"] == 0.5
    assert snap["avg_latency_ms"] == 200.0


def test_im_delivery_success_rate(collector):
    collector.inc_im_delivery("t1", delivered=True)
    collector.inc_im_delivery("t1", delivered=True)
    collector.inc_im_delivery("t1", delivered=False)
    snap = collector.snapshot()["t1"]
    assert snap["im_delivered"] == 2
    assert snap["im_failed"] == 1
    assert snap["im_success_rate"] == round(2 / 3, 4)


def test_tokens_and_tool_calls(collector):
    collector.add_tokens("t1", 100)
    collector.add_tokens("t1", 50)
    collector.add_tokens("t1", 0)  # 非正值忽略
    collector.inc_tool_call("t1", "query_order")
    snap = collector.snapshot()["t1"]
    assert snap["tokens"] == 150
    assert snap["tool_calls"] == 1


def test_tenants_isolated(collector):
    collector.inc_request("t1", "web")
    collector.inc_request("t2", "feishu")
    snap = collector.snapshot()
    assert set(snap) == {"t1", "t2"}
    assert snap["t1"]["requests"] == 1 and snap["t2"]["requests"] == 1


def test_global_collector_reentrant_lock_no_deadlock():
    """回归：调用方持锁内再调 _stats（原 Lock 自锁死锁）不得挂起。"""
    metrics_collector.add_tokens("tenant_g", 10)
    metrics_collector.inc_tool_call("tenant_g", "t")
    metrics_collector.inc_im_delivery("tenant_g", delivered=True)
    assert metrics_collector.snapshot()["tenant_g"]["tokens"] == 10
    metrics_collector.reset()
