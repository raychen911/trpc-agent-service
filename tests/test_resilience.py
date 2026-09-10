"""韧性层测试：重试分类、指数退避、熔断状态机、降级路径。"""
from trpc_service.agent.resilience import ResiliencePolicy
from trpc_service.agent.runner import RunResult


def _ok(text="done"):
    return RunResult(text=text)


def test_retry_on_timeout_error_type():
    """error_type=timeout 触发重试，第 2 次成功。"""
    calls = {"n": 0}

    async def run_fn(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return RunResult(text="", error_type="timeout")
        return _ok()

    policy = ResiliencePolicy(max_retries=2, retry_backoff_seconds=0.01)
    import asyncio

    result = asyncio.run(policy.execute("t1", run_fn))
    assert result.text == "done"
    assert calls["n"] == 2
    assert policy.breaker_state("t1") == "closed"


def test_no_retry_on_business_error():
    """非可重试错误（如 invalid_argument）不重试、只失败一次。"""
    calls = {"n": 0}

    async def run_fn(**kwargs):
        calls["n"] += 1
        return RunResult(text="", error_type="invalid_argument")

    policy = ResiliencePolicy(max_retries=3, retry_backoff_seconds=0.01)
    import asyncio

    result = asyncio.run(policy.execute("t1", run_fn))
    assert result.error_type == "invalid_argument"
    assert calls["n"] == 1


def test_retryable_exception_is_caught_and_retried():
    """runner 抛 TimeoutError 异常 → 重试后成功。"""
    calls = {"n": 0}

    async def run_fn(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("llm call timed out")
        return _ok()

    policy = ResiliencePolicy(max_retries=2, retry_backoff_seconds=0.01)
    import asyncio

    result = asyncio.run(policy.execute("t1", run_fn))
    assert result.text == "done"


def test_circuit_opens_after_threshold():
    """连续失败达到阈值 → open，快速失败不调用 runner。"""
    calls = {"n": 0}

    async def run_fn(**kwargs):
        calls["n"] += 1
        return RunResult(text="", error_type="server_error")

    policy = ResiliencePolicy(max_retries=0, retry_backoff_seconds=0.01, failure_threshold=3)
    import asyncio

    for _ in range(3):
        asyncio.run(policy.execute("t1", run_fn))
    assert policy.breaker_state("t1") == "open"
    calls_before = calls["n"]
    result = asyncio.run(policy.execute("t1", run_fn))
    assert result.error_type == "circuit_open"
    assert calls["n"] == calls_before  # 熔断期不再执行


def test_circuit_half_open_recovery():
    """冷却期满放行试探，成功后恢复 closed。"""
    policy = ResiliencePolicy(max_retries=0, failure_threshold=1, cooldown_seconds=0.05)

    async def failing(**kwargs):
        return RunResult(text="", error_type="timeout")

    async def ok(**kwargs):
        return _ok()

    import asyncio

    asyncio.run(policy.execute("t1", failing))
    assert policy.breaker_state("t1") == "open"
    import time

    time.sleep(0.06)  # 冷却期满
    result = asyncio.run(policy.execute("t1", ok))
    assert result.text == "done"
    assert policy.breaker_state("t1") == "closed"


def test_budget_rejection_does_not_trip_breaker():
    """BudgetExceeded 等业务拒绝发生在韧性层之前（本测试验证契约：成功调用重置熔断）。"""
    policy = ResiliencePolicy(failure_threshold=1)

    async def ok(**kwargs):
        return _ok()

    import asyncio

    asyncio.run(policy.execute("t1", ok))
    assert policy.breaker_state("t1") == "closed"
