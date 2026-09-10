import pytest

from tests.integration.shared.faults import AgentSpy, BackendFailureProxy, FaultController


async def test_fault_controller_proves_exact_injection_point_and_only_fails_once() -> None:
    class Backend:
        async def claim(self, value: str) -> str:
            return value

    controller = FaultController()
    proxy = BackendFailureProxy(Backend(), controller, "redis")
    controller.arm("redis.claim")
    with pytest.raises(RuntimeError, match="injected:redis.claim"):
        await proxy.claim("first")
    controller.assert_observed("redis.claim")
    assert await proxy.claim("second") == "second"
    assert controller.observed == ["redis.claim", "redis.claim"]

    spy = AgentSpy()
    assert await spy.execute() == "executed"
    assert spy.calls == 1
