from __future__ import annotations

import asyncio

import pytest

from trpc_service.agent.session_recovery import (
    RECOVERY_COMPONENTS,
    SessionRecoveryService,
)
from trpc_service.config.settings import Role, ServiceSettings


class FakeMailboxRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []
        self.fail_component: str | None = None

    async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int:
        return await self._record("lease_sweeper", owner_id, limit, 2)

    async def schedule_retries(self, *, owner_id: str, limit: int) -> int:
        return await self._record("retry_scheduler", owner_id, limit, 3)

    async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int:
        return await self._record("session_reconciler", owner_id, limit, 5)

    async def _record(self, component: str, owner_id: str, limit: int, count: int) -> int:
        self.calls.append((component, owner_id, limit))
        if component == self.fail_component:
            raise RuntimeError(component)
        return count


@pytest.mark.asyncio
async def test_recovery_run_once_calls_all_components_with_bounded_claims() -> None:
    repository = FakeMailboxRepository()
    service = SessionRecoveryService(
        repository, owner_id="recovery-test", batch_size=7, poll_seconds=0.1
    )

    result = await service.run_once()

    assert result.status == "pass"
    assert result.counts == {
        "lease_sweeper": 2,
        "retry_scheduler": 3,
        "session_reconciler": 5,
    }
    assert result.total == 10
    assert repository.calls == [
        (component, "recovery-test", 7) for component in RECOVERY_COMPONENTS
    ]


@pytest.mark.asyncio
async def test_recovery_run_once_keeps_partial_failure_machine_readable() -> None:
    repository = FakeMailboxRepository()
    repository.fail_component = "retry_scheduler"
    service = SessionRecoveryService(repository, owner_id="recovery-test", batch_size=4)

    result = await service.run_once()

    assert result.status == "fail"
    assert result.counts == {
        "lease_sweeper": 2,
        "retry_scheduler": 0,
        "session_reconciler": 5,
    }
    assert result.failures == {"retry_scheduler": "RuntimeError"}
    assert len(repository.calls) == 3


@pytest.mark.asyncio
async def test_recovery_service_stops_all_independent_loops_cooperatively() -> None:
    repository = FakeMailboxRepository()
    service = SessionRecoveryService(repository, owner_id="recovery-test", poll_seconds=0.01)

    task = asyncio.create_task(service.run())
    await asyncio.sleep(0.02)
    service.stop()
    await asyncio.wait_for(task, timeout=1)

    assert service.loops
    assert {call[0] for call in repository.calls} == set(RECOVERY_COMPONENTS)


def test_recovery_rejects_unsafe_limits_and_defaults_are_conservative() -> None:
    repository = FakeMailboxRepository()
    with pytest.raises(ValueError, match="batch_size"):
        SessionRecoveryService(repository, batch_size=0)
    with pytest.raises(ValueError, match="poll_seconds"):
        SessionRecoveryService(repository, poll_seconds=0)

    settings = ServiceSettings()
    assert settings.recovery_batch_size == 25
    assert settings.recovery_poll_seconds == 5
    assert settings.recovery_ready_replay_cooldown_seconds == 30
    assert Role.SESSION_RECOVERY.value == "session-recovery"
