from __future__ import annotations

import asyncio
import pytest

from trpc_service.storage.locks import SessionLockManager


async def test_same_session_serializes_and_different_sessions_run_in_parallel() -> None:
    manager = SessionLockManager()
    active = 0
    same_peak = 0

    async def same() -> None:
        nonlocal active, same_peak
        async with manager.acquire("sess_" + "a" * 64):
            active += 1
            same_peak = max(same_peak, active)
            await asyncio.sleep(0.01)
            active -= 1

    await asyncio.gather(same(), same(), same())
    assert same_peak == 1

    entered = asyncio.Event()
    count = 0
    async def different(key: str) -> None:
        nonlocal count
        async with manager.acquire(key):
            count += 1
            if count == 2:
                entered.set()
            await asyncio.wait_for(entered.wait(), 0.2)

    await asyncio.gather(different("sess_" + "b" * 64), different("sess_" + "c" * 64))


async def test_lease_releases_after_exception() -> None:
    manager = SessionLockManager()
    try:
        async with manager.acquire("sess_" + "d" * 64):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    async with manager.acquire("sess_" + "d" * 64):
        assert True
    with pytest.raises(ValueError, match="tenant-scoped"):
        async with manager.acquire("raw-conversation-id"):
            pass
