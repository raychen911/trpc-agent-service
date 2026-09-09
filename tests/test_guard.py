# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import asyncio

import pytest

from trpc_service.storage import InMemorySessionExecutionGuard
from trpc_service.storage import SessionLockTimeoutError


@pytest.mark.asyncio
async def test_guard_serializes_same_session():
    guard = InMemorySessionExecutionGuard()
    active = 0
    maximum = 0

    async def run_once():
        nonlocal active, maximum
        async with guard.hold("same", wait_timeout=1, lease_seconds=1):
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1

    await asyncio.gather(run_once(), run_once(), run_once())
    assert maximum == 1


@pytest.mark.asyncio
async def test_guard_wait_timeout():
    guard = InMemorySessionExecutionGuard()
    async with guard.hold("same", wait_timeout=1, lease_seconds=1):
        with pytest.raises(SessionLockTimeoutError):
            async with guard.hold("same", wait_timeout=0.01, lease_seconds=1):
                pass
