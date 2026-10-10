from __future__ import annotations

import asyncio
import os
import time

import pytest

from trpc_service.lifecycle import (
    ProcessLifecycle,
    is_process_live,
    is_process_ready,
    request_drain,
)


@pytest.mark.asyncio
async def test_lifecycle_heartbeat_and_external_drain(tmp_path) -> None:
    lifecycle = ProcessLifecycle("worker", tmp_path, heartbeat_interval_seconds=0.01)
    await lifecycle.start()
    try:
        assert is_process_live("worker", tmp_path, max_age_seconds=1)
        assert is_process_ready("worker", tmp_path)

        request_drain("worker", tmp_path)
        await asyncio.wait_for(lifecycle.stop_event.wait(), timeout=1)
        assert not is_process_ready("worker", tmp_path)
    finally:
        await lifecycle.close()


def test_liveness_rejects_missing_and_stale_heartbeat(tmp_path) -> None:
    assert not is_process_live("worker", tmp_path, max_age_seconds=1)
    heartbeat = tmp_path / "worker.heartbeat"
    heartbeat.write_text("stale\n", encoding="ascii")
    stale = time.time() - 10
    os.utime(heartbeat, (stale, stale))
    assert not is_process_live("worker", tmp_path, max_age_seconds=1)
