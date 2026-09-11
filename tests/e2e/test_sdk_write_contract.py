"""Executable release gaps; xfail is evidence of missing safety, never acceptance.

Run with --strict-acceptance to turn these known gaps into failing release checks.
Only public SDK methods are used. Two processes hold independent snapshots/pools.
"""

import asyncio
from datetime import timedelta
import multiprocessing
import uuid

import pytest
from sqlalchemy import select
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import RedisSessionService, SqlSessionService
from trpc_agent_sdk.types import Content, Part

from tests.e2e.conftest import required_env
from tests.e2e.sdk_write_actor import writer
from trpc_service.persistence.models import SessionLeaseRow, utcnow
from trpc_service.reliability import SessionCoordinator


def native(kind, url):
    return (SqlSessionService(db_url=url, is_async=False, expire_on_commit=False)
            if kind == "sql" else RedisSessionService(db_url=url))


def event(identifier, token):
    return Event(id=identifier,
                 author="test_agent",
                 invocation_id="new-execution",
                 content=Content(role="model", parts=[Part.from_text(text="current owner")]),
                 custom_metadata={"platform_fencing_token": token})


def backend_url(kind, request, tmp_path):
    if request.config.getoption("--backend-mode") == "real":
        return required_env("TRPC_SESSION_DATABASE_URL" if kind == "sql" else "TRPC_REDIS_URL")
    if kind == "redis":
        pytest.skip("native Redis contract requires --backend-mode=real")
    return f"sqlite:///{tmp_path / 'native.db'}"


async def receive(connection):
    if not await asyncio.to_thread(connection.poll, 30):
        pytest.fail("SDK actor timed out")
    return connection.recv()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sql", "redis"])
@pytest.mark.parametrize("method", ["append", "update"])
@pytest.mark.known_gap("official native writes do not atomically reject a superseded platform fencing token")
async def test_native_rejects_old_process_after_lease_takeover(kind, method, request, tmp_path, platform_database):
    url = backend_url(kind, request, tmp_path)
    identity = dict(app_name="contract:" + uuid.uuid4().hex, user_id="user", session_id="session")
    service = native(kind, url)
    parent, child = multiprocessing.get_context("spawn").Pipe()
    process = multiprocessing.get_context("spawn").Process(target=writer, args=(child, kind, url, identity))
    coordinator = SessionCoordinator(platform_database)
    try:
        await service.create_session(**identity)
        old = await coordinator.acquire("contract", identity["app_name"], "user", "session", "old", 60)
        process.start()
        child.close()
        if await receive(parent) != ("ready", True):
            pytest.fail("independent writer could not read the native snapshot")
        # Controlled lease expiry, with no timing-dependent sleep.
        with platform_database.sessions.begin() as session:
            session.scalar(select(SessionLeaseRow)).expires_at = utcnow() - timedelta(seconds=1)
        current = await coordinator.acquire("contract", identity["app_name"], "user", "session", "new", 60)
        if current.fencing_token <= old.fencing_token:
            pytest.fail("test setup did not supersede the platform lease")
        snapshot = await service.get_session(**identity)
        await service.append_event(snapshot, event("current-owner-event", current.fencing_token))
        parent.send((method, old.fencing_token))
        result = await receive(parent)
        if result[0] == "actor_error":
            pytest.fail("SDK actor infrastructure error: " + result[1])
        loaded = await service.get_session(**identity)
        ids = [item.id for item in loaded.events]
        assert result[0] == "rejected" and ids == ["current-owner-event"], (
            f"{kind}/{method}: stale_write={result[0]}, current_event_retained="
            f"{'current-owner-event' in ids}, stale_event_persisted={'stale-owner-event' in ids}")
    finally:
        if process.pid is not None:
            await asyncio.to_thread(process.join, 5)
            if process.is_alive():
                process.terminate()
                await asyncio.to_thread(process.join, 5)
        parent.close()
        child.close()
        await service.delete_session(**identity)
        await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sql", "redis"])
@pytest.mark.known_gap("official append does not expose an atomic idempotent write receipt")
async def test_native_duplicate_event_returns_success_once(kind, request, tmp_path):
    service = native(kind, backend_url(kind, request, tmp_path))
    identity = dict(app_name="duplicate:" + uuid.uuid4().hex, user_id="user", session_id="session")
    try:
        snapshot = await service.create_session(**identity)
        original = event("same-event", 1)
        await service.append_event(snapshot, original.model_copy(deep=True))
        snapshot = await service.get_session(**identity)
        rejected = None
        try:
            await service.append_event(snapshot, original.model_copy(deep=True))
        except Exception as error:
            rejected = type(error).__name__
        loaded = await service.get_session(**identity)
        assert rejected is None and [item.id for item in loaded.events] == ["same-event"], (
            f"{kind}: retry_error={rejected}, persisted_count={len(loaded.events)}")
    finally:
        await service.delete_session(**identity)
        await service.close()
