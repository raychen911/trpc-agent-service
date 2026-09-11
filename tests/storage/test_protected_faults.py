"""Independent clients and processes exercise ownership loss and durable recovery."""

import asyncio
import multiprocessing
import os
import uuid

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from trpc_service.storage.protected_session import ProtectedSessionService, WriteRejected
from trpc_service.reliability.execution import SessionBusyError


def make_service(kind, url, app):
    return getattr(ProtectedSessionService, kind)(url, app)


@pytest.fixture(params=["sql", "redis"])
def placement(request, tmp_path):
    kind = request.param
    if request.config.getoption("--backend-mode") == "real":
        name = "TRPC_SESSION_DATABASE_URL" if kind == "sql" else "TRPC_REDIS_URL"
        if not os.environ.get(name):
            pytest.fail("real protected acceptance requires " + name)
        url = os.environ[name]
    elif kind == "sql":
        url = "sqlite:///" + (tmp_path / "fault.db").as_posix()
    else:
        pytest.skip("Redis requires --backend-mode=real; no emulation")
    return kind, url, "fault:" + uuid.uuid4().hex


def event(identifier):
    return Event(id=identifier, author="agent", content=Content(role="model", parts=[Part.from_text(text=identifier)]))


def paused_writer(kind, url, app, ready, resume, result):

    async def run():
        service = make_service(kind, url, app)
        try:
            lease = await service.acquire(user_id="u", session_id="s", seconds=5)
            with service.write_scope(lease):
                session = await service.create_session(app_name=app, user_id="u", session_id="s")
                await service.append_event(session, event("before"))
                ready.set()
                await asyncio.to_thread(resume.wait, 40)
                rejected = 0
                for operation in (lambda: service.append_event(session, event("stale")),
                                  lambda: service.update_session(session),
                                  lambda: service.delete_session(app_name=app, user_id="u", session_id="s"),
                                  lambda: service.renew(lease)):
                    try:
                        await operation()
                    except WriteRejected:
                        rejected += 1
                await service.release(lease)
                result.put(rejected)
        finally:
            await service.close()

    asyncio.run(run())


@pytest.mark.asyncio
async def test_expired_process_cannot_write_after_takeover(placement):
    kind, url, app = placement
    service = make_service(kind, url, app)
    await service.initialize()
    ctx = multiprocessing.get_context("spawn")
    ready, resume, result = ctx.Event(), ctx.Event(), ctx.Queue()
    child = ctx.Process(target=paused_writer, args=(kind, url, app, ready, resume, result))
    child.start()
    try:
        assert await asyncio.to_thread(ready.wait, 30)
        with pytest.raises(SessionBusyError):
            await service.acquire(user_id="u", session_id="s")
        await asyncio.sleep(5.2)
        async with service.execution(user_id="u", session_id="s") as lease:
            assert lease.generation == 2
            current = await service.get_session(app_name=app, user_id="u", session_id="s")
            await service.append_event(current, event("replacement"))
            resume.set()
            assert await asyncio.to_thread(result.get, True, 20) == 4
            await service.append_event(current, event("still-owner"))
        reader = make_service(kind, url, app)
        try:
            loaded = await reader.get_session(app_name=app, user_id="u", session_id="s")
            assert [item.id for item in loaded.events] == ["before", "replacement", "still-owner"]
        finally:
            await reader.close()
    finally:
        resume.set()
        await asyncio.to_thread(child.join, 5)
        if child.is_alive():
            child.terminate()
            await asyncio.to_thread(child.join, 5)
        result.close()
        await service.close()


@pytest.mark.asyncio
async def test_delete_tombstone_and_partial_stream_do_not_resurrect(placement):
    kind, url, app = placement
    service = make_service(kind, url, app)
    await service.initialize()
    try:
        identity = dict(app_name=app, user_id="u", session_id="s")
        async with service.execution(user_id="u", session_id="s"):
            session = await service.create_session(**identity)
            partial = event("partial")
            partial.partial = True
            await service.append_event(session, partial)
            assert not (await service.get_session(**identity)).events
            await service.delete_session(**identity)
        async with service.execution(user_id="u", session_id="s") as lease:
            assert lease.generation == 2
            with pytest.raises(WriteRejected):
                await service.create_session(**identity)
            with pytest.raises(WriteRejected):
                await service.append_event(session, event("late"))
            assert await service.get_session(**identity) is None
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_two_clients_claim_once_and_preserve_receipts(placement):
    kind, url, app = placement
    clients = [make_service(kind, url, app), make_service(kind, url, app)]
    await clients[0].initialize()
    try:
        grants = await asyncio.gather(*(client.acquire(user_id="u", session_id="s") for client in clients),
                                      return_exceptions=True)
        winners = [index for index, grant in enumerate(grants) if not isinstance(grant, Exception)]
        assert len(winners) == 1
        winner = winners[0]
        assert isinstance(grants[1 - winner], SessionBusyError)
        client, lease = clients[winner], grants[winner]
        identity = dict(app_name=app, user_id="u", session_id="s")
        with client.write_scope(lease):
            session = await client.create_session(**identity)
            committed = event("stable-id")
            original = session.model_copy(deep=True)
            await client.append_event(session, committed)
            await client.append_event(original, committed)
            assert original.native_revision == session.native_revision
            assert len(original.events) == 1
            stale = original.model_copy(deep=True)
            original.state["key"] = "new"
            await client.update_session(original)
            with pytest.raises(WriteRejected):
                await client.update_session(stale)
        await client.release(lease)
        observer = await clients[1 - winner].get_session(**identity)
        assert observer.state["key"] == "new" and len(observer.events) == 1
    finally:
        for client in clients:
            await client.close()
