"""Actual public SDK objects; independent storage leases and compare-and-swap."""

from contextlib import asynccontextmanager
import asyncio

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part
from trpc_service.storage.protected_session import ProtectedSessionService, WriteRejected


def event(identifier="event", text="hello"):
    return Event(id=identifier, author="agent", content=Content(role="model", parts=[Part.from_text(text=text)]))


@asynccontextmanager
async def service(tmp_path):
    native = ProtectedSessionService.sql("sqlite:///" + (tmp_path / "protected.db").as_posix(), "tenant:app")
    await native.initialize()
    try:
        yield native
    finally:
        await native.close()


@pytest.mark.asyncio
async def test_append_receipt_revision_and_stale_owner(tmp_path):
    async with service(tmp_path) as native:
        identity = dict(app_name="tenant:app", user_id="user", session_id="session")
        old = await native.acquire(user_id="user", session_id="session")
        with native.write_scope(old):
            session = await native.create_session(**identity)
            first = event()
            await native.append_event(session, first)
            revision = session.native_revision
            await native.append_event(session, first)
            assert session.native_revision == revision and len(session.events) == 1
            with pytest.raises(WriteRejected):
                await native.append_event(session, event(text="different"))
        await native.release(old)
        new = await native.acquire(user_id="user", session_id="session")
        assert new.generation > old.generation
        with native.write_scope(new):
            current = await native.get_session(**identity)
            await native.append_event(current, event("new"))
        with native.write_scope(old):
            for operation in (lambda: native.append_event(session, event("stale")),
                              lambda: native.update_session(session), lambda: native.delete_session(**identity),
                              lambda: native.create_session(**identity)):
                with pytest.raises(WriteRejected):
                    await operation()
        await native.release(old)
        with native.write_scope(new):
            await native.append_event(current, event("new2"))
        assert [item.id for item in (await native.get_session(**identity)).events] == ["event", "new", "new2"]
        await native.release(new)


@pytest.mark.asyncio
async def test_snapshot_cas_summary_retry_and_scope(tmp_path):
    async with service(tmp_path) as native:
        async with native.execution(user_id="user", session_id="session"):
            session = await native.create_session(app_name="tenant:app", user_id="user", session_id="session")
            await native.append_event(session, event())
            stale = session.model_copy(deep=True)
            await native.append_event(session, event("two"))
            with pytest.raises(WriteRejected):
                await native.update_session(stale)
            session.historical_events = list(session.events)
            session.events = [event("summary", "summary content")]
            retry = session.model_copy(deep=True)
            await native.update_session(session)
            await native.update_session(retry)
            assert retry.native_revision == session.native_revision
            assert len(retry.historical_events) == 2
            with pytest.raises(WriteRejected):
                await native.get_session(app_name="other:app", user_id="user", session_id="session")
        with pytest.raises(WriteRejected):
            await native.append_event(session, event("no-grant"))


@pytest.mark.asyncio
async def test_shared_state_cas_across_sessions(tmp_path):
    async with service(tmp_path) as native:
        one = await native.acquire(user_id="user", session_id="one")
        two = await native.acquire(user_id="user", session_id="two")
        with native.write_scope(one):
            a = await native.create_session(app_name="tenant:app",
                                            user_id="user",
                                            session_id="one",
                                            state={
                                                "app:shared": "initial",
                                                "temp:secret": "ephemeral"
                                            })
        with native.write_scope(two):
            b = await native.create_session(app_name="tenant:app", user_id="user", session_id="two")
            b.state["app:shared"] = "new"
            await native.update_session(b)
        with native.write_scope(one):
            a.state["app:shared"] = "old"
            with pytest.raises(WriteRejected):
                await native.update_session(a)
        loaded = await native.get_session(app_name="tenant:app", user_id="user", session_id="one")
        assert loaded.state == {"app:shared": "new"}
        await native.release(one)
        await native.release(two)


@pytest.mark.asyncio
async def test_renewal_failure_becomes_retryable_error_and_releases_grant(tmp_path, monkeypatch):
    async with service(tmp_path) as native:

        async def unavailable(*args):
            raise ConnectionError("test outage")

        monkeypatch.setattr(native, "renew", unavailable)
        with pytest.raises(WriteRejected, match="renewal failed"):
            async with native.execution(user_id="u", session_id="s", seconds=5):
                await asyncio.sleep(10)
        replacement = await native.acquire(user_id="u", session_id="s")
        assert replacement.generation == 2
        await native.release(replacement)
