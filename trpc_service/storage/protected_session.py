"""Service-owned Session adapter using only public SDK types and extension points.

The storage lease is authoritative for native writes. Platform scheduling leases
do not grant native write permission. A replacement must acquire both before it
executes; an expired native grant cannot be resurrected by renewal.
"""

from contextlib import contextmanager, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import asyncio
import hashlib
import uuid

from trpc_agent_sdk.sessions import BaseSessionService, Session, SessionServiceConfig, ListSessionsResponse

from .protected_backend import SqlAtomicBackend, RedisAtomicBackend, WriteRejected, encode


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


class VersionedSession(Session):
    native_revision: int = 0
    app_state_revision: int = 0
    user_state_revision: int = 0


@dataclass(frozen=True)
class NativeLease:
    app_name: str
    user_id: str
    session_id: str
    owner: str
    generation: int


class ProtectedSessionService(BaseSessionService):

    def __init__(self, *, app_name, backend, session_config=None):
        super().__init__(session_config=session_config or SessionServiceConfig(store_historical_events=True))
        self.app_name, self.backend = app_name, backend
        self.prefix = "trpc-session-v1:{" + digest(app_name) + "}:"
        self._grant = ContextVar("native_grant_" + uuid.uuid4().hex, default=None)

    @classmethod
    def sql(cls, url, app_name):
        return cls(app_name=app_name, backend=SqlAtomicBackend(url))

    @classmethod
    def redis(cls, url, app_name):
        return cls(app_name=app_name, backend=RedisAtomicBackend(url))

    async def initialize(self):
        await self.backend.initialize()

    def keys(self, app_name, user_id, session_id):
        if app_name != self.app_name or not user_id or not session_id:
            raise WriteRejected("native scope mismatch")
        return (self.prefix + "app", self.prefix + "user:" + digest(user_id),
                self.prefix + "session:" + digest([user_id, session_id]))

    @staticmethod
    def require(docs, now, lease):
        record = docs[2]
        if (record.get("owner"), record.get("generation")) != (lease.owner, lease.generation):
            raise WriteRejected("native owner superseded")
        if record.get("expires", 0) <= now:
            raise WriteRejected("native owner expired")
        return record["expires"]

    def current(self, app_name, user_id, session_id):
        lease = self._grant.get()
        if lease is None or (lease.app_name, lease.user_id, lease.session_id) != (app_name, user_id, session_id):
            raise WriteRejected("native write requires an exact scoped lease")
        return lease

    async def acquire(self, *, user_id, session_id, seconds=60):
        if not 5 <= seconds <= 300:
            raise ValueError("native lease duration must be 5..300 seconds")
        owner = uuid.uuid4().hex

        def claim(docs, now):
            record = docs[2]
            if record.get("expires", 0) > now:
                from trpc_service.reliability.execution import SessionBusyError
                raise SessionBusyError("native session is busy")
            generation = record.get("generation", 0) + 1
            record.update(owner=owner, generation=generation, expires=now + seconds)
            return NativeLease(self.app_name, user_id, session_id, owner, generation), now + seconds

        return await self.backend.transact(self.keys(self.app_name, user_id, session_id), claim)

    async def renew(self, lease, seconds=60):
        if not 5 <= seconds <= 300:
            raise ValueError("native lease duration must be 5..300 seconds")

        def extend(docs, now):
            deadline = self.require(docs, now, lease)
            docs[2]["expires"] = now + seconds
            return None, deadline

        await self.backend.transact(self.keys(lease.app_name, lease.user_id, lease.session_id), extend)

    async def release(self, lease):

        def release(docs, now):
            record = docs[2]
            if (record.get("owner"), record.get("generation")) == (lease.owner, lease.generation):
                record.update(owner=None, expires=0)
            return None, 0

        await self.backend.transact(self.keys(lease.app_name, lease.user_id, lease.session_id), release)

    @contextmanager
    def write_scope(self, lease):
        self.keys(lease.app_name, lease.user_id, lease.session_id)
        token = self._grant.set(lease)
        try:
            yield lease
        finally:
            self._grant.reset(token)

    @asynccontextmanager
    async def execution(self, *, user_id, session_id, seconds=60):
        lease = await self.acquire(user_id=user_id, session_id=session_id, seconds=seconds)
        current = asyncio.current_task()
        ownership_lost = asyncio.Event()

        async def renew():
            try:
                while True:
                    await asyncio.sleep(seconds / 3)
                    await self.renew(lease, seconds)
            except Exception:
                ownership_lost.set()
                current.cancel()
                raise

        heartbeat = asyncio.create_task(renew())
        try:
            with self.write_scope(lease):
                yield lease
        except asyncio.CancelledError:
            if ownership_lost.is_set():
                raise WriteRejected("native lease renewal failed") from None
            raise
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await self.release(lease)

    @staticmethod
    def load(docs):
        app, user, record = docs
        if record.get("session") is None:
            return None
        result = VersionedSession.model_validate(record["session"])
        result.state.update({"app:" + key: value for key, value in app.get("state", {}).items()})
        result.state.update({"user:" + key: value for key, value in user.get("state", {}).items()})
        result.native_revision = record.get("revision", 0)
        result.app_state_revision, result.user_state_revision = app.get("revision", 0), user.get("revision", 0)
        return result

    @staticmethod
    def store(docs, session, now):
        value = session.model_dump(mode="json",
                                   exclude={"native_revision", "app_state_revision", "user_state_revision"})
        value["state"] = {
            key: item
            for key, item in value["state"].items() if not key.startswith(("app:", "user:", "temp:"))
        }
        value["last_update_time"] = now
        docs[2]["session"] = value
        docs[2]["revision"] = docs[2].get("revision", 0) + 1

    @staticmethod
    def update_shared(docs, values):
        for index, prefix in ((0, "app:"), (1, "user:")):
            delta = {key[len(prefix):]: value for key, value in values.items() if key.startswith(prefix)}
            if delta:
                docs[index].setdefault("state", {}).update(delta)
                docs[index]["revision"] = docs[index].get("revision", 0) + 1

    @staticmethod
    def copy_result(target, source, temp=None):
        for key in VersionedSession.model_fields:
            setattr(target, key, getattr(source, key))
        target.state.update(temp or {})

    async def create_session(self, *, app_name, user_id, state=None, session_id=None, agent_context=None):
        lease = self.current(app_name, user_id, session_id)
        seed = VersionedSession(app_name=app_name,
                                user_id=user_id,
                                id=session_id,
                                save_key=f"{app_name}/{user_id}",
                                state=state or {})

        def create(docs, now):
            deadline = self.require(docs, now, lease)
            if docs[2].get("session") is None:
                if docs[2].get("deleted"):
                    raise WriteRejected("deleted session requires a new session identity")
                self.update_shared(docs, seed.state)
                self.store(docs, seed, now)
            return self.load(docs), deadline

        return await self.backend.transact(self.keys(app_name, user_id, session_id), create)

    async def get_session(self, *, app_name, user_id, session_id, agent_context=None):
        return await self.backend.transact(self.keys(app_name, user_id, session_id), lambda docs, now:
                                           (self.load(docs), 0))

    async def list_sessions(self, *, app_name, user_id=None):
        if app_name != self.app_name:
            raise WriteRejected("native scope mismatch")
        result = []
        for record in await self.backend.list_records(self.prefix + "session:"):
            value = record.get("session")
            if value and (user_id is None or value["user_id"] == user_id):
                session = await self.get_session(app_name=app_name, user_id=value["user_id"], session_id=value["id"])
                if session:
                    session.events, session.historical_events = [], []
                    result.append(session)
        return ListSessionsResponse(sessions=result)

    async def append_event(self, session, event):
        if event.partial:
            return event
        lease = self.current(session.app_name, session.user_id, session.id)
        original_digest = digest(event.model_dump(mode="json"))
        candidate, stored_event = session.model_copy(deep=True), event.model_copy(deep=True)
        await super().append_event(candidate, stored_event)
        temp = {key: value for key, value in candidate.state.items() if key.startswith("temp:")}

        def append(docs, now):
            deadline = self.require(docs, now, lease)
            receipt = docs[2].get("receipts", {}).get(event.id)
            if receipt:
                if receipt["digest"] != original_digest:
                    raise WriteRejected("event id conflicts with its committed content")
                return self.load(docs), deadline
            if docs[2].get("session") is None or session.native_revision != docs[2].get("revision"):
                raise WriteRejected("native session revision conflict")
            delta = stored_event.actions.state_delta if stored_event.actions else {}
            for index, prefix, revision in ((0, "app:", session.app_state_revision), (1, "user:",
                                                                                      session.user_state_revision)):
                if any(key.startswith(prefix) for key in delta) and revision != docs[index].get("revision", 0):
                    raise WriteRejected("native shared state revision conflict")
            self.update_shared(docs, delta)
            self.store(docs, candidate, now)
            docs[2].setdefault("receipts", {})[event.id] = {"digest": original_digest, "revision": docs[2]["revision"]}
            return self.load(docs), deadline

        result = await self.backend.transact(self.keys(session.app_name, session.user_id, session.id), append)
        self.copy_result(session, result, temp)
        return stored_event

    async def update_session(self, session):
        lease = self.current(session.app_name, session.user_id, session.id)
        operation_id = digest(["update", session.native_revision, session.model_dump(mode="json")])

        def update(docs, now):
            deadline = self.require(docs, now, lease)
            if operation_id in docs[2].get("updates", {}):
                return self.load(docs), deadline
            if (docs[2].get("session") is None or session.native_revision != docs[2].get("revision")
                    or session.app_state_revision != docs[0].get("revision", 0)
                    or session.user_state_revision != docs[1].get("revision", 0)):
                raise WriteRejected("native snapshot or shared state revision conflict")
            previous = self.load(docs)
            incoming = {item.id: item.model_dump(mode="json") for item in [*session.historical_events, *session.events]}
            for item in [*previous.historical_events, *previous.events]:
                if not item.is_summary_event() and incoming.get(item.id) != item.model_dump(mode="json"):
                    raise WriteRejected("committed original events must remain immutable and retained")
            self.update_shared(docs, session.state)
            self.store(docs, session, now)
            docs[2].setdefault("updates", {})[operation_id] = docs[2]["revision"]
            return self.load(docs), deadline

        result = await self.backend.transact(self.keys(session.app_name, session.user_id, session.id), update)
        self.copy_result(session, result)

    async def delete_session(self, *, app_name, user_id, session_id):
        lease = self.current(app_name, user_id, session_id)

        def delete(docs, now):
            deadline = self.require(docs, now, lease)
            docs[2].update(session=None, deleted=True, revision=docs[2].get("revision", 0) + 1)
            return None, deadline

        await self.backend.transact(self.keys(app_name, user_id, session_id), delete)

    async def close(self):
        await self.backend.close()
