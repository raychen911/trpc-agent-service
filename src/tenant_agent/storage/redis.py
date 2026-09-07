"""Redis-backed session, summary, memory, and renewable lease adapters."""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis, RedisCluster
from redis.exceptions import WatchError

from tenant_agent.models import MemoryRecord, SessionEvent, SessionSnapshot, SummaryRecord
from tenant_agent.storage.base import ConcurrentWriteError, SessionLeaseTimeout, lexical_terms

APPEND_EVENT_SCRIPT = """
local session_raw = redis.call('GET', KEYS[1])
if not session_raw then return {-1, '', ''} end
local duplicate = redis.call('HGET', KEYS[3], ARGV[2])
if duplicate then return {2, session_raw, duplicate} end
local session = cjson.decode(session_raw)
if tonumber(session.revision) ~= tonumber(ARGV[1]) then
  return {0, session_raw, ''}
end
local delta = cjson.decode(ARGV[4])
for key, value in pairs(delta) do session.state[key] = value end
session.revision = tonumber(session.revision) + 1
session.last_event_sequence = tonumber(session.last_event_sequence) + 1
session.updated_at = ARGV[5]
local event = cjson.decode(ARGV[3])
event.sequence = session.last_event_sequence
local next_session = cjson.encode(session)
local next_event = cjson.encode(event)
redis.call('SET', KEYS[1], next_session)
redis.call('ZADD', KEYS[2], event.sequence, next_event)
redis.call('HSET', KEYS[3], ARGV[2], next_event)
redis.call('PUBLISH', KEYS[4], next_session)
return {1, next_session, next_event}
"""

PUT_SUMMARY_SCRIPT = """
local summary = cjson.decode(ARGV[1])
local session_raw = redis.call('GET', KEYS[3])
if session_raw then
  local session = cjson.decode(session_raw)
  if tonumber(summary.through_event_sequence) > tonumber(session.last_event_sequence) then
    return -3
  end
end
local current_raw = redis.call('GET', KEYS[1])
if current_raw then
  local current = cjson.decode(current_raw)
  if tonumber(current.version) > tonumber(summary.version) then return 2 end
  if tonumber(current.version) == tonumber(summary.version) then
    if tonumber(current.through_event_sequence) ~= tonumber(summary.through_event_sequence)
      or current.content ~= summary.content then return -2 end
    return 2
  end
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('PUBLISH', KEYS[2], ARGV[1])
return 1
"""

RENEW_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

RELEASE_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def _json(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class RedisPlane:
    """Shared, immediately visible data plane for horizontally scaled workers."""

    backend_name = "redis"

    def __init__(
        self,
        url: str,
        *,
        namespace: str = "tap:v1",
        cluster: bool = False,
    ) -> None:
        client_type = RedisCluster if cluster else Redis
        client: Any = client_type.from_url(
            url,
            decode_responses=True,
            health_check_interval=30,
        )
        self.redis = client
        self.cluster = cluster
        self.namespace = namespace.strip(":")
        self._append_event = client.register_script(APPEND_EVENT_SCRIPT)
        self._put_summary = client.register_script(PUT_SUMMARY_SCRIPT)
        self._renew_lock = client.register_script(RENEW_LOCK_SCRIPT)
        self._release_lock = client.register_script(RELEASE_LOCK_SCRIPT)

    async def initialize(self) -> None:
        await self.redis.ping()

    async def close(self) -> None:
        await self.redis.aclose()

    async def healthcheck(self) -> bool:
        return bool(await self.redis.ping())

    @staticmethod
    def _tenant(tenant_id: str) -> str:
        return hashlib.sha256(tenant_id.encode()).hexdigest()[:20]

    def _key(self, tenant_id: str, resource: str, *parts: str) -> str:
        suffix = ":".join(parts)
        return f"{self.namespace}:{{{self._tenant(tenant_id)}}}:{resource}:{suffix}"

    def _session_key(self, tenant_id: str, session_id: str) -> str:
        return self._key(tenant_id, "session", session_id)

    async def get_or_create_session(
        self,
        *,
        tenant_id: str,
        app_id: str,
        session_id: str,
        user_id: str,
        channel: str,
    ) -> SessionSnapshot:
        snapshot = SessionSnapshot(
            tenant_id=tenant_id,
            app_id=app_id,
            session_id=session_id,
            user_id=user_id,
            channel=channel,
        )
        key = self._session_key(tenant_id, session_id)
        await self.redis.set(key, _json(snapshot), nx=True)
        raw = await self.redis.get(key)
        if raw is None:
            raise RuntimeError("Redis lost a newly created session")
        current = SessionSnapshot.model_validate_json(raw)
        if current.app_id != app_id or current.user_id != user_id:
            raise ConcurrentWriteError("session identity is immutable")
        return current

    async def get_session(self, tenant_id: str, session_id: str) -> SessionSnapshot | None:
        raw = await self.redis.get(self._session_key(tenant_id, session_id))
        return SessionSnapshot.model_validate_json(raw) if raw else None

    async def append_event(
        self,
        *,
        snapshot: SessionSnapshot,
        event_id: str,
        kind: str,
        actor_id: str,
        payload: dict[str, Any],
        state_delta: dict[str, Any],
        trace_id: str,
    ) -> tuple[SessionSnapshot, SessionEvent]:
        now = datetime.now(UTC)
        event = SessionEvent(
            event_id=event_id,
            tenant_id=snapshot.tenant_id,
            session_id=snapshot.session_id,
            sequence=0,
            kind=kind,
            actor_id=actor_id,
            payload=payload,
            state_delta=state_delta,
            trace_id=trace_id,
            created_at=now,
        )
        base = self._key(snapshot.tenant_id, "session", snapshot.session_id)
        result = await self._append_event(
            keys=[
                base,
                self._key(snapshot.tenant_id, "events", snapshot.session_id),
                self._key(snapshot.tenant_id, "event-ids", snapshot.session_id),
                self._key(snapshot.tenant_id, "session-updates", snapshot.session_id),
            ],
            args=[
                snapshot.revision,
                event_id,
                _json(event),
                _json(state_delta),
                now.isoformat(),
            ],
        )
        code = int(result[0])
        if code == -1:
            raise KeyError("session does not exist")
        current = SessionSnapshot.model_validate_json(result[1])
        if code == 0:
            raise ConcurrentWriteError(
                f"expected session revision {snapshot.revision}, found {current.revision}"
            )
        return current, SessionEvent.model_validate_json(result[2])

    async def list_events(
        self,
        tenant_id: str,
        session_id: str,
        *,
        after_sequence: int = 0,
    ) -> Sequence[SessionEvent]:
        rows = await self.redis.zrangebyscore(
            self._key(tenant_id, "events", session_id),
            min=f"({after_sequence}",
            max="+inf",
        )
        return tuple(SessionEvent.model_validate_json(row) for row in rows)

    async def get_event(self, tenant_id: str, session_id: str, event_id: str) -> SessionEvent | None:
        raw = await self.redis.hget(self._key(tenant_id, "event-ids", session_id), event_id)
        return SessionEvent.model_validate_json(raw) if raw else None

    async def iter_sessions(self, tenant_id: str) -> AsyncIterator[SessionSnapshot]:
        pattern = self._key(tenant_id, "session", "*")
        async for key in self.redis.scan_iter(match=pattern, count=200):
            raw = await self.redis.get(key)
            if raw:
                yield SessionSnapshot.model_validate_json(raw)

    async def put_summary(self, summary: SummaryRecord) -> None:
        result = await self._put_summary(
            keys=[
                self._key(summary.tenant_id, "summary", summary.session_id),
                self._key(summary.tenant_id, "summary-updates", summary.session_id),
                self._session_key(summary.tenant_id, summary.session_id),
            ],
            args=[_json(summary)],
        )
        if int(result) == -2:
            raise ConcurrentWriteError("summary version is immutable")
        if int(result) == -3:
            raise ConcurrentWriteError("summary cannot cover events that have not committed")

    async def get_summary(self, tenant_id: str, session_id: str) -> SummaryRecord | None:
        raw = await self.redis.get(self._key(tenant_id, "summary", session_id))
        return SummaryRecord.model_validate_json(raw) if raw else None

    async def iter_summaries(self, tenant_id: str) -> AsyncIterator[SummaryRecord]:
        async for key in self.redis.scan_iter(match=self._key(tenant_id, "summary", "*"), count=200):
            raw = await self.redis.get(key)
            if raw:
                yield SummaryRecord.model_validate_json(raw)

    async def put_memory(self, memory: MemoryRecord) -> None:
        key = self._key(memory.tenant_id, "memory", memory.user_id, memory.memory_id)
        serialized = _json(memory)
        index_key = self._key(memory.tenant_id, "memory-index", memory.user_id)
        updates_key = self._key(memory.tenant_id, "memory-updates", memory.user_id)
        while True:
            async with self.redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(key)
                    current_raw = await pipe.get(key)
                    if current_raw:
                        current = MemoryRecord.model_validate_json(current_raw)
                        if current.revision > memory.revision:
                            return
                        if current.revision == memory.revision:
                            if current.content != memory.content or current.metadata != memory.metadata:
                                raise ConcurrentWriteError("memory revision is immutable")
                            return
                    pipe.multi()
                    pipe.set(key, serialized)
                    pipe.sadd(index_key, key)
                    pipe.publish(updates_key, serialized)
                    await pipe.execute()
                    return
                except WatchError:
                    continue

    async def search_memory(
        self, tenant_id: str, user_id: str, query: str, *, limit: int = 10
    ) -> Sequence[MemoryRecord]:
        keys = await self.redis.smembers(self._key(tenant_id, "memory-index", user_id))
        if not keys:
            return ()
        values = await self.redis.mget(list(keys))
        query_terms = lexical_terms(query)
        candidates: list[tuple[int, MemoryRecord]] = []
        for raw in values:
            if not raw:
                continue
            memory = MemoryRecord.model_validate_json(raw)
            score = len(query_terms & lexical_terms(memory.content))
            if query_terms and score == 0:
                continue
            candidates.append((score, memory))
        candidates.sort(key=lambda item: (item[0], item[1].updated_at), reverse=True)
        return tuple(memory for _, memory in candidates[:limit])

    async def iter_memories(self, tenant_id: str) -> AsyncIterator[MemoryRecord]:
        pattern = self._key(tenant_id, "memory", "*")
        async for key in self.redis.scan_iter(match=pattern, count=200):
            raw = await self.redis.get(key)
            if raw:
                yield MemoryRecord.model_validate_json(raw)

    @asynccontextmanager
    async def acquire_session(
        self,
        *,
        tenant_id: str,
        session_id: str,
        owner: str,
        wait_timeout: float,
        lease_seconds: float,
    ) -> AsyncIterator[None]:
        key = self._key(tenant_id, "lease", session_id)
        lease_ms = max(1_000, int(lease_seconds * 1_000))
        deadline = asyncio.get_running_loop().time() + wait_timeout
        while asyncio.get_running_loop().time() < deadline:
            if await self.redis.set(key, owner, nx=True, px=lease_ms):
                break
            await asyncio.sleep(0.04 + random.random() * 0.04)  # noqa: S311 - lock jitter only
        else:
            raise SessionLeaseTimeout("timed out waiting for the Redis session lease")

        lost = asyncio.Event()

        async def renew() -> None:
            interval = max(0.5, lease_seconds / 3)
            while True:
                await asyncio.sleep(interval)
                try:
                    renewed = await self._renew_lock(keys=[key], args=[owner, lease_ms])
                except Exception:
                    lost.set()
                    return
                if not renewed:
                    lost.set()
                    return

        renew_task = asyncio.create_task(renew(), name=f"renew-session-lease:{session_id}")
        try:
            yield
            if lost.is_set():
                raise ConcurrentWriteError("Redis session lease was lost during execution")
        finally:
            renew_task.cancel()
            await asyncio.gather(renew_task, return_exceptions=True)
            await self._release_lock(keys=[key], args=[owner])
