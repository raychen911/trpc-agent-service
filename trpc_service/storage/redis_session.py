"""Official tRPC-Agent SessionService backed by tenant-scoped Redis keys."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import BaseSessionService, ListSessionsResponse, Session

from trpc_service.storage.contracts import StateBackendUnavailable, StaleFence
from trpc_service.storage.redis_leases import current_session_fence
from trpc_service.storage.redis_codec import RedisKeyCodec
from trpc_service.storage.redis_scripts.loader import RedisScriptLoader


_SCRIPTS = Path(__file__).with_name("redis_scripts")
_APPEND = (_SCRIPTS / "session_append.lua").read_text(encoding="utf-8")
_READ = (_SCRIPTS / "session_read.lua").read_text(encoding="utf-8")
_UPDATE = (_SCRIPTS / "session_update.lua").read_text(encoding="utf-8")


class FencedRedisSessionService(BaseSessionService):
    """SDK-compatible storage; fencing is activated when a fence is bound."""

    def __init__(self, redis: Any, *, tenant_id: str, agent_id: str, namespace: str, ttl_seconds: int = 86400,
                 require_fence: bool = False) -> None:
        super().__init__()
        self.redis = redis
        self.tenant_id = tenant_id
        self.agent_id = agent_id
        self.codec = RedisKeyCodec(namespace)
        self.loader = RedisScriptLoader(redis)
        self.ttl_seconds = ttl_seconds
        self.require_fence = require_fence

    def _key(self, session_id: str) -> str:
        return self.codec.session(self.tenant_id, self.agent_id, session_id)

    def _index(self, app_name: str, user_id: str) -> str:
        return f"{self.codec.namespace}:session-index:{self.tenant_id}:{self.agent_id}:{app_name}:{user_id}"

    async def create_session(self, *, app_name: str, user_id: str, state: dict[str, Any] | None = None,
                             session_id: str | None = None, agent_context: Any | None = None) -> Session:
        del agent_context
        session = Session(id=(session_id or str(uuid4())), app_name=app_name, user_id=user_id,
                          state=state or {}, last_update_time=time.time(), save_key=f"{app_name}:{user_id}")
        key = self._key(session.id)
        try:
            created = await self.redis.set(key, session.model_dump_json(by_alias=True), ex=self.ttl_seconds, nx=True)
            if not created:
                existing = await self.get_session(app_name=app_name, user_id=user_id, session_id=session.id)
                if existing is not None:
                    return existing
            await self.redis.sadd(self._index(app_name, user_id), key)
            await self.redis.expire(self._index(app_name, user_id), self.ttl_seconds)
            return session
        except Exception:
            raise StateBackendUnavailable() from None

    async def get_session(self, *, app_name: str, user_id: str, session_id: str,
                          agent_context: Any | None = None) -> Session | None:
        del agent_context
        try:
            raw = await self.loader.execute_source("session_read_v1", _READ, [self._key(session_id)], [self.ttl_seconds])
        except Exception:
            raise StateBackendUnavailable() from None
        if raw is None:
            return None
        session = Session.model_validate_json(raw)
        if session.app_name != app_name or session.user_id != user_id:
            return None
        return self.filter_events(session, need_copy=False)

    async def append_event(self, session: Session, event: Event) -> Event:
        if event.partial:
            return event
        event = await super().append_event(session, event)
        key = self._key(session.id)
        fence = current_session_fence()
        if self.require_fence and fence is None:
            raise StaleFence("A current session fence is required.")
        lease_key = self.codec.session_lease(self.tenant_id, self.agent_id, session.id)
        try:
            result = await self.loader.execute_source(
                "session_append_v2", _APPEND, [key, key + ":event-ids", lease_key],
                [event.id, session.model_dump_json(by_alias=True), self.ttl_seconds,
                 fence.owner_token if fence else "", fence.generation if fence else 0],
            )
        except Exception:
            raise StateBackendUnavailable() from None
        disposition = result[0] if isinstance(result, (list, tuple)) else result
        if disposition == "missing":
            raise StateBackendUnavailable("Shared session disappeared during append.")
        if disposition == "stale":
            raise StaleFence("The session generation is stale.")
        return event

    async def update_session(self, session: Session) -> None:
        fence = current_session_fence()
        if self.require_fence and fence is None:
            raise StaleFence("A current session fence is required.")
        try:
            result = await self.loader.execute_source(
                "session_update_v1", _UPDATE,
                [self._key(session.id), self.codec.session_lease(self.tenant_id, self.agent_id, session.id)],
                [session.model_dump_json(by_alias=True), self.ttl_seconds,
                 fence.owner_token if fence else "", fence.generation if fence else 0],
            )
        except Exception:
            raise StateBackendUnavailable() from None
        if result[0] == "stale":
            raise StaleFence("The session generation is stale.")
        if result[0] == "missing":
            raise StateBackendUnavailable("Shared session disappeared during update.")

    async def list_sessions(self, *, app_name: str, user_id: str | None = None) -> ListSessionsResponse:
        if user_id is None:
            return ListSessionsResponse()
        try:
            keys = await self.redis.smembers(self._index(app_name, user_id))
            values = await self.redis.mget(list(keys)) if keys else []
        except Exception:
            raise StateBackendUnavailable() from None
        sessions = [Session.model_validate_json(value) for value in values if value]
        for session in sessions:
            session.events = []
            session.historical_events = []
        return ListSessionsResponse(sessions=sessions)

    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        key = self._key(session_id)
        try:
            await self.redis.delete(key, key + ":event-ids")
            await self.redis.srem(self._index(app_name, user_id), key)
        except Exception:
            raise StateBackendUnavailable() from None


class SharedSessionBackendFactory:
    def __init__(self, redis: Any, *, namespace: str = "trpc:v1", require_fence: bool = False) -> None:
        self.redis = redis
        self.namespace = namespace
        self.require_fence = require_fence
        self._services: dict[tuple[str, str], FencedRedisSessionService] = {}
        self.closed = False

    def get_backend(self, tenant_id: str, agent_id: str) -> FencedRedisSessionService:
        if self.closed:
            raise RuntimeError("session backend factory is closed")
        key = (tenant_id, agent_id)
        if key not in self._services:
            self._services[key] = FencedRedisSessionService(
                self.redis, tenant_id=tenant_id, agent_id=agent_id, namespace=self.namespace,
                require_fence=self.require_fence,
            )
        return self._services[key]

    async def close(self) -> None:
        self.closed = True
        self._services.clear()
