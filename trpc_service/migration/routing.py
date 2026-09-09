"""SDK-compatible Session/Memory routing during an online migration."""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Callable, Awaitable

from trpc_agent_sdk.sessions import BaseSessionService, Session
from trpc_agent_sdk.memory import BaseMemoryService

from .control import StorageRouteMode

DirtyRecorder = Callable[[str, str, Exception], Awaitable[None]]


def _resource_key(*parts: str) -> str:
    """Return the canonical resource identity shared with migration items."""
    return json.dumps(list(parts), ensure_ascii=False, separators=(",", ":"))


class MigrationAwareSessionService(BaseSessionService):
    """Read one primary and synchronously mirror every acknowledged mutation."""

    def __init__(self,
                 source: Any,
                 target: Any,
                 mode: StorageRouteMode,
                 dirty_recorder: DirtyRecorder | None = None,
                 *,
                 shadow_sample_rate: float = 0.1) -> None:
        primary = target if mode in {StorageRouteMode.TARGET_PRIMARY_MIRROR, StorageRouteMode.TARGET_ONLY} else source
        super().__init__(session_config=primary.session_config)
        self.source, self.target, self.mode = source, target, mode
        self._dirty = dirty_recorder
        self._shadow_sample_rate = max(0.0, min(1.0, shadow_sample_rate))

    @property
    def primary(self):
        return self.target if self.mode in {StorageRouteMode.TARGET_PRIMARY_MIRROR, StorageRouteMode.TARGET_ONLY
                                            } else self.source

    @property
    def secondary(self):
        if self.mode in {StorageRouteMode.DUAL_WRITE, StorageRouteMode.SHADOW_READ}:
            return self.target
        if self.mode == StorageRouteMode.TARGET_PRIMARY_MIRROR:
            return self.source
        return None

    @staticmethod
    def _key(session: Session) -> str:
        return _resource_key(session.app_name, session.user_id, session.id)

    async def _replace_secondary(self, session: Session) -> None:
        secondary = self.secondary
        if secondary is None:
            return
        try:
            existing = await secondary.get_session(app_name=session.app_name,
                                                   user_id=session.user_id,
                                                   session_id=session.id)
            if existing is None:
                await secondary.create_session(app_name=session.app_name,
                                               user_id=session.user_id,
                                               session_id=session.id,
                                               state=dict(session.state))
            await secondary.update_session(session.model_copy(deep=True))
        except Exception as error:
            if self._dirty:
                await self._dirty("session", self._key(session), error)
            raise

    async def create_session(self, **kwargs):
        session = await self.primary.create_session(**kwargs)
        await self._replace_secondary(session)
        return session

    async def get_session(self, **kwargs):
        result = await self.primary.get_session(**kwargs)
        sample_key = f"{kwargs.get('app_name')}:{kwargs.get('user_id')}:{kwargs.get('session_id')}"
        sample_value = int(hashlib.sha256(sample_key.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
        if self.mode == StorageRouteMode.SHADOW_READ and sample_value < self._shadow_sample_rate:
            shadow = await self.target.get_session(**kwargs)

            def digest(session):
                if session is None:
                    return ""
                value = session.model_dump(mode="json", exclude={"last_update_time"})
                return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                                 default=str).encode()).hexdigest()

            if digest(result) != digest(shadow) and self._dirty:
                key = _resource_key(kwargs["app_name"], kwargs["user_id"], kwargs["session_id"])
                await self._dirty("session", key, RuntimeError("shadow read hash mismatch"))
        return result

    async def list_sessions(self, **kwargs):
        return await self.primary.list_sessions(**kwargs)

    async def delete_session(self, **kwargs):
        await self.primary.delete_session(**kwargs)
        if self.secondary:
            try:
                await self.secondary.delete_session(**kwargs)
            except Exception as error:
                if self._dirty:
                    key = _resource_key(kwargs["app_name"], kwargs["user_id"], kwargs["session_id"])
                    await self._dirty("session", key, error)
                raise

    async def append_event(self, session, event):
        result = await self.primary.append_event(session, event)
        if not event.partial:
            await self._replace_secondary(session)
        return result

    async def update_session(self, session):
        await self.primary.update_session(session)
        await self._replace_secondary(session)

    async def close(self):
        await asyncio.gather(self.source.close(), self.target.close())


class MigrationAwareMemoryService(BaseMemoryService):
    """Memory mirror; search results always come from the pinned primary."""

    def __init__(self,
                 source: Any,
                 target: Any,
                 mode: StorageRouteMode,
                 dirty_recorder: DirtyRecorder | None = None) -> None:
        primary = target if mode in {StorageRouteMode.TARGET_PRIMARY_MIRROR, StorageRouteMode.TARGET_ONLY} else source
        super().__init__(memory_service_config=primary._memory_service_config)
        self.source, self.target, self.mode = source, target, mode
        self._dirty = dirty_recorder

    @property
    def primary(self):
        return self.target if self.mode in {StorageRouteMode.TARGET_PRIMARY_MIRROR, StorageRouteMode.TARGET_ONLY
                                            } else self.source

    @property
    def secondary(self):
        if self.mode in {StorageRouteMode.DUAL_WRITE, StorageRouteMode.SHADOW_READ}:
            return self.target
        if self.mode == StorageRouteMode.TARGET_PRIMARY_MIRROR:
            return self.source
        return None

    async def store_session(self, session, agent_context=None):
        await self.primary.store_session(session, agent_context=agent_context)
        if self.secondary:
            try:
                await self.secondary.store_session(session.model_copy(deep=True), agent_context=agent_context)
            except Exception as error:
                if self._dirty:
                    await self._dirty("memory", _resource_key(session.save_key, session.id), error)
                raise

    async def search_memory(self, key, query, limit=10, agent_context=None):
        return await self.primary.search_memory(key, query, limit=limit, agent_context=agent_context)

    async def close(self):
        await asyncio.gather(self.source.close(), self.target.close())
