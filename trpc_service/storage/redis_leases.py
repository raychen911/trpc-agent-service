"""Redis PTTL-authoritative session leases with monotonic fencing generations."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import Any
import secrets

from trpc_service.storage.contracts import LeaseLost, SessionBusy, StateBackendUnavailable
from trpc_service.storage.models import NodeIdentity, SessionFence
from trpc_service.storage.redis_codec import RedisKeyCodec
from trpc_service.storage.redis_scripts.loader import RedisScriptLoader


_DIR = Path(__file__).with_name("redis_scripts")
_ACQUIRE = (_DIR / "lease_acquire.lua").read_text(encoding="utf-8")
_RENEW = (_DIR / "lease_renew.lua").read_text(encoding="utf-8")
_RELEASE = (_DIR / "lease_release.lua").read_text(encoding="utf-8")
_CURRENT_FENCE: ContextVar[SessionFence | None] = ContextVar("trpc_session_fence", default=None)


def current_session_fence() -> SessionFence | None:
    return _CURRENT_FENCE.get()


@asynccontextmanager
async def use_session_fence(fence: SessionFence):
    token = _CURRENT_FENCE.set(fence)
    try:
        yield fence
    finally:
        _CURRENT_FENCE.reset(token)


class RedisSessionLease:
    def __init__(self, manager: "RedisSessionLeaseManager", key: str, fence: SessionFence,
                 lease_ms: int, heartbeat_ms: int | None = None) -> None:
        self.manager, self.key, self.fence = manager, key, fence
        self.lease_ms = lease_ms
        self.heartbeat_ms = heartbeat_ms or max(1, lease_ms // 3)
        self._heartbeat: asyncio.Task[None] | None = None
        self._context_token: Token[SessionFence | None] | None = None
        self.released = False
        self.lost = False

    async def pttl(self) -> int:
        try:
            return int(await self.manager.redis.pttl(self.key))
        except Exception:
            raise StateBackendUnavailable() from None

    async def renew(self, lease_ms: int | None = None) -> SessionFence:
        ttl = lease_ms or self.lease_ms
        try:
            result = await self.manager.loader.execute_source(
                "lease_renew_v1", _RENEW, [self.key],
                [self.fence.owner_token, self.fence.generation, ttl],
            )
        except Exception:
            raise StateBackendUnavailable() from None
        if result[0] != "renewed":
            self.lost = True
            raise LeaseLost("The session lease is no longer owned.")
        self.fence = self.fence.model_copy(update={
            "expires_at": datetime.now(timezone.utc) + timedelta(milliseconds=ttl)
        })
        _CURRENT_FENCE.set(self.fence)
        return self.fence

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_ms / 1000)
                await self.renew()
        except asyncio.CancelledError:
            raise
        except (LeaseLost, StateBackendUnavailable):
            self.lost = True

    async def release(self) -> None:
        if self.released:
            return
        self.released = True
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            try:
                await self._heartbeat
            except asyncio.CancelledError:
                pass
        try:
            await self.manager.loader.execute_source(
                "lease_release_v1", _RELEASE, [self.key],
                [self.fence.owner_token, self.fence.generation],
            )
        except Exception:
            pass
        if self._context_token is not None:
            _CURRENT_FENCE.reset(self._context_token)
            self._context_token = None

    async def __aenter__(self) -> "RedisSessionLease":
        self._context_token = _CURRENT_FENCE.set(self.fence)
        self._heartbeat = asyncio.create_task(self._heartbeat_loop())
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.release()


class RedisSessionLeaseManager:
    def __init__(self, redis: Any, *, namespace: str = "trpc:v1", node: NodeIdentity | None = None,
                 lease_ms: int = 10_000, heartbeat_ms: int = 3_000, wait_ms: int = 2_000,
                 metrics: Any | None = None) -> None:
        self.redis = redis
        self.codec = RedisKeyCodec(namespace)
        self.loader = RedisScriptLoader(redis)
        self.node = node
        self.lease_ms, self.heartbeat_ms, self.wait_ms = lease_ms, heartbeat_ms, wait_ms
        self.metrics = metrics
        self._keys_by_digest: dict[str, str] = {}

    async def validate_fence(self, fence: SessionFence) -> bool:
        key = self._keys_by_digest.get(fence.session_key_digest)
        if key is None or await self.redis.pttl(key) <= 0:
            return False
        values = await self.redis.hmget(key, "token", "generation")
        return values == [fence.owner_token, str(fence.generation)]

    async def acquire(self, tenant_id: str, agent_id: str, platform_session_id: str,
                      message_key_digest: str, node_identity: NodeIdentity,
                      lease_ms: int, wait_ms: int) -> RedisSessionLease:
        del message_key_digest
        key = self.codec.session_lease(tenant_id, agent_id, platform_session_id)
        generation_key = key + ":generation"
        token = secrets.token_urlsafe(24)
        deadline = monotonic() + wait_ms / 1000
        while True:
            try:
                result = await self.loader.execute_source(
                    "lease_acquire_v1", _ACQUIRE, [key, generation_key],
                    [token, node_identity.node_id, lease_ms],
                )
            except Exception:
                raise StateBackendUnavailable() from None
            if result[0] == "acquired":
                generation = int(result[1])
                fence = SessionFence(
                    session_key_digest=sha256(key.encode()).hexdigest(), generation=generation,
                    owner_node=node_identity, owner_token=token,
                    expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=lease_ms),
                )
                self._keys_by_digest[fence.session_key_digest] = key
                return RedisSessionLease(self, key, fence, lease_ms, min(self.heartbeat_ms, max(1, lease_ms // 3)))
            if monotonic() >= deadline:
                raise SessionBusy("The session is currently being processed.")
            # A short bounded poll avoids starving a waiter behind many very
            # short holders while the authoritative PTTL remains in Redis.
            await asyncio.sleep(min(0.002, max(0.001, int(result[1]) / 1000)))

    async def acquire_for_message(self, context: Any, identity: Any, key: Any) -> RedisSessionLease:
        if self.node is None:
            raise StateBackendUnavailable("Node identity is unavailable.")
        # Reuse the collision-safe, channel-scoped idempotency identity.  The
        # digest is diagnostic input only; the authoritative lease key is the
        # tenant/agent/session key constructed below.
        digest = self.codec.idempotency(key).rsplit(":", 1)[-1]
        started = monotonic()
        try:
            lease = await self.acquire(
                context.tenant_id, context.agent_id, identity.platform_session_id,
                digest, self.node, self.lease_ms, self.wait_ms,
            )
        except (SessionBusy, StateBackendUnavailable) as error:
            if self.metrics is not None:
                from trpc_service.audit.models import TenantScope
                self.metrics.observe_lease(
                    TenantScope(tenant_id=context.tenant_id), backend="redis",
                    session_id=identity.platform_session_id,
                    outcome="busy" if isinstance(error, SessionBusy) else "unavailable",
                    wait_ms=(monotonic() - started) * 1000,
                )
            raise
        if self.metrics is not None:
            from trpc_service.audit.models import TenantScope
            self.metrics.observe_lease(
                TenantScope(tenant_id=context.tenant_id), backend="redis",
                session_id=identity.platform_session_id, outcome="acquired",
                wait_ms=(monotonic() - started) * 1000,
            )
        return lease
