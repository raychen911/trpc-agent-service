"""Redis-authoritative active/standby ownership for long-lived IM adapters."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets
from typing import Any, Callable

from trpc_service.storage.contracts import (
    LeaseBusy,
    LeaseLost,
    StateBackendUnavailable,
)
from trpc_service.storage.models import (
    AdapterFence,
    AdapterOwnershipPhase,
    AdapterOwnershipState,
)
from trpc_service.storage.redis_codec import RedisKeyCodec
from trpc_service.storage.redis_scripts.loader import RedisScriptLoader


_DIR = Path(__file__).with_name("redis_scripts")
_ACQUIRE = (_DIR / "adapter_lease_acquire.lua").read_text(encoding="utf-8")
_RENEW = (_DIR / "adapter_lease_renew.lua").read_text(encoding="utf-8")
_READY = (_DIR / "adapter_lease_ready.lua").read_text(encoding="utf-8")
_RELEASE = (_DIR / "adapter_lease_release.lua").read_text(encoding="utf-8")


class InMemoryAdapterLeaseHandle:
    def __init__(self, repository, identity_digest: str, fence: AdapterFence) -> None:
        self.repository = repository
        self.identity_digest = identity_digest
        self.fence = fence

    async def renew(self, lease_ms: int) -> AdapterFence:
        self.fence = self.repository._renew(self.fence, lease_ms)
        return self.fence

    async def mark_ready(self, runtime_bot_identity: Any) -> AdapterFence:
        if runtime_bot_identity.channel_identity_digest != self.identity_digest:
            raise LeaseLost("Adapter identity does not match the lease.")
        self.repository._mark_ready(self.fence)
        return self.fence

    async def release(self, reason: str) -> None:
        del reason
        self.repository._release(self.fence)


class InMemoryAdapterOwnershipRepository:
    """Deterministic contract implementation; never used as shared fallback."""

    def __init__(self, *, now: Callable[[], datetime] | None = None) -> None:
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._active: dict[str, AdapterFence] = {}
        self._phase: dict[str, AdapterOwnershipPhase] = {}
        self._generation: dict[str, int] = {}

    def _valid(self, fence: AdapterFence) -> bool:
        current = self._active.get(fence.identity_digest)
        return bool(
            current == fence
            and current.expires_at > self._now()
        )

    async def validate_fence(self, fence: AdapterFence) -> bool:
        return self._valid(fence)

    async def acquire(
        self, identity_digest: str, node_id: str, lease_ms: int
    ) -> InMemoryAdapterLeaseHandle:
        current = self._active.get(identity_digest)
        if current is not None and self._valid(current):
            raise LeaseBusy("Adapter identity is already owned.")
        generation = self._generation.get(identity_digest, 0) + 1
        self._generation[identity_digest] = generation
        fence = AdapterFence(
            identity_digest=identity_digest,
            node_id=node_id,
            generation=generation,
            owner_token=secrets.token_urlsafe(24),
            expires_at=self._now() + timedelta(milliseconds=lease_ms),
        )
        self._active[identity_digest] = fence
        self._phase[identity_digest] = AdapterOwnershipPhase.CONNECTING
        return InMemoryAdapterLeaseHandle(self, identity_digest, fence)

    def _renew(self, fence: AdapterFence, lease_ms: int) -> AdapterFence:
        if not self._valid(fence):
            raise LeaseLost("Adapter ownership lease was lost.")
        renewed = fence.model_copy(
            update={"expires_at": self._now() + timedelta(milliseconds=lease_ms)}
        )
        self._active[fence.identity_digest] = renewed
        return renewed

    def _mark_ready(self, fence: AdapterFence) -> None:
        if not self._valid(fence):
            raise LeaseLost("Adapter ownership lease was lost.")
        self._phase[fence.identity_digest] = AdapterOwnershipPhase.READY

    def _release(self, fence: AdapterFence) -> None:
        current = self._active.get(fence.identity_digest)
        if current == fence:
            self._active.pop(fence.identity_digest, None)
            self._phase[fence.identity_digest] = AdapterOwnershipPhase.STANDBY

    async def inspect(self, identity_digest: str) -> AdapterOwnershipState:
        current = self._active.get(identity_digest)
        if current is None or not self._valid(current):
            return AdapterOwnershipState(
                identity_digest=identity_digest,
                generation=self._generation.get(identity_digest, 0),
            )
        ttl = max(0, int((current.expires_at - self._now()).total_seconds() * 1000))
        return AdapterOwnershipState(
            identity_digest=identity_digest,
            owner_node_id=current.node_id,
            generation=current.generation,
            phase=self._phase.get(identity_digest, AdapterOwnershipPhase.CONNECTING),
            expires_in_ms=ttl,
        )


class RedisAdapterLeaseHandle:
    def __init__(
        self,
        repository: "RedisAdapterOwnershipRepository",
        key: str,
        identity_digest: str,
        fence: AdapterFence,
    ) -> None:
        self.repository = repository
        self.key = key
        self.identity_digest = identity_digest
        self.fence = fence

    def _args(self) -> list[object]:
        return [
            self.fence.owner_token,
            self.fence.node_id,
            self.fence.generation,
        ]

    async def renew(self, lease_ms: int) -> AdapterFence:
        try:
            result = await self.repository.loader.execute_source(
                "adapter_renew_v1", _RENEW, [self.key], self._args() + [lease_ms]
            )
        except Exception:
            raise StateBackendUnavailable() from None
        if result[0] != "renewed":
            raise LeaseLost("Adapter ownership lease was lost.")
        self.fence = self.fence.model_copy(
            update={
                "expires_at": datetime.now(timezone.utc)
                + timedelta(milliseconds=lease_ms)
            }
        )
        return self.fence

    async def mark_ready(self, runtime_bot_identity: Any) -> AdapterFence:
        if runtime_bot_identity.channel_identity_digest != self.identity_digest:
            raise LeaseLost("Adapter identity does not match the lease.")
        try:
            result = await self.repository.loader.execute_source(
                "adapter_ready_v1", _READY, [self.key], self._args()
            )
        except Exception:
            raise StateBackendUnavailable() from None
        if result[0] != "ready":
            raise LeaseLost("Adapter ownership lease was lost.")
        return self.fence

    async def release(self, reason: str) -> None:
        del reason
        try:
            await self.repository.loader.execute_source(
                "adapter_release_v1", _RELEASE, [self.key], self._args()
            )
        except Exception:
            return


class RedisAdapterOwnershipRepository:
    def __init__(self, redis: Any, *, namespace: str = "trpc:v1") -> None:
        self.redis = redis
        self.codec = RedisKeyCodec(namespace)
        self.loader = RedisScriptLoader(redis)
        self._key_by_identity: dict[str, str] = {}

    async def acquire(
        self, identity_digest: str, node_id: str, lease_ms: int
    ) -> RedisAdapterLeaseHandle:
        key = self.codec.adapter_ownership(identity_digest)
        token = secrets.token_urlsafe(24)
        try:
            result = await self.loader.execute_source(
                "adapter_acquire_v1",
                _ACQUIRE,
                [key, key + ":generation"],
                [token, node_id, lease_ms],
            )
        except Exception:
            raise StateBackendUnavailable() from None
        if result[0] != "acquired":
            raise LeaseBusy("Adapter identity is already owned.")
        fence = AdapterFence(
            identity_digest=identity_digest,
            node_id=node_id,
            generation=int(result[1]),
            owner_token=token,
            expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=lease_ms),
        )
        self._key_by_identity[identity_digest] = key
        return RedisAdapterLeaseHandle(self, key, identity_digest, fence)

    async def validate_fence(self, fence: AdapterFence) -> bool:
        key = self.codec.adapter_ownership(fence.identity_digest)
        try:
            if int(await self.redis.pttl(key)) <= 0:
                return False
            values = await self.redis.hmget(key, "token", "node", "generation")
        except Exception:
            raise StateBackendUnavailable() from None
        return values == [
            fence.owner_token,
            fence.node_id,
            str(fence.generation),
        ]

    async def inspect(self, identity_digest: str) -> AdapterOwnershipState:
        key = self.codec.adapter_ownership(identity_digest)
        try:
            values = await self.redis.hgetall(key)
            ttl = int(await self.redis.pttl(key))
            generation = int(await self.redis.get(key + ":generation") or 0)
        except Exception:
            raise StateBackendUnavailable() from None
        if ttl <= 0 or not values:
            return AdapterOwnershipState(
                identity_digest=identity_digest, generation=generation
            )
        return AdapterOwnershipState(
            identity_digest=identity_digest,
            owner_node_id=values.get("node"),
            generation=int(values.get("generation", generation)),
            phase=AdapterOwnershipPhase(values.get("phase", "connecting")),
            expires_in_ms=ttl,
        )


__all__ = [
    "InMemoryAdapterOwnershipRepository",
    "RedisAdapterOwnershipRepository",
]
