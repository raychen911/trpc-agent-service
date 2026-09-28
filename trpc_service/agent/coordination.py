"""Redis-backed ephemeral coordination around durable Agent facts."""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from trpc_service.tenant.context import TenantContext


class RedisCoordinationClient(Protocol):
    """Minimal asynchronous Redis commands required by coordination adapters."""

    async def incr(self, key: str) -> int:
        ...

    async def set(self, key: str, value: str, *, nx: bool, px: int) -> bool:
        ...

    async def eval(self, script: str, numkeys: int, *values: object) -> int:
        ...

    async def xadd(self, stream: str, fields: dict[str, str]) -> str:
        ...


@dataclass(frozen=True, slots=True)
class ExecutionLease:
    """Opaque lease ownership plus a monotonically increasing fencing token."""

    key: str
    fencing_token: int
    ttl_ms: int


_RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""


class RedisExecutionLeaseCoordinator:
    """Serialize one Session with expiring leases and fencing tokens."""

    def __init__(
        self,
        client: RedisCoordinationClient,
        *,
        prefix: str,
        ttl_ms: int,
    ) -> None:
        if ttl_ms <= 0:
            raise ValueError("Redis lease TTL must be positive")
        self._client = client
        self._prefix = prefix.strip(":")
        self._ttl_ms = ttl_ms

    def _key(self, context: TenantContext, session_id: str) -> str:
        return f"{self._prefix}:lease:{context.tenant_id}:{context.agent_app_id}:{session_id}"

    async def acquire(
        self,
        context: TenantContext,
        session_id: str,
    ) -> ExecutionLease | None:
        """Return a new fenced lease or None while another Worker owns it."""

        key = self._key(context, session_id)
        token = await self._client.incr(f"{key}:fence")
        acquired = await self._client.set(key, str(token), nx=True, px=self._ttl_ms)
        if not acquired:
            return None
        return ExecutionLease(key=key, fencing_token=token, ttl_ms=self._ttl_ms)

    async def renew(self, lease: ExecutionLease) -> bool:
        """Extend only the lease still owned by this fencing token."""

        result = await self._client.eval(
            _RENEW_SCRIPT,
            1,
            lease.key,
            str(lease.fencing_token),
            lease.ttl_ms,
        )
        return bool(result)

    async def release(self, lease: ExecutionLease) -> bool:
        """Delete only the lease still owned by this fencing token."""

        result = await self._client.eval(
            _RELEASE_SCRIPT,
            1,
            lease.key,
            str(lease.fencing_token),
        )
        return bool(result)


class RedisOutboxNotifier:
    """Wake consumers after SQL commits without treating Redis as fact storage."""

    def __init__(self, client: RedisCoordinationClient, *, prefix: str) -> None:
        self._client = client
        self._stream = f"{prefix.strip(':')}:outbox:wake"

    async def notify(self, context: TenantContext, outbox_ids: Sequence[str]) -> None:
        """Append scoped durable Outbox identifiers to the wake-up stream."""

        if not outbox_ids:
            return
        await self._client.xadd(
            self._stream,
            {
                "tenant_id": str(context.tenant_id),
                "agent_app_id": str(context.agent_app_id),
                "trace_id": context.trace_id,
                "outbox_ids": json.dumps(list(outbox_ids), separators=(",", ":")),
            },
        )
