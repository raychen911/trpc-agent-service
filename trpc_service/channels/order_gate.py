"""Fail-closed Redis watermark for ordered inbound IM conversations.

The platform timestamp is only used when the adapter can provide one.  A
timestamp equal to the stored watermark is intentionally accepted: receipt
idempotency, rather than ordering, owns duplicate delivery handling.
"""

from __future__ import annotations

import hashlib
import os
from typing import Final, Mapping
from uuid import UUID

from trpc_service.tenant.context import validate_tenant_id

ORDER_GATE_TTL_SECONDS: Final[int] = 86_400
_KEY_PREFIX: Final[str] = "trpc:channel-order:"
_UNAVAILABLE_MESSAGE: Final[str] = "channel order backend failed"

_ORDER_GATE_LUA: Final[str] = """
local current = redis.call('GET', KEYS[1])
local incoming = tonumber(ARGV[1])
if not current then
    redis.call('SET', KEYS[1], incoming, 'EX', ARGV[2])
    return 1
end
if incoming < tonumber(current) then
    return 0
end
if incoming > tonumber(current) then
    redis.call('SET', KEYS[1], incoming, 'EX', ARGV[2])
else
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 1
"""


class ChannelOrderGateUnavailableError(RuntimeError):
    """Redis is unavailable; callers must reject before creating a Worker task."""


def _nonempty_string(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("invalid channel order identity")
    return value.strip()


def order_gate_key(tenant_id: str, binding_id: UUID, conversation_id: str) -> str:
    """Return a stable, tenant/binding scoped key without raw external IDs."""
    validate_tenant_id(_nonempty_string(tenant_id))
    if not isinstance(binding_id, UUID):
        raise ValueError("invalid channel order identity")
    conversation_digest = hashlib.sha256(_nonempty_string(conversation_id).encode()).hexdigest()
    return f"{_KEY_PREFIX}{tenant_id}:{binding_id.hex}:{conversation_digest}"


class RedisChannelOrderGate:
    """One shared Redis client and one atomic Lua compare-and-advance script."""

    def __init__(self, redis_client: object) -> None:
        self._redis = redis_client
        self._script: object | None = None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "RedisChannelOrderGate":
        import redis.asyncio as aioredis

        values = os.environ if environ is None else environ
        url = values.get("TRPC_REDIS_URL", "").strip()
        if not url:
            raise ChannelOrderGateUnavailableError("channel order backend is not configured")
        return cls(aioredis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2.0,
            socket_timeout=2.0,
        ))

    async def accept(
        self,
        tenant_id: str,
        binding_id: UUID,
        conversation_id: str,
        occurred_at_ms: int | None,
        message_id: str,
    ) -> bool:
        """Accept a new/equal timestamp; reject a strictly older one.

        When a platform omits a trustworthy timestamp, no watermark is
        touched.  The existing shared session lease serializes those arrivals.
        ``message_id`` is deliberately not put into Redis or errors; receipt
        idempotency owns duplicate detection.
        """
        _nonempty_string(message_id)
        if occurred_at_ms is None:
            return True
        if isinstance(occurred_at_ms, bool) or not isinstance(occurred_at_ms, int) or occurred_at_ms < 0:
            raise ValueError("invalid channel occurrence time")
        key = order_gate_key(tenant_id, binding_id, conversation_id)
        script = self._script
        if script is None:
            script = self._redis.register_script(_ORDER_GATE_LUA)  # type: ignore[attr-defined]
            self._script = script
        try:
            result = await script(  # type: ignore[operator]
                keys=[key], args=[occurred_at_ms, ORDER_GATE_TTL_SECONDS])
        except Exception:
            raise ChannelOrderGateUnavailableError(_UNAVAILABLE_MESSAGE) from None
        return bool(int(result))

    async def check_ready(self) -> None:
        try:
            await self._redis.ping()  # type: ignore[attr-defined]
        except Exception:
            raise ChannelOrderGateUnavailableError(_UNAVAILABLE_MESSAGE) from None

    async def close(self) -> None:
        close = getattr(self._redis, "aclose", None) or getattr(self._redis, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:
                pass


__all__ = [
    "ChannelOrderGateUnavailableError",
    "ORDER_GATE_TTL_SECONDS",
    "RedisChannelOrderGate",
    "order_gate_key",
]
