"""Redis ordering-watermark contract for inbound IM messages."""

from __future__ import annotations

from uuid import UUID

import pytest

from trpc_service.channels.order_gate import (
    ChannelOrderGateUnavailableError,
    RedisChannelOrderGate,
    order_gate_key,
)

TENANT = "tenant_default"
BINDING = UUID("11111111-1111-1111-1111-111111111111")
SENTINEL_URL = "redis://user:secret-password@redis.example:6379/0"


class _Script:

    def __init__(self, result: int = 1, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[tuple[list[object], list[object]]] = []

    async def __call__(self, *, keys: list[object], args: list[object]) -> int:
        self.calls.append((keys, args))
        if self.error is not None:
            raise self.error
        return self.result


class _Redis:

    def __init__(self, script: _Script) -> None:
        self.script = script
        self.register_calls = 0
        self.closed = False

    def register_script(self, lua: str) -> _Script:
        self.register_calls += 1
        assert "GET" in lua
        assert "SET" in lua
        return self.script

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_newer_and_equal_timestamps_are_accepted_by_atomic_script() -> None:
    script = _Script(result=1)
    gate = RedisChannelOrderGate(_Redis(script))

    assert await gate.accept(TENANT, BINDING, "conversation-A", 1234, "message-1") is True
    assert await gate.accept(TENANT, BINDING, "conversation-A", 1234, "message-2") is True

    assert len(script.calls) == 2
    assert script.calls[0][0] == script.calls[1][0]
    assert script.calls[0][1][0] == 1234
    assert script.calls[1][1][0] == 1234


@pytest.mark.asyncio
async def test_older_timestamp_is_rejected() -> None:
    gate = RedisChannelOrderGate(_Redis(_Script(result=0)))

    assert await gate.accept(TENANT, BINDING, "conversation-A", 1233, "message-old") is False


@pytest.mark.asyncio
async def test_missing_platform_time_bypasses_order_gate() -> None:
    script = _Script()
    gate = RedisChannelOrderGate(_Redis(script))

    assert await gate.accept(TENANT, BINDING, "conversation-A", None, "message-arrival") is True
    assert script.calls == []


@pytest.mark.asyncio
async def test_redis_failure_is_fixed_fail_closed_error_without_connection_details() -> None:
    script = _Script(error=ConnectionError(f"could not connect {SENTINEL_URL}"))
    gate = RedisChannelOrderGate(_Redis(script))

    with pytest.raises(ChannelOrderGateUnavailableError) as raised:
        await gate.accept(TENANT, BINDING, "conversation-A", 1234, "message-1")

    assert raised.value.__cause__ is None
    assert SENTINEL_URL not in str(raised.value)
    assert SENTINEL_URL not in repr(raised.value)


def test_key_is_scoped_by_tenant_binding_and_hides_external_conversation() -> None:
    key = order_gate_key(TENANT, BINDING, "conversation-A")

    assert key.startswith(f"trpc:channel-order:{TENANT}:{BINDING.hex}:")
    assert "conversation-A" not in key


@pytest.mark.asyncio
async def test_close_is_safe_and_idempotent() -> None:
    redis = _Redis(_Script())
    gate = RedisChannelOrderGate(redis)

    await gate.close()
    await gate.close()

    assert redis.closed
