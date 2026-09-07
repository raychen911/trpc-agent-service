# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Redis-backed one-time confirmations shared by all workers."""

from __future__ import annotations

import time
from typing import Any
from typing import Optional

from ._hitl import DEFAULT_CONFIRMATION_TTL_SECONDS
from ._hitl import PendingConfirmation


class RedisConfirmationManager:
    """Store confirmation tokens in Redis and atomically consume with GETDEL."""

    def __init__(
        self,
        *,
        redis_url: Optional[str] = None,
        client: Any = None,
        ttl_seconds: float = DEFAULT_CONFIRMATION_TTL_SECONDS,
    ) -> None:
        if client is not None:
            self._redis = client
        elif redis_url:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("RedisConfirmationManager requires redis_url or client")
        self._ttl = max(1, int(ttl_seconds))

    @staticmethod
    def _key(token: str) -> str:
        return f"agent:confirmation:{token}"

    async def request(
        self,
        tenant_id: str,
        tool_name: str,
        tool_args: Optional[dict[str, Any]] = None,
        *,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> PendingConfirmation:
        import secrets

        pending = PendingConfirmation(
            token=secrets.token_urlsafe(16),
            tenant_id=tenant_id,
            tool_name=tool_name,
            tool_args=tool_args or {},
            user_id=user_id,
            session_id=session_id,
            expires_at=time.time() + self._ttl,
        )
        await self._redis.set(self._key(pending.token), pending.model_dump_json(), ex=self._ttl, nx=True)
        return pending

    async def get(self, token: str) -> Optional[PendingConfirmation]:
        value = await self._redis.get(self._key(token))
        if value is None:
            return None
        pending = PendingConfirmation.model_validate_json(value)
        return None if pending.is_expired() else pending

    async def resolve(self, token: str, approve: bool) -> Optional[PendingConfirmation]:
        value = await self._redis.getdel(self._key(token))
        if value is None:
            return None
        pending = PendingConfirmation.model_validate_json(value)
        return None if pending.is_expired() else pending

    async def close(self) -> None:
        await self._redis.aclose()
