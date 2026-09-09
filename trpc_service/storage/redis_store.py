# ===================================================================
# storage.redis_store - Redis 存储实现（生产推荐）
# ===================================================================
# 说明: 承载 Session（热）、幂等去重、分布式锁（PRD 2.2/2.3）。
#   key 一律带 tenant_id 前缀实现隔离；session 使用 hash 存储 state+version。
# 规范: 分布式锁 = SET NX EX；幂等 = SET NX EX 24h；操作前检查连接。
# ===================================================================

from __future__ import annotations

import json
import time
from typing import Any, Optional

from .base import (
    DistributedLock,
    IdempotencyStore,
    SessionStore,
    SummaryStore,
)

_SESSION_KEY = "session:{tenant}:{sid}"
_LOCK_KEY = "lock:session:{tenant}:{sid}"
_SUMMARY_KEY = "summary:{tenant}:{sid}"


class RedisSessionStore(SessionStore):
    """基于 Redis hash 的 Session 存储（租户 key 前缀隔离）。"""

    def __init__(self, redis: Any) -> None:
        """Args: redis: redis.asyncio.Redis 客户端实例。"""
        self._redis = redis

    def _key(self, tenant_id: str, session_id: str) -> str:
        return _SESSION_KEY.format(tenant=tenant_id, sid=session_id)

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[dict[str, Any]]:
        raw = await self._redis.hgetall(self._key(tenant_id, session_id))
        if not raw:
            return None
        session: dict[str, Any] = {"session_id": session_id, "version": 0}
        for field, value in raw.items():
            field_s = field.decode() if isinstance(field, bytes) else field
            value_s = value.decode() if isinstance(value, bytes) else value
            if field_s in ("state", "events"):
                session[field_s] = json.loads(value_s)
            elif field_s == "version":
                session[field_s] = int(value_s)
            else:
                session[field_s] = value_s
        return session

    async def save_session(self, tenant_id: str, session: dict[str, Any]) -> None:
        key = self._key(tenant_id, session["session_id"])
        mapping: dict[str, str] = {}
        for field, value in session.items():
            if field in ("state", "events"):
                mapping[field] = json.dumps(value, ensure_ascii=False)
            else:
                mapping[field] = str(value)
        await self._redis.hset(key, mapping=mapping)

    async def update_state(self, tenant_id: str, session_id: str, state: dict[str, Any]) -> dict[str, Any]:
        """乐观锁版本号更新（PRD 2.3-A，配合外部分布式锁串行化）。"""
        session = await self.get_session(tenant_id, session_id)
        if session is None:
            session = {"session_id": session_id, "state": {}, "version": 0}
        session["state"] = dict(state)
        session["version"] = int(session.get("version", 0)) + 1
        await self.save_session(tenant_id, session)
        return session

    async def delete_session(self, tenant_id: str, session_id: str) -> None:
        await self._redis.delete(self._key(tenant_id, session_id))


class RedisSummaryStore(SummaryStore):
    """基于 Redis 字符串的 Summary 存储（每 session 一条，PRD 2.2）。"""

    def __init__(self, redis: Any) -> None:
        self._redis = redis

    def _key(self, tenant_id: str, session_id: str) -> str:
        return _SUMMARY_KEY.format(tenant=tenant_id, sid=session_id)

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        raw = await self._redis.get(self._key(tenant_id, session_id))
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else raw

    async def save_summary(self, tenant_id: str, session_id: str, content: str) -> None:
        await self._redis.set(self._key(tenant_id, session_id), content)

    async def delete_summary(self, tenant_id: str, session_id: str) -> None:
        await self._redis.delete(self._key(tenant_id, session_id))

    async def list_summaries(self, tenant_id: str) -> list[tuple[str, str]]:
        pattern = _SUMMARY_KEY.format(tenant=tenant_id, sid="*")
        out: list[tuple[str, str]] = []
        async for key in self._redis.scan_iter(match=pattern):
            raw = await self._redis.get(key)
            if raw is None:
                continue
            content = raw.decode() if isinstance(raw, bytes) else raw
            # key = summary:{tenant}:{sid}
            sid = key.split(":", 2)[2] if isinstance(key, str) else key.decode("utf-8", "ignore").split(":", 2)[2]
            out.append((sid, content))
        return out


class RedisIdempotencyStore(IdempotencyStore):
    """Redis SET NX EX 幂等去重（PRD 2.3-E，24h TTL）。"""

    def __init__(self, redis: Any) -> None:
        self._redis = redis

    async def try_acquire(self, key: str, ttl_seconds: int = 86400) -> bool:
        ok = await self._redis.set(key, "1", nx=True, ex=ttl_seconds)
        return bool(ok)

    async def release(self, key: str) -> None:
        await self._redis.delete(key)


class RedisDistributedLock(DistributedLock):
    """Redis 分布式锁: SET key token NX EX（PRD 2.3-A）。"""

    def __init__(self, redis: Any) -> None:
        self._redis = redis
        self._tokens: dict[str, str] = {}

    async def acquire(self, key: str, timeout: float = 10.0) -> bool:
        token = f"{time.time_ns()}:{id(self)}"
        ok = await self._redis.set(key, token, nx=True, ex=max(1, int(timeout)))
        if ok:
            self._tokens[key] = token
        return bool(ok)

    async def release(self, key: str) -> None:
        token = self._tokens.pop(key, None)
        if token is None:
            return
        # Lua 脚本: 仅当持有者仍持有才删除（防误删他人锁）
        script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
        await self._redis.eval(script, 1, key, token)
