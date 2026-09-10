"""租户预算管理器。"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

import redis as redis_lib

from trpc_service.config.registry import config_manager


@dataclass
class _Usage:
    day: str = ""
    api_calls: int = 0
    tokens: int = 0
    extra: Dict[str, float] = field(default_factory=dict)


class BudgetExceeded(Exception):
    """租户预算超限。"""

    def __init__(self, tenant_id: str, reason: str):
        self.tenant_id = tenant_id
        self.reason = reason
        super().__init__(f"租户 {tenant_id} 预算超限: {reason}")


class BudgetManager:
    """租户预算计数器（Redis 共享 / 内存降级双模式）。"""

    REDIS_TTL_SECONDS = 172800  # 48h：跨日滚动保留，防键永久残留

    def __init__(self) -> None:
        self._usage: Dict[str, _Usage] = {}
        self._redis = None
        url = os.getenv("BUDGET_REDIS_URL")
        if url:
            try:
                self._redis = redis_lib.Redis.from_url(
                    url,
                    decode_responses=True,
                    socket_connect_timeout=2,
                )
            except Exception:
                self._redis = None

    @staticmethod
    def _today() -> str:
        return time.strftime("%Y-%m-%d", time.localtime())

    @classmethod
    def _redis_keys(cls, tenant_id: str) -> tuple[str, str]:
        base = f"budget:{tenant_id}:{cls._today()}"
        return f"{base}:calls", f"{base}:tokens"

    def _bucket(self, tenant_id: str) -> _Usage:
        today = self._today()
        usage = self._usage.get(tenant_id)
        if usage is None or usage.day != today:
            usage = _Usage(day=today)
            self._usage[tenant_id] = usage
        return usage

    def _degrade(self) -> None:
        """运行期 Redis 故障 → 永久降级内存，不再反复撞死掉的 Redis。"""
        self._redis = None

    @staticmethod
    def _limits_of(tenant_id: str) -> tuple[int, int]:
        tenant = config_manager.get(tenant_id)
        return (
            tenant.daily_api_calls if tenant else 10000,
            tenant.daily_token_budget if tenant else 100000,
        )

    def check(self, tenant_id: str, tokens: int = 0) -> None:
        """检查是否超限，超限抛 BudgetExceeded。"""
        limits_calls, limits_tokens = self._limits_of(tenant_id)
        usage = self.usage_of(tenant_id)
        if usage.api_calls >= limits_calls:
            raise BudgetExceeded(tenant_id, "daily_api_calls")
        if usage.tokens + tokens > limits_tokens:
            raise BudgetExceeded(tenant_id, "daily_token_budget")

    def record(self, tenant_id: str, api_calls: int = 1, tokens: int = 0) -> None:
        """记录一次用量（Redis 模式为多节点共享的原子计数）。"""
        if self._redis is not None:
            try:
                calls_key, tokens_key = self._redis_keys(tenant_id)
                pipe = self._redis.pipeline()
                pipe.incrby(calls_key, api_calls)
                pipe.incrby(tokens_key, tokens)
                pipe.expire(calls_key, self.REDIS_TTL_SECONDS)
                pipe.expire(tokens_key, self.REDIS_TTL_SECONDS)
                pipe.execute()
                return
            except redis_lib.RedisError:
                self._degrade()
        usage = self._bucket(tenant_id)
        usage.api_calls += api_calls
        usage.tokens += tokens

    def usage_of(self, tenant_id: str) -> _Usage:
        if self._redis is not None:
            try:
                calls_key, tokens_key = self._redis_keys(tenant_id)
                values = self._redis.mget(calls_key, tokens_key)
                return _Usage(
                    day=self._today(),
                    api_calls=int(values[0] or 0),
                    tokens=int(values[1] or 0),
                )
            except redis_lib.RedisError:
                self._degrade()
        return self._bucket(tenant_id)

    def reset(self, tenant_id: Optional[str] = None) -> None:
        if self._redis is not None:
            try:
                if tenant_id:
                    self._redis.delete(*self._redis_keys(tenant_id))
                else:
                    keys = list(
                        self._redis.scan_iter(match="budget:*")
                    )
                    if keys:
                        self._redis.delete(*keys)
            except redis_lib.RedisError:
                self._degrade()
        if tenant_id:
            self._usage.pop(tenant_id, None)
        else:
            self._usage.clear()
