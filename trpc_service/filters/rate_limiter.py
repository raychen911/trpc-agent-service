# ===================================================================
# filters.rate_limiter - 租户级限流器（令牌桶 / Redis 固定窗口）
# ===================================================================
# 说明: 租户级每分钟请求上限（PRD 1.1 rate_limit_per_min / 4.1 RateLimitFilter）。
#   单机用进程内令牌桶；多节点用 Redis 固定窗口计数（INCR+EXPIRE 原子
#   累加，各节点共享同一额度，不再放大 N 倍），接口保持一致。
# 规范: 令牌桶平滑突发；超限返回剩余等待秒数供 Filter 记录。
# ===================================================================

from __future__ import annotations

import threading
import time
from typing import Any, Optional


class TokenBucketLimiter:
    """进程内令牌桶限流器（单机开发 / 单进程部署）。"""

    def __init__(self, rate_per_min: int) -> None:
        self.rate_per_min = rate_per_min
        """记录创建时的速率：租户配置热更新后据此重建桶（审查 09-04）。"""
        self._capacity = max(1, rate_per_min)
        self._tokens = float(self._capacity)
        self._refill_rate = rate_per_min / 60.0  # 每秒补充
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def allow(self, cost: int = 1) -> tuple[bool, float]:
        """尝试消费令牌。

        Returns:
            (是否放行, 若未放行还需等待秒数)
        """
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._last_refill) * self._refill_rate)
            self._last_refill = now
            if self._tokens >= cost:
                self._tokens -= cost
                return True, 0.0
            wait = (cost - self._tokens) / self._refill_rate
            return False, wait


class TenantRateLimiter:
    """租户级限流器集合（tenant_id -> 令牌桶）。

    `_buckets` 以租户数为上界（租户量级有限），不做淘汰；桶在租户
    `rate_limit_per_min` 热更新后自动重建（审查 09-04 修复：此前桶按
    首次创建时的速率固定，Admin 改配置对已建桶不生效）。
    """

    def __init__(self, default_rate_per_min: int = 60) -> None:
        self._default = default_rate_per_min
        self._buckets: dict[str, TokenBucketLimiter] = {}
        self._lock = threading.Lock()

    def allow(self, tenant_id: str, rate_per_min: Optional[int] = None) -> tuple[bool, float]:
        """按租户限流；rate_per_min 为 0 或 None 时取租户配置。"""
        rate = self._default if rate_per_min is None else rate_per_min
        if rate <= 0:
            return True, 0.0  # 不限流
        with self._lock:
            bucket = self._buckets.get(tenant_id)
            if bucket is None or bucket.rate_per_min != rate:
                # 桶不存在或速率已热更新 → 以最新速率重建
                bucket = TokenBucketLimiter(rate)
                self._buckets[tenant_id] = bucket
        return bucket.allow()

    def reset(self, tenant_id: str) -> None:
        """重置租户桶（测试 / 配置变更）。"""
        with self._lock:
            self._buckets.pop(tenant_id, None)


_RATELIMIT_KEY = "ratelimit:{tenant}:{window}"
"""Redis 固定窗口 key：window 为 epoch 分钟号（多节点共享同一额度）。"""


class RedisFixedWindowLimiter:
    """Redis 固定窗口租户限流器（多节点共享额度）。

    每分钟一个窗口（key 含 epoch 分钟号），INCR 原子累加 + 首次 EXPIRE 60s
    过期自清理；超限返回到下一窗口的等待秒数。与 TenantRateLimiter 的
    allow(tenant_id, rate_per_min) 签名对齐（返回协程，Filter 侧 await）。
    """

    def __init__(self, redis: Any) -> None:
        """Args: redis: redis.asyncio.Redis 客户端实例。"""
        self._redis = redis

    async def allow(self, tenant_id: str, rate_per_min: Optional[int] = None) -> tuple[bool, float]:
        """按租户限流；rate_per_min 为 0 或 None 时不限流。"""
        if rate_per_min is None or rate_per_min <= 0:
            return True, 0.0
        window = int(time.time() // 60)
        key = _RATELIMIT_KEY.format(tenant=tenant_id, window=window)
        count = await self._redis.incr(key)
        if count == 1:
            # 仅首个写入者设过期，避免反复刷新 TTL 拖长窗口
            await self._redis.expire(key, 60)
        if count <= rate_per_min:
            return True, 0.0
        wait = 60.0 - (time.time() % 60.0)
        return False, wait
