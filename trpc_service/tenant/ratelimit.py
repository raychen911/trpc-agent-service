"""租户级频率限制器（IM 消息洪峰防护）。"""
from __future__ import annotations

import os
import threading
import time
from typing import Dict, Tuple

import redis as redis_lib

WINDOW_SECONDS = 60


class RateLimitExceeded(Exception):
    """触发频率限制。"""

    def __init__(self, tenant_id: str, user_id: str):
        self.tenant_id = tenant_id
        self.user_id = user_id
        super().__init__(f"用户 {user_id} 触发租户 {tenant_id} 的频率限制")


class RateLimiter:
    """固定窗口频率限制（Redis 共享 / 内存降级双模式）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._windows: Dict[Tuple[str, str, int], int] = {}
        self._redis = None
        url = os.getenv("RATE_LIMIT_REDIS_URL")
        if url:
            try:
                self._redis = redis_lib.Redis.from_url(
                    url, decode_responses=True, socket_connect_timeout=2
                )
            except Exception:
                self._redis = None

    @staticmethod
    def _window() -> int:
        return int(time.time()) // WINDOW_SECONDS

    @staticmethod
    def _redis_key(tenant_id: str, user_id: str) -> str:
        return f"ratelimit:{tenant_id}:{user_id}:{time.time() // WINDOW_SECONDS:.0f}"

    def _degrade(self) -> None:
        self._redis = None

    def check(self, tenant_id: str, user_id: str, limit_per_minute: int) -> None:
        """超限抛 RateLimitExceeded；limit_per_minute<=0 不限制。"""
        if limit_per_minute <= 0:
            return
        if self._redis is not None:
            try:
                key = self._redis_key(tenant_id, user_id)
                pipe = self._redis.pipeline()
                pipe.incr(key)
                pipe.expire(key, WINDOW_SECONDS * 2)  # 双倍窗口，防键残留
                count = pipe.execute()[0]
                if int(count) > limit_per_minute:
                    raise RateLimitExceeded(tenant_id, user_id)
                return
            except redis_lib.RedisError:
                self._degrade()
        self._memory_check(tenant_id, user_id, limit_per_minute)

    def _memory_check(self, tenant_id: str, user_id: str, limit_per_minute: int) -> None:
        window = self._window()
        key = (tenant_id, user_id, window)
        with self._lock:
            # 清理过期窗口，防内存膨胀
            stale = [k for k in self._windows if k[2] != window]
            for k in stale:
                self._windows.pop(k, None)
            self._windows[key] = self._windows.get(key, 0) + 1
            if self._windows[key] > limit_per_minute:
                raise RateLimitExceeded(tenant_id, user_id)

    def reset(self) -> None:
        with self._lock:
            self._windows.clear()


rate_limiter = RateLimiter()
