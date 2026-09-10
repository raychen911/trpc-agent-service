"""消息去重器（三层幂等的第一层）。"""
from __future__ import annotations

import os
import time
from typing import Dict, Optional


class Deduper:
    """TTL 去重缓存（Redis 共享 / 内存降级双模式）。"""

    KEY_PREFIX = "dedupe:"

    def __init__(
        self,
        ttl_seconds: int = 300,
        redis_client=None,
        redis_url: Optional[str] = None,
    ):
        self._ttl = ttl_seconds
        self._seen: Dict[str, float] = {}
        self._redis = redis_client
        if self._redis is None:
            url = redis_url or os.getenv("DEDUPE_REDIS_URL")
            if url:
                import redis

                try:
                    self._redis = redis.Redis.from_url(
                        url,
                        decode_responses=True,
                        socket_connect_timeout=2,
                    )
                except Exception:
                    self._redis = None

    def seen(self, key: str) -> bool:
        """返回 True 表示重复消息；首次登记返回 False。"""
        if self._redis is not None:
            try:
                # SET NX EX：原子“不存在才写入”，多节点仅一个节点首次成功
                return self._redis.set(
                    self.KEY_PREFIX + key, 1, nx=True, ex=self._ttl
                ) is not True
            except Exception:
                self._redis = None  # 永久降级内存，服务不中断
        now = time.monotonic()
        self._evict(now)
        if key in self._seen:
            return True
        self._seen[key] = now
        return False

    def _evict(self, now: float) -> None:
        expired = [k for k, ts in self._seen.items() if now - ts > self._ttl]
        for k in expired:
            self._seen.pop(k, None)

    def forget(self, key: str) -> None:
        self._seen.pop(key, None)
        if self._redis is not None:
            try:
                self._redis.delete(self.KEY_PREFIX + key)
            except Exception:
                self._redis = None

    def clear(self) -> None:
        self._seen.clear()
