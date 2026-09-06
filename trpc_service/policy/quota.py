"""Tenant QPS, daily token, and daily cost enforcement."""

from __future__ import annotations

import os
from collections import defaultdict, deque
from datetime import UTC, datetime
from threading import RLock
from time import time

from trpc_service.tenant.models import QuotaPolicy


class QuotaExceeded(PermissionError):
    pass


class QuotaEnforcer:
    def __init__(self, redis_url: str | None = None, prefix: str = "trpc-agent") -> None:
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._usage: dict[tuple[str, str], tuple[int, float]] = {}
        self._lock = RLock()
        self._redis = None
        self._prefix = prefix
        url = redis_url or os.getenv("REDIS_URL", "").strip()
        self._coordination_required = bool(url and os.getenv("REQUIRE_SHARED_COORDINATION", "0") == "1")
        if url:
            try:
                import redis

                self._redis = redis.Redis.from_url(url, decode_responses=True)
                self._redis.ping()
            except Exception as exc:
                self._redis = None
                if self._coordination_required:
                    raise RuntimeError("shared quota coordination backend is unavailable") from exc

    def check(
        self,
        tenant_id: str,
        policy: QuotaPolicy,
        persisted_usage: tuple[int, float] = (0, 0.0),
        requested_tokens: int = 0,
        requested_cost: float = 0.0,
    ) -> None:
        self.reserve(
            tenant_id,
            policy,
            persisted_usage=persisted_usage,
            requested_tokens=requested_tokens,
            requested_cost=requested_cost,
        )

    def reserve(
        self,
        tenant_id: str,
        policy: QuotaPolicy,
        persisted_usage: tuple[int, float] = (0, 0.0),
        requested_tokens: int = 0,
        requested_cost: float = 0.0,
    ) -> None:
        """Atomically reserve QPS and daily budget for one request."""
        if self._redis is not None:
            self._reserve_redis(tenant_id, policy, persisted_usage, requested_tokens, requested_cost)
            return
        now = time()
        today = datetime.now(UTC).date().isoformat()
        with self._lock:
            requests = self._requests[tenant_id]
            while requests and now - requests[0] >= 1.0:
                requests.popleft()
            if len(requests) >= policy.qps_limit:
                raise QuotaExceeded("tenant QPS limit exceeded")
            tokens, cost = self._usage.get((tenant_id, today), persisted_usage)
            if tokens + requested_tokens > policy.daily_token_limit:
                raise QuotaExceeded("tenant daily token limit exceeded")
            if cost + requested_cost > policy.daily_cost_limit:
                raise QuotaExceeded("tenant daily cost limit exceeded")
            requests.append(now)
            self._usage[(tenant_id, today)] = (
                tokens + requested_tokens,
                cost + requested_cost,
            )

    def record(
        self,
        tenant_id: str,
        tokens: int,
        cost: float,
        reserved_tokens: int = 0,
        reserved_cost: float = 0.0,
    ) -> None:
        tokens -= reserved_tokens
        cost -= reserved_cost
        if tokens == 0 and abs(cost) < 1e-12:
            return
        today = datetime.now(UTC).date().isoformat()
        if self._redis is not None:
            key = f"{self._prefix}:quota:usage:{tenant_id}:{today}"
            self._redis.hincrby(key, "tokens", int(tokens))
            self._redis.hincrbyfloat(key, "cost", float(cost))
            self._redis.expire(key, 3 * 86400)
            return
        with self._lock:
            previous = self._usage.get((tenant_id, today), (0, 0.0))
            self._usage[(tenant_id, today)] = (previous[0] + tokens, previous[1] + cost)

    def release(self, tenant_id: str, reserved_tokens: int, reserved_cost: float) -> None:
        """Release a reservation when execution never reached final accounting."""
        if reserved_tokens == 0 and abs(reserved_cost) < 1e-12:
            return
        today = datetime.now(UTC).date().isoformat()
        if self._redis is not None:
            key = f"{self._prefix}:quota:usage:{tenant_id}:{today}"
            script = """
            local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens') or '0')
            local cost = tonumber(redis.call('HGET', KEYS[1], 'cost') or '0')
            tokens = math.max(0, tokens - tonumber(ARGV[1]))
            cost = math.max(0, cost - tonumber(ARGV[2]))
            redis.call('HSET', KEYS[1], 'tokens', tokens, 'cost', cost)
            return 1
            """
            self._redis.eval(script, 1, key, int(reserved_tokens), float(reserved_cost))
            return
        with self._lock:
            previous = self._usage.get((tenant_id, today), (0, 0.0))
            self._usage[(tenant_id, today)] = (
                max(0, previous[0] - reserved_tokens),
                max(0.0, previous[1] - reserved_cost),
            )

    def _reserve_redis(
        self,
        tenant_id: str,
        policy: QuotaPolicy,
        persisted_usage: tuple[int, float],
        requested_tokens: int,
        requested_cost: float,
    ) -> None:
        second = int(time())
        qps_key = f"{self._prefix}:quota:qps:{tenant_id}:{second}"
        usage_key = f"{self._prefix}:quota:usage:{tenant_id}:{datetime.now(UTC).date().isoformat()}"
        script = """
        local qps = tonumber(redis.call('GET', KEYS[1]) or '0')
        local usage_tokens = tonumber(redis.call('HGET', KEYS[2], 'tokens') or ARGV[5])
        local usage_cost = tonumber(redis.call('HGET', KEYS[2], 'cost') or ARGV[6])
        if qps + 1 > tonumber(ARGV[1]) then return 1 end
        if usage_tokens + tonumber(ARGV[2]) > tonumber(ARGV[3]) then return 2 end
        if usage_cost + tonumber(ARGV[4]) > tonumber(ARGV[7]) then return 3 end
        redis.call('INCR', KEYS[1])
        redis.call('EXPIRE', KEYS[1], 2)
        redis.call('HINCRBY', KEYS[2], 'tokens', tonumber(ARGV[2]))
        redis.call('HINCRBYFLOAT', KEYS[2], 'cost', tonumber(ARGV[4]))
        redis.call('EXPIRE', KEYS[2], 259200)
        return 0
        """
        result = self._redis.eval(
            script,
            2,
            qps_key,
            usage_key,
            policy.qps_limit,
            int(requested_tokens),
            policy.daily_token_limit,
            float(requested_cost),
            float(persisted_usage[0]),
            float(persisted_usage[1]),
            policy.daily_cost_limit,
        )
        if int(result) == 1:
            raise QuotaExceeded("tenant QPS limit exceeded")
        if int(result) == 2:
            raise QuotaExceeded("tenant daily token limit exceeded")
        if int(result) == 3:
            raise QuotaExceeded("tenant daily cost limit exceeded")
