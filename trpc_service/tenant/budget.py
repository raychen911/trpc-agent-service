"""Atomic distributed budget reservation and durable usage accounting."""

from __future__ import annotations

from datetime import date
from typing import Any
import uuid

from redis.asyncio import Redis
from redis.asyncio import from_url
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from trpc_service.config import TenantConfig

from .governance import BudgetExceededError


class BudgetBackendUnavailableError(RuntimeError):
    """Raised when Redis cannot safely complete a budget operation."""


_RESERVE = """
if redis.call('sismember', KEYS[2], ARGV[10]) == 1 then return 2 end
local requests = tonumber(redis.call('hget', KEYS[1], 'requests') or '0')
local input = tonumber(redis.call('hget', KEYS[1], 'input') or '0')
local output = tonumber(redis.call('hget', KEYS[1], 'output') or '0')
local cost = tonumber(redis.call('hget', KEYS[1], 'cost_micros') or '0')
if requests + ARGV[1] > tonumber(ARGV[5]) or
   input + ARGV[2] > tonumber(ARGV[6]) or
   output + ARGV[3] > tonumber(ARGV[7]) or
   cost + ARGV[4] > tonumber(ARGV[8]) then return 0 end
redis.call('hincrby', KEYS[1], 'requests', ARGV[1])
redis.call('hincrby', KEYS[1], 'input', ARGV[2])
redis.call('hincrby', KEYS[1], 'output', ARGV[3])
redis.call('hincrby', KEYS[1], 'cost_micros', ARGV[4])
redis.call('expire', KEYS[1], ARGV[9])
redis.call('sadd', KEYS[2], ARGV[10])
redis.call('expire', KEYS[2], ARGV[9])
return 1
"""

_SETTLE = """
if redis.call('sismember', KEYS[2], ARGV[5]) == 1 then return 0 end
redis.call('hincrby', KEYS[1], 'input', ARGV[1])
redis.call('hincrby', KEYS[1], 'output', ARGV[2])
redis.call('hincrby', KEYS[1], 'cost_micros', ARGV[3])
redis.call('expire', KEYS[1], ARGV[4])
redis.call('sadd', KEYS[2], ARGV[5])
redis.call('expire', KEYS[2], ARGV[4])
return 1
"""


class RedisBudgetLedger:
    """Lua-backed reservation; concurrent Gateways cannot overrun a quota."""

    def __init__(self, redis_url: str, client: Redis | None = None) -> None:
        self._redis = client or from_url(redis_url, decode_responses=True)
        self._owns_client = client is None

    async def reserve(self,
                      tenant: TenantConfig,
                      *,
                      input_tokens: int = 0,
                      output_tokens: int = 0,
                      cost_usd: float = 0,
                      requests: int = 1,
                      request_id: str = "") -> None:
        key = f"trpc-service:budget:{tenant.tenant_id}:{date.today().isoformat()}"
        reservation_key = f"{key}:reserved"
        operation_id = request_id or uuid.uuid4().hex
        limits = tenant.budget
        arguments = (requests, input_tokens, output_tokens, round(cost_usd * 1_000_000),
                     limits.daily_requests, limits.daily_input_tokens, limits.daily_output_tokens,
                     round(limits.daily_cost_usd * 1_000_000), 172800, operation_id)
        try:
            accepted = await self._redis.eval(_RESERVE, 2, key, reservation_key, *arguments)
        except (RedisConnectionError, RedisTimeoutError):
            # The server may have committed the Lua script before the socket was
            # lost. Clear stale pooled connections and retry with the same
            # operation id; the script then returns 2 without charging twice.
            await self._redis.connection_pool.disconnect()
            try:
                accepted = await self._redis.eval(_RESERVE, 2, key, reservation_key, *arguments)
            except (RedisConnectionError, RedisTimeoutError) as error:
                raise BudgetBackendUnavailableError("budget backend is unavailable") from error
        if not accepted:
            raise BudgetExceededError(f"daily budget exceeded for tenant {tenant.tenant_id}")

    async def reserve_request(self, tenant: TenantConfig) -> None:
        await self.reserve(tenant)

    async def ping(self) -> bool:
        """Report whether the Redis connection used by budget operations works."""
        try:
            return bool(await self._redis.ping())
        except (RedisConnectionError, RedisTimeoutError):
            await self._redis.connection_pool.disconnect()
            return False

    async def settle_actual(self,
                            tenant: TenantConfig,
                            *,
                            input_tokens: int,
                            output_tokens: int,
                            cost_usd: float,
                            request_id: str,
                            budget_day: str = "") -> None:
        """Charge actual usage without retrying a completed model on quota overage."""
        key = f"trpc-service:budget:{tenant.tenant_id}:{budget_day or date.today().isoformat()}"
        await self._redis.eval(_SETTLE, 2, key, f"{key}:settled", input_tokens, output_tokens,
                               round(cost_usd * 1_000_000), 604800, request_id)

    async def close(self) -> None:
        if self._owns_client:
            await self._redis.aclose()


class PostgresUsageLedger:
    """Idempotent actual token/cost ledger keyed by request and model."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def record(self, *, tenant_id: str, app_id: str, request_id: str, model_name: str, input_tokens: int,
                     output_tokens: int, cost_usd: float) -> bool:
        status = await self._pool.execute(
            """
            INSERT INTO usage_ledger
                (tenant_id,app_id,request_id,model_name,input_tokens,output_tokens,cost_usd)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (tenant_id,request_id,model_name) DO NOTHING
            """, tenant_id, app_id, request_id, model_name, input_tokens, output_tokens, cost_usd)
        return status.endswith("1")
