"""Stage 6C Task 2 integration: rate limiting + usage accounting on real backends.

Proves against real Redis and Alembic-managed PostgreSQL that:
- the Lua fixed-window limiter is atomic across independent clients (two
  Gateway/Worker processes can never jointly exceed the limit) and fail-closed
  when Redis is unreachable;
- migration 0007 backfills an explicit ``null`` limits policy over legacy
  head/history rows without touching versions, preserves explicit policies,
  and its downgrade is symmetric; the extended audit error vocabulary accepts
  ``usage_budget_exceeded`` and still rejects foreign codes;
- ``tenant_usage_daily`` upserts are atomic under real concurrency and SQL
  NULL arithmetic keeps a day with any unknown component unknown (never a
  fabricated zero).
"""

from __future__ import annotations

import asyncio
import subprocess
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.governance.limits import (
    RateLimiterUnavailableError,
    RedisTenantRateLimiter,
    rate_limit_key,
)
from trpc_service.storage.usage_repository import (
    SqlUsageRepository,
    UsageRepositoryDataError,
)
from trpc_service.usage.models import UsageIncrement

from .pg_helpers import PostgreSQLContainer, docker_is_available, requires_docker, run_alembic

pytestmark = requires_docker


def _docker_redis_available() -> bool:
    try:
        return subprocess.run(["docker", "pull", "redis:7"], capture_output=True, timeout=60).returncode == 0
    except Exception:
        return False


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def redis_url():
    if not docker_is_available() or not _docker_redis_available():
        pytest.skip("Docker/redis image not available")
    name = f"trpc-6c-redis-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    subprocess.run(["docker", "run", "-d", "--name", name, "-p", f"{port}:6379", "redis:7"],
                   capture_output=True,
                   check=True,
                   timeout=60)
    url = f"redis://127.0.0.1:{port}"
    import redis.asyncio as aioredis

    async def _wait():
        for _ in range(40):
            try:
                client = aioredis.from_url(url)
                await client.ping()
                await client.aclose()
                return True
            except Exception:
                await asyncio.sleep(0.25)
        return False

    assert asyncio.run(_wait()), "Redis container not ready"
    try:
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)


@pytest.fixture(scope="module")
def migrated_pg():
    pg = PostgreSQLContainer(name_prefix="trpc-6c-pg")
    pg.start()
    try:
        result = run_alembic(pg.url, "upgrade", "head")
        assert result.returncode == 0, result.stderr
        yield pg
    finally:
        pg.stop()


@pytest.fixture(scope="module")
def migrated_url(migrated_pg):
    return migrated_pg.url


class TestRedisAtomicRateLimiting:

    def test_two_independent_clients_never_exceed_limit(self, redis_url):
        limit = 10
        attempts = 40

        async def _scenario():
            # Two limiter instances = two Gateway/Worker processes.
            limiter_a = RedisTenantRateLimiter.from_env({"TRPC_REDIS_URL": redis_url})
            limiter_b = RedisTenantRateLimiter.from_env({"TRPC_REDIS_URL": redis_url})
            tenant = f"t{uuid.uuid4().hex[:10]}"
            try:
                results = await asyncio.gather(*[(limiter_a if i % 2 else limiter_b).acquire(tenant, limit)
                                                 for i in range(attempts)])
            finally:
                await limiter_a.close()
                await limiter_b.close()
            return tenant, results

        tenant, results = asyncio.run(_scenario())
        assert sum(1 for r in results if r) == limit

    def test_window_key_has_ttl_and_new_minute_is_independent(self, redis_url):

        async def _scenario():
            import redis.asyncio as aioredis

            limiter = RedisTenantRateLimiter.from_env({"TRPC_REDIS_URL": redis_url})
            raw = aioredis.from_url(redis_url)
            tenant = f"t{uuid.uuid4().hex[:10]}"
            try:
                assert await limiter.acquire(tenant, 1) is True
                assert await limiter.acquire(tenant, 1) is False
                key = rate_limit_key(tenant)
                ttl = await raw.ttl(key)
                assert 0 < ttl <= 120
                # a future UTC minute sees a fresh window
                future_key = rate_limit_key(tenant, now=datetime.now(timezone.utc) + timedelta(minutes=1))
                assert future_key != key
                assert await limiter.acquire(tenant, 1) is False  # same minute: still limited
            finally:
                await raw.aclose()
                await limiter.close()

        asyncio.run(_scenario())

    def test_unreachable_redis_fails_closed(self):

        async def _scenario():
            limiter = RedisTenantRateLimiter.from_env({"TRPC_REDIS_URL": "redis://127.0.0.1:1"})
            try:
                with pytest.raises(RateLimiterUnavailableError):
                    await limiter.acquire("tenant_x", 5)
            finally:
                await limiter.close()

        asyncio.run(_scenario())


class TestMigration0007:

    def test_backfill_sets_explicit_null_limits_and_preserves_explicit(self, migrated_url, migrated_pg):
        down = run_alembic(migrated_url, "downgrade", "0006_add_execution_audit")
        assert down.returncode == 0, down.stderr
        legacy = f"t{uuid.uuid4().hex[:10]}"
        explicit = f"t{uuid.uuid4().hex[:10]}"
        legacy_gov = ('{"allowed_channels":["web_console"],"allowed_user_ids":[],'
                      '"tool_decisions":{},"content_policy":{"enabled":false,'
                      '"input_action":"block","output_action":"block"}}')
        explicit_gov = ('{"allowed_channels":["web_console"],"allowed_user_ids":[],'
                        '"tool_decisions":{},"content_policy":{"enabled":false,'
                        '"input_action":"block","output_action":"block"},'
                        '"limits":{"requests_per_minute":7,"daily_total_tokens":null,'
                        '"daily_cost_microunits":null}}')
        inserted = migrated_pg.run_sql(
            "INSERT INTO tenant_configs "
            "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,governance) VALUES "
            f"('{legacy}',true,9,'app_demo','l','default','[]'::jsonb,'{legacy_gov}'::jsonb),"
            f"('{explicit}',true,4,'app_demo','e','default','[]'::jsonb,'{explicit_gov}'::jsonb);"
            "INSERT INTO tenant_config_versions "
            "(tenant_id,version,enabled,app_id,instruction,model_profile,allowed_tools,governance) VALUES "
            f"('{legacy}',9,true,'app_demo','l','default','[]'::jsonb,'{legacy_gov}'::jsonb);")
        assert inserted.success, inserted.output
        up = run_alembic(migrated_url, "upgrade", "head")
        assert up.returncode == 0, up.stderr
        check = migrated_pg.run_sql("SELECT h.tenant_id, jsonb_exists(h.governance,'limits'), "
                                    "(h.governance->>'limits') IS NULL, h.version "
                                    "FROM tenant_configs h WHERE h.tenant_id IN "
                                    f"('{legacy}','{explicit}') ORDER BY h.tenant_id")
        assert check.success, check.output
        rows = {line.split("|")[0]: line.split("|")[1:] for line in check.stdout.strip().splitlines()}
        # key present + JSON-null value => explicit unlimited backfill; version untouched
        assert rows[legacy] == ["t", "t", "9"]
        # explicit limits preserved (key present, value NOT json-null)
        assert rows[explicit] == ["t", "f", "4"]

        down2 = run_alembic(migrated_url, "downgrade", "0006_add_execution_audit")
        assert down2.returncode == 0, down2.stderr
        stripped = migrated_pg.run_sql("SELECT COUNT(*) FROM tenant_configs WHERE jsonb_exists(governance,'limits') OR "
                                       "jsonb_exists(governance->'content_policy','limits')")
        assert stripped.success and int(stripped.stdout) == 0
        gone = migrated_pg.run_sql(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='tenant_usage_daily'")
        assert int(gone.stdout) == 0
        up2 = run_alembic(migrated_url, "upgrade", "head")
        assert up2.returncode == 0, up2.stderr

    def test_audit_error_vocabulary_extended(self, migrated_pg):
        # receipt-bound agent_result/error rows exercise the new vocabulary:
        # usage_budget_exceeded accepted, foreign codes still rejected.
        tenant = f"t{uuid.uuid4().hex[:10]}"
        msg_id = f"v-{uuid.uuid4().hex[:8]}"
        inserted = migrated_pg.run_sql("INSERT INTO message_receipts "
                                       "(receipt_id,tenant_id,channel,user_id,session_id,message_id,app_id,"
                                       "config_version,request_id,message_digest,state,started_at) VALUES "
                                       f"(gen_random_uuid(),'{tenant}','web_console','u','s','{msg_id}','a',1,"
                                       "gen_random_uuid(),repeat('0',64),'processing',now())")
        assert inserted.success, inserted.output

        def _insert(code):
            return migrated_pg.run_sql(
                "INSERT INTO execution_audit_events (audit_id, tenant_id, receipt_id, request_id, "
                "config_version, trace_id, event_type, outcome, category, tool_name, error_code, "
                "latency_ms, occurred_at) SELECT gen_random_uuid(), r.tenant_id, r.receipt_id, "
                "r.request_id, r.config_version, NULL, 'agent_result', 'error', NULL, NULL, "
                f"'{code}', NULL, now() FROM message_receipts r "
                f"WHERE r.tenant_id='{tenant}' AND r.message_id='{msg_id}'")

        accepted = _insert("usage_budget_exceeded")
        assert accepted.success, accepted.output
        # the accepted row must REALLY exist (guard against 0-row SELECT)
        count = migrated_pg.run_sql(
            "SELECT COUNT(*) FROM execution_audit_events e "
            f"JOIN message_receipts r ON r.receipt_id = e.receipt_id WHERE r.message_id='{msg_id}'")
        assert count.success and count.stdout.strip() == "1", count.output
        rejected = _insert("rate_limited")
        assert not rejected.success, "foreign error code must violate the CHECK"
        assert "violates check constraint" in rejected.output.lower(), rejected.output


def _increment(tenant, profile="default", requests=1, input_tokens=5, output_tokens=7, cost=90):
    return UsageIncrement(
        usage_date=datetime.now(timezone.utc).date(),
        tenant_id=tenant,
        model_profile=profile,
        requests=requests,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_microunits=cost,
    )


class TestUsageRepositoryReal:

    def test_concurrent_upserts_accumulate_atomically(self, migrated_url):
        tenant = f"t{uuid.uuid4().hex[:10]}"

        async def _one(seed):
            engine = create_async_engine(migrated_url)
            repo = SqlUsageRepository(engine, owns_engine=False)
            try:
                await repo.add_usage(
                    _increment(tenant, profile="default", requests=1, input_tokens=10, output_tokens=20, cost=5))
            finally:
                await engine.dispose()

        async def _main():
            await asyncio.gather(*[_one(i) for i in range(6)])
            engine = create_async_engine(migrated_url)
            repo = SqlUsageRepository(engine, owns_engine=False)
            try:
                agg = await repo.get_daily(tenant, datetime.now(timezone.utc).date())
            finally:
                await engine.dispose()
            return agg

        agg = asyncio.run(_main())
        assert agg.total_requests == 6
        assert agg.profiles[0].input_tokens == 60
        assert agg.profiles[0].output_tokens == 120
        assert agg.profiles[0].cost_microunits == 30

    def test_unknown_propagates_never_zero(self, migrated_url):
        tenant = f"t{uuid.uuid4().hex[:10]}"

        async def _main():
            engine = create_async_engine(migrated_url)
            repo = SqlUsageRepository(engine, owns_engine=False)
            try:
                await repo.add_usage(_increment(tenant, input_tokens=10, output_tokens=10, cost=5))
                await repo.add_usage(_increment(tenant, input_tokens=None, output_tokens=None, cost=None))
                return await repo.get_daily(tenant, datetime.now(timezone.utc).date())
            finally:
                await engine.dispose()

        agg = asyncio.run(_main())
        row = agg.profiles[0]
        assert row.requests == 2  # requests are never "unknown"
        assert row.input_tokens is None and row.output_tokens is None
        assert row.cost_microunits is None
        assert agg.tokens_or_none() == (None, None)
        assert agg.cost_microunits_or_none() is None

    def test_profiles_isolated_and_check_constraints(self, migrated_url):
        tenant = f"t{uuid.uuid4().hex[:10]}"

        async def _main():
            engine = create_async_engine(migrated_url)
            repo = SqlUsageRepository(engine, owns_engine=False)
            try:
                await repo.add_usage(_increment(tenant, profile="default"))
                await repo.add_usage(_increment(tenant, profile="premium", input_tokens=1, output_tokens=1, cost=1))
                agg = await repo.get_daily(tenant, datetime.now(timezone.utc).date())
                # DB-level CHECK (bypass pydantic ge=0): the negative value
                # must be rejected by the database, not only by the model.
                bogus = UsageIncrement.model_construct(
                    usage_date=datetime.now(timezone.utc).date(),
                    tenant_id=tenant,
                    model_profile="default",
                    requests=-1,
                )
                with pytest.raises(UsageRepositoryDataError):
                    await repo.add_usage(bogus)
                return agg
            finally:
                await engine.dispose()

        agg = asyncio.run(_main())
        assert [p.model_profile for p in agg.profiles] == ["default", "premium"]
