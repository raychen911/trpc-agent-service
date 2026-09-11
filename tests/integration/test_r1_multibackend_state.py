"""R1A integration: tenant BackendProfile migration + real Redis/SQL runtime state.

Part 1 (migration 0008, real PostgreSQL): head and history rows gain the
explicit default profile without any version changing; an already-explicit
profile survives a re-upgrade untouched; the column is NOT NULL with the
server default dropped; non-object JSON is refused by the CHECK; live
constraint/column names match ``storage/schema.py``; downgrade is symmetric
and re-upgrade restores the backfilled default.

Part 2 (runtime selection, real Redis + real PostgreSQL): two Workers (two
resolvers) share the backends; tenant A keeps Redis state and tenant B SQL
state; a turn executed by Worker A is visible to Worker B on the same
backend; the same external session ID for different tenants never collides;
an unreachable selected backend fails closed with zero model executions and
never falls back to the other backend.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
import uuid

import pytest

from trpc_service.agent.app import AgentApp
from trpc_service.storage.backend_resolver import TenantStateBackendResolver
from trpc_service.config.tenant import TenantConfig
from trpc_service.tenant.context import TenantContext
from tests.tenant_helpers import (
    FakeLLMModel,
    FakeModelProvider,
    make_app_config,
    make_audit_policy,
    make_backend_profile,
    make_governance,
)

from .pg_helpers import PostgreSQLContainer, docker_is_available, requires_docker, run_alembic

pytestmark = requires_docker

_REVISION_0007 = "0007_add_tenant_usage"

_LEGACY_GOV = ('{"allowed_channels":["web_console"],"allowed_user_ids":[],'
               '"tool_decisions":{},"content_policy":{"enabled":false,"input_action":"block",'
               '"output_action":"block"},"limits":null}')
_AUDIT_POLICY = '{"retention_days":365,"delivery_events":"all"}'


def _unique_tenant() -> str:
    return f"t{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="module")
def r1_pg():
    """One container for the whole module, Alembic-migrated to head."""
    if not docker_is_available():
        pytest.skip("Docker not available")
    pg = PostgreSQLContainer(name_prefix="trpc-r1a-pg")
    pg.start()
    try:
        result = run_alembic(pg.url, "upgrade", "head")
        assert result.returncode == 0, f"alembic upgrade failed: {result.stderr}"
        yield pg
    finally:
        pg.stop()


def _insert_legacy_rows(pg, tenant: str, head_version: int, history_versions: tuple[int, ...]) -> None:
    """Insert head + history rows while the database is still at 0007 (no
    backend_profile column exists at that revision)."""
    result = pg.run_sql("INSERT INTO tenant_configs "
                        "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,governance) VALUES "
                        f"('{tenant}',true,{head_version},'app_demo','legacy','default','[]'::jsonb,"
                        f"'{_LEGACY_GOV}'::jsonb);")
    assert result.success, result.output
    hist = ";".join("INSERT INTO tenant_config_versions "
                    "(tenant_id,version,enabled,app_id,instruction,model_profile,allowed_tools,governance) VALUES "
                    f"('{tenant}',{v},true,'app_demo','legacy','default','[]'::jsonb,'{_LEGACY_GOV}'::jsonb)"
                    for v in history_versions)
    if hist:
        result = pg.run_sql(hist)
        assert result.success, result.output


_PROFILE_PROJECTION = ("backend_profile->>'state_backend',"
                       " backend_profile->>'artifact_backend',"
                       " backend_profile->>'knowledge_backend',"
                       " backend_profile->>'audit_backend',"
                       " (SELECT count(*) FROM jsonb_object_keys(backend_profile))")


def _profile_row(pg, table: str, tenant: str) -> list[str]:
    """DISTINCT profile projection across every stored row of the tenant —
    all head/history rows must carry the identical profile."""
    result = pg.run_sql(f"SELECT DISTINCT {_PROFILE_PROJECTION} FROM {table} WHERE tenant_id = '{tenant}'")
    assert result.success, result.output
    lines = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]
    assert len(lines) == 1, f"expected one distinct profile row, got: {lines}"
    return lines[0].split("|")


class TestMigration0008BackendProfile:

    @pytest.fixture(autouse=True)
    def _at_head(self, r1_pg):
        """Each migration scenario starts from head; a failed mid-test
        downgrade can never strand the shared database for later tests."""
        run_alembic(r1_pg.url, "upgrade", "head", check=True)

    def test_head_and_history_backfill_default_profile_versions_unchanged(self, r1_pg):
        down = run_alembic(r1_pg.url, "downgrade", _REVISION_0007)
        assert down.returncode == 0, down.stderr
        tenant = _unique_tenant()
        _insert_legacy_rows(r1_pg, tenant, head_version=7, history_versions=(7, 3))
        up = run_alembic(r1_pg.url, "upgrade", "head")
        assert up.returncode == 0, up.stderr

        # The 0008 backfill value, exactly, on both tables — and every row
        # keeps four keys only (no fabricated extras).
        assert _profile_row(r1_pg, "tenant_configs", tenant) == ["redis", "s3", "sql", "sql", "4"]
        assert _profile_row(r1_pg, "tenant_config_versions", tenant) == ["redis", "s3", "sql", "sql", "4"]
        versions = r1_pg.run_sql(f"SELECT version FROM tenant_configs WHERE tenant_id = '{tenant}'")
        assert versions.success and versions.stdout.strip() == "7"
        hist = r1_pg.run_sql(f"SELECT string_agg(version::text, ',' ORDER BY version) "
                             f"FROM tenant_config_versions WHERE tenant_id = '{tenant}'")
        assert hist.success and hist.stdout.strip() == "3,7"

    def test_explicit_profile_survives_re_upgrade_and_default_is_dropped(self, r1_pg):
        tenant = _unique_tenant()
        explicit = ('{"state_backend": "sql", "artifact_backend": "s3",'
                    ' "knowledge_backend": "sql", "audit_backend": "sql"}')
        result = r1_pg.run_sql("INSERT INTO tenant_configs "
                               "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,"
                               "governance,backend_profile,audit_policy) VALUES "
                               f"('{tenant}',true,1,'app_demo','explicit','default','[]'::jsonb,"
                               f"'{_LEGACY_GOV}'::jsonb,'{explicit}'::jsonb,'{_AUDIT_POLICY}'::jsonb);")
        assert result.success, result.output
        result = r1_pg.run_sql("INSERT INTO tenant_config_versions "
                               "(tenant_id,version,enabled,app_id,instruction,model_profile,allowed_tools,"
                               "governance,backend_profile,audit_policy) VALUES "
                               f"('{tenant}',1,true,'app_demo','explicit','default','[]'::jsonb,"
                               f"'{_LEGACY_GOV}'::jsonb,'{explicit}'::jsonb,'{_AUDIT_POLICY}'::jsonb);")
        assert result.success, result.output

        up = run_alembic(r1_pg.url, "upgrade", "head")
        assert up.returncode == 0, up.stderr
        # re-running the migration must not rewrite an explicit sql profile
        assert _profile_row(r1_pg, "tenant_configs", tenant) == ["sql", "s3", "sql", "sql", "4"]

        # temporary server defaults were dropped: a write without the column fails
        no_profile = r1_pg.run_sql("INSERT INTO tenant_configs "
                                   "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,"
                                   f"governance,audit_policy) VALUES ('{_unique_tenant()}',true,1,'a','i','d',"
                                   "'[]'::jsonb,"
                                   f"'{_LEGACY_GOV}'::jsonb,'{_AUDIT_POLICY}'::jsonb);")
        assert not no_profile.success, "backend_profile must have no surviving server default"
        no_profile_hist = r1_pg.run_sql("INSERT INTO tenant_config_versions "
                                        "(tenant_id,version,enabled,app_id,instruction,model_profile,"
                                        "allowed_tools,governance,audit_policy) VALUES "
                                        f"('{tenant}',2,true,'a','i','d','[]'::jsonb,'{_LEGACY_GOV}'::jsonb,"
                                        f"'{_AUDIT_POLICY}'::jsonb);")
        assert not no_profile_hist.success, "backend_profile must have no surviving server default"

        # and the object CHECK refuses non-object JSON on both tables
        array_value = r1_pg.run_sql("INSERT INTO tenant_configs "
                                    "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,"
                                    "governance,backend_profile,audit_policy) VALUES "
                                    f"('{_unique_tenant()}',true,1,'a','i','d',"
                                    "'[]'::jsonb,"
                                    f"'{_LEGACY_GOV}'::jsonb,'[]'::jsonb,'{_AUDIT_POLICY}'::jsonb);")
        assert not array_value.success, "jsonb_typeof object CHECK must reject arrays"
        array_hist = r1_pg.run_sql("INSERT INTO tenant_config_versions "
                                   "(tenant_id,version,enabled,app_id,instruction,model_profile,allowed_tools,"
                                   f"governance,backend_profile,audit_policy) VALUES ('{tenant}',2,true,'a','i','d',"
                                   f"'[]'::jsonb,'{_LEGACY_GOV}'::jsonb,'\"redis\"'::jsonb,'{_AUDIT_POLICY}'::jsonb);")
        assert not array_hist.success, "jsonb_typeof object CHECK must reject scalars"

    def test_live_objects_match_storage_schema_metadata(self, r1_pg):
        from trpc_service.storage.schema import tenant_config_versions, tenant_configs

        for table, names in (
            (tenant_configs, {"tenant_configs_backend_profile_is_object"}),
            (tenant_config_versions, {"tenant_config_versions_backend_profile_is_object"}),
        ):
            declared = {getattr(c, "name", None) for c in table.constraints}
            assert names <= declared, f"schema.py must declare {names}, declared={declared}"
            for name in names:
                found = r1_pg.run_sql(f"SELECT COUNT(*) FROM pg_constraint WHERE conname = '{name}' AND contype = 'c'")
                assert found.success and int(found.stdout) == 1, f"{name} missing from live database"
            column = r1_pg.run_sql("SELECT data_type, is_nullable, column_default FROM information_schema.columns "
                                   f"WHERE table_name = '{table.name}' AND column_name = 'backend_profile'")
            assert column.success
            data_type, nullable, default = (part.strip() for part in column.stdout.strip().split("|"))
            assert data_type == "jsonb"
            assert nullable == "NO"
            assert default in ("", "NULL"), f"server default survived the migration: {default!r}"

    def test_downgrade_is_symmetric_and_re_upgrade_restores_backfill(self, r1_pg):
        tenant = _unique_tenant()
        result = r1_pg.run_sql("INSERT INTO tenant_configs "
                               "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,"
                               "governance,backend_profile,audit_policy) VALUES "
                               f"('{tenant}',true,5,'app_demo','keep','default','[]'::jsonb,"
                               f"'{_LEGACY_GOV}'::jsonb,"
                               '\'{"state_backend": "sql", "artifact_backend": "s3",'
                               ' "knowledge_backend": "sql", "audit_backend": "sql"}\'::jsonb,'
                               f"'{_AUDIT_POLICY}'::jsonb);")
        assert result.success, result.output

        down = run_alembic(r1_pg.url, "downgrade", _REVISION_0007)
        assert down.returncode == 0, down.stderr
        gone = r1_pg.run_sql(
            "SELECT (SELECT COUNT(*) FROM information_schema.columns WHERE column_name = 'backend_profile')"
            " || '|' ||"
            " (SELECT COUNT(*) FROM pg_constraint WHERE conname IN ('tenant_configs_backend_profile_is_object',"
            " 'tenant_config_versions_backend_profile_is_object'))")
        assert gone.success and gone.stdout.strip() == "0|0", gone.stdout

        up = run_alembic(r1_pg.url, "upgrade", "head")
        assert up.returncode == 0, up.stderr
        # the row survived the round trip and re-gained the explicit backfilled default
        assert _profile_row(r1_pg, "tenant_configs", tenant) == ["redis", "s3", "sql", "sql", "4"]
        versions = r1_pg.run_sql(f"SELECT version FROM tenant_configs WHERE tenant_id = '{tenant}'")
        assert versions.success and versions.stdout.strip() == "5"


# ---------------------------------------------------------------------------
# Part 2 — runtime state selection on REAL Redis + PostgreSQL
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_redis(redis_url: str, timeout: float = 15.0) -> bool:
    import redis

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = redis.from_url(redis_url, socket_timeout=1.0, socket_connect_timeout=1.0)
            r.ping()
            r.close()
            return True
        except Exception:
            time.sleep(0.2)
    return False


@pytest.fixture(scope="module")
def r1_redis_url():
    """A temporary Redis 7 container shared by the runtime-selection tests."""
    external = os.environ.get("TRPC_REDIS_URL")
    if external:
        yield external
        return
    container = f"trpc-r1a-redis-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    url = f"redis://127.0.0.1:{port}"
    subprocess.run(
        ["docker", "run", "-d", "--name", container, "-p", f"{port}:6379", "redis:7"],
        capture_output=True,
        check=True,
        timeout=30,
    )
    if not _wait_for_redis(url):
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=10)
        pytest.fail("Redis container did not become ready")
    try:
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=10)


def _resolver(redis_url: str, pg_url: str) -> TenantStateBackendResolver:
    return TenantStateBackendResolver.from_env({"TRPC_REDIS_URL": redis_url, "TRPC_DATABASE_URL": pg_url})


def _tenant_config(tenant_id: str, state_backend: str) -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        enabled=True,
        version=1,
        app=make_app_config(),
        governance=make_governance(),
        backend_profile=make_backend_profile(state_backend),
        audit_policy=make_audit_policy(),
    )


def _context(tenant_id: str) -> TenantContext:
    return TenantContext(tenant_id=tenant_id, app_id="app_demo", user_id="user_r1", channel="web")


async def _run_turn(app: AgentApp, config: TenantConfig, context: TenantContext, session_id: str, text: str) -> None:
    async for _ in app.run(config=config, context=context, session_id=session_id, user_input=text):
        pass


async def _session_history(resolver: TenantStateBackendResolver, config: TenantConfig, user_id: str,
                           session_id: str) -> list[str]:
    state = resolver.resolve(config.backend_profile)
    app_name = f"{config.tenant_id}:{config.app.app_id}:v{config.version}"
    session = await state.session_service.get_session(app_name=app_name, user_id=user_id, session_id=session_id)
    if session is None:
        return []
    return [
        event.content.parts[0].text for event in session.events
        if event.content and event.content.parts and event.content.parts[0].text
    ]


@pytest.fixture()
def worker_pair(r1_redis_url, r1_pg):
    """A factory making independent Workers, each owning one Redis + one SQL
    backend.  ``close_all`` MUST be awaited inside the scenario's own event
    loop (same loop that created the SDK pools); a last-resort sync guard at
    teardown swallows any cross-loop error from an aborted scenario."""
    created = []

    def make_worker():
        resolver = _resolver(r1_redis_url, r1_pg.url)
        model = FakeLLMModel()
        app = AgentApp(model_provider=FakeModelProvider({"default": model}), backend_resolver=resolver)
        created.append(resolver)
        return app, resolver, model

    async def close_all():
        for resolver in created:
            await resolver.close()

    yield make_worker, close_all
    for resolver in created:
        if getattr(resolver, "_closed", False):
            continue
        try:
            asyncio.run(resolver.close())
        except Exception:
            pass


class TestTwoTenantRuntimeSelection:

    def _unique_tenant(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    def test_tenant_a_redis_state_written_by_worker_a_read_by_worker_b(self, worker_pair):
        tenant = self._unique_tenant("r1a")
        config = _tenant_config(tenant, "redis")
        ctx = _context(tenant)
        session_id = "sess-cross-worker"

        make_worker, close_all = worker_pair

        async def _scenario():
            try:
                app_a, resolver_a, _ = make_worker()
                app_b, resolver_b, _ = make_worker()
                await _run_turn(app_a, config, ctx, session_id, "marker written via redis backend")
                # same session visible from the SECOND worker's Redis backend
                history = await _session_history(resolver_b, config, ctx.sdk_user_id, session_id)
                assert any("marker written via redis backend" in text for text in history)
                # and a second turn from worker B continues the same session
                await _run_turn(app_b, config, ctx, session_id, "second turn from worker B")
                history_after = await _session_history(resolver_a, config, ctx.sdk_user_id, session_id)
                assert any("second turn from worker B" in text for text in history_after)
            finally:
                await close_all()

        asyncio.run(_scenario())

    def test_tenant_b_sql_state_written_by_worker_a_read_by_worker_b(self, worker_pair):
        tenant = self._unique_tenant("r1b")
        config = _tenant_config(tenant, "sql")
        ctx = _context(tenant)
        session_id = "sess-cross-worker-sql"

        make_worker, close_all = worker_pair

        async def _scenario():
            try:
                app_a, _, _ = make_worker()
                app_b, resolver_b, _ = make_worker()
                await _run_turn(app_a, config, ctx, session_id, "marker written via sql backend")
                # worker B's own SQL service reloads the session worker A wrote
                history = await _session_history(resolver_b, config, ctx.sdk_user_id, session_id)
                assert any("marker written via sql backend" in text for text in history)
                await _run_turn(app_b, config, ctx, session_id, "second sql turn from worker B")
                # and the second worker sees its own continuation from the
                # shared database
                history_after = await _session_history(resolver_b, config, ctx.sdk_user_id, session_id)
                assert any("second sql turn from worker B" in text for text in history_after)
                # a session that was never written reads back empty (no phantom)
                assert await _session_history(resolver_b, config, ctx.sdk_user_id, "never-created-session") == []
            finally:
                await close_all()

        asyncio.run(_scenario())

    def test_same_external_session_id_does_not_collide_across_tenants(self, worker_pair):
        tenant_a = self._unique_tenant("r1c")
        tenant_b = self._unique_tenant("r1d")
        config_a = _tenant_config(tenant_a, "redis")
        config_b = _tenant_config(tenant_b, "sql")
        ctx_a = _context(tenant_a)
        ctx_b = _context(tenant_b)
        session_id = "same-external-session-id"

        make_worker, close_all = worker_pair

        async def _scenario():
            try:
                app_w, resolver_w, _ = make_worker()
                await _run_turn(app_w, config_a, ctx_a, session_id, "tenant A payload")
                await _run_turn(app_w, config_b, ctx_b, session_id, "tenant B payload")

                # each tenant reads back ONLY its own payload on its own backend
                hist_a = await _session_history(resolver_w, config_a, ctx_a.sdk_user_id, session_id)
                hist_b = await _session_history(resolver_w, config_b, ctx_b.sdk_user_id, session_id)
                assert any("tenant A payload" in t for t in hist_a)
                assert not any("tenant B payload" in t for t in hist_a)
                assert any("tenant B payload" in t for t in hist_b)
                assert not any("tenant A payload" in t for t in hist_b)

                # the identical external session id exists on neither backend
                # for the other tenant (Redis has no tenant-B session, SQL has
                # none for tenant A under any identity)
                assert await _session_history(resolver_w, _tenant_config(tenant_b, "redis"), ctx_b.sdk_user_id,
                                              session_id) == []
                assert await _session_history(resolver_w, _tenant_config(tenant_a, "sql"), ctx_a.sdk_user_id,
                                              session_id) == []
            finally:
                await close_all()

        asyncio.run(_scenario())

    @pytest.mark.parametrize("state_backend", ["redis", "sql"])
    def test_selected_backend_outage_fails_closed_zero_model_execution(self, r1_pg, r1_redis_url, state_backend):
        """The healthy OTHER backend must never absorb the request."""
        tenant = self._unique_tenant("r1e")
        config = _tenant_config(tenant, state_backend)
        ctx = _context(tenant)
        dead_port = _free_port()
        dead_url = (f"redis://127.0.0.1:{dead_port}"
                    if state_backend == "redis" else f"postgresql+asyncpg://u:p@127.0.0.1:{dead_port}/d")
        model = FakeLLMModel()

        async def _scenario():
            # constructed inside the loop: the SQL backend's background TTL
            # task must live on the same loop that later closes it
            # The chosen backend is dead; the OTHER backend stays perfectly
            # healthy to prove a "fallback" would have succeeded — and must
            # not be used.
            resolver = (_resolver(dead_url, r1_pg.url) if state_backend == "redis" else _resolver(
                r1_redis_url, dead_url))
            healthy = _resolver(r1_redis_url, r1_pg.url)
            app = AgentApp(model_provider=FakeModelProvider({"default": model}), backend_resolver=resolver)
            try:
                with pytest.raises(Exception):
                    await _run_turn(app, config, ctx, "sess-outage", "must never execute")
                assert model.call_count == 0
                # nothing was persisted anywhere — neither the dead backend
                # nor a fallback wrote the session into the healthy pair
                assert await _session_history(healthy, config, ctx.sdk_user_id, "sess-outage") == []
                assert model.calls == []
            finally:
                await resolver.close()
                await healthy.close()

        asyncio.run(_scenario())


__all__ = ["TestMigration0008BackendProfile", "TestTwoTenantRuntimeSelection"]
