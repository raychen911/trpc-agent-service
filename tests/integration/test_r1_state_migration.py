"""R1B integration: durable Session Summary + offline Redis<->SQL state migration.

Part 1 (restart durability, REAL Redis + REAL PostgreSQL): a summary produced
through the runtime's per-runtime SDK summarizer, together with the retained
historical Events, session state, and Memory, must remain visible to a NEW
backend (closed + recreated services, i.e. the next Worker) — the acceptance
criterion for "Summary durable" (no custom tables).

Part 2 (offline migration): real Redis<->SQL copies through the public SDK
services, count/canonical-summary validation before the single
expected_version CAS on ``backend_profile.state_backend``, refusal on
``processing`` receipts (real message-receipt repository), duplicate IM
receipt regression after cutover, continuation turns by a second Worker on
the target backend, source retention, deterministic re-entry, and the
reverse SQL->Redis rollback path.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
import uuid

import pytest

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import EventActions

from trpc_service.agent.app import AgentApp
from trpc_service.storage.backend_resolver import TenantStateBackendResolver
from trpc_service.config.tenant import TenantConfig
from trpc_service.storage.message_repository import ReceiptAction, SqlMessageReceiptRepository
from trpc_service.storage.state_migration import (
    MSG_NOT_OFFLINE,
    MSG_PROCESSING_RECEIPTS,
    MSG_SAME_BACKEND,
    StateMigrationPreconditionError,
    migrate_tenant_state,
)
from trpc_service.storage.tenant_repository import SqlTenantConfigRepository
from trpc_service.tenant.context import TenantContext
from trpc_service.transport.models import WorkerTask
from tests.tenant_helpers import (
    FakeLLMModel,
    FakeModelProvider,
    make_app_config,
    make_audit_policy,
    make_backend_profile,
    make_governance,
)

from .pg_helpers import PostgreSQLContainer, docker_is_available, free_port, requires_docker, run_alembic

pytestmark = requires_docker


def _unique_tenant(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:10]}"


@pytest.fixture(scope="module")
def r1b_pg():
    """One PostgreSQL container for the module, Alembic-migrated to head."""
    if not docker_is_available():
        pytest.skip("Docker not available")
    pg = PostgreSQLContainer(name_prefix="trpc-r1b-pg")
    pg.start()
    try:
        result = run_alembic(pg.url, "upgrade", "head")
        assert result.returncode == 0, f"alembic upgrade failed: {result.stderr}"
        yield pg
    finally:
        pg.stop()


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
def r1b_redis_url():
    """A temporary Redis 7 container shared by the migration scenarios."""
    external = os.environ.get("TRPC_REDIS_URL")
    if external:
        yield external
        return
    container = f"trpc-r1b-redis-{uuid.uuid4().hex[:8]}"
    port = free_port()
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


def _tenant_config(tenant_id: str, state_backend: str, version: int = 1) -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        enabled=True,
        version=version,
        app=make_app_config(),
        governance=make_governance(),
        backend_profile=make_backend_profile(state_backend),
        audit_policy=make_audit_policy(),
    )


def _context(tenant_id: str) -> TenantContext:
    return TenantContext(tenant_id=tenant_id, app_id="app_demo", user_id="user_r1b", channel="web")


def _namespace(config: TenantConfig) -> str:
    return f"{config.tenant_id}:{config.app.app_id}:v{config.version}"


async def _run_turn(app: AgentApp, config: TenantConfig, context: TenantContext, session_id: str, text: str) -> None:
    async for _ in app.run(config=config, context=context, session_id=session_id, user_input=text):
        pass


async def _drive_summary(app: AgentApp, config: TenantConfig, context: TenantContext, session_id: str) -> None:
    """Run the SDK post-turn summary path through the runtime's injected
    per-runtime summarizer (same call the Runner makes at turn completion).

    The default SDK trigger is a conversation-count threshold; setting the
    counter crosses that SAME public threshold without 100 model turns.
    """
    key = (context.tenant_id, config.app.app_id, config.version)
    runtime = app._cache[key]
    service = runtime._runner.session_service
    session = await service.get_session(app_name=_namespace(config), user_id=context.sdk_user_id, session_id=session_id)
    assert session is not None
    # A state delta persisted alongside Events (state durability check).
    await service.append_event(
        session,
        Event(
            invocation_id="r1b-state",
            author="app_demo",
            actions=EventActions(state_delta={"r1b_persisted": "yes"}),
        ),
    )
    session.conversation_count = 101
    await service.create_session_summary(session)


def _summary_and_history(session):
    summary_events = [e for e in session.events if e.is_summary_event()]
    return summary_events


# ---------------------------------------------------------------------------
# Part 1 — restart durability (Redis and SQL)
# ---------------------------------------------------------------------------


class TestSummaryRestartDurability:

    @pytest.mark.parametrize("state_backend", ["redis", "sql"])
    def test_summary_history_state_memory_survive_backend_recreate(self, r1b_redis_url, r1b_pg, state_backend):
        tenant = _unique_tenant("r1bdur")
        config = _tenant_config(tenant, state_backend)
        ctx = _context(tenant)
        session_id = "sess-durable"
        marker = f"durable marker {uuid.uuid4().hex[:8]}"

        # Two backends are constructed inside one asyncio.run loop; the SQL
        # service's background cleanup task must live on its closing loop.
        async def _scenario():
            resolver = _resolver(r1b_redis_url, r1b_pg.url)
            app = AgentApp(model_provider=FakeModelProvider({"default": FakeLLMModel()}), backend_resolver=resolver)
            save_key = None
            try:
                await _run_turn(app, config, ctx, session_id, marker)
                await _drive_summary(app, config, ctx, session_id)
                # Capture the memory key while the session is readable.
                pre = await resolver.resolve(config.backend_profile
                                             ).session_service.get_session(app_name=_namespace(config),
                                                                           user_id=ctx.sdk_user_id,
                                                                           session_id=session_id)
                assert pre is not None
                save_key = pre.save_key
            finally:
                await app.close()  # drains runtimes, closes BOTH owned backends once

            # ---- next Worker: brand-new backend services ----------------
            resolver2 = _resolver(r1b_redis_url, r1b_pg.url)
            app2 = None
            try:
                state = resolver2.resolve(config.backend_profile)
                reloaded = await state.session_service.get_session(app_name=_namespace(config),
                                                                   user_id=ctx.sdk_user_id,
                                                                   session_id=session_id)
                assert reloaded is not None
                summaries = _summary_and_history(reloaded)
                assert len(summaries) == 1, "SDK summary Event must survive backend recreate"
                assert summaries[0].author == "system"
                # Retained historical Events still contain the ORIGINAL user
                # turn text (not just the compressed summary).
                retained_text = " ".join(part.text for e in reloaded.historical_events
                                         for part in (e.content.parts if e.content else []) if part.text)
                assert marker in retained_text
                # Session state survived the summary round trip.
                assert reloaded.state.get("r1b_persisted") == "yes"
                # Memory remains searchable from the new backend connection.
                mem = await state.memory_service.search_memory(key=save_key, query=marker.split()[0], limit=10)
                assert mem.memories, "stored Memory must be visible after backend recreate"
                # The new Worker can CONTINUE the same session on this backend.
                app2 = AgentApp(model_provider=FakeModelProvider({"default": FakeLLMModel()}),
                                backend_resolver=resolver2)
                await _run_turn(app2, config, ctx, session_id, "continuation after restart")
                after = await resolver2.resolve(config.backend_profile
                                                ).session_service.get_session(app_name=_namespace(config),
                                                                              user_id=ctx.sdk_user_id,
                                                                              session_id=session_id)
                assert any("continuation after restart" in (part.text or "") for e in after.events
                           for part in (e.content.parts if e.content else []))
            finally:
                # AgentApp owns the resolver; closing it disposes both
                # backends exactly once (resolver.close is idempotent).
                if app2 is not None:
                    await app2.close()
                await resolver2.close()

        asyncio.run(_scenario())


def _task(tenant: str, session_id: str) -> WorkerTask:
    return WorkerTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=tenant,
        app_id="app_demo",
        config_version=1,
        user_id="ext-user-1",
        channel="web",
        session_id=session_id,
        message_id=f"msg-{uuid.uuid4().hex}",
        message="in flight",
    )


async def _snapshot_event_ids(session_service, app_name: str) -> dict:
    resp = await session_service.list_sessions(app_name=app_name)
    out = {}
    for listed in resp.sessions:
        session = await session_service.get_session(app_name=app_name, user_id=listed.user_id, session_id=listed.id)
        out[f"{listed.user_id}/{listed.id}"] = {
            "events": [(e.id, e.timestamp) for e in session.events],
            "historical": [(e.id, e.timestamp) for e in session.historical_events],
            "state": session.state,
            "conversation_count": session.conversation_count,
        }
    return out


class TestOfflineStateCutover:

    @pytest.mark.parametrize(("source", "target"), [("redis", "sql"), ("sql", "redis")])
    def test_cutover_refuses_validates_flips_and_survives_two_workers(self, r1b_redis_url, r1b_pg, source, target):
        tenant = _unique_tenant("r1bcut")
        config = _tenant_config(tenant, source)
        ctx = _context(tenant)
        marker = f"cutover-{uuid.uuid4().hex[:8]}"
        task = _task(tenant, "sess-pending")

        async def _scenario():
            resolver = _resolver(r1b_redis_url, r1b_pg.url)
            app = AgentApp(model_provider=FakeModelProvider({"default": FakeLLMModel()}), backend_resolver=resolver)
            tenant_repo = SqlTenantConfigRepository.from_env({"TRPC_DATABASE_URL": r1b_pg.url})
            receipts = SqlMessageReceiptRepository.from_env({"TRPC_DATABASE_URL": r1b_pg.url})
            head = None
            try:
                await tenant_repo.create(config)
                await _run_turn(app, config, ctx, "sess-m", marker)
                await _drive_summary(app, config, ctx, "sess-m")
                await _run_turn(app, config, ctx, "sess-m2", "second conversation thread")

                src_svc = resolver.resolve(make_backend_profile(source)).session_service
                source_before = await _snapshot_event_ids(src_svc, _namespace(config))
                assert "sess-m" in " ".join(source_before) and source_before

                # -- preconditions against the REAL receipt repository ------
                claim = await receipts.claim(task, task.message)
                assert claim.action == ReceiptAction.EXECUTE
                with pytest.raises(StateMigrationPreconditionError) as busy:
                    await migrate_tenant_state(
                        tenant_id=tenant,
                        target_backend=target,
                        expected_version=1,
                        offline=True,
                        tenant_repository=tenant_repo,
                        receipt_repository=receipts,
                        resolver=resolver,
                    )
                assert str(busy.value) == MSG_PROCESSING_RECEIPTS
                unchanged = await tenant_repo.get(tenant)
                assert unchanged.version == 1 and unchanged.backend_profile.state_backend == source

                with pytest.raises(StateMigrationPreconditionError) as same:
                    await migrate_tenant_state(
                        tenant_id=tenant,
                        target_backend=source,
                        expected_version=1,
                        offline=True,
                        tenant_repository=tenant_repo,
                        receipt_repository=receipts,
                        resolver=resolver,
                    )
                assert str(same.value) == MSG_SAME_BACKEND
                with pytest.raises(StateMigrationPreconditionError) as notoffline:
                    await migrate_tenant_state(
                        tenant_id=tenant,
                        target_backend=target,
                        expected_version=1,
                        offline=False,
                        tenant_repository=tenant_repo,
                        receipt_repository=receipts,
                        resolver=resolver,
                    )
                assert str(notoffline.value) == MSG_NOT_OFFLINE

                await receipts.complete(claim.receipt_id, "done", 5)

                # -- the actual cutover -------------------------------------
                result = await migrate_tenant_state(
                    tenant_id=tenant,
                    target_backend=target,
                    expected_version=1,
                    offline=True,
                    tenant_repository=tenant_repo,
                    receipt_repository=receipts,
                    resolver=resolver,
                )
                assert (result.source_backend, result.target_backend) == (source, target)
                assert (result.source_version, result.target_version) == (1, 2)
                assert result.session_count == 2
                assert result.event_count == sum(
                    len(v["events"]) + len(v["historical"]) for v in source_before.values())

                head = await tenant_repo.get(tenant)
                assert head is not None
                assert head.version == 2 and head.backend_profile.state_backend == target
                # only state_backend changed inside the profile; the rest of
                # the configuration is carried through untouched.
                assert head.app == config.app and head.governance == config.governance

                # -- target holds summary/history/state/memory --------------
                tgt_svc = resolver.resolve(make_backend_profile(target))
                s1 = await tgt_svc.session_service.get_session(app_name=_namespace(head),
                                                               user_id=ctx.sdk_user_id,
                                                               session_id="sess-m")
                assert s1 is not None
                assert len(_summary_and_history(s1)) == 1
                retained = " ".join(part.text for e in s1.historical_events
                                    for part in (e.content.parts if e.content else []) if part.text)
                assert marker in retained
                assert s1.state.get("r1b_persisted") == "yes"
                mem = await tgt_svc.memory_service.search_memory(key=s1.save_key, query=marker.split()[0], limit=10)
                assert mem.memories

                # -- source namespace retained byte-identical ---------------
                source_after = await _snapshot_event_ids(src_svc, _namespace(config))
                assert source_after == source_before

                # -- duplicate IM delivery regression AFTER cutover ---------
                replay = await receipts.claim(task, task.message)
                assert replay.action == ReceiptAction.REPLAY
                assert replay.response_text == "done"
            finally:
                await app.close()  # the AgentApp owns and closes the resolver
                await receipts.close()
                await tenant_repo.close()

            # -- second Worker continues on the TARGET backend --------------
            resolver2 = _resolver(r1b_redis_url, r1b_pg.url)
            app2 = AgentApp(model_provider=FakeModelProvider({"default": FakeLLMModel()}), backend_resolver=resolver2)
            try:
                await _run_turn(app2, head, ctx, "sess-m", "post-cutover continuation")
                tgt_svc2 = resolver2.resolve(make_backend_profile(target))
                after = await tgt_svc2.session_service.get_session(app_name=_namespace(head),
                                                                   user_id=ctx.sdk_user_id,
                                                                   session_id="sess-m")
                texts = " ".join(part.text or "" for e in after.events + after.historical_events
                                 for part in (e.content.parts if e.content else []))
                assert marker in texts
                assert "post-cutover continuation" in texts
                # The OLD backend gained no next-version writes.
                old_svc = resolver2.resolve(make_backend_profile(source))
                old_ns = await _snapshot_event_ids(old_svc.session_service, _namespace(head))
                assert old_ns == {}
            finally:
                await app2.close()

        asyncio.run(_scenario())


__all__ = ["TestSummaryRestartDurability", "TestOfflineStateCutover"]
