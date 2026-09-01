"""Fail-closed routing and observable shadow migration tests."""

from __future__ import annotations

import pytest

from trpc_service.backends import (
    BackendMode,
    BackendNotRegisteredError,
    BackendRouter,
    InMemoryBackend,
    InMemoryMigrationStatusBackend,
    MigrationPhase,
    MigrationPlan,
    MigrationStateError,
    SessionProjection,
    SessionShadowMigrator,
    UnsafeBackendError,
    WriteDisposition,
)
from trpc_service.tenant.models import TenantSpec


def tenant_spec(tenant_id: str = "tenant-a") -> TenantSpec:
    return TenantSpec.model_validate(
        {
            "tenant_id": tenant_id,
            "revision": 3,
            "display_name": tenant_id,
            "apps": [
                {
                    "app_id": "support",
                    "revision": 3,
                    "name": "support_agent",
                    "prompt": "Be useful",
                    "model": {"provider": "mock", "model": "deterministic"},
                }
            ],
            "channels": [],
            "storage": {
                "session": "inmemory",
                "memory": "external",
                "summary": "redis",
                "knowledge": "inmemory",
                "artifact": "local",
            },
        }
    )


def development_router(backend: InMemoryBackend) -> BackendRouter:
    return BackendRouter(
        sessions={"inmemory": backend},
        scoped_states={"inmemory": backend},
        memories={"external": backend},
        summaries={"redis": backend},
        knowledge={"inmemory": backend},
        artifacts={"local": backend},
        mode=BackendMode.TEST,
    )


def test_router_follows_tenant_spec_without_implicit_fallback() -> None:
    backend = InMemoryBackend()
    bundle = development_router(backend).route(tenant_spec())
    assert bundle.tenant_id == "tenant-a"
    assert bundle.tenant_revision == 3
    assert bundle.session_name == bundle.scoped_state_name == "inmemory"
    assert bundle.memory_name == "external"
    assert bundle.summary_name == "redis"
    assert bundle.knowledge_name == "inmemory"
    assert bundle.artifact_name == "local"
    assert bundle.session is bundle.scoped_state is backend

    with pytest.raises(BackendNotRegisteredError, match="scoped_state"):
        BackendRouter(
            sessions={"inmemory": backend},
            mode=BackendMode.TEST,
        ).route(tenant_spec())
    with pytest.raises(BackendNotRegisteredError, match="session"):
        BackendRouter(mode=BackendMode.TEST).route(tenant_spec())


def test_router_rejects_process_local_backend_in_production() -> None:
    backend = InMemoryBackend()
    with pytest.raises(UnsafeBackendError, match="development-only"):
        BackendRouter(
            sessions={"inmemory": backend},
            scoped_states={"inmemory": backend},
            memories={"external": backend},
            summaries={"redis": backend},
            knowledge={"inmemory": backend},
            artifacts={"local": backend},
            mode=BackendMode.PRODUCTION,
        ).route(tenant_spec())


def session(tenant_id: str, state: dict[str, object]) -> SessionProjection:
    return SessionProjection(
        tenant_id=tenant_id,
        session_id="session-1",
        version=4,
        committed_through=3,
        state=state,
    )


@pytest.mark.asyncio
async def test_shadow_compare_tracks_evidence_before_cutover() -> None:
    source = InMemoryBackend()
    target = InMemoryBackend()
    statuses = InMemoryMigrationStatusBackend()
    plan = MigrationPlan(
        tenant_id="tenant-a",
        category="session",
        source_backend="postgresql",
        target_backend="redis",
    )
    await statuses.start(plan)
    canonical = session("tenant-a", {"turn": 3})
    await source.compare_and_set_session(canonical, expected_version=None)
    migrator = SessionShadowMigrator(
        plan,
        source=source,
        target=target,
        statuses=statuses,
    )
    assert (
        await migrator.write_shadow(canonical, expected_target_version=None)
    ).disposition is WriteDisposition.APPLIED
    comparison = await migrator.compare("session-1")
    assert comparison.matched is True
    assert comparison.reason is None

    verifying = await statuses.transition(
        "tenant-a",
        "session",
        expected_phase=MigrationPhase.SHADOWING,
        target_phase=MigrationPhase.VERIFYING,
    )
    assert verifying.compared == verifying.matches == 0
    verified = await migrator.compare("session-1")
    assert verified.matched is True
    ready = await statuses.transition(
        "tenant-a",
        "session",
        expected_phase=MigrationPhase.VERIFYING,
        target_phase=MigrationPhase.CUTOVER_READY,
    )
    assert ready.mismatches == 0
    cutover = await statuses.transition(
        "tenant-a",
        "session",
        expected_phase=MigrationPhase.CUTOVER_READY,
        target_phase=MigrationPhase.CUTOVER,
    )
    assert cutover.phase is MigrationPhase.CUTOVER


@pytest.mark.asyncio
async def test_shadow_mismatch_blocks_cutover_and_statuses_are_tenant_isolated() -> None:
    source = InMemoryBackend()
    target = InMemoryBackend()
    statuses = InMemoryMigrationStatusBackend()
    plan = MigrationPlan("tenant-a", "session", "postgresql", "redis")
    await statuses.start(plan)
    await source.compare_and_set_session(
        session("tenant-a", {"source": True}),
        expected_version=None,
    )
    migrator = SessionShadowMigrator(
        plan,
        source=source,
        target=target,
        statuses=statuses,
    )
    comparison = await migrator.compare("session-1")
    assert comparison.matched is False
    assert comparison.reason == "target_missing"
    await target.compare_and_set_session(
        session("tenant-a", {"target": "different"}),
        expected_version=None,
    )
    mismatch = await migrator.compare("session-1")
    assert mismatch.reason == "projection_mismatch"
    await statuses.transition(
        "tenant-a",
        "session",
        expected_phase=MigrationPhase.SHADOWING,
        target_phase=MigrationPhase.VERIFYING,
    )
    await migrator.compare("session-1")
    with pytest.raises(MigrationStateError, match="zero mismatches"):
        await statuses.transition(
            "tenant-a",
            "session",
            expected_phase=MigrationPhase.VERIFYING,
            target_phase=MigrationPhase.CUTOVER_READY,
        )
    assert await statuses.get("tenant-b", "session") is None
    with pytest.raises(MigrationStateError, match="tenant"):
        await migrator.write_shadow(
            session("tenant-b", {}),
            expected_target_version=None,
        )


@pytest.mark.asyncio
async def test_migration_state_machine_rejects_unsafe_or_conflicting_plans() -> None:
    statuses = InMemoryMigrationStatusBackend()
    with pytest.raises(ValueError, match="must differ"):
        await statuses.start(MigrationPlan("tenant-a", "session", "redis", "redis"))
    plan = MigrationPlan("tenant-a", "session", "postgresql", "redis")
    first = await statuses.start(plan)
    assert await statuses.start(plan) == first
    with pytest.raises(MigrationStateError, match="different active plan"):
        await statuses.start(MigrationPlan("tenant-a", "session", "postgresql", "inmemory"))
    with pytest.raises(MigrationStateError, match="illegal"):
        await statuses.transition(
            "tenant-a",
            "session",
            expected_phase=MigrationPhase.SHADOWING,
            target_phase=MigrationPhase.CUTOVER,
        )
    with pytest.raises(MigrationStateError, match="phase changed"):
        await statuses.transition(
            "tenant-a",
            "session",
            expected_phase=MigrationPhase.VERIFYING,
            target_phase=MigrationPhase.CUTOVER_READY,
        )

    source = InMemoryBackend()
    target = InMemoryBackend()
    with pytest.raises(ValueError, match="category='session'"):
        SessionShadowMigrator(
            MigrationPlan("tenant-a", "memory", "postgresql", "redis"),
            source=source,
            target=target,
            statuses=statuses,
        )
    inactive = SessionShadowMigrator(
        MigrationPlan("tenant-b", "session", "postgresql", "redis"),
        source=source,
        target=target,
        statuses=statuses,
    )
    with pytest.raises(MigrationStateError, match="not accepting shadow writes"):
        await inactive.write_shadow(
            session("tenant-b", {}),
            expected_target_version=None,
        )
    failed = await statuses.transition(
        "tenant-a",
        "session",
        expected_phase=MigrationPhase.SHADOWING,
        target_phase=MigrationPhase.FAILED,
    )
    restarted = await statuses.start(plan)
    assert restarted.phase is MigrationPhase.SHADOWING
    assert restarted.revision == failed.revision + 1


@pytest.mark.asyncio
async def test_shadow_compare_treats_missing_source_as_mismatch() -> None:
    source = InMemoryBackend()
    target = InMemoryBackend()
    statuses = InMemoryMigrationStatusBackend()
    plan = MigrationPlan("tenant-a", "session", "postgresql", "redis")
    await statuses.start(plan)
    await target.compare_and_set_session(
        session("tenant-a", {"target-only": True}),
        expected_version=None,
    )
    migrator = SessionShadowMigrator(
        plan,
        source=source,
        target=target,
        statuses=statuses,
    )
    comparison = await migrator.compare("session-1")
    assert comparison.matched is False
    assert comparison.reason == "source_missing"
