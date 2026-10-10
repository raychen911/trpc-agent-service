"""Opt-in live Redis-to-PostgreSQL migration contract.

This test intentionally skips unless the operator supplies an isolated tenant,
source Redis, target PostgreSQL, and an expected non-zero source record count.
It must never turn a missing environment into a passing production gate.
"""

from __future__ import annotations

import os
from argparse import Namespace
from pathlib import Path

import asyncpg
import pytest
import redis.asyncio as redis_async

import scripts.migration_full_acceptance as full_acceptance
from trpc_service.storage.migration import (
    MigrationCoordinator,
    MigrationPhase,
    PostgresMigrationCheckpointStore,
    PostgresMigrationTarget,
    RedisMigrationSource,
)

pytestmark = [pytest.mark.integration, pytest.mark.online]


def _backend_contract_enabled() -> bool:
    if os.getenv("TRPC_MIGRATION_BACKEND_CONTRACT") != "1":
        pytest.skip("set TRPC_MIGRATION_BACKEND_CONTRACT=1 for the isolated migration contract")
    return True


def _phase_scope() -> tuple[str, str, str]:
    tenant = os.environ["TRPC_MIGRATION_TENANT_ID"]
    migration = os.environ["TRPC_MIGRATION_ID"]
    app = os.environ.get("TRPC_MIGRATION_APP_ID", "migration-acceptance-app")
    return (
        os.getenv("TRPC_MIGRATION_PHASE_TENANT_ID", f"{tenant}-phase"),
        os.getenv("TRPC_MIGRATION_PHASE_ID", f"{migration}-phase"),
        os.getenv("TRPC_MIGRATION_PHASE_APP_ID", f"{app}-phase"),
    )


@pytest.mark.asyncio
async def test_live_redis_to_postgres_migration_requires_explicit_nonzero_fixture() -> None:
    _backend_contract_enabled()
    if os.getenv("TRPC_RUN_REAL_MIGRATION") != "1":
        pytest.skip("set TRPC_RUN_REAL_MIGRATION=1 to execute live migration")
    names = (
        "TRPC_MIGRATION_SOURCE_REDIS_URL",
        "TRPC_MIGRATION_TARGET_DATABASE_DSN",
        "TRPC_MIGRATION_TENANT_ID",
        "TRPC_MIGRATION_ID",
        "TRPC_MIGRATION_EXPECTED_RECORDS",
        "TRPC_MIGRATION_APP_ID",
    )
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        pytest.skip(f"missing live migration environment: {', '.join(missing)}")
    expected = int(os.environ["TRPC_MIGRATION_EXPECTED_RECORDS"])
    if expected < 1:
        raise AssertionError("TRPC_MIGRATION_EXPECTED_RECORDS must be positive")

    redis = redis_async.from_url(
        os.environ["TRPC_MIGRATION_SOURCE_REDIS_URL"], decode_responses=False
    )
    dsn = os.environ["TRPC_MIGRATION_TARGET_DATABASE_DSN"].replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    try:
        phase_tenant_id, phase_migration_id, _phase_app_id = _phase_scope()
        source = RedisMigrationSource(redis)
        target = PostgresMigrationTarget(pool)
        checkpoints = PostgresMigrationCheckpointStore(pool)
        coordinator = MigrationCoordinator(source, target, checkpoints, batch_size=100)
        tenant_id = phase_tenant_id
        migration_id = phase_migration_id
        for phase in (
            MigrationPhase.PREPARE,
            MigrationPhase.BACKFILL,
            MigrationPhase.SHADOW_READ,
        ):
            result = await coordinator.run(tenant_id, migration_id, phase)
        assert result.gate == "pass"
        assert result.case_deltas["source_count"] == expected
        assert result.case_deltas["source_count"] == result.case_deltas["target_count"]
    finally:
        await redis.aclose()
        await pool.close()


@pytest.mark.asyncio
async def test_live_full_migration_executes_controlled_cutover_cleanup_and_rollback(
    tmp_path: Path,
) -> None:
    _backend_contract_enabled()
    if os.getenv("TRPC_RUN_REAL_MIGRATION") != "1":
        pytest.skip("set TRPC_RUN_REAL_MIGRATION=1 to execute live migration")
    if os.getenv("TRPC_MIGRATION_FULL_ACCEPTANCE") != "1":
        pytest.skip("set TRPC_MIGRATION_FULL_ACCEPTANCE=1 to execute full migration acceptance")
    names = (
        "TRPC_MIGRATION_SOURCE_REDIS_URL",
        "TRPC_MIGRATION_TARGET_DATABASE_DSN",
        "TRPC_MIGRATION_TENANT_ID",
        "TRPC_MIGRATION_ID",
        "TRPC_MIGRATION_EXPECTED_RECORDS",
        "TRPC_MIGRATION_CONTROL_FACTORY",
        "TRPC_MIGRATION_APP_ID",
        "TRPC_MIGRATION_APP_REVISION",
        "TRPC_MIGRATION_CONFIG_VERSION",
        "TRPC_MIGRATION_BINDING_ID",
        "TRPC_MIGRATION_BINDING_REVISION",
    )
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        pytest.skip(f"missing live full migration environment: {', '.join(missing)}")
    expected = int(os.environ["TRPC_MIGRATION_EXPECTED_RECORDS"])
    if expected < 2:
        raise AssertionError("TRPC_MIGRATION_EXPECTED_RECORDS must exceed one for resume proof")
    report = await full_acceptance._run(
        Namespace(
            output=tmp_path / "migration-full-live.json",
            batch_size=max(1, min(100, expected - 1)),
            db_pool_size=4,
        )
    )
    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["case_deltas"]["cutover"] == "pass"
    assert report["case_deltas"]["cleanup"] == "pass"
    assert report["case_deltas"]["rollback"] == "pass"
