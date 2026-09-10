"""T048 shared-backend: cross-node alert CAS and fingerprint merging.

Runs only with ``TRPC_SHARED_DATABASE_URL`` injected (Docker shared
environment); offline runs skip — the offline contract is covered by
T042.
"""

from __future__ import annotations

import asyncio

import pytest

from tests.observability_support import obs_tenants, stable_scope_digest

pytestmark = pytest.mark.shared_backend


def test_alert_repository_merges_fingerprint_and_enforces_cas(
    ops_namespace: str,
    shared_database_url: str,
) -> None:
    async def scenario() -> tuple:
        from trpc_service.storage.postgres.database import PostgresDatabase
        from trpc_service.storage.postgres.operations_repositories import (
            AlertStateConflict,
            PostgresAlertRepository,
        )

        scope = stable_scope_digest(
            f"{obs_tenants()[0].tenant_id}:{ops_namespace}"
        )
        database = PostgresDatabase(shared_database_url)
        # Two independent repository instances = two nodes.
        node_a = PostgresAlertRepository(database)
        node_b = PostgresAlertRepository(database)
        try:
            await database.initialize_schema()
            first = await node_a.observe(
                "dependency_down", "critical", scope, "postgres_authoritative_down"
            )
            assert first.state == "pending"
            assert first.state_version == 1
            assert first.occurrence_count == 1
            # Same fingerprint from another node: merged, one logical row.
            merged = await node_b.observe(
                "dependency_down", "critical", scope, "postgres_authoritative_down"
            )
            assert merged.incident_id == first.incident_id
            assert merged.occurrence_count == 2
            assert merged.state_version == 1
            # CAS transition succeeds from the current version ...
            firing = await node_a.transition(
                first.incident_id,
                expected_version=1,
                state="firing",
                notification_id=f"{first.fingerprint}:2",
            )
            assert firing.state == "firing"
            assert firing.state_version == 2
            # ... and a stale version from the other node fails loudly.
            conflict = None
            try:
                await node_b.transition(
                    first.incident_id, expected_version=1, state="firing"
                )
            except AlertStateConflict:
                conflict = "conflict"
            assert conflict == "conflict"
            # Recovery closure through CAS.
            recovering = await node_b.transition(
                first.incident_id, expected_version=2, state="recovering"
            )
            resolved = await node_b.transition(
                first.incident_id, expected_version=3, state="resolved"
            )
            assert resolved.state == "resolved"
            assert resolved.resolved_at is not None
            persisted = await node_a.get_by_fingerprint(first.fingerprint)
            assert persisted is not None
            assert persisted.state == "resolved"
            assert persisted.occurrence_count == 2
            return (resolved.state, persisted.occurrence_count)
        finally:
            await database.close()

    state, occurrences = asyncio.run(scenario())
    assert state == "resolved"
    assert occurrences == 2
