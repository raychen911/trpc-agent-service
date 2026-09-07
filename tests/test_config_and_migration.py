from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from tenant_agent.models import (
    ArtifactRecord,
    KnowledgeRecord,
    MemoryRecord,
    SummaryRecord,
)
from tenant_agent.services.config import TenantConfigService, configuration_checksum
from tenant_agent.services.migration import DataMigrator, GoldenQuery, migrate_trpc_session_history
from tenant_agent.storage.base import TenantDataPlane
from tenant_agent.storage.memory import InMemoryPlane
from tenant_agent.storage.sql import SqlPlane
from tests.helpers import make_tenant


def plane(value: InMemoryPlane) -> TenantDataPlane:
    return TenantDataPlane(
        sessions=value,
        memories=value,
        summaries=value,
        artifacts=value,
        knowledge=value,
        audit=value,
        receipts=value,
        usage=value,
        concurrency=value,
        outbox=value,
        leases=value,
    )


async def test_config_versions_activate_and_rollback_atomically() -> None:
    repository = InMemoryPlane()
    service = TenantConfigService(repository)
    first = make_tenant()
    await service.create_version(first, actor="test", activate=True)
    second = first.model_copy(update={"revision": 2, "display_name": "New Name"})
    await service.create_version(second, actor="test", activate=True)
    assert (await repository.get_active_tenant("alpha")).display_name == "New Name"  # type: ignore[union-attr]
    await service.rollback("alpha", 1)
    assert (await repository.get_active_tenant("alpha")).display_name == "Alpha Tenant"  # type: ignore[union-attr]
    same = await service.create_version(first, actor="test", activate=True)
    assert same.revision == 1
    conflicting_revision = first.model_copy(update={"display_name": "Conflict"})
    conflict = await service.create_version(conflicting_revision, actor="test")
    assert conflict.revision == 3
    assert await service.exact_revision("alpha", 3) == conflict.config
    with pytest.raises(KeyError):
        await service.activate("alpha", 999)
    with pytest.raises(KeyError):
        await service.resolve_binding("web", "missing-binding")
    with pytest.raises(KeyError):
        await service.exact_revision("alpha", 999)


async def test_two_nodes_can_bootstrap_the_same_config_concurrently(tmp_path: Path) -> None:
    database_url = f"sqlite+aiosqlite:///{(tmp_path / 'control.db').as_posix()}"
    first_repository = SqlPlane(database_url)
    second_repository = SqlPlane(database_url)
    await first_repository.initialize()
    await second_repository.initialize()
    config_path = tmp_path / "bootstrap.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"tenants": [make_tenant().model_dump(mode="json")]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    await asyncio.gather(
        TenantConfigService(first_repository).bootstrap(config_path),
        TenantConfigService(second_repository).bootstrap(config_path),
    )

    versions = await first_repository.list_config_versions("alpha")
    assert len(versions) == 1
    assert versions[0].status == "active"
    assert await first_repository.get_active_tenant("alpha") == make_tenant()
    await first_repository.close()
    await second_repository.close()


async def test_first_bootstrap_runs_preflight_before_activation(tmp_path: Path) -> None:
    repository = InMemoryPlane()
    service = TenantConfigService(repository)
    config_path = tmp_path / "bootstrap-preflight.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"tenants": [make_tenant().model_dump(mode="json")]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    calls = 0

    async def reject(config: object) -> None:
        nonlocal calls
        del config
        calls += 1
        raise ValueError("runtime model is invalid")

    with pytest.raises(ValueError, match="runtime model"):
        await service.bootstrap(config_path, preflight=reject)
    assert calls == 1
    assert not await repository.list_config_versions("alpha")
    assert await repository.get_active_tenant("alpha") is None


async def test_restart_bootstrap_never_reactivates_an_obsolete_revision(tmp_path: Path) -> None:
    repository = InMemoryPlane()
    service = TenantConfigService(repository)
    revision_one = make_tenant()
    config_path = tmp_path / "bootstrap.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"tenants": [revision_one.model_dump(mode="json")]},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    loaded_revision_one = type(revision_one).model_validate(
        yaml.safe_load(config_path.read_text(encoding="utf-8"))["tenants"][0]
    )
    assert configuration_checksum(loaded_revision_one) == configuration_checksum(revision_one)
    assert await service.bootstrap(config_path) == 1
    revision_two = revision_one.model_copy(update={"revision": 2, "display_name": "Revision Two"})
    await service.create_version(revision_two, actor="admin", activate=True)

    assert await service.bootstrap(config_path) == 0
    active = await repository.get_active_tenant(revision_one.tenant_id)
    assert active is not None
    assert active.revision == 2

    interrupted_repository = InMemoryPlane()
    interrupted_service = TenantConfigService(interrupted_repository)
    await interrupted_service.create_version(revision_one, actor="interrupted-bootstrap")
    assert await interrupted_service.bootstrap(config_path) == 1
    recovered = await interrupted_repository.get_active_tenant(revision_one.tenant_id)
    assert recovered is not None and recovered.revision == 1


async def test_migration_replays_and_verifies_all_supported_resources() -> None:
    source = InMemoryPlane()
    target = InMemoryPlane()
    session = await source.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )
    session, _ = await source.append_event(
        snapshot=session,
        event_id="event",
        kind="user_message",
        actor_id="user",
        payload={"text": "hello"},
        state_delta={"status": "ok"},
        trace_id="0" * 32,
    )
    await source.put_summary(
        SummaryRecord(
            tenant_id="alpha",
            session_id="session",
            version=1,
            through_event_sequence=1,
            content="hello",
        )
    )
    await source.put_memory(
        MemoryRecord(
            memory_id="memory",
            tenant_id="alpha",
            user_id="user",
            content="remember hello",
        )
    )
    content = b"artifact"
    import hashlib

    await source.put_artifact(
        ArtifactRecord(
            tenant_id="alpha",
            session_id="session",
            artifact_id="artifact",
            filename="artifact.txt",
            content_type="text/plain",
            size_bytes=len(content),
            checksum_sha256=hashlib.sha256(content).hexdigest(),
            storage_uri="memory://artifact",
        ),
        content,
    )
    await source.put_knowledge(
        KnowledgeRecord(
            tenant_id="alpha",
            document_id="doc",
            chunk_id="chunk",
            text="knowledge",
            embedding=(1.0, 0.0),
        )
    )
    migrator = DataMigrator(plane(source), plane(target))
    resources = ("sessions", "summaries", "memories", "artifacts", "knowledge")
    report = await migrator.migrate("alpha", resources=resources)
    assert report.verified
    assert report.copied == {
        "sessions": 1,
        "summaries": 1,
        "memories": 1,
        "artifacts": 1,
        "knowledge": 1,
    }
    second = await migrator.migrate("alpha", resources=resources)
    assert second.verified
    assert second.copied["sessions"] == 0

    target_session = await target.get_session("alpha", "session")
    assert target_session is not None
    target._sessions[("alpha", "session")] = target_session.model_copy(  # type: ignore[attr-defined]
        update={"state": {"corrupted": True}}
    )
    corrupted = await migrator.migrate("alpha", resources=("sessions",))
    assert not corrupted.verified
    assert corrupted.mismatches == ["sessions"]


async def test_reembedding_migration_uses_identity_hash_and_golden_recall() -> None:
    source = InMemoryPlane()
    target = InMemoryPlane()
    original = KnowledgeRecord(
        tenant_id="alpha",
        document_id="document",
        chunk_id="chunk",
        text="tenant knowledge",
        embedding=(1.0, 0.0),
        metadata={"category": "guide"},
        embedding_model="old-model",
    )
    await source.put_knowledge(original)

    async def reembed(record: KnowledgeRecord) -> KnowledgeRecord:
        return record.model_copy(
            update={
                "embedding": (0.0, 1.0),
                "embedding_model": "new-model",
            }
        )

    with pytest.raises(ValueError, match="golden-query"):
        await DataMigrator(source=plane(source), target=plane(target)).migrate(
            "alpha",
            resources=("knowledge",),
            embedding_transform=reembed,
        )

    report = await DataMigrator(source=plane(source), target=plane(target)).migrate(
        "alpha",
        resources=("knowledge",),
        embedding_transform=reembed,
        golden_queries=(
            GoldenQuery(
                embedding=(0.0, 1.0),
                expected_ids=("document/chunk",),
            ),
        ),
    )
    assert report.verified
    assert report.verification_modes["knowledge"] == "identity-and-recall"
    assert report.recall_checks == 1
    migrated = [item async for item in target.iter_knowledge("alpha")]
    assert migrated[0].embedding == (0.0, 1.0)
    assert migrated[0].embedding_model == "new-model"

    invalid_target = InMemoryPlane()

    async def corrupt(record: KnowledgeRecord) -> KnowledgeRecord:
        return record.model_copy(update={"text": "changed", "embedding": (0.0, 1.0)})

    with pytest.raises(ValueError, match="immutable knowledge"):
        await DataMigrator(source=plane(source), target=plane(invalid_target)).migrate(
            "alpha",
            resources=("knowledge",),
            embedding_transform=corrupt,
            golden_queries=(
                GoldenQuery(
                    embedding=(0.0, 1.0),
                    expected_ids=("document/chunk",),
                ),
            ),
        )


async def test_qdrant_cosine_normalization_does_not_create_false_mismatch() -> None:
    import math

    class QdrantLikePlane(InMemoryPlane):
        backend_name = "qdrant"

        async def put_knowledge(self, record: KnowledgeRecord) -> None:
            norm = math.sqrt(sum(value * value for value in record.embedding))
            normalized = tuple(round(value / norm, 8) for value in record.embedding)
            await super().put_knowledge(record.model_copy(update={"embedding": normalized}))

    source = InMemoryPlane()
    target = QdrantLikePlane()
    record = KnowledgeRecord(
        tenant_id="alpha",
        document_id="document",
        chunk_id="chunk",
        text="cosine knowledge",
        embedding=(0.1, 0.2, 0.3),
    )
    await source.put_knowledge(record)
    report = await DataMigrator(plane(source), plane(target)).migrate(
        "alpha",
        resources=("knowledge",),
    )
    assert report.verified
    assert report.verification_modes["knowledge"] == "cosine-normalized-content"
    migrated = [item async for item in target.iter_knowledge("alpha")]
    assert migrated[0].embedding != record.embedding


async def test_migration_rejects_non_prefix_target_history() -> None:
    source = InMemoryPlane()
    target = InMemoryPlane()
    source_session = await source.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )
    for event_id in ("event-1", "event-2"):
        source_session, _ = await source.append_event(
            snapshot=source_session,
            event_id=event_id,
            kind="user_message",
            actor_id="user",
            payload={"text": event_id},
            state_delta={},
            trace_id="0" * 32,
        )
    target_session = await target.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )
    await target.append_event(
        snapshot=target_session,
        event_id="event-2",
        kind="user_message",
        actor_id="user",
        payload={"text": "event-2"},
        state_delta={},
        trace_id="0" * 32,
    )

    with pytest.raises(RuntimeError, match="not a source prefix"):
        await DataMigrator(plane(source), plane(target)).migrate(
            "alpha",
            resources=("sessions",),
        )


async def test_native_trpc_history_replay_is_idempotent() -> None:
    manifest = InMemoryPlane()
    await manifest.get_or_create_session(
        tenant_id="alpha",
        app_id="assistant",
        session_id="session",
        user_id="user",
        channel="web",
    )
    source_session = SimpleNamespace(
        state={"step": 2},
        historical_events=[SimpleNamespace(id="historical", step=1)],
        events=[SimpleNamespace(id="active", step=2)],
    )
    target_session: object | None = None

    class SourceService:
        async def get_session(self, **kwargs: object) -> object:
            del kwargs
            return source_session

    class TargetService:
        fail_after_first = False

        async def get_session(self, **kwargs: object) -> object | None:
            del kwargs
            return target_session

        async def create_session(self, **kwargs: object) -> object:
            nonlocal target_session
            target_session = SimpleNamespace(
                state=dict(kwargs.get("state", {})),
                historical_events=[],
                events=[],
            )
            return target_session

        async def append_event(self, *, session: object, event: object) -> None:
            session.events.append(event)  # type: ignore[attr-defined]
            session.state["step"] = event.step  # type: ignore[attr-defined]
            if self.fail_after_first and len(session.events) == 1:  # type: ignore[attr-defined]
                raise RuntimeError("simulated migration crash")

    target = TargetService()
    copied = await migrate_trpc_session_history(
        tenant_id="alpha",
        manifest_sessions=manifest,
        source_service=SourceService(),
        target_service=target,
    )
    assert copied.copied == 2
    assert copied.source_hash == copied.target_hash
    second = await migrate_trpc_session_history(
        tenant_id="alpha",
        manifest_sessions=manifest,
        source_service=SourceService(),
        target_service=target,
    )
    assert second.copied == 0
    assert second.source_hash == second.target_hash
    assert target_session is not None
    target_session.state["step"] = 999  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="canonical verification"):
        await migrate_trpc_session_history(
            tenant_id="alpha",
            manifest_sessions=manifest,
            source_service=SourceService(),
            target_service=target,
        )

    target_session = None
    target.fail_after_first = True
    with pytest.raises(RuntimeError, match="simulated migration crash"):
        await migrate_trpc_session_history(
            tenant_id="alpha",
            manifest_sessions=manifest,
            source_service=SourceService(),
            target_service=target,
        )
    assert target_session is not None
    assert target_session.state == {"step": 1}  # type: ignore[attr-defined]
    target.fail_after_first = False
    resumed = await migrate_trpc_session_history(
        tenant_id="alpha",
        manifest_sessions=manifest,
        source_service=SourceService(),
        target_service=target,
    )
    assert resumed.copied == 1
    assert resumed.source_hash == resumed.target_hash
