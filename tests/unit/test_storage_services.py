from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest

from trpc_service.storage.models import OutboxRecord, SessionSnapshot, SummarySnapshot
from trpc_service.storage.projector import PostTurnProjector
from trpc_service.storage.services import (
    PostgresArtifactStore,
    PostgresAuditStore,
    PostgresKnowledgeStore,
    PostgresMemoryStore,
    PostgresSessionStore,
    PostgresSummaryStore,
    PostgresTenantServiceFactory,
    ProfileServiceFactory,
    RegisteredTenantServiceBundle,
    TenantDataServices,
)
from trpc_service.tenant.models import (
    AuditPolicy,
    BudgetPolicy,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
    TenantContext,
    ToolPolicy,
)


class Connection:
    def __init__(self, *, fetchrow_values: list[Any] | None = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrow_values = list(fetchrow_values or [])
        self.fetch_values: list[Any] = []
        self.fetchval_value: Any = UUID("11111111-1111-1111-1111-111111111111")

    def transaction(self) -> Connection:
        return self

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.calls.append((query, args))
        return "UPDATE 1"

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.calls.append((query, args))
        return self.fetchrow_values.pop(0) if self.fetchrow_values else None

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self.calls.append((query, args))
        return self.fetch_values

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.calls.append((query, args))
        return self.fetchval_value


class Acquire:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> Connection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


def pool(connection: Connection) -> SimpleNamespace:
    return SimpleNamespace(acquire=lambda: Acquire(connection))


def config(tenant_id: str = "tenant-a") -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        app_id="app",
        version=1,
        model=ModelPolicy(provider="fake", model="fake"),
        tools=ToolPolicy(),
        budget=BudgetPolicy(),
        audit=AuditPolicy(),
        storage=StorageSelection(profile_id="profile-a"),
    )


def context(tenant_id: str = "tenant-a") -> TenantContext:
    return TenantContext(
        tenant_id=tenant_id,
        app_id="app",
        config_version=1,
        channel_binding_id="binding",
        principal_id="user",
        session_id="session",
        request_id="request",
        trace_id="trace",
    )


@pytest.mark.asyncio
async def test_postgres_memory_and_summary_set_rls_context_and_cas() -> None:
    memory_connection = Connection()
    memory = PostgresMemoryStore(pool(memory_connection))
    memory_id = str(uuid4())
    assert await memory.put("tenant-a", "user", {"kind": "fact"}, memory_id=memory_id)
    assert memory_connection.calls[0][0].startswith("SELECT set_config('app.tenant_id'")
    assert memory_connection.calls[1][1][0] == "tenant-a"
    assert memory_connection.calls[1][1][1] == UUID(memory_id)

    summary_connection = Connection(fetchrow_values=[{"version": 2}, None])
    summary = PostgresSummaryStore(pool(summary_connection))
    assert await summary.put("tenant-a", "session", up_to_sequence=3, summary={"text": "x"})
    assert not await summary.put("tenant-a", "session", up_to_sequence=2, summary={"text": "old"})
    assert (
        "session_summaries.up_to_sequence <= excluded.up_to_sequence"
        in summary_connection.calls[1][0]
    )

    memory_connection.fetch_values = [
        {
            "memory_id": memory_id,
            "session_id": "session",
            "source_sequence": 4,
            "memory_json": '{"kind":"fact"}',
            "created_at": datetime.now(),
        }
    ]
    recent = await memory.list_recent("tenant-a", "user")
    assert recent[0]["memory_id"] == memory_id
    with pytest.raises(ValueError, match="between"):
        await memory.list_recent("tenant-a", "user", limit=0)
    memory_connection.fetch_values = [
        {
            "memory_id": memory_id,
            "session_id": None,
            "source_sequence": None,
            "memory_json": {"kind": "dict"},
            "created_at": "now",
        }
    ]
    assert (await memory.list_recent("tenant-a", "user"))[0]["memory"] == {"kind": "dict"}

    summary_empty = PostgresSummaryStore(pool(Connection(fetchrow_values=[None])))
    assert await summary_empty.get("tenant-a", "missing") is None
    with pytest.raises(ValueError, match="negative"):
        await summary.put("tenant-a", "session", up_to_sequence=-1, summary={})


@pytest.mark.asyncio
async def test_postgres_summary_read_and_audit_are_tenant_scoped() -> None:
    summary_connection = Connection(
        fetchrow_values=[
            {
                "up_to_sequence": 4,
                "summary_json": '{"text":"ok"}',
                "version": 1,
            }
        ]
    )
    result = await PostgresSummaryStore(pool(summary_connection)).get("tenant-a", "session")
    assert result == SummarySnapshot(
        tenant_id="tenant-a",
        session_id="session",
        up_to_sequence=4,
        summary={"text": "ok"},
        version=1,
    )
    assert summary_connection.calls[0][1] == ("tenant-a",)

    audit_connection = Connection()
    audit_id = await PostgresAuditStore(pool(audit_connection)).append(
        "tenant-a",
        decision="tool_allowed",
        trace_id="trace",
        session_id="session",
        metadata={"redacted": True},
    )
    assert audit_id == "11111111-1111-1111-1111-111111111111"
    assert "redaction_applied" in audit_connection.calls[1][0]
    assert audit_connection.calls[1][1][0] == "tenant-a"


@pytest.mark.asyncio
async def test_postgres_artifact_and_knowledge_keep_metadata_in_the_same_tenant_scope() -> None:
    class Objects:
        async def stage(self, *_args: object, **_kwargs: object) -> str:
            return "tenants/scope/staging/key"

        async def commit(self, *_args: object, **_kwargs: object) -> str:
            return "tenants/scope/artifacts/item/checksum"

        async def discard(self, _key: str) -> None:
            return None

    connection = Connection()
    artifact = PostgresArtifactStore(pool(connection), Objects())
    staged = await artifact.stage("tenant-a", "artifact", b"content", checksum="checksum")
    assert staged.endswith("staging/key")
    connection.fetchrow_values = [{"checksum": "checksum", "size_bytes": 7, "status": "staged"}]
    target = await artifact.commit("tenant-a", "artifact", staged)
    assert target.endswith("artifacts/item/checksum")
    await artifact.discard_for_tenant("tenant-a", "artifact", staged)
    await artifact.discard(staged)
    with pytest.raises(LookupError, match="metadata"):
        await artifact.commit("tenant-a", "missing", staged)
    connection.fetchrow_values = [{"checksum": "checksum", "size_bytes": 7, "status": "committed"}]
    assert await artifact.commit("tenant-a", "artifact", "already-committed") == "already-committed"

    knowledge_connection = Connection()
    knowledge = PostgresKnowledgeStore(
        pool(knowledge_connection), profile_id="profile", dimension=1536
    )
    embedding = [0.1] * 1536
    await knowledge.upsert(
        "tenant-a",
        "item",
        embedding,
        {"chunk_id": "chunk", "content_checksum": "checksum"},
    )
    assert len(knowledge_connection.calls) == 3
    assert all(call[1][0] == "tenant-a" for call in knowledge_connection.calls)
    await knowledge.upsert("tenant-a", "item", embedding, {})
    with pytest.raises(ValueError, match="dimension"):
        PostgresKnowledgeStore(pool(Connection()), profile_id="profile", dimension=0)
    with pytest.raises(ValueError, match="dimension"):
        await knowledge.upsert("tenant-a", "item", [0.1], {})


@pytest.mark.asyncio
async def test_profile_factory_supports_tenant_scoped_profiles() -> None:
    sentinel = object()
    services = TenantDataServices(sentinel, sentinel, sentinel, sentinel, sentinel, sentinel)  # type: ignore[arg-type]
    registration = RegisteredTenantServiceBundle(selection=config().storage, services=services)
    factory = ProfileServiceFactory({("tenant-a", "profile-a"): registration})
    assert await factory.for_context(context(), config()) is services
    with pytest.raises(ValueError, match="another tenant"):
        await factory.for_context(context("tenant-b"), config())


@pytest.mark.asyncio
async def test_postgres_factory_fails_closed_when_object_store_is_missing() -> None:
    factory = PostgresTenantServiceFactory(SimpleNamespace(), repository=SimpleNamespace())
    with pytest.raises(LookupError, match="S3 artifact backend"):
        await factory.for_context(context(), config())
    with pytest.raises(ValueError, match="another tenant"):
        await factory.for_context(context("tenant-b"), config())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("session_backend", "redis"),
        ("memory_backend", "inmemory"),
        ("artifact_backend", "inmemory"),
        ("knowledge_backend", "external_memory"),
    ],
)
async def test_postgres_factory_rejects_each_non_postgres_backend(field: str, value: str) -> None:
    storage = config().storage.model_copy(update={field: value})
    selected = config().model_copy(update={"storage": storage})
    factory = PostgresTenantServiceFactory(SimpleNamespace(), repository=SimpleNamespace())
    with pytest.raises(ValueError, match=field):
        await factory.for_context(context(), selected)


@pytest.mark.asyncio
async def test_session_service_delegates_authoritative_read_and_opens_sdk_turn() -> None:
    snapshot = SessionSnapshot(
        tenant_id="tenant-a",
        app_id="app",
        session_id="session",
        principal_id="user",
    )

    class Repository:
        async def get_session_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot:
            assert (tenant_id, session_id) == ("tenant-a", "session")
            return snapshot

    service = PostgresSessionStore(Repository())
    assert await service.get_snapshot("tenant-a", "session") == snapshot
    assert service.open_turn(snapshot).__class__.__name__ == "TurnBufferSessionService"


@pytest.mark.asyncio
async def test_audit_errors_are_explicit() -> None:
    audit = PostgresAuditStore(pool(Connection()))
    with pytest.raises(ValueError, match="negative"):
        await audit.append("tenant-a", decision="bad", trace_id="trace", cost_units=-1)
    connection = Connection()
    connection.fetchval_value = None
    with pytest.raises(RuntimeError, match="audit"):
        await PostgresAuditStore(pool(connection)).append(
            "tenant-a", decision="ok", trace_id="trace"
        )


@pytest.mark.asyncio
async def test_projector_can_read_authoritative_session_through_service() -> None:
    class Repository:
        def __init__(self) -> None:
            self.published = False

        async def claim_outbox(self, **_kwargs: object) -> tuple[OutboxRecord, ...]:
            return (
                (
                    OutboxRecord(
                        outbox_id="outbox",
                        tenant_id="tenant-a",
                        event_type="post_turn.ready",
                        aggregate_id="turn",
                        payload={"session_id": "session"},
                    ),
                )
                if not self.published
                else ()
            )

        async def mark_outbox_published(self, *_args: object, **_kwargs: object) -> None:
            self.published = True

        async def release_outbox(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("session should be visible")

        async def get_session_snapshot(self, *_args: object) -> None:
            raise AssertionError("projector should use SessionStore")

    class Session:
        async def get_snapshot(self, tenant_id: str, session_id: str) -> SessionSnapshot:
            assert (tenant_id, session_id) == ("tenant-a", "session")
            return SessionSnapshot(
                tenant_id=tenant_id,
                app_id="app",
                session_id=session_id,
                principal_id="user",
                next_sequence=2,
            )

    class Projection:
        async def put_session(self, *_args: object, **_kwargs: object) -> None:
            return None

    repository = Repository()
    projector = PostTurnProjector(
        repository, Projection(), owner_id="projector", session_store=Session()
    )
    assert await projector.project_once() == 1
