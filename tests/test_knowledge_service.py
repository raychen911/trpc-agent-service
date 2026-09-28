from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.agent.models import AgentApp  # noqa: F401
from trpc_service.storage.adapters.inmemory import build_inmemory_backend
from trpc_service.storage.database import build_session_factory
from trpc_service.storage.knowledge import TenantKnowledgeService
from trpc_service.storage.knowledge_orm import (
    KnowledgeArtifactRow,
    KnowledgeBaseRow,
    KnowledgeDocumentRow,
)
from trpc_service.storage.orm import Base
from trpc_service.storage.ports import ArtifactStore
from trpc_service.storage.registry import StorageBackendRegistry
from trpc_service.storage.router import StorageRouter
from trpc_service.storage.types import ArtifactMetadata, ArtifactRef
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import Tenant


async def _content(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


class _FailOnceArtifactStore(ArtifactStore):
    """Expose a transient object-read failure for ingestion recovery tests."""

    def __init__(self, delegate: ArtifactStore) -> None:
        self._delegate = delegate
        self._fail_next_open = True

    async def put(
        self,
        context: TenantContext,
        content: AsyncIterator[bytes],
        metadata: ArtifactMetadata,
    ) -> ArtifactRef:
        return await self._delegate.put(context, content, metadata)

    def open(self, context: TenantContext, artifact_id: str) -> AsyncIterator[bytes]:

        async def stream() -> AsyncIterator[bytes]:
            if self._fail_next_open:
                self._fail_next_open = False
                raise OSError("temporary object storage outage")
            async for block in self._delegate.open(context, artifact_id):
                yield block

        return stream()

    async def create_download_url(
        self,
        context: TenantContext,
        artifact_id: str,
        ttl_seconds: int,
    ) -> str:
        return await self._delegate.create_download_url(context, artifact_id, ttl_seconds)


@pytest.mark.anyio
async def test_tenant_knowledge_ingestion_is_idempotent_versioned_and_isolated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'knowledge.db'}")
    sessions = build_session_factory(engine)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    tenant_id = uuid4()
    foreign_tenant_id = uuid4()
    agent_id = uuid4()
    foreign_agent_id = uuid4()
    async with sessions.begin() as database:
        database.add_all([
            Tenant(tenant_id=tenant_id, name="Tenant A"),
            Tenant(tenant_id=foreign_tenant_id, name="Tenant B"),
            AgentApp(agent_app_id=agent_id, tenant_id=tenant_id, name="Agent A"),
            AgentApp(
                agent_app_id=foreign_agent_id,
                tenant_id=foreign_tenant_id,
                name="Agent B",
            ),
        ])
    backend = build_inmemory_backend()
    assert backend.knowledge is not None
    assert backend.artifact is not None
    service = TenantKnowledgeService(
        sessions,
        knowledge=backend.knowledge,
        artifacts=backend.artifact,
    )
    context = TenantContext(tenant_id=tenant_id,
                            agent_app_id=agent_id,
                            config_version=1,
                            request_id="request-a",
                            trace_id="trace-a")
    foreign_context = TenantContext(tenant_id=foreign_tenant_id,
                                    agent_app_id=foreign_agent_id,
                                    config_version=1,
                                    request_id="request-b",
                                    trace_id="trace-b")
    config = {"knowledge_base_names": ["handbook"]}

    first_upload = await service.upload(
        context,
        principal_id="employee-a",
        filename="leave.md",
        media_type="text/markdown",
        content=_content("年假为十天。".encode()),
    )
    first = await service.ingest(
        context,
        config,
        knowledge_base_name="handbook",
        artifact_ids=[first_upload.artifact_id],
    )
    duplicate = await service.ingest(
        context,
        config,
        knowledge_base_name="handbook",
        artifact_ids=[first_upload.artifact_id],
    )
    second_upload = await service.upload(
        context,
        principal_id="employee-a",
        filename="leave.md",
        media_type="text/markdown",
        content=_content("年假更新为十二天。".encode()),
    )
    second = await service.ingest(
        context,
        config,
        knowledge_base_name="handbook",
        artifact_ids=[second_upload.artifact_id],
    )

    documents = await service.list_documents(context, config, "handbook")
    hits = await service.search(context, config, "年假有多少天", limit=5)
    foreign_documents = await service.list_documents(
        foreign_context,
        {"knowledge_base_names": ["handbook"]},
        "handbook",
    )
    vector_delete = backend.knowledge.delete

    async def complete_delete_on_another_worker(
        delete_context: TenantContext,
        knowledge_base_id: str,
        document_ids: list[str],
    ) -> None:
        """Model a concurrent Worker committing the same confirmed deletion."""

        await vector_delete(delete_context, knowledge_base_id, document_ids)
        async with sessions.begin() as database:
            concurrent = await database.get(
                KnowledgeDocumentRow,
                (tenant_id, second[0].document_id),
            )
            assert concurrent is not None
            concurrent.status = "DELETED"
            concurrent.deleted_at = datetime.now(timezone.utc)

    monkeypatch.setattr(backend.knowledge, "delete", complete_delete_on_another_worker)
    deleted = await service.delete_document(
        context,
        config,
        "handbook",
        second[0].document_id,
    )
    after_delete = await service.search(context, config, "年假", limit=5)

    assert first[0].document_id == duplicate[0].document_id
    assert first[0].version == 1
    assert second[0].version == 2
    assert [(item.version, item.status) for item in documents] == [(2, "READY")]
    assert hits[0].document.content == "年假更新为十二天。"
    assert hits[0].document.attributes["filename"] == "leave.md"
    assert foreign_documents == ()
    assert deleted.status == "DELETED"
    assert after_delete == ()

    monkeypatch.setattr(backend.knowledge, "delete", vector_delete)
    competing_upload = await service.upload(
        context,
        principal_id="employee-a",
        filename="expense.md",
        media_type="text/markdown",
        content=_content("报销标准。".encode()),
    )
    competing = await service.ingest(
        context,
        config,
        knowledge_base_name="handbook",
        artifact_ids=[competing_upload.artifact_id],
    )

    async def supersede_on_another_worker(
        delete_context: TenantContext,
        knowledge_base_id: str,
        document_ids: list[str],
    ) -> None:
        """Expose an update/delete race that must not report false success."""

        await vector_delete(delete_context, knowledge_base_id, document_ids)
        async with sessions.begin() as database:
            concurrent = await database.get(
                KnowledgeDocumentRow,
                (tenant_id, competing[0].document_id),
            )
            assert concurrent is not None
            concurrent.status = "SUPERSEDED"

    monkeypatch.setattr(backend.knowledge, "delete", supersede_on_another_worker)
    with pytest.raises(RuntimeError, match="changed during deletion"):
        await service.delete_document(
            context,
            config,
            "handbook",
            competing[0].document_id,
        )

    async with sessions() as database:
        assert await database.get(KnowledgeBaseRow, (tenant_id, first[0].knowledge_base_id))
        assert await database.get(
            KnowledgeArtifactRow,
            (tenant_id, first_upload.artifact_id),
        )
    await engine.dispose()


@pytest.mark.anyio
async def test_tenant_knowledge_rejects_an_agent_without_base_access(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'denied.db'}")
    sessions = build_session_factory(engine)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    backend = build_inmemory_backend()
    assert backend.knowledge is not None
    assert backend.artifact is not None
    service = TenantKnowledgeService(
        sessions,
        knowledge=backend.knowledge,
        artifacts=backend.artifact,
    )
    context = TenantContext(tenant_id=uuid4(),
                            agent_app_id=uuid4(),
                            config_version=1,
                            request_id="request-denied",
                            trace_id="trace-denied")

    with pytest.raises(PermissionError, match="knowledge base is not granted"):
        await service.list_documents(context, {"knowledge_base_names": []}, "handbook")
    await engine.dispose()


@pytest.mark.anyio
async def test_failed_object_read_is_marked_failed_and_can_retry(tmp_path: Path) -> None:
    """A transient object outage cannot strand a document in INGESTING."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'retry.db'}")
    sessions = build_session_factory(engine)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    tenant_id = uuid4()
    agent_id = uuid4()
    async with sessions.begin() as database:
        database.add(Tenant(tenant_id=tenant_id, name="Retry Tenant"))
        database.add(AgentApp(agent_app_id=agent_id, tenant_id=tenant_id, name="Retry Agent"))
    backend = build_inmemory_backend()
    assert backend.knowledge is not None
    assert backend.artifact is not None
    service = TenantKnowledgeService(
        sessions,
        knowledge=backend.knowledge,
        artifacts=_FailOnceArtifactStore(backend.artifact),
    )
    context = TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        config_version=1,
        request_id="request-retry",
        trace_id="trace-retry",
    )
    config = {"knowledge_base_names": ["handbook"]}
    artifact = await service.upload(
        context,
        principal_id="employee",
        filename="policy.md",
        media_type="text/markdown",
        content=_content("制度内容".encode()),
    )

    with pytest.raises(OSError, match="temporary object storage outage"):
        await service.ingest(
            context,
            config,
            knowledge_base_name="handbook",
            artifact_ids=[artifact.artifact_id],
        )
    async with sessions() as database:
        failed = await database.scalar(
            select(KnowledgeDocumentRow).where(KnowledgeDocumentRow.tenant_id == tenant_id))
        assert failed is not None and failed.status == "FAILED"
    with pytest.raises(LookupError, match="document does not exist"):
        # Idempotency applies only to a committed tombstone; incomplete
        # ingestion must never be presented as a successful deletion.
        await service.delete_document(
            context,
            config,
            "handbook",
            failed.document_id,
        )

    retried = await service.ingest(
        context,
        config,
        knowledge_base_name="handbook",
        artifact_ids=[artifact.artifact_id],
    )

    assert retried[0].status == "READY"
    assert retried[0].version == 1
    await engine.dispose()


@pytest.mark.anyio
async def test_knowledge_service_routes_each_agent_backend_profile(tmp_path: Path) -> None:
    """Artifact and vector ports are selected from the pinned Agent profile."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'routing.db'}")
    sessions = build_session_factory(engine)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    tenant_id = uuid4()
    agent_id = uuid4()
    async with sessions.begin() as database:
        database.add(Tenant(tenant_id=tenant_id, name="Routing Tenant"))
        database.add(AgentApp(agent_app_id=agent_id, tenant_id=tenant_id, name="Routing Agent"))
    first = build_inmemory_backend("first")
    second = build_inmemory_backend("second")
    registry = StorageBackendRegistry()
    registry.register(first)
    registry.register(second)
    service = TenantKnowledgeService(
        sessions,
        storage=StorageRouter(registry),
        default_backends={
            "session": "first",
            "knowledge": "first",
            "artifact": "first",
        },
    )
    context = TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        config_version=2,
        request_id="request-routing",
        trace_id="trace-routing",
    )
    selected = {
        "session": "second",
        "knowledge": "second",
        "artifact": "second",
    }

    artifact = await service.upload(
        context,
        principal_id="employee",
        filename="routing.md",
        media_type="text/markdown",
        content=_content(b"selected backend"),
        backends=selected,
    )
    documents = await service.ingest(
        context,
        {"knowledge_base_names": ["handbook"]},
        knowledge_base_name="handbook",
        artifact_ids=[artifact.artifact_id],
        backends=selected,
    )

    assert second.artifact is not None
    stored = b"".join(
        [block async for block in second.artifact.open(context, artifact.artifact_id)])
    assert stored == b"selected backend"
    assert documents[0].status == "READY"
    await engine.dispose()


@pytest.mark.anyio
async def test_knowledge_service_validates_artifact_and_document_lifecycles(
    tmp_path: Path, ) -> None:
    """Tenant RAG validates artifacts, duplicates, and document lifecycles."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'boundaries.db'}")
    sessions = build_session_factory(engine)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    tenant_id, agent_id = uuid4(), uuid4()
    async with sessions.begin() as database:
        database.add(Tenant(tenant_id=tenant_id, name="Knowledge Boundaries"))
        database.add(AgentApp(agent_app_id=agent_id, tenant_id=tenant_id, name="Knowledge Agent"))
    backend = build_inmemory_backend()
    assert backend.knowledge is not None and backend.artifact is not None
    service = TenantKnowledgeService(
        sessions,
        knowledge=backend.knowledge,
        artifacts=backend.artifact,
        max_file_bytes=16,
    )
    context = TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        config_version=1,
        request_id="request-boundary",
        trace_id="trace-boundary",
    )
    config = {"knowledge_base_names": ["handbook"]}

    assert service.supports_upload("policy.md")
    assert not service.supports_upload("archive.zip")
    with pytest.raises(ValueError, match="at least one"):
        await service.ingest(
            context,
            config,
            knowledge_base_name="handbook",
            artifact_ids=[],
        )
    with pytest.raises(LookupError, match="artifact does not exist"):
        await service.ingest(
            context,
            config,
            knowledge_base_name="handbook",
            artifact_ids=["missing"],
        )
    with pytest.raises(ValueError, match="one uploaded artifact"):
        await service.update_document(
            context,
            config,
            "handbook",
            uuid4(),
            "",
        )
    with pytest.raises(LookupError, match="artifact does not exist"):
        await service.update_document(
            context,
            config,
            "handbook",
            uuid4(),
            "missing",
        )

    reference = ArtifactRef("registered", "memory://registered", "a" * 64)
    metadata = ArtifactMetadata("registered.md", "text/markdown", "a" * 64, 1)
    with pytest.raises(ValueError, match="identity and file metadata"):
        await service.register_artifact(
            context,
            principal_id="",
            reference=reference,
            metadata=metadata,
        )
    with pytest.raises(ValueError, match="unsupported"):
        await service.register_artifact(
            context,
            principal_id="employee",
            reference=reference,
            metadata=ArtifactMetadata("archive.zip", "application/zip", "a" * 64, 1),
        )
    with pytest.raises(ValueError, match="10 MB limit"):
        await service.register_artifact(
            context,
            principal_id="employee",
            reference=reference,
            metadata=ArtifactMetadata("large.md", "text/markdown", "a" * 64, 17),
        )
    with pytest.raises(ValueError, match="checksum"):
        await service.register_artifact(
            context,
            principal_id="employee",
            reference=reference,
            metadata=ArtifactMetadata("other.md", "text/markdown", "b" * 64, 1),
        )
    await service.register_artifact(
        context,
        principal_id="employee",
        reference=reference,
        metadata=metadata,
    )
    with pytest.raises(RuntimeError, match="collision"):
        await service.register_artifact(
            context,
            principal_id="employee",
            reference=ArtifactRef("registered", "memory://registered", "b" * 64),
            metadata=ArtifactMetadata("registered.md", "text/markdown", "b" * 64, 1),
        )

    first_upload = await service.upload(
        context,
        principal_id="employee",
        filename="first.md",
        media_type="text/markdown",
        content=_content("第一份制度".encode()),
    )
    second_upload = await service.upload(
        context,
        principal_id="employee",
        filename="second.md",
        media_type="text/markdown",
        content=_content("第二份制度".encode()),
    )
    first = await service.ingest(
        context,
        config,
        knowledge_base_name="handbook",
        artifact_ids=[first_upload.artifact_id, first_upload.artifact_id],
    )
    second = await service.ingest(
        context,
        config,
        knowledge_base_name="handbook",
        artifact_ids=[second_upload.artifact_id],
    )
    assert len(first) == 1
    with pytest.raises(ValueError, match="replacement content already exists"):
        await service.update_document(
            context,
            config,
            "handbook",
            second[0].document_id,
            first_upload.artifact_id,
        )
    with pytest.raises(LookupError, match="document does not exist"):
        await service.update_document(
            context,
            config,
            "handbook",
            uuid4(),
            second_upload.artifact_id,
        )
    with pytest.raises(LookupError, match="document does not exist"):
        await service.delete_document(context, config, "handbook", uuid4())

    deleted = await service.delete_document(context, config, "handbook", first[0].document_id)
    assert deleted.status == "DELETED"
    restored = await service.ingest(
        context,
        config,
        knowledge_base_name="handbook",
        artifact_ids=[first_upload.artifact_id],
    )
    restored_hits = await service.search(context, config, "第一份制度", limit=5)
    assert restored[0].document_id == first[0].document_id
    assert restored[0].version == 2
    assert restored[0].status == "READY"
    assert restored_hits[0].document.content == "第一份制度"

    for query, limit in [("", 5), ("valid", 0), ("valid", 51)]:
        with pytest.raises(ValueError, match="query and a limit"):
            await service.search(context, config, query, limit=limit)
    assert await service.search(context, {"knowledge_base_names": []}, "anything", limit=1) == ()
    for malformed in ["handbook", [""], [1]]:
        with pytest.raises(ValueError, match="knowledge_base_names"):
            await service.search(
                context,
                {"knowledge_base_names": malformed},
                "anything",
                limit=1,
            )
    await engine.dispose()


def test_knowledge_service_rejects_mixed_storage_composition() -> None:
    backend = build_inmemory_backend()
    assert backend.knowledge is not None and backend.artifact is not None
    registry = StorageBackendRegistry()
    registry.register(backend)
    with pytest.raises(ValueError, match="cannot mix"):
        TenantKnowledgeService(
            None,  # type: ignore[arg-type]
            knowledge=backend.knowledge,
            artifacts=backend.artifact,
            storage=StorageRouter(registry),
        )


@pytest.mark.anyio
async def test_embedding_model_change_requires_reindex_before_read_or_write(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'model-change.db'}")
    sessions = build_session_factory(engine)
    tenant_id, agent_id = uuid4(), uuid4()
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with sessions.begin() as database:
            database.add(Tenant(tenant_id=tenant_id, name="model-change"))
            database.add(AgentApp(agent_app_id=agent_id, tenant_id=tenant_id, name="agent"))
        backend = build_inmemory_backend()
        service = TenantKnowledgeService(sessions,
                                         knowledge=backend.knowledge,
                                         artifacts=backend.artifact,
                                         embedding_model="qwen3.7-text-embedding")
        context = TenantContext(tenant_id=tenant_id,
                                agent_app_id=agent_id,
                                config_version=1,
                                request_id="model-change",
                                trace_id="test")
        config = {"knowledge_base_names": ["handbook"]}
        upload = await service.upload(context,
                                      principal_id="admin",
                                      filename="test.txt",
                                      media_type="text/plain",
                                      content=_content(b"Knowledge"))
        await service.ingest(context,
                             config,
                             knowledge_base_name="handbook",
                             artifact_ids=[upload.artifact_id])
        async with sessions() as database:
            base = await database.scalar(select(KnowledgeBaseRow))
            assert base is not None
            assert base.embedding_model == "qwen3.7-text-embedding"
        changed = TenantKnowledgeService(sessions,
                                         knowledge=backend.knowledge,
                                         artifacts=backend.artifact,
                                         embedding_model="other-model")
        with pytest.raises(ValueError, match="reindex"):
            await changed.search(context, config, "Knowledge")
        with pytest.raises(ValueError, match="reindex"):
            await changed.ingest(context,
                                 config,
                                 knowledge_base_name="handbook",
                                 artifact_ids=[upload.artifact_id])
    finally:
        await engine.dispose()
