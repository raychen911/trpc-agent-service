from collections.abc import AsyncIterator, Sequence
from dataclasses import replace
from datetime import datetime
from uuid import uuid4

import pytest

from tests.test_agent_task_queue import _request
from trpc_service.agent import AgentExecutionClaim, AgentRuntimeConfig, PolicyAction, PolicyDecision
from trpc_service.agent.runtime import StorageContextBuilder
from trpc_service.channels import MessageKind
from trpc_service.storage import (
    ArtifactMetadata,
    ArtifactRef,
    ArtifactStore,
    AuditRecord,
    AuditStore,
    BackendProfile,
    ExecutionClaim,
    ExecutionCommit,
    InboxClaimRequest,
    KnowledgeDocument,
    KnowledgeHit,
    KnowledgeStore,
    MemoryHit,
    MemoryRecord,
    MemoryStore,
    OutboxMessage,
    OutboxStore,
    SessionStore,
    SessionSnapshot,
    SessionSummary,
    StorageBackend,
    StorageBackendAlreadyRegistered,
    StorageBackendRegistry,
    StorageCapabilityMissing,
    StorageRouter,
    SummaryStore,
)
from trpc_service.tenant import TenantContext


class StubSessionStore(SessionStore, OutboxStore):

    async def load(self, context: TenantContext, session_id: str) -> SessionSnapshot | None:
        return None

    async def claim_execution(
        self,
        context: TenantContext,
        request: InboxClaimRequest,
        worker_id: str,
        lease_until: datetime,
    ) -> ExecutionClaim:
        return ExecutionClaim(inbox_id="inbox-1", request_id=context.request_id)

    async def commit_execution(
        self,
        context: TenantContext,
        commit: ExecutionCommit,
    ) -> SessionSnapshot:
        return SessionSnapshot(session_id=commit.session_id, version=commit.expected_version + 1)

    async def fail_execution(
        self,
        context: TenantContext,
        inbox_id: str,
        *,
        fencing_token: int,
        error_code: str,
        error_summary: str,
        next_attempt_at: datetime | None,
    ) -> None:
        return None

    async def claim_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        lease_until: datetime,
    ) -> OutboxMessage | None:
        return None

    async def complete_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        external_receipt_id: str,
        completed_at: datetime,
    ) -> None:
        return None

    async def fail_outbox(
        self,
        context: TenantContext,
        outbox_id: str,
        *,
        worker_id: str,
        attempt_no: int,
        error_code: str,
        error_summary: str,
        next_attempt_at: datetime | None,
        completed_at: datetime,
        outcome_unknown: bool = False,
    ) -> None:
        return None


class TaggedSessionStore(StubSessionStore):
    """Expose the selected backend name through a deterministic snapshot."""

    def __init__(self, version: int) -> None:
        self._version = version

    async def load(self, context: TenantContext, session_id: str) -> SessionSnapshot:
        del context
        return SessionSnapshot(session_id=session_id, version=self._version)


class StubMemoryStore(MemoryStore):

    async def upsert(
        self,
        context: TenantContext,
        records: Sequence[MemoryRecord],
    ) -> None:
        return None

    async def search(
        self,
        context: TenantContext,
        principal_id: str,
        query: str,
        limit: int,
    ) -> Sequence[MemoryHit]:
        return ()


class StubSummaryStore(SummaryStore):

    async def put_if_newer(
        self,
        context: TenantContext,
        summary: SessionSummary,
    ) -> bool:
        return True


class StubKnowledgeStore(KnowledgeStore):

    async def index(
        self,
        context: TenantContext,
        documents: Sequence[KnowledgeDocument],
    ) -> None:
        return None

    async def search(
        self,
        context: TenantContext,
        knowledge_base_id: str,
        query: str,
        limit: int,
    ) -> Sequence[KnowledgeHit]:
        return ()


class StubArtifactStore(ArtifactStore):

    def __init__(self, content: bytes = b"") -> None:
        self._content = content

    async def put(
        self,
        context: TenantContext,
        content: AsyncIterator[bytes],
        metadata: ArtifactMetadata,
    ) -> ArtifactRef:
        return ArtifactRef(
            artifact_id="artifact-1",
            uri="stub://artifact-1",
            checksum=metadata.checksum,
        )

    def open(self, context: TenantContext, artifact_id: str) -> AsyncIterator[bytes]:

        async def stream() -> AsyncIterator[bytes]:
            yield self._content

        return stream()

    async def create_download_url(
        self,
        context: TenantContext,
        artifact_id: str,
        ttl_seconds: int,
    ) -> str:
        return f"stub://{artifact_id}?ttl={ttl_seconds}"


class StubAuditStore(AuditStore):

    async def append(self, context: TenantContext, record: AuditRecord) -> None:
        return None


def _context() -> TenantContext:
    return TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )


def test_storage_router_resolves_each_capability_independently() -> None:
    session_store = StubSessionStore()
    memory_store = StubMemoryStore()
    summary_store = StubSummaryStore()
    knowledge_store = StubKnowledgeStore()
    artifact_store = StubArtifactStore()
    audit_store = StubAuditStore()
    registry = StorageBackendRegistry()
    registry.register(
        StorageBackend(
            name="sql",
            session=session_store,
            outbox=session_store,
            summary=summary_store,
            audit=audit_store,
        ))
    registry.register(StorageBackend(name="redis", memory=memory_store))
    registry.register(StorageBackend(name="vector", knowledge=knowledge_store))
    registry.register(StorageBackend(name="object", artifact=artifact_store))

    stores = StorageRouter(registry).resolve(
        BackendProfile(
            session="sql",
            memory="redis",
            summary="sql",
            knowledge="vector",
            artifact="object",
            audit="sql",
        ))

    assert stores.session is session_store
    assert stores.memory is memory_store
    assert stores.summary is summary_store
    assert stores.knowledge is knowledge_store
    assert stores.artifact is artifact_store
    assert stores.audit is audit_store
    assert stores.profile.session == "sql"


def test_storage_registry_and_router_reject_invalid_composition() -> None:
    registry = StorageBackendRegistry()
    session_store = StubSessionStore()
    registry.register(StorageBackend(name="sql", session=session_store, outbox=session_store))

    with pytest.raises(StorageBackendAlreadyRegistered):
        duplicate = StubSessionStore()
        registry.register(StorageBackend(name="sql", session=duplicate, outbox=duplicate))
    with pytest.raises(StorageCapabilityMissing):
        StorageRouter(registry).resolve(
            BackendProfile(
                session="sql",
                memory="sql",
                summary="sql",
                knowledge="sql",
                artifact="sql",
                audit="sql",
            ))


def test_partial_backend_profile_keeps_unused_capabilities_optional() -> None:
    registry = StorageBackendRegistry()
    session_store = StubSessionStore()
    registry.register(StorageBackend(name="postgresql", session=session_store,
                                     outbox=session_store))

    stores = StorageRouter(registry).resolve(
        BackendProfile.from_mapping({"session": " PostgreSQL "}))

    assert stores.session is session_store
    assert stores.profile.session == "postgresql"
    assert stores.memory is None
    assert stores.summary is None
    assert stores.knowledge is None
    assert stores.artifact is None
    assert stores.audit is None


@pytest.mark.anyio
async def test_resolved_session_store_executes_the_registered_implementation() -> None:
    registry = StorageBackendRegistry()
    session_store = StubSessionStore()
    registry.register(StorageBackend(name="sql", session=session_store, outbox=session_store))
    commit = ExecutionCommit(session_id="session-1", expected_version=3)
    resolved_session = registry.resolve("sql").session

    assert resolved_session is not None
    snapshot = await resolved_session.commit_execution(_context(), commit)

    assert snapshot.version == 4
    assert snapshot.session_id == "session-1"


@pytest.mark.anyio
async def test_runtime_uses_each_agents_backend_profile() -> None:
    """Two tenant configurations can route the same port to different stores."""

    first = TaggedSessionStore(version=11)
    second = TaggedSessionStore(version=22)
    registry = StorageBackendRegistry()
    registry.register(StorageBackend(name="facts-a", session=first, outbox=first))
    registry.register(StorageBackend(name="facts-b", session=second, outbox=second))
    builder = StorageContextBuilder(StorageRouter(registry))
    request = _request()
    claim = AgentExecutionClaim(claim_id="claim-1")
    policy = PolicyDecision(action=PolicyAction.ALLOW)

    first_context = await builder.build(
        request,
        AgentRuntimeConfig(
            config_version=request.tenant.config_version,
            runner_name="trpc_agent",
            backends={"session": "facts-a"},
        ),
        policy,
        claim,
    )
    second_context = await builder.build(
        request,
        AgentRuntimeConfig(
            config_version=request.tenant.config_version,
            runner_name="trpc_agent",
            backends={"session": "facts-b"},
        ),
        policy,
        claim,
    )

    assert first_context.session is not None and first_context.session.version == 11
    assert second_context.session is not None and second_context.session.version == 22


@pytest.mark.anyio
async def test_runtime_loads_tenant_image_from_configured_artifact_backend() -> None:
    session_store = StubSessionStore()
    artifact_store = StubArtifactStore(b"\x89PNG\r\n\x1a\nimage")
    registry = StorageBackendRegistry()
    registry.register(StorageBackend(name="facts", session=session_store, outbox=session_store))
    registry.register(StorageBackend(name="objects", artifact=artifact_store))
    builder = StorageContextBuilder(StorageRouter(registry))
    request = _request()
    request = replace(
        request,
        incoming=replace(
            request.incoming,
            kind=MessageKind.IMAGE,
            text=None,
            artifact_refs=("image-artifact-id", ),
            attributes={"provider_media": [{
                "filename": "photo.png"
            }]},
        ),
        channel=replace(request.channel, capabilities={"max_model_image_bytes": 1024}),
    )

    context = await builder.build(
        request,
        AgentRuntimeConfig(
            config_version=request.tenant.config_version,
            runner_name="trpc_agent",
            backends={
                "session": "facts",
                "artifact": "objects"
            },
        ),
        PolicyDecision(action=PolicyAction.ALLOW),
        AgentExecutionClaim(claim_id="claim-image"),
    )

    assert len(context.input_artifacts) == 1
    assert context.input_artifacts[0].media_type == "image/png"
    assert context.input_artifacts[0].filename == "photo.png"
