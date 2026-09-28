"""Agent runtime tests at the public storage and execution boundaries."""

import hashlib
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from trpc_service.agent.contracts import (
    AgentExecutionClaim,
    AgentExecutionContext,
    AgentExecutionOutcome,
    AgentExecutionRequest,
    AgentReply,
    AgentRunResult,
    AgentRuntimeConfig,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.agent.recovery import RetryableOperationError
from trpc_service.agent.runtime import (
    DeferredOutboxPublisher,
    PassThroughOutputFilter,
    SettingsAgentConfigProvider,
    StorageContextBuilder,
    StorageExecutionCoordinator,
    StorageResultCommitter,
)
from trpc_service.channels.contracts import (
    ChannelBindingConfig,
    IncomingMessage,
    MessageKind,
)
from trpc_service.config import Settings
from trpc_service.metrics import PlatformTelemetry
from trpc_service.storage.adapters.inmemory import build_inmemory_backend
from trpc_service.storage.router import BackendProfile, ResolvedStorage
from trpc_service.storage.types import (
    ArtifactMetadata,
    ExecutionCommit,
    KnowledgeDocument,
    KnowledgeHit,
    MemoryRecord,
    OutboxMessage,
    SessionEvent,
    SessionSnapshot,
)
from trpc_service.tenant.context import TenantContext


def _request(
    *,
    kind: MessageKind = MessageKind.TEXT,
    text: str | None = "你好",
    artifact_refs: tuple[str, ...] = (),
    attributes: dict[str, object] | None = None,
    attempt: int = 1,
) -> AgentExecutionRequest:
    tenant = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id=f"request-{uuid4()}",
        trace_id=f"trace-{uuid4()}",
    )
    return AgentExecutionRequest(
        tenant=tenant,
        session_id="session-1",
        incoming=IncomingMessage(
            external_message_id=f"message-{uuid4()}",
            principal_id="employee-1",
            conversation_id="conversation-1",
            kind=kind,
            occurred_at=datetime.now(timezone.utc),
            text=text,
            artifact_refs=artifact_refs,
            attributes=attributes or {},
        ),
        channel=ChannelBindingConfig(
            binding_id=uuid4(),
            tenant_id=tenant.tenant_id,
            agent_app_id=tenant.agent_app_id,
            channel_type="feishu",
        ),
        attempt=attempt,
    )


def _storage() -> ResolvedStorage:
    backend = build_inmemory_backend()
    assert backend.session is not None
    assert backend.outbox is not None
    return ResolvedStorage(
        profile=BackendProfile(
            session="inmemory",
            memory="inmemory",
            summary="inmemory",
            knowledge="inmemory",
            artifact="inmemory",
            audit="inmemory",
        ),
        session=backend.session,
        outbox=backend.outbox,
        memory=backend.memory,
        summary=backend.summary,
        knowledge=backend.knowledge,
        artifact=backend.artifact,
        audit=backend.audit,
    )


def _config(**changes: object) -> AgentRuntimeConfig:
    values: dict[str, object] = {
        "config_version": 1,
        "runner_name": "trpc_agent",
        "backends": {
            "session": "inmemory"
        },
    }
    values.update(changes)
    return AgentRuntimeConfig(**values)  # type: ignore[arg-type]


async def _bytes(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


class RecordingSessionCache:
    """Return an exact cached version and record write-through snapshots."""

    def __init__(self, snapshot: SessionSnapshot | None = None) -> None:
        self.snapshot = snapshot
        self.expected_versions: list[int] = []
        self.writes: list[SessionSnapshot] = []

    async def get(
        self,
        context: TenantContext,
        session_id: str,
        *,
        expected_version: int,
    ) -> SessionSnapshot | None:
        del context, session_id
        self.expected_versions.append(expected_version)
        return self.snapshot

    async def put(self, context: TenantContext, snapshot: SessionSnapshot) -> None:
        del context
        self.writes.append(snapshot)

    async def close(self) -> None:
        return None


@pytest.mark.anyio
async def test_settings_config_and_noop_runtime_stages_are_stable(
        tmp_path) -> None:  # type: ignore[no-untyped-def]
    settings = Settings(_env_file=None, workspace_root=tmp_path)
    request = _request()
    provider = SettingsAgentConfigProvider(settings)
    config = await provider.load(request)
    backends = await provider.load_backends(request.tenant)
    result = AgentRunResult(replies=(AgentReply(MessageKind.TEXT, text="ok"), ))
    context = AgentExecutionContext(
        request=request,
        config=config,
        policy=PolicyDecision(PolicyAction.ALLOW),
        claim=AgentExecutionClaim("claim"),
    )

    assert config.config_version == request.tenant.config_version
    assert config.application["instruction"] == settings.agent_instruction
    assert backends == settings.storage_profile.model_dump(mode="json")
    assert await PassThroughOutputFilter().apply(context, result) is result
    assert await DeferredOutboxPublisher().publish(request, config, context.claim.completed) is None


@pytest.mark.anyio
async def test_storage_coordinator_claim_renew_commit_and_replay() -> None:
    storage = _storage()
    coordinator = StorageExecutionCoordinator(storage, lease_seconds=30)
    request = _request()
    config = _config()

    claim = await coordinator.begin(request, config)
    assert claim.request == request
    assert claim.runtime_config == config
    assert claim.fencing_token is not None
    assert await coordinator.renew(claim)
    assert not await coordinator.renew(AgentExecutionClaim("without-context"))
    assert not await coordinator.renew(
        AgentExecutionClaim("without-config", request=request, fencing_token=1))

    context = AgentExecutionContext(
        request=request,
        config=config,
        policy=PolicyDecision(PolicyAction.ALLOW),
        claim=claim,
    )
    receipt = await StorageResultCommitter(storage).commit(
        context,
        AgentRunResult(
            replies=(AgentReply(MessageKind.TEXT, text="已完成"), ),
            events=(SessionEvent(
                event_id="event-1",
                event_type="agent.replied",
                occurred_at=datetime.now(timezone.utc),
                payload={"text": "已完成"},
            ), ),
            state={"turn": 1},
        ),
    )
    replay = await coordinator.begin(request, config)

    assert receipt.session.version == 1
    assert len(receipt.committed_outbox_ids) == 1
    assert replay.completed is not None
    assert replay.completed.replayed
    assert replay.completed.session == receipt.session
    assert replay.completed.committed_outbox_ids == receipt.committed_outbox_ids


@pytest.mark.anyio
async def test_storage_coordinator_persists_policy_outcomes_and_failures() -> None:
    # Each terminal classification owns an independent Inbox lifecycle.
    deny_storage = _storage()
    deny_coordinator = StorageExecutionCoordinator(deny_storage)
    deny_request = _request()
    deny_claim = await deny_coordinator.begin(deny_request, _config())
    denied = await deny_coordinator.reject(
        deny_claim,
        PolicyDecision(PolicyAction.DENY, reason="tenant policy denied request"),
    )

    review_storage = _storage()
    review_coordinator = StorageExecutionCoordinator(review_storage)
    review_request = _request()
    review_claim = await review_coordinator.begin(review_request, _config())
    reviewed = await review_coordinator.reject(
        review_claim,
        PolicyDecision(PolicyAction.REVIEW, reason="operator approval required"),
    )

    retry_storage = _storage()
    retry_coordinator = StorageExecutionCoordinator(retry_storage)
    retry_request = _request(attempt=2)
    retry_claim = await retry_coordinator.begin(retry_request, _config())
    await retry_coordinator.fail(
        retry_claim,
        RetryableOperationError("provider busy", retry_after_seconds=1),
    )
    # The durable Agent task queue owns retry timing. The nested Inbox must be
    # immediately reclaimable once that outer queue releases the next attempt.
    retry_reclaim = await retry_coordinator.begin(retry_request, _config())

    permanent_storage = _storage()
    permanent_coordinator = StorageExecutionCoordinator(permanent_storage)
    permanent_request = _request()
    permanent_claim = await permanent_coordinator.begin(permanent_request, _config())
    await permanent_coordinator.fail(permanent_claim, ValueError("invalid configuration"))
    await permanent_coordinator.fail(AgentExecutionClaim("no-request"), RuntimeError("ignored"))

    assert denied.outcome is AgentExecutionOutcome.DENIED
    assert denied.session.version == 0
    assert reviewed.outcome is AgentExecutionOutcome.REVIEW_REQUIRED
    assert reviewed.policy is not None
    assert retry_reclaim.fencing_token is not None
    with pytest.raises(RuntimeError, match="no request context"):
        await deny_coordinator.reject(
            AgentExecutionClaim("invalid", runtime_config=_config()),
            PolicyDecision(PolicyAction.DENY),
        )
    with pytest.raises(RuntimeError, match="no runtime configuration"):
        await deny_coordinator.reject(
            AgentExecutionClaim("invalid", request=_request()),
            PolicyDecision(PolicyAction.DENY),
        )


@pytest.mark.anyio
async def test_context_builder_restores_memory_without_legacy_upload_state() -> None:
    storage = _storage()
    request = _request()
    assert storage.memory is not None
    await storage.memory.upsert(
        request.tenant,
        [MemoryRecord("memory-1", "employee-1", "员工喜欢白兔")],
    )
    await storage.session.commit_execution(
        request.tenant,
        ExecutionCommit(
            session_id=request.session_id,
            expected_version=0,
            state={
                "pending_knowledge_upload": {
                    "principal_id": "employee-1",
                    "artifact_refs": ["artifact-1"],
                    "expires_at": "2999-01-01T00:00:00+00:00",
                }
            },
        ),
    )
    claim = AgentExecutionClaim("claim")
    context = await StorageContextBuilder(storage).build(
        request,
        _config(knowledge={
            "auto_retrieve": False,
            "retrieval_limit": 3
        }),
        PolicyDecision(PolicyAction.ALLOW),
        claim,
    )

    assert context.session is not None
    assert context.session.version == 1
    # Session data written by an older release must not silently turn the next
    # user message into a knowledge-base mutation request.
    assert context.request.incoming.artifact_refs == ()
    assert context.memories[0].record.memory_id == "memory-1"
    assert context.knowledge == ()
    assert context.input_artifacts == ()


@pytest.mark.anyio
async def test_context_builder_reads_an_exact_short_term_session_from_cache() -> None:
    storage = _storage()
    request = _request()
    cached = SessionSnapshot(
        session_id=request.session_id,
        version=4,
        events=(SessionEvent(
            event_id="cached-event",
            event_type="agent.replied",
            occurred_at=datetime.now(timezone.utc),
            payload={"text": "cached reply"},
        ), ),
    )
    cache = RecordingSessionCache(cached)

    context = await StorageContextBuilder(storage, session_cache=cache).build(
        request,
        _config(knowledge={"auto_retrieve": False}),
        PolicyDecision(PolicyAction.ALLOW),
        AgentExecutionClaim("claim", session_version=4),
    )

    assert context.session == cached
    assert cache.expected_versions == [4]


@pytest.mark.anyio
async def test_context_builder_loads_validated_image_artifacts() -> None:
    storage = _storage()
    assert storage.artifact is not None
    request = _request(kind=MessageKind.IMAGE, text=None)
    payload = b"\x89PNG\r\n\x1a\nimage-payload"
    reference = await storage.artifact.put(
        request.tenant,
        _bytes(payload),
        ArtifactMetadata(
            filename="picture.png",
            media_type="image/png",
            checksum=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
        ),
    )
    incoming = replace(
        request.incoming,
        artifact_refs=(reference.artifact_id, ),
        attributes={"provider_media": [{
            "filename": "企业图片.png"
        }]},
    )
    request = replace(
        request,
        incoming=incoming,
        channel=replace(
            request.channel,
            capabilities={
                "max_model_image_bytes": 1024,
                "max_model_images": 1
            },
        ),
    )

    context = await StorageContextBuilder(storage).build(
        request,
        _config(knowledge={"auto_retrieve": False}),
        PolicyDecision(PolicyAction.ALLOW),
        AgentExecutionClaim("claim"),
    )

    assert len(context.input_artifacts) == 1
    assert context.input_artifacts[0].filename == "企业图片.png"
    assert context.input_artifacts[0].media_type == "image/png"
    assert context.input_artifacts[0].content == payload


@pytest.mark.anyio
async def test_context_builder_rejects_invalid_retrieval_and_image_limits() -> None:
    storage = _storage()
    decision = PolicyDecision(PolicyAction.ALLOW)
    claim = AgentExecutionClaim("claim")
    request = _request()

    with pytest.raises(ValueError, match="auto_retrieve"):
        await StorageContextBuilder(storage).build(request,
                                                   _config(knowledge={"auto_retrieve": "yes"}),
                                                   decision, claim)
    with pytest.raises(ValueError, match="retrieval_limit"):
        await StorageContextBuilder(storage).build(request,
                                                   _config(knowledge={"retrieval_limit": True}),
                                                   decision, claim)

    image_without_ref = _request(kind=MessageKind.IMAGE, text=None)
    with pytest.raises(ValueError, match="persisted Artifact"):
        await StorageContextBuilder(storage).build(
            image_without_ref,
            _config(knowledge={"auto_retrieve": False}),
            decision,
            claim,
        )
    no_artifact_storage = replace(storage, artifact=None)
    image_with_ref = replace(
        image_without_ref,
        incoming=replace(image_without_ref.incoming, artifact_refs=("missing", )),
    )
    with pytest.raises(LookupError, match="Artifact backend"):
        await StorageContextBuilder(no_artifact_storage).build(
            image_with_ref,
            _config(knowledge={"auto_retrieve": False}),
            decision,
            claim,
        )

    for capabilities, message in [
        ({
            "max_model_image_bytes": True
        }, "size limit"),
        ({
            "max_model_images": 0
        }, "count limit"),
        ({
            "max_model_images": 1
        }, "image count"),
    ]:
        refs = ("a", "b") if message == "image count" else ("a", )
        invalid = replace(
            image_without_ref,
            incoming=replace(image_without_ref.incoming, artifact_refs=refs),
            channel=replace(image_without_ref.channel, capabilities=capabilities),
        )
        with pytest.raises(ValueError, match=message):
            await StorageContextBuilder(storage).build(
                invalid,
                _config(knowledge={"auto_retrieve": False}),
                decision,
                claim,
            )

    assert storage.artifact is not None
    oversized_payload = b"\x89PNG\r\n\x1a\nlarge-image"
    oversized_ref = await storage.artifact.put(
        image_without_ref.tenant,
        _bytes(oversized_payload),
        ArtifactMetadata(
            filename="large.png",
            media_type="image/png",
            checksum=hashlib.sha256(oversized_payload).hexdigest(),
            size_bytes=len(oversized_payload),
        ),
    )
    oversized = replace(
        image_without_ref,
        incoming=replace(
            image_without_ref.incoming,
            artifact_refs=(oversized_ref.artifact_id, ),
        ),
        channel=replace(
            image_without_ref.channel,
            capabilities={"max_model_image_bytes": 8},
        ),
    )
    with pytest.raises(ValueError, match="model size limit"):
        await StorageContextBuilder(storage).build(
            oversized,
            _config(knowledge={"auto_retrieve": False}),
            decision,
            claim,
        )


@pytest.mark.anyio
async def test_result_committer_preserves_reply_correlation_and_derived_work() -> None:
    storage = _storage()
    request = _request(attributes={"reply_context": {"message_id": "om_1"}})
    coordinator = StorageExecutionCoordinator(storage)
    config = _config()
    claim = await coordinator.begin(request, config)
    context = AgentExecutionContext(
        request=request,
        config=config,
        policy=PolicyDecision(PolicyAction.ALLOW),
        claim=claim,
    )
    derived = OutboxMessage(
        outbox_id="audit-derived",
        category="AUDIT",
        idempotency_key="audit-derived-key",
    )
    cache = RecordingSessionCache()
    receipt = await StorageResultCommitter(storage, session_cache=cache).commit(
        context,
        AgentRunResult(
            replies=(
                AgentReply(
                    MessageKind.TEXT,
                    text="第一段",
                    artifact_refs=("artifact-output", ),
                    attributes={"streaming": True},
                ),
                AgentReply(MessageKind.TEXT, text="第二段"),
            ),
            state={"completed": True},
            derived_outbox=(derived, ),
        ),
    )

    assert receipt.session.state == {"completed": True}
    assert cache.writes == [receipt.session]
    assert len(receipt.committed_outbox_ids) == 2
    first = await storage.outbox.claim_outbox(
        request.tenant,
        receipt.committed_outbox_ids[0],
        worker_id="delivery",
        lease_until=datetime(2999, 1, 1, tzinfo=timezone.utc),
    )
    assert first is not None
    assert first.payload["attributes"] == {
        "streaming": True,
        "reply_context": {
            "message_id": "om_1"
        },
        "in_reply_to": request.incoming.external_message_id,
    }
    assert first.payload["artifact_refs"] == ["artifact-output"]


@pytest.mark.anyio
async def test_result_committer_rejects_unsupported_replies_and_context() -> None:
    storage = _storage()
    config = _config()
    request = _request()
    claim = await StorageExecutionCoordinator(storage).begin(request, config)
    context = AgentExecutionContext(
        request=request,
        config=config,
        policy=PolicyDecision(PolicyAction.ALLOW),
        claim=claim,
    )

    with pytest.raises(ValueError, match="text replies only"):
        await StorageResultCommitter(storage).commit(
            context, AgentRunResult(replies=(AgentReply(MessageKind.IMAGE), )))

    invalid_request = replace(
        request,
        incoming=replace(request.incoming, attributes={"reply_context": "invalid"}),
    )
    invalid_context = replace(context, request=invalid_request)
    with pytest.raises(ValueError, match="reply_context"):
        await StorageResultCommitter(storage).commit(
            invalid_context,
            AgentRunResult(replies=(AgentReply(MessageKind.TEXT, text="reply"), )),
        )


@pytest.mark.anyio
async def test_runtime_storage_stages_emit_success_telemetry() -> None:
    """Session, memory, knowledge, and commit operations expose terminal metrics."""

    class KnowledgeService:

        async def search(self, *args: object, **kwargs: object) -> tuple[KnowledgeHit, ...]:
            del args, kwargs
            return (KnowledgeHit(KnowledgeDocument("doc-1", "handbook", "白兔制度"), 0.9), )

    storage = _storage()
    telemetry = PlatformTelemetry(
        service_name="trpc-agent-service",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    request = _request()
    config = _config(knowledge={
        "auto_retrieve": True,
        "retrieval_limit": 2,
        "knowledge_base_names": ["handbook"],
    })
    claim = await StorageExecutionCoordinator(storage).begin(request, config)
    context = await StorageContextBuilder(
        storage,
        telemetry,
        KnowledgeService(),  # type: ignore[arg-type]
    ).build(request, config, PolicyDecision(PolicyAction.ALLOW), claim)
    receipt = await StorageResultCommitter(storage, telemetry).commit(
        context,
        AgentRunResult(replies=(AgentReply(MessageKind.TEXT, text="ok"), )),
    )
    metrics = telemetry.render_prometheus().decode()

    assert context.knowledge[0].document.content == "白兔制度"
    assert receipt.session.version == 1
    for operation in ("session.load", "memory.search", "knowledge.search", "session.commit"):
        assert f'operation="{operation}",result="success"' in metrics


@pytest.mark.anyio
async def test_runtime_storage_stages_emit_error_telemetry() -> None:
    """Adapter failures are observable and still propagate to recovery handling."""

    class FailingSession:

        async def load(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            raise RuntimeError("session unavailable")

        async def commit_execution(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            raise RuntimeError("commit unavailable")

    class FailingMemory:

        async def search(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            raise RuntimeError("memory unavailable")

    class FailingKnowledge:

        async def search(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            raise RuntimeError("knowledge unavailable")

    storage = _storage()
    telemetry = PlatformTelemetry(
        service_name="trpc-agent-service",
        environment="test",
        node_role="worker",
        otlp_endpoint=None,
    )
    request = _request()
    decision = PolicyDecision(PolicyAction.ALLOW)
    claim = AgentExecutionClaim("claim", request=request, runtime_config=_config())

    with pytest.raises(RuntimeError, match="session unavailable"):
        await StorageContextBuilder(
            replace(storage, session=FailingSession()),  # type: ignore[arg-type]
            telemetry,
        ).build(request, _config(), decision, claim)
    with pytest.raises(RuntimeError, match="memory unavailable"):
        await StorageContextBuilder(
            replace(storage, memory=FailingMemory()),  # type: ignore[arg-type]
            telemetry,
        ).build(request, _config(), decision, claim)
    with pytest.raises(RuntimeError, match="knowledge unavailable"):
        await StorageContextBuilder(
            storage,
            telemetry,
            FailingKnowledge(),  # type: ignore[arg-type]
        ).build(
            request,
            _config(knowledge={"knowledge_base_names": ["handbook"]}),
            decision,
            claim,
        )

    context = AgentExecutionContext(
        request=request,
        config=_config(),
        policy=decision,
        claim=claim,
    )
    with pytest.raises(RuntimeError, match="commit unavailable"):
        await StorageResultCommitter(
            replace(storage, session=FailingSession()),  # type: ignore[arg-type]
            telemetry,
        ).commit(context, AgentRunResult())

    metrics = telemetry.render_prometheus().decode()
    for operation in ("session.load", "memory.search", "knowledge.search", "session.commit"):
        assert f'operation="{operation}",result="error"' in metrics
