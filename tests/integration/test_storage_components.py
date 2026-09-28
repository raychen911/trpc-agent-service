import asyncio
import hashlib
import os
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta, timezone
from typing import cast
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from trpc_service.agent import RedisExecutionLeaseCoordinator, RedisOutboxNotifier
from trpc_service.agent.contracts import AgentExecutionRequest, AgentRuntimeConfig
from trpc_service.agent.models import AgentApp
from trpc_service.agent.coordination import RedisCoordinationClient
from trpc_service.channels.models import ChannelBinding
from trpc_service.channels.contracts import ChannelBindingConfig, IncomingMessage, MessageKind
from trpc_service.config import Settings
from trpc_service.storage import (
    ArtifactMetadata,
    EmbeddingProvider,
    EmbeddingDimensionError,
    ExecutionCommit,
    InboxClaimRequest,
    KnowledgeDocument,
    RedisSessionSnapshotCache,
    SessionEvent,
)
from trpc_service.storage.adapters.pgvector import PgVectorKnowledgeStore
from trpc_service.storage.adapters.postgresql_usage import PostgreSQLUsageRecorder
from trpc_service.storage.database import build_session_factory
from trpc_service.storage.factory import build_storage_composition
from trpc_service.storage.knowledge import TenantKnowledgeService
from trpc_service.tenant import TenantContext
from trpc_service.tenant.models import Tenant

pytestmark = pytest.mark.skipif(
    "TRPC_TEST_POSTGRES_URL" not in os.environ,
    reason="storage_check.sh owns external component startup",
)


class DeterministicEmbedding(EmbeddingProvider):
    """Small deterministic provider used only to verify pgvector wiring."""

    @property
    def dimensions(self) -> int:
        return 1024

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return [self._embed(text) for text in texts]

    async def embed_query(self, text: str) -> Sequence[float]:
        return self._embed(text)

    @staticmethod
    def _embed(text: str) -> Sequence[float]:
        normalized = text.casefold()
        prefix = [
            float("seaweed" in normalized),
            float("postgres" in normalized),
            1.0,
        ]
        return prefix + [0.0] * (1024 - len(prefix))


class IncompatibleEmbedding(DeterministicEmbedding):
    """Represent a second profile that cannot share the existing vector column."""

    @property
    def dimensions(self) -> int:
        return 3


async def _content(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


@pytest.mark.anyio
async def test_real_storage_components_complete_the_provider_neutral_chain() -> None:
    password_ref = os.environ["TRPC_TEST_POSTGRES_PASSWORD_REF"]
    settings = Settings(
        _env_file=None,
        storage_backends={
            "facts": {
                "kind": "postgresql",
                "url": os.environ["TRPC_TEST_POSTGRES_URL"],
                "password_ref": password_ref,
            },
            "vectors": {
                "kind": "pgvector",
                "url": os.environ["TRPC_TEST_POSTGRES_URL"],
                "password_ref": password_ref,
                "embedding_provider": "test",
            },
            "objects": {
                "kind": "s3",
                "endpoint_url": os.environ["TRPC_TEST_S3_ENDPOINT"],
                "bucket": "trpc-integration-artifacts",
            },
        },
        storage_profile={
            "session": "facts",
            "memory": "facts",
            "summary": "facts",
            "knowledge": "vectors",
            "artifact": "objects",
            "audit": "facts",
        },
    )
    composition = build_storage_composition(
        settings,
        embedding_providers={"test": DeterministicEmbedding()},
    )
    assert len(composition.engines) == 1
    await composition.provision()
    await composition.initialize()
    async with composition.engines[0].begin() as connection:
        await connection.execute(text("SET TRANSACTION READ ONLY"))
        assert await connection.scalar(text("SHOW transaction_read_only")) == "on"
        readonly_store = PgVectorKnowledgeStore(async_sessionmaker(connection),
                                                DeterministicEmbedding())
        await readonly_store.validate_schema()
    stores = composition.router.resolve(settings.storage_profile.to_domain())
    tenant_id = uuid4()
    agent_app_id = uuid4()
    binding_id = uuid4()
    # Runtime foreign keys intentionally require a real control-plane scope.
    async with async_sessionmaker(composition.engines[0],
                                  expire_on_commit=False).begin() as database:
        database.add(
            Tenant(
                tenant_id=tenant_id,
                name=f"Integration {tenant_id}",
                status="active",
                isolation_mode="shared",
                audit_policy={},
            ))
        await database.flush()
        database.add(
            AgentApp(
                tenant_id=tenant_id,
                agent_app_id=agent_app_id,
                name="Integration Agent",
            ))
        await database.flush()
        database.add(
            ChannelBinding(
                binding_id=binding_id,
                binding_public_id=f"integration-{binding_id}",
                tenant_id=tenant_id,
                agent_app_id=agent_app_id,
                channel_type="integration",
                external_account_hash=f"sha256:{binding_id}",
            ))
    context = TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_app_id,
        config_version=1,
        request_id="integration-request",
        trace_id="integration-trace",
    )
    claim = await stores.session.claim_execution(
        context,
        InboxClaimRequest(
            binding_id=binding_id,
            external_message_id="integration-inbox",
            payload_hash=hashlib.sha256(b"integration").hexdigest(),
            session_id="integration-session",
            received_at=datetime.now(timezone.utc),
        ),
        worker_id="integration-worker",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    snapshot = await stores.session.commit_execution(
        context,
        ExecutionCommit(
            session_id="integration-session",
            expected_version=0,
            fencing_token=claim.fencing_token,
            events=(SessionEvent(
                event_id="integration-event",
                event_type="test",
                occurred_at=datetime.now(timezone.utc),
            ), ),
            state={"ready": True},
            inbox_id=claim.inbox_id,
            runner_request_id=claim.request_id,
        ),
    )
    assert snapshot.version == 1

    assert stores.knowledge is not None
    document = KnowledgeDocument(
        document_id="integration-document",
        knowledge_base_id="integration-kb",
        content="Seaweed object storage",
    )
    await stores.knowledge.index(context, [document])
    hits = await stores.knowledge.search(context, "integration-kb", "seaweed", 1)
    assert hits[0].document == document

    incompatible_store = PgVectorKnowledgeStore(
        build_session_factory(composition.engines[0]),
        IncompatibleEmbedding(),
    )
    with pytest.raises(EmbeddingDimensionError):
        await incompatible_store.ensure_schema()

    assert stores.artifact is not None
    payload = b"seaweedfs-artifact"
    artifact = await stores.artifact.put(
        context,
        _content(payload),
        ArtifactMetadata(
            # IM providers commonly return Unicode filenames. This exercises
            # the real boto3/SeaweedFS metadata boundary that unit doubles do
            # not validate.
            filename="企业微信图片.png",
            media_type="image/png",
            checksum=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
        ),
    )
    assert b"".join([chunk async for chunk in stores.artifact.open(context, artifact.artifact_id)
                     ]) == payload

    # Exercise the public RAG seam across SQL metadata, SeaweedFS, and pgvector.
    knowledge_service = TenantKnowledgeService(
        build_session_factory(composition.engines[0]),
        knowledge=stores.knowledge,
        artifacts=stores.artifact,
    )
    upload = await knowledge_service.upload(
        context,
        principal_id="integration-user",
        filename="storage-policy.md",
        media_type="text/markdown",
        content=_content(b"Seaweed is the approved object storage."),
    )
    indexed = await knowledge_service.ingest(
        context,
        {"knowledge_base_names": ["handbook"]},
        knowledge_base_name="handbook",
        artifact_ids=[upload.artifact_id],
    )
    rag_hits = await knowledge_service.search(
        context,
        {"knowledge_base_names": ["handbook"]},
        "seaweed",
        limit=1,
    )

    assert indexed[0].status == "READY"
    assert rag_hits[0].document.attributes["filename"] == "storage-policy.md"

    redis = Redis.from_url(os.environ["TRPC_TEST_REDIS_URL"], decode_responses=True)
    redis_client = cast(RedisCoordinationClient, redis)
    lease_coordinator = RedisExecutionLeaseCoordinator(
        redis_client,
        prefix="trpc-integration",
        ttl_ms=30000,
    )
    lease = await lease_coordinator.acquire(context, "integration-session")
    assert lease is not None
    assert await lease_coordinator.release(lease)
    await RedisOutboxNotifier(redis_client,
                              prefix="trpc-integration").notify(context, ["integration-outbox"])
    session_cache = RedisSessionSnapshotCache(
        redis_client,
        ttl_seconds=300,
        max_events=10,
        key_prefix="trpc-integration:session",
    )
    await session_cache.put(context, snapshot)
    assert await session_cache.get(
        context,
        snapshot.session_id,
        expected_version=snapshot.version,
    ) == snapshot
    await redis.aclose()
    await composition.close()


@pytest.mark.anyio
async def test_postgresql_budget_reservation_is_atomic_across_connections() -> None:
    """Concurrent Workers cannot both consume the final tenant Token range."""

    settings = Settings(
        _env_file=None,
        storage_backends={
            "facts": {
                "kind": "postgresql",
                "url": os.environ["TRPC_TEST_POSTGRES_URL"],
                "password_ref": os.environ["TRPC_TEST_POSTGRES_PASSWORD_REF"],
            },
        },
        storage_profile={
            "session": "facts",
            "memory": "facts",
            "summary": "facts",
            "audit": "facts",
        },
    )
    composition = build_storage_composition(settings)
    await composition.provision()
    await composition.initialize()
    sessions = async_sessionmaker(composition.engines[0], expire_on_commit=False)
    tenant_id = uuid4()
    agent_app_id = uuid4()
    async with sessions.begin() as database:
        database.add(
            Tenant(
                tenant_id=tenant_id,
                name=f"Budget {tenant_id}",
                status="active",
                isolation_mode="shared",
                audit_policy={},
            ))
        await database.flush()
        database.add(AgentApp(
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            name="Budget Agent",
        ))

    def request(request_id: str) -> AgentExecutionRequest:
        context = TenantContext(
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            config_version=1,
            request_id=request_id,
            trace_id=f"trace-{request_id}",
        )
        return AgentExecutionRequest(
            tenant=context,
            session_id=f"session-{request_id}",
            incoming=IncomingMessage(
                external_message_id=f"message-{request_id}",
                principal_id="integration-user",
                conversation_id="integration-budget",
                kind=MessageKind.TEXT,
                occurred_at=datetime.now(timezone.utc),
                text="并发预算测试",
            ),
            channel=ChannelBindingConfig(
                binding_id=uuid4(),
                tenant_id=tenant_id,
                agent_app_id=agent_app_id,
                channel_type="integration",
            ),
        )

    recorder = PostgreSQLUsageRecorder(sessions)
    config = AgentRuntimeConfig(
        config_version=1,
        runner_name="trpc_agent",
        model={"context_window_tokens": 600},
    )
    since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    barrier = asyncio.Barrier(8)

    async def reserve(request_id: str) -> str | None:
        await barrier.wait()
        return await recorder.reserve(
            request(request_id),
            config,
            since=since,
            daily_calls=None,
            daily_tokens=1000,
        )

    results = await asyncio.gather(*(reserve(f"node-{index}") for index in range(8)))

    assert results.count(None) == 1
    assert results.count("DAILY_TOKEN_BUDGET_EXCEEDED") == 7
    await composition.close()
