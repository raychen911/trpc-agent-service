import asyncio
import logging
import uuid
from contextlib import suppress
from dataclasses import dataclass

from trpc_service.agent import AgentFactory, AgentRuntimeConfigRepository
from trpc_service.channels import ChannelBindingRepository, ImDeliveryHandlers
from trpc_service.config import (
    AwsKmsSecretResolver,
    CompositeSecretResolver,
    EnvironmentSecretResolver,
    Settings,
    VaultSecretResolver,
)
from trpc_service.gateway import (
    AgentMessageService,
    CrossNodeRouter,
    InMemoryNodeDirectory,
    RedisNodeDirectory,
    TrpcAgentExecutor,
)
from trpc_service.gateway.contracts import NodeDirectory
from trpc_service.gateway.queue import ExecutionLedger, InboundWorker, SqlInboundQueue
from trpc_service.governance import GovernanceService
from trpc_service.metrics import PlatformMetrics
from trpc_service.storage.artifacts import LocalArtifactStore, MinioArtifactStore
from trpc_service.storage.contracts import (
    ArtifactStore,
    CoordinationStore,
    DataPlaneStore,
)
from trpc_service.storage.coordinator import TurnCoordinator
from trpc_service.storage.database import Database
from trpc_service.storage.inmemory import InMemoryConversationStore, InMemoryCoordinationStore
from trpc_service.storage.outbox import OutboxWorker, VectorOutboxHandlers
from trpc_service.storage.redis_backend import RedisCoordinationStore
from trpc_service.storage.sql_backend import SqlDataPlane
from trpc_service.storage.tenant_backends import (
    CompositeOutboxStore,
    TenantBackendResolver,
    TenantTurnCoordinator,
)
from trpc_service.storage.vector import InMemoryVectorStore, QdrantVectorStore, SemanticStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StorageRuntime:
    conversation: DataPlaneStore
    coordination: CoordinationStore
    semantic: SemanticStore
    artifacts: ArtifactStore
    outbox_worker: OutboxWorker
    agent_factory: AgentFactory
    turn_coordinator: TenantTurnCoordinator
    backend_resolver: TenantBackendResolver
    node_directory: NodeDirectory
    gateway_router: CrossNodeRouter
    inbound_queue: SqlInboundQueue
    execution_ledger: ExecutionLedger
    inbound_worker: InboundWorker
    im_delivery: ImDeliveryHandlers

    @classmethod
    def build(
        cls,
        settings: Settings,
        database: Database,
        metrics: PlatformMetrics | None = None,
    ) -> "StorageRuntime":
        sql_conversation = SqlDataPlane(database.session_factory)
        memory_conversation = InMemoryConversationStore()
        conversation: DataPlaneStore = (
            sql_conversation if settings.conversation_backend == "sql" else memory_conversation
        )

        if settings.coordination_backend == "redis":
            assert settings.redis_url is not None
            coordination = RedisCoordinationStore.from_url(settings.redis_url.get_secret_value())
        else:
            coordination = InMemoryCoordinationStore()

        vector_store = (
            QdrantVectorStore(
                settings.qdrant_url or "",
                settings.qdrant_collection,
                settings.qdrant_api_key.get_secret_value() if settings.qdrant_api_key else None,
            )
            if settings.vector_backend == "qdrant"
            else InMemoryVectorStore()
        )
        semantic = SemanticStore(vector_store)
        vault = (
            VaultSecretResolver(
                settings.vault_address,
                settings.vault_token.get_secret_value(),
                settings.vault_namespace,
            )
            if settings.vault_address and settings.vault_token
            else None
        )
        secret_resolver = CompositeSecretResolver(
            EnvironmentSecretResolver(),
            vault,
            AwsKmsSecretResolver(settings.aws_region),
        )
        binding_repository = ChannelBindingRepository(database.session_factory)
        im_delivery = ImDeliveryHandlers(binding_repository, secret_resolver)
        outbox_handlers = dict(VectorOutboxHandlers(semantic).handlers())
        outbox_handlers.update(im_delivery.handlers())
        outbox_worker = OutboxWorker(
            CompositeOutboxStore((sql_conversation, memory_conversation)),
            outbox_handlers,
            worker_id=f"worker-{uuid.uuid4()}",
            metrics=metrics,
        )
        agent_factory = AgentFactory(
            AgentRuntimeConfigRepository(database.session_factory),
            secret_resolver,
            audit_store=conversation,
            metrics=metrics,
        )
        if settings.coordination_backend == "redis":
            assert settings.redis_url is not None
            node_directory: NodeDirectory = RedisNodeDirectory.from_url(
                settings.redis_url.get_secret_value()
            )
        else:
            node_directory = InMemoryNodeDirectory()
        backend_resolver = TenantBackendResolver(
            database.session_factory,
            settings.conversation_backend,
            allow_inmemory_session=settings.environment != "production",
        )
        turn_coordinator = TenantTurnCoordinator(
            backend_resolver,
            {
                "sql": TurnCoordinator(sql_conversation, coordination, metrics=metrics),
                "inmemory": TurnCoordinator(memory_conversation, coordination, metrics=metrics),
            },
        )
        inbound_queue = SqlInboundQueue(database.session_factory)
        execution_ledger = ExecutionLedger(database.session_factory)
        message_service = AgentMessageService(
            TrpcAgentExecutor(
                agent_factory,
                execution_timeout_seconds=settings.runner_timeout_seconds,
                max_llm_calls=settings.runner_max_llm_calls,
                max_tool_calls=settings.runner_max_tool_calls,
            ),
            turn_coordinator,
            GovernanceService(database.session_factory, conversation),
            metrics=metrics,
            execution_ledger=execution_ledger,
            max_concurrent_sessions=settings.node_capacity,
        )
        gateway_router = CrossNodeRouter(
            settings.node_id,
            node_directory,
            message_service,
            settings.gateway_internal_secret.get_secret_value(),
            forward_timeout_seconds=settings.gateway_forward_timeout_seconds,
            metrics=metrics,
            tls_ca_file=settings.internal_tls_ca_file,
            tls_cert_file=settings.internal_tls_cert_file,
            tls_key_file=settings.internal_tls_key_file,
            canary_tenant_ids={
                item.strip() for item in settings.canary_tenant_ids.split(",") if item.strip()
            },
        )
        inbound_worker = InboundWorker(
            inbound_queue,
            gateway_router,
            worker_id=f"inbound-{settings.node_id}",
            metrics=metrics,
        )
        artifacts: ArtifactStore
        if settings.artifact_backend == "minio":
            from minio import Minio

            assert settings.minio_endpoint is not None
            assert settings.minio_access_key is not None
            assert settings.minio_secret_key is not None
            artifacts = MinioArtifactStore(
                Minio(
                    settings.minio_endpoint,
                    access_key=settings.minio_access_key.get_secret_value(),
                    secret_key=settings.minio_secret_key.get_secret_value(),
                    secure=settings.minio_secure,
                ),
                settings.minio_bucket,
            )
        else:
            artifacts = LocalArtifactStore(settings.artifact_root)
        return cls(
            conversation=conversation,
            coordination=coordination,
            semantic=semantic,
            artifacts=artifacts,
            outbox_worker=outbox_worker,
            agent_factory=agent_factory,
            turn_coordinator=turn_coordinator,
            backend_resolver=backend_resolver,
            node_directory=node_directory,
            gateway_router=gateway_router,
            inbound_queue=inbound_queue,
            execution_ledger=execution_ledger,
            inbound_worker=inbound_worker,
            im_delivery=im_delivery,
        )

    async def close(self) -> None:
        await self.im_delivery.close()
        await self.semantic.close()
        await self.gateway_router.close()
        close_directory = getattr(self.node_directory, "close", None)
        if close_directory is not None:
            await close_directory()
        await self.agent_factory.close()
        close = getattr(self.coordination, "close", None)
        if close is not None:
            await close()


async def run_outbox_loop(
    worker: OutboxWorker,
    stop: asyncio.Event,
    poll_seconds: float,
) -> None:
    failures = 0
    while not stop.is_set():
        try:
            processed = await worker.poll_once()
            failures = 0
        except Exception:
            failures += 1
            logger.exception("outbox_loop_failed", extra={"status": "retrying"})
            await _wait_or_stop(stop, min(30.0, poll_seconds * (2 ** min(failures, 8))))
            continue
        if processed:
            continue
        await _wait_or_stop(stop, poll_seconds)


async def run_node_heartbeat(
    directory: NodeDirectory,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    failures = 0
    while not stop.is_set():
        try:
            await directory.heartbeat(
                settings.node_id,
                settings.node_base_url,
                capacity=settings.node_capacity,
                ttl_seconds=settings.node_ttl_seconds,
                metadata={
                    "environment": settings.environment,
                    "release_track": settings.release_track,
                },
            )
            failures = 0
        except Exception:
            failures += 1
            logger.exception("node_heartbeat_failed", extra={"status": "retrying"})
        delay = (
            settings.node_heartbeat_seconds
            if failures == 0
            else min(30.0, settings.node_heartbeat_seconds * (2 ** min(failures, 8)))
        )
        await _wait_or_stop(stop, delay)


async def run_inbound_loop(
    worker: InboundWorker,
    stop: asyncio.Event,
    poll_seconds: float,
) -> None:
    failures = 0
    while not stop.is_set():
        try:
            processed = await worker.poll_once()
            failures = 0
        except Exception:
            failures += 1
            logger.exception("inbound_loop_failed", extra={"status": "retrying"})
            await _wait_or_stop(stop, min(30.0, poll_seconds * (2 ** min(failures, 8))))
            continue
        if processed:
            continue
        await _wait_or_stop(stop, poll_seconds)


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    with suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
