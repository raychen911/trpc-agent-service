"""Application composition root for all independently implemented adapters.

This module lives at the package root because it wires Agent, Channel, Storage,
MCP, telemetry, and process-lifecycle components.  No presentation layer owns
that dependency graph.
"""

from collections.abc import Callable
from dataclasses import dataclass
from inspect import isawaitable

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.approval import ApprovalService
from trpc_service.agent.adapters.trpc import TRPCAgentRunner
from trpc_service.agent.nodes import (
    PostgreSQLRuntimeNodeRegistry,
    RuntimeNodeHeartbeatService,
)
from trpc_service.agent.pipeline import AgentExecutionPipeline
from trpc_service.agent.ports import AgentRunner, AgentTaskQueue
from trpc_service.agent.queue import PostgreSQLAgentTaskQueue
from trpc_service.agent.scaling import PostgreSQLWorkerPoolStore
from trpc_service.agent.runtime import DatabaseAgentConfigProvider, build_local_agent_pipeline
from trpc_service.agent.worker import AgentWorkerService
from trpc_service.admin.secret_store import TenantSecretStore
from trpc_service.channels.adapters.wecom import WeComChannelAdapter, WeComTransportRegistry
from trpc_service.channels.adapters.feishu import FeishuChannelAdapter, FeishuTransportRegistry
from trpc_service.channels.approval import ApprovalCommandProcessor
from trpc_service.channels.registry import ChannelAdapterRegistry
from trpc_service.channels.delivery import (
    DeliveryTaskQueue,
    DeliveryWorkerService,
    PostgreSQLDeliveryTaskQueue,
)
from trpc_service.channels.identity import PostgreSQLChannelIdentityService
from trpc_service.channels.media import ChannelMediaStore
from trpc_service.channels.wecom import WeComMessageService
from trpc_service.channels.wecom_runtime import SDKWeComClientFactory, WeComBindingSupervisor
from trpc_service.channels.feishu import FeishuMessageService
from trpc_service.channels.feishu_runtime import SDKFeishuClientFactory, FeishuBindingSupervisor
from trpc_service.config import Settings
from trpc_service.metrics import PlatformTelemetry
from trpc_service.mcp import TenantMCPService
from trpc_service.skill import BuiltinSkillCatalog
from trpc_service.storage.factory import StorageComposition, build_storage_composition
from trpc_service.storage.adapters.postgresql_approval import PostgreSQLApprovalStore
from trpc_service.storage.adapters.postgresql_usage import PostgreSQLUsageRecorder
from trpc_service.storage.adapters.postgresql_tool_ledger import PostgreSQLToolLedger
from trpc_service.storage.adapters.postgresql_runner_recovery import PostgreSQLRunnerRecoveryStore
from trpc_service.storage.registry import StorageBackendRegistry
from trpc_service.storage.router import BackendProfile, StorageRouter
from trpc_service.storage.knowledge import TenantKnowledgeService
from trpc_service.storage.session_cache import RedisSessionSnapshotCache, SessionSnapshotCache
from trpc_service.workspace import LocalWorkspaceProvider, WorkspaceProvider


@dataclass(frozen=True, slots=True)
class ApplicationContainer:
    """Explicit module dependencies shared by one service process."""

    channels: ChannelAdapterRegistry
    storage_backends: StorageBackendRegistry
    storage_router: StorageRouter
    storage_composition: StorageComposition
    knowledge: TenantKnowledgeService
    session_factory: async_sessionmaker[AsyncSession]
    agent_pipeline: AgentExecutionPipeline
    agent_runner: AgentRunner
    approvals: ApprovalService
    task_queue: AgentTaskQueue
    agent_workers: AgentWorkerService | None
    delivery_workers: DeliveryWorkerService | None
    delivery_queue: DeliveryTaskQueue
    node_registry: PostgreSQLRuntimeNodeRegistry
    worker_pool: PostgreSQLWorkerPoolStore
    node_heartbeat: RuntimeNodeHeartbeatService
    wecom: WeComMessageService
    wecom_supervisor: WeComBindingSupervisor | None
    feishu: FeishuMessageService
    feishu_supervisor: FeishuBindingSupervisor | None
    telemetry: PlatformTelemetry
    tenant_secrets: TenantSecretStore
    workspace: WorkspaceProvider
    mcp: TenantMCPService
    skills: BuiltinSkillCatalog
    session_cache: SessionSnapshotCache | None
    http_client: httpx.AsyncClient

    async def start(self) -> None:
        """Start local Worker slots when this process owns an execution role."""

        await self.node_heartbeat.start()
        if self.wecom_supervisor is not None:
            await self.wecom_supervisor.start()
        if self.feishu_supervisor is not None:
            await self.feishu_supervisor.start()
        if self.agent_workers is not None:
            await self.agent_workers.start()
        if self.delivery_workers is not None:
            await self.delivery_workers.start()

    async def close(self) -> None:
        """Stop work in order and attempt every resource cleanup step."""

        cleanup_errors: list[BaseException] = []

        async def attempt(operation: Callable[[], object]) -> None:
            try:
                result = operation()
                if isawaitable(result):
                    await result
            except BaseException as error:
                cleanup_errors.append(error)

        if self.wecom_supervisor is not None:
            await attempt(self.wecom_supervisor.stop_ingress)
        if self.feishu_supervisor is not None:
            await attempt(self.feishu_supervisor.stop_ingress)
        if self.agent_workers is not None:
            await attempt(self.agent_workers.stop_claiming)
            await attempt(self.node_heartbeat.begin_drain)
            await attempt(self.agent_workers.close)
        # Agent slots stop first so no new reply Outbox rows appear while the
        # delivery slots drain their last leases.
        if self.delivery_workers is not None:
            await attempt(self.delivery_workers.close)
        # Keep the provider connection alive until delivery slots release their
        # final leases, then stop accepting new long-connection callbacks.
        if self.wecom_supervisor is not None:
            await attempt(self.wecom_supervisor.close)
        if self.feishu_supervisor is not None:
            await attempt(self.feishu_supervisor.close)
        await attempt(self.node_heartbeat.close)
        close_runner = getattr(self.agent_runner, "close", None)
        if callable(close_runner):
            await attempt(close_runner)
        if self.session_cache is not None:
            await attempt(self.session_cache.close)
        await attempt(self.http_client.aclose)
        await attempt(self.telemetry.shutdown)

        if cleanup_errors:
            first_error = cleanup_errors[0]
            for additional_error in cleanup_errors[1:]:
                first_error.add_note(
                    f"additional cleanup failure: {type(additional_error).__name__}: "
                    f"{additional_error}")
            raise first_error


def _build_trpc_runner(
    settings: Settings,
    mcp: TenantMCPService,
    skills: BuiltinSkillCatalog,
) -> AgentRunner:
    """Create the single production Runner at the composition boundary."""

    return TRPCAgentRunner(settings, mcp=mcp, skills=skills)


def build_application_container(
    *,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    agent_runner_factory: Callable[
        [Settings, TenantMCPService, BuiltinSkillCatalog],
        AgentRunner,
    ] = _build_trpc_runner,
) -> ApplicationContainer:
    """Build the production graph; only Runner construction is replaceable."""

    if not isinstance(settings, Settings):
        raise TypeError("settings must be a Settings object")
    if not isinstance(session_factory, async_sessionmaker):
        raise TypeError("session_factory must be an async_sessionmaker")
    if not callable(agent_runner_factory):
        raise TypeError("agent_runner_factory must be callable")

    settings.validate_execution_backends({})
    app_settings = settings
    telemetry = PlatformTelemetry(
        service_name=app_settings.service_name,
        environment=app_settings.environment,
        node_role=app_settings.runtime_role,
        otlp_endpoint=(app_settings.otlp_http_endpoint if app_settings.telemetry_enabled else None),
    )
    composition = build_storage_composition(app_settings)
    tenant_secrets = TenantSecretStore(
        session_factory,
        app_settings.resolved_tenant_secret_master_key,
    )
    mcp = TenantMCPService(
        session_factory,
        tenant_secrets,
        private_allowed_hosts=app_settings.resolved_mcp_private_allowed_hosts,
    )
    skills = BuiltinSkillCatalog()
    knowledge = TenantKnowledgeService(
        session_factory,
        storage=composition.router,
        default_backends=app_settings.storage_profile.model_dump(mode="json"),
        embedding_model=app_settings.embedding.model_name,
    )
    channels = ChannelAdapterRegistry()
    config_provider = DatabaseAgentConfigProvider(app_settings, session_factory)
    wecom_transports = WeComTransportRegistry()
    default_storage = composition.router.resolve(
        BackendProfile.from_mapping(app_settings.storage_profile.model_dump(mode="json")))
    channel_media = (None if default_storage.artifact is None else ChannelMediaStore(
        default_storage.artifact,
        storage_router=composition.router,
        backend_profiles=config_provider,
    ))
    wecom_adapter = WeComChannelAdapter(wecom_transports, channel_media)
    feishu_transports = FeishuTransportRegistry()
    feishu_adapter = FeishuChannelAdapter(feishu_transports, channel_media)
    channels.register(wecom_adapter)
    channels.register(feishu_adapter)
    cache_url = app_settings.resolved_session_cache_url
    session_cache = (RedisSessionSnapshotCache.from_url(
        cache_url,
        ttl_seconds=app_settings.session_cache_ttl_seconds,
        max_events=app_settings.session_cache_max_events,
    ) if app_settings.worker_concurrency > 0 and cache_url is not None else None)
    approvals = ApprovalService(
        PostgreSQLApprovalStore(session_factory),
        ttl_seconds=app_settings.approval_ttl_seconds,
    )
    usage = PostgreSQLUsageRecorder(session_factory)
    # Every local Worker process owns a provider instance, while the shared root
    # makes a request workspace recoverable by either configured WorkNode.
    workspace = LocalWorkspaceProvider(
        app_settings.workspace_root,
        retention_seconds=app_settings.workspace_retention_seconds,
        cleanup_interval_seconds=app_settings.workspace_cleanup_interval_seconds,
    )
    approval_commands = ApprovalCommandProcessor(approvals)
    channel_identities = PostgreSQLChannelIdentityService(session_factory)
    runtime_runner = agent_runner_factory(app_settings, mcp, skills)
    if not callable(getattr(runtime_runner, "run", None)):
        raise TypeError("agent_runner_factory must return an AgentRunner")
    http_client = httpx.AsyncClient(timeout=10, follow_redirects=False)
    pipeline = build_local_agent_pipeline(
        settings=app_settings,
        storage=composition.router,
        runner=runtime_runner,
        config_provider=config_provider,
        telemetry=telemetry,
        approvals=approvals,
        usage_recorder=usage,
        knowledge_service=knowledge,
        tool_ledger=PostgreSQLToolLedger(session_factory),
        workspace_provider=workspace,
        mcp_service=mcp,
        http_client=http_client,
        session_cache=session_cache,
    )
    task_queue = PostgreSQLAgentTaskQueue(session_factory)
    workers = (None if app_settings.worker_concurrency == 0 else AgentWorkerService(
        queue=task_queue,
        pipeline=pipeline,
        node_id=app_settings.resolved_node_id,
        concurrency=app_settings.worker_concurrency,
        runtime=app_settings.agent_worker_runtime,
        telemetry=telemetry,
        recovery_store=PostgreSQLRunnerRecoveryStore(session_factory),
    ))
    delivery_types = app_settings.resolved_delivery_channel_types
    delivery_queue = PostgreSQLDeliveryTaskQueue(
        session_factory,
        allowed_channel_types=delivery_types,
    )
    delivery_workers = (None if not delivery_types or app_settings.delivery_concurrency == 0 else
                        DeliveryWorkerService(
                            queue=delivery_queue,
                            channels=channels,
                            node_id=app_settings.resolved_node_id,
                            concurrency=app_settings.delivery_concurrency,
                            runtime=app_settings.delivery_worker_runtime,
                            telemetry=telemetry,
                        ))
    node_registry = PostgreSQLRuntimeNodeRegistry(
        session_factory,
        stale_after_seconds=app_settings.node_stale_after_seconds,
    )
    worker_pool = PostgreSQLWorkerPoolStore(session_factory)
    node_role = app_settings.runtime_role
    if node_role == "api" and app_settings.worker_concurrency > 0:
        node_role = "api_worker"
    node_heartbeat = RuntimeNodeHeartbeatService(
        node_registry,
        node_id=app_settings.resolved_node_id,
        role=node_role,
        worker_concurrency=app_settings.worker_concurrency,
        heartbeat_interval_seconds=app_settings.node_heartbeat_interval_seconds,
    )
    wecom_messages = WeComMessageService(
        wecom_adapter,
        task_queue,
        telemetry,
        approval_commands,
        channel_identities,
    )
    wecom_supervisor = (WeComBindingSupervisor(
        session_factory,
        wecom_adapter,
        wecom_transports,
        wecom_messages,
        telemetry,
        client_factory=SDKWeComClientFactory(),
        poll_interval_seconds=app_settings.channel_reconcile_interval_seconds,
        secret_store=tenant_secrets,
    ) if app_settings.runtime_role == "channel" else None)
    feishu_messages = FeishuMessageService(
        feishu_adapter,
        task_queue,
        telemetry,
        approval_commands,
        channel_identities,
    )
    feishu_supervisor = (FeishuBindingSupervisor(
        session_factory,
        feishu_adapter,
        feishu_transports,
        feishu_messages,
        telemetry,
        client_factory=SDKFeishuClientFactory(),
        poll_interval_seconds=app_settings.channel_reconcile_interval_seconds,
        media_store=channel_media,
        secret_store=tenant_secrets,
    ) if app_settings.runtime_role == "channel" else None)
    return ApplicationContainer(
        channels=channels,
        storage_backends=composition.registry,
        storage_router=composition.router,
        storage_composition=composition,
        knowledge=knowledge,
        session_factory=session_factory,
        agent_pipeline=pipeline,
        agent_runner=runtime_runner,
        approvals=approvals,
        task_queue=task_queue,
        agent_workers=workers,
        delivery_workers=delivery_workers,
        delivery_queue=delivery_queue,
        node_registry=node_registry,
        worker_pool=worker_pool,
        node_heartbeat=node_heartbeat,
        wecom=wecom_messages,
        wecom_supervisor=wecom_supervisor,
        feishu=feishu_messages,
        feishu_supervisor=feishu_supervisor,
        telemetry=telemetry,
        tenant_secrets=tenant_secrets,
        workspace=workspace,
        mcp=mcp,
        skills=skills,
        session_cache=session_cache,
        http_client=http_client,
    )
