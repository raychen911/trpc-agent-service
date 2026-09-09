# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Application composition root."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from trpc_service.agent import AgentWorker
from trpc_service.agent import TenantRuntimeManager
from trpc_service.agent import TenantRuntimeFactory
from trpc_service.channels import ChannelAdapter
from trpc_service.channels import AibotWeComClient
from trpc_service.channels import WeComChannelAdapter
from trpc_service.channels import WeComChannelRuntime
from trpc_service.channels.customer_service import CallbackCrypto
from trpc_service.channels.customer_service import CustomerServiceAdapter
from trpc_service.channels.customer_service import HttpCustomerServiceClient
from trpc_service.channels import TelegramChannelAdapter
from trpc_service.config import ServiceSettings
from trpc_service.config import ServiceRole
from trpc_service.config import TenantConfig
from trpc_service.config import ChannelType
from trpc_service.config import SecretProviderRegistry
from trpc_service.config import BackendType
from trpc_service.gateway.idempotency import InMemoryIdempotencyStore
from trpc_service.gateway.idempotency import RedisIdempotencyStore
from trpc_service.gateway.dispatcher import AgentTaskProcessor
from trpc_service.gateway.dispatcher import DeliveryWorker
from trpc_service.gateway.outbox import InMemoryOutboxStore
from trpc_service.gateway.outbox import PostgresOutboxStore
from trpc_service.gateway.queue import InMemoryAgentTaskQueue
from trpc_service.gateway.queue import AgentTaskEnvelope
from trpc_service.gateway.queue import RedisStreamAgentTaskQueue
from trpc_service.gateway.requests import PostgresRequestStore
from trpc_service.gateway.requests import InMemoryRequestStore
from trpc_service.gateway.repair import RequestRepairService
from trpc_service.gateway.ordering import RedisOrderingStore
from trpc_service.gateway.service import GatewayService
from trpc_service.log import LoggingAuditSink
from trpc_service.log import PostgresAuditSink
from trpc_service.metrics import MetricsRegistry
from trpc_service.metrics import configure_otlp_tracing
from trpc_service.storage import InMemorySessionExecutionGuard
from trpc_service.storage import RedisSessionExecutionGuard
from trpc_service.storage import StorageProviderFactory
from trpc_service.storage.fencing import PostgresExecutionGuard
from trpc_service.tenant import InMemoryBudgetLedger
from trpc_service.tenant import InMemoryTenantRegistry
from trpc_service.tenant import TenantConfigurationService
from trpc_service.tenant import TenantRegistry
from trpc_service.tenant import PostgresTenantRegistry
from trpc_service.tenant import RedisBudgetLedger
from trpc_service.tenant import BudgetBackendUnavailableError
from trpc_service.tenant import PostgresUsageLedger
from trpc_service.tenant import TenantPolicyEnforcer
from trpc_service.tenant import TenantNotFoundError
from trpc_service.tenant import InMemoryApprovalStore
from trpc_service.tenant import PostgresApprovalStore
from trpc_service.resources import InMemoryArtifactStore
from trpc_service.resources import InMemoryKnowledgeProvider
from trpc_service.resources import PostgresLocalArtifactStore
from trpc_service.resources import PostgresKnowledgeProvider
from trpc_service.resources.attachments import AttachmentIngestor
from trpc_service.migration import InMemoryMigrationStore
from trpc_service.migration import MigrationCoordinator
from trpc_service.migration import PostgresMigrationStore
from trpc_service.migration.control import PostgresMigrationControlStore
from trpc_service.migration.provider import RedisPostgresMigrationProvider
from trpc_service.tool import ToolRegistry
from trpc_service.tool.execution import PostgresToolExecutionStore
from trpc_service.channels.customer_store import InMemoryCustomerStore, PostgresCustomerStore
from trpc_service.channels.customer_runtime import CustomerServiceRuntime
from trpc_service.gateway.service import DuplicateRequestError
from datetime import date


@dataclass(slots=True)
class ServiceContainer:
    """Long-lived process services; request handlers contain no global state."""

    settings: ServiceSettings
    registry: TenantRegistry
    runtimes: TenantRuntimeManager
    gateway: GatewayService
    metrics: MetricsRegistry
    policy: TenantPolicyEnforcer
    budgets: Any
    guard: Any
    idempotency: Any
    channel_adapters: dict[str, ChannelAdapter]
    audit: Any
    queue: Any = None
    outbox: Any = None
    task_processor: AgentTaskProcessor | None = None
    request_repair: RequestRepairService | None = None
    delivery_worker: DeliveryWorker | None = None
    configuration: TenantConfigurationService | None = None
    artifacts: Any = None
    knowledge: Any = None
    approvals: Any = None
    migrations: Any = None
    postgres_pool: Any = None
    channel_runtimes: list[Any] = field(default_factory=list)
    telemetry_provider: Any = None
    secrets: Any = None
    usage_ledger: Any = None
    ordering: Any = None
    customer_store: Any = None
    customer_runtimes: dict = field(default_factory=dict)
    migration_control: Any = None
    attachment_ingestor: Any = None
    im_simulator: Any = None
    _tasks: list[asyncio.Task[Any]] = field(default_factory=list)
    _stopping: asyncio.Event = field(default_factory=asyncio.Event)

    async def start(self) -> None:
        """Start only the background loops assigned to this process role."""
        if self._tasks:
            return
        self._stopping.clear()
        await self.setup_customer_channels()
        if ServiceRole.WORKER in self.settings.roles and self.task_processor:
            self._tasks.append(asyncio.create_task(self._worker_loop(), name="agent-task-worker"))
            if self.customer_runtimes:
                self._tasks.append(asyncio.create_task(self._customer_loop(), name="customer-service-sync"))
        if self.settings.roles & {ServiceRole.DELIVERY, ServiceRole.WECOM} and self.delivery_worker:
            self._tasks.append(asyncio.create_task(self._delivery_loop(), name="outbox-delivery"))
        if ServiceRole.WECOM in self.settings.roles:
            for index, runtime in enumerate(self.channel_runtimes):
                self._tasks.append(asyncio.create_task(self._channel_loop(runtime), name=f"wecom-channel-{index}"))

    async def setup_customer_channels(self):
        if self.customer_store is None:
            self.customer_store = InMemoryCustomerStore()
        for tenant in await self.registry.list_active():
            for binding in tenant.channels:
                if binding.channel != ChannelType.WECOM_KF or not binding.enabled:
                    continue
                if binding.binding_id in self.customer_runtimes:
                    continue
                adapter = self.channel_adapters.get(binding.binding_id)
                if adapter is None:
                    secret = await self.secrets.resolve(binding.secret_ref)
                    token = await self.secrets.resolve(binding.webhook_secret_ref)
                    aes_key = await self.secrets.resolve(binding.encoding_aes_key_ref)
                    client = HttpCustomerServiceClient(binding.corp_id, secret)
                    adapter = CustomerServiceAdapter(binding, client, self.customer_store,
                                                     CallbackCrypto(token, aes_key, binding.corp_id), self.artifacts)
                    self.channel_adapters[binding.binding_id] = adapter
                self.customer_runtimes[binding.binding_id] = CustomerServiceRuntime(adapter, self.customer_store,
                                                                                    self.admit_channel, self.audit,
                                                                                    tenant.tenant_id)
        # Keep diagnostics, demos and Delivery traversal in the documented
        # order without changing binding semantics.
        rank = {ChannelType.WECOM: 0, ChannelType.WECOM_KF: 1, ChannelType.TELEGRAM: 2}
        binding_types = {
            binding.binding_id: binding.channel
            for tenant in await self.registry.list_active()
            for binding in tenant.channels
        }
        ordered = sorted(self.channel_adapters.items(),
                         key=lambda item: (rank.get(binding_types.get(item[0]), 99), item[0]))
        self.channel_adapters.clear()
        self.channel_adapters.update(ordered)

    async def admit_channel(self, normalized):
        tenant, binding = await self.registry.resolve_binding(normalized.binding_id)
        self.policy.validate_inbound(tenant, binding, normalized)
        if normalized.attachments and self.attachment_ingestor is not None:
            normalized = await self.attachment_ingestor.materialize_inbound(tenant.tenant_id, binding.app_id,
                                                                            normalized)
        try:
            request, key = await self.gateway.inbound_request(normalized)
        except DuplicateRequestError as duplicate:
            return duplicate.request_id
        app = await self.registry.get_app(tenant.tenant_id, request.app_id, request.config_version)
        estimated_input, estimated_output = max(1, len(request.text) // 4), app.runtime.estimated_output_tokens
        cost = (estimated_input * app.model.input_cost_per_million_usd +
                estimated_output * app.model.output_cost_per_million_usd) / 1_000_000
        reserve = getattr(self.budgets, "reserve", None)
        try:
            if reserve:
                await reserve(tenant,
                              input_tokens=estimated_input,
                              output_tokens=estimated_output,
                              cost_usd=cost,
                              request_id=request.request_id)
                request.metadata["budget_reservation"] = {
                    "input_tokens": estimated_input,
                    "output_tokens": estimated_output,
                    "cost_usd": cost,
                    "day": date.today().isoformat()
                }
            else:
                await self.budgets.reserve_request(tenant)
        except Exception as error:
            if isinstance(error, BudgetBackendUnavailableError):
                await self.gateway.mark_retryable(request, "budget_backend_unavailable")
            else:
                await self.gateway.mark_failed(request, "channel_admission_failed")
            raise
        await self.gateway.prepare(request)
        await self.queue.enqueue(AgentTaskEnvelope(request=request, idempotency_key=key))
        await self.gateway.mark_queued(request)
        return request.request_id

    async def _customer_loop(self):
        while not self._stopping.is_set():
            for runtime in self.customer_runtimes.values():
                try:
                    await runtime.step()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.metrics.increment("trpc_service_background_errors_total", role="customer_service")
            await self._pause(.5)

    async def _channel_loop(self, runtime) -> None:
        while not self._stopping.is_set():
            try:
                await runtime.run(self._stopping)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.metrics.increment("trpc_service_background_errors_total", role="wecom")
                await self._pause(1)

    async def _pause(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def readiness(self) -> dict[str, Any]:
        """Check dependencies used by this role; failures make the process unready."""
        checks: dict[str, bool] = {}
        checks["background_tasks"] = all(not task.done() for task in self._tasks)
        tenants = await self.registry.list_active()
        checks["tenant_registry"] = bool(tenants)
        if self.settings.environment != "development":
            if self.postgres_pool is not None:
                checks["postgres"] = bool(await self.postgres_pool.fetchval("SELECT 1"))
            if ServiceRole.GATEWAY in self.settings.roles:
                checks["idempotency"] = bool(await self.idempotency.ping())
                checks["queue"] = bool(await self.queue.ping())
                ping_budget = getattr(self.budgets, "ping", None)
                if ping_budget:
                    checks["budget"] = bool(await ping_budget())
            if ServiceRole.WORKER in self.settings.roles:
                checks["queue"] = bool(await self.queue.ping())
                checks["session_guard"] = bool(await self.guard.ping())
                ping_budget = getattr(self.budgets, "ping", None)
                if ping_budget:
                    checks["budget"] = bool(await ping_budget())
            if ServiceRole.DELIVERY in self.settings.roles:
                checks["outbox"] = bool(await self.outbox.ping())
        return {
            "status": "ready" if all(checks.values()) else "not_ready",
            "roles": sorted(role.value for role in self.settings.roles),
            "checks": checks,
            "active_tenants": len(tenants)
        }

    async def _worker_loop(self) -> None:
        assert self.task_processor is not None
        while not self._stopping.is_set():
            iteration = asyncio.create_task(self._worker_iteration(), name="agent-task-iteration")
            try:
                # A lost Session lease cancels the task that owns the SDK Run.
                # Keep that cancellation inside this per-iteration child task so
                # one abandoned Run cannot terminate the long-lived consumer.
                await asyncio.shield(iteration)
            except asyncio.CancelledError:
                if self._stopping.is_set():
                    iteration.cancel()
                    await asyncio.gather(iteration, return_exceptions=True)
                    raise
                if iteration.cancelled():
                    self.metrics.increment("trpc_service_background_recoveries_total",
                                           role="worker",
                                           reason="execution_cancelled")
                    await self._pause(1)
                    continue
                # An unexpected cancellation of the loop itself must not leave
                # an untracked model/Tool execution running in the background.
                iteration.cancel()
                await asyncio.gather(iteration, return_exceptions=True)
                raise
            except Exception:
                self.metrics.increment("trpc_service_background_errors_total", role="worker")
                await self._pause(1)

    async def _worker_iteration(self) -> None:
        """Run one isolated consume cycle, preferring the existing Stream item."""
        assert self.task_processor is not None
        # A pending Redis Stream entry is the primary recovery record. Reclaim
        # it before PostgreSQL repair can enqueue a second copy of the request.
        await self.task_processor.reclaim_stale(idle_ms=300000, limit=1)
        if self.request_repair:
            await self.request_repair.repair_stale(older_than_seconds=300, limit=20)
        await self.task_processor.process_one(timeout_seconds=1.0)

    async def _delivery_loop(self) -> None:
        assert self.delivery_worker is not None
        while not self._stopping.is_set():
            try:
                recover = getattr(self.outbox, "recover_expired", None)
                if recover:
                    await recover()
                delivered = await self.delivery_worker.deliver_due(limit=1)
                if not delivered:
                    await self._pause(0.5)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.metrics.increment("trpc_service_background_errors_total", role="delivery")
                await self._pause(1)

    async def close(self) -> None:
        self._stopping.set()
        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=15)
            for task in pending:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        # The consume loop is now stopped, so no new Stream entry can become
        # pending for this identity between the safety check and DELCONSUMER.
        # If an interrupted task is still pending, keep the Consumer metadata:
        # another Worker must recover that entry through XAUTOCLAIM.
        if ServiceRole.WORKER in self.settings.roles and self.task_processor is not None:
            unregister = getattr(self.queue, "unregister_consumer", None)
            if unregister:
                try:
                    removed = await unregister(self.task_processor.consumer)
                    if not removed:
                        self.metrics.increment("trpc_service_worker_consumer_cleanup_total", result="pending")
                    else:
                        self.metrics.increment("trpc_service_worker_consumer_cleanup_total", result="removed")
                except Exception:
                    # Redis may itself be the reason the process is stopping.
                    # Cleanup is best effort and must not skip other resources.
                    self.metrics.increment("trpc_service_worker_consumer_cleanup_total", result="error")
        for adapter in self.channel_adapters.values():
            await adapter.close()
        if self.im_simulator is not None:
            await self.im_simulator.close()
        for runtime in self.channel_runtimes:
            await runtime.close()
        await self.runtimes.close()
        close_guard = getattr(self.guard, "close", None)
        if close_guard:
            await close_guard()
        close_idempotency = getattr(self.idempotency, "close", None)
        if close_idempotency:
            await close_idempotency()
        for resource in (self.queue, self.outbox, self.ordering):
            close = getattr(resource, "close", None)
            if close:
                await close()
        close_budget = getattr(self.budgets, "close", None)
        if close_budget:
            await close_budget()
        if self.postgres_pool is not None:
            await self.postgres_pool.close()
        if self.telemetry_provider is not None:
            self.telemetry_provider.shutdown()


def build_container(settings: ServiceSettings,
                    configs: list[TenantConfig],
                    *,
                    channel_adapters: dict[str, ChannelAdapter] | None = None,
                    runtime_factory_override=None) -> ServiceContainer:
    """Build default local or production-oriented process dependencies."""
    if settings.environment != "development":
        raise ValueError("production requires await build_production_container; in-memory fallback is forbidden")
    channel_adapters = channel_adapters if channel_adapters is not None else {}
    registry = InMemoryTenantRegistry(configs)
    knowledge = InMemoryKnowledgeProvider()
    artifacts = InMemoryArtifactStore()
    metrics = MetricsRegistry()
    audit = LoggingAuditSink()
    if runtime_factory_override is None:
        runtime_factory = TenantRuntimeFactory(storage_factory=StorageProviderFactory(metrics=metrics),
                                               tool_registry=ToolRegistry(knowledge_provider=knowledge,
                                                                          metrics=metrics,
                                                                          audit=audit),
                                               artifact_store=artifacts)
    elif callable(runtime_factory_override):
        runtime_factory = runtime_factory_override(artifacts, knowledge, metrics, audit)
    else:
        runtime_factory = runtime_factory_override
    runtimes = TenantRuntimeManager(registry, runtime_factory)
    if settings.environment == "development" or not configs:
        guard = InMemorySessionExecutionGuard()
        idempotency = InMemoryIdempotencyStore()
    else:
        redis_url = settings.redis_url
        guard = RedisSessionExecutionGuard(redis_url)
        from trpc_service.gateway.idempotency import RedisIdempotencyStore
        idempotency = RedisIdempotencyStore(redis_url)
    worker = AgentWorker(runtimes, guard)
    requests = InMemoryRequestStore()
    gateway = GatewayService(registry, worker, idempotency, requests)
    queue = InMemoryAgentTaskQueue()
    outbox = InMemoryOutboxStore()
    attachment_ingestor = AttachmentIngestor(artifacts, channel_adapters)
    processor = AgentTaskProcessor(queue,
                                   gateway,
                                   outbox,
                                   consumer=settings.worker_id or "local-worker",
                                   metrics=metrics,
                                   attachments=attachment_ingestor)
    delivery = DeliveryWorker(outbox, channel_adapters, metrics)

    async def invalidate(tenant_id: str, keep_version: int | None) -> None:
        await runtimes.invalidate(tenant_id, keep_version=keep_version)

    configuration = TenantConfigurationService(registry,
                                               invalidate=invalidate,
                                               validate=runtime_factory.validate,
                                               audit=audit)
    return ServiceContainer(
        settings=settings,
        registry=registry,
        runtimes=runtimes,
        gateway=gateway,
        metrics=metrics,
        policy=TenantPolicyEnforcer(),
        budgets=InMemoryBudgetLedger(),
        guard=guard,
        idempotency=idempotency,
        channel_adapters=channel_adapters,
        audit=audit,
        queue=queue,
        outbox=outbox,
        task_processor=processor,
        request_repair=RequestRepairService(requests, queue),
        delivery_worker=delivery,
        configuration=configuration,
        artifacts=artifacts,
        knowledge=knowledge,
        approvals=InMemoryApprovalStore(),
        migrations=MigrationCoordinator(InMemoryMigrationStore()),
        secrets=SecretProviderRegistry(),
        attachment_ingestor=attachment_ingestor,
    )


async def build_production_container(settings: ServiceSettings,
                                     configs: list[TenantConfig],
                                     *,
                                     channel_adapters: dict[str, ChannelAdapter] | None = None) -> ServiceContainer:
    """Build production state adapters without an implicit in-memory fallback."""
    if settings.environment == "development":
        raise ValueError("production container requires a non-development environment")
    for config in configs:
        if BackendType.MEMORY in {config.storage.session, config.storage.memory}:
            raise ValueError("production requires shared Session and Memory backends")
    try:
        import asyncpg
    except ImportError as error:
        raise RuntimeError("install PostgreSQL support with: pip install -e '.[postgres]'") from error

    pool = await asyncpg.create_pool(settings.postgres_url, min_size=1, max_size=10, command_timeout=5)
    metrics = MetricsRegistry()
    audit = PostgresAuditSink(pool)
    registry = PostgresTenantRegistry(pool)
    migration_control = PostgresMigrationControlStore(pool)
    knowledge = PostgresKnowledgeProvider(pool)
    artifacts = PostgresLocalArtifactStore("data/artifacts", pool)
    runtime_factory = TenantRuntimeFactory(storage_factory=StorageProviderFactory(metrics=metrics),
                                           secret_resolver=SecretProviderRegistry(),
                                           tool_registry=ToolRegistry(execution_store=PostgresToolExecutionStore(pool),
                                                                      knowledge_provider=knowledge,
                                                                      metrics=metrics,
                                                                      audit=audit),
                                           artifact_store=artifacts,
                                           migration_control=migration_control)

    async def validate_production(config: TenantConfig) -> None:
        if BackendType.MEMORY in {config.storage.session, config.storage.memory}:
            raise ValueError("production requires shared Session and Memory backends")
        if BackendType.SQL in {config.storage.session, config.storage.memory
                               } and not config.storage.sql_url.startswith("postgresql"):
            raise ValueError("production fenced SQL storage requires PostgreSQL")
        for binding in config.channels:
            if binding.channel == ChannelType.WECOM_KF and binding.enabled and not all(
                (binding.corp_id, binding.open_kfid, binding.secret_ref, binding.webhook_secret_ref,
                 binding.encoding_aes_key_ref)):
                raise ValueError("enabled customer-service bindings require account and secret references")
        for app in config.apps.values():
            if not app.runtime.enable_post_turn_processing or app.runtime.defer_post_turn_processing:
                raise ValueError("production requires synchronous post-turn processing")
        await runtime_factory.validate(config)

    for config in configs:
        try:
            await registry.get(config.tenant_id, config.version)
        except TenantNotFoundError:
            await validate_production(config)
            try:
                await registry.publish(config)
            except (ValueError, asyncpg.UniqueViolationError):
                existing = await registry.get(config.tenant_id, config.version)
                if existing.model_dump() != config.model_dump():
                    raise ValueError("bootstrap configuration differs from the immutable stored version") from None
    for active in await registry.list_active():
        if BackendType.MEMORY in {active.storage.session, active.storage.memory}:
            await pool.close()
            raise ValueError("stored production configuration uses an in-memory backend")
        await validate_production(active)
    runtimes = TenantRuntimeManager(registry, runtime_factory, migration_control=migration_control)
    guard = PostgresExecutionGuard(pool=pool, control=True)
    idempotency = RedisIdempotencyStore(settings.redis_url)
    ordering = RedisOrderingStore(settings.redis_url)
    requests = PostgresRequestStore(pool)
    gateway = GatewayService(registry,
                             AgentWorker(runtimes, guard),
                             idempotency,
                             requests,
                             ordering,
                             migration_routes=migration_control)
    queue = RedisStreamAgentTaskQueue(settings.redis_url)
    outbox = PostgresOutboxStore(pool, worker_id=settings.worker_id)
    adapters = channel_adapters or {}
    budget = RedisBudgetLedger(settings.redis_url)
    usage_ledger = PostgresUsageLedger(pool)

    async def invalidate(tenant_id: str, keep_version: int | None) -> None:
        await runtimes.invalidate(tenant_id, keep_version=keep_version)

    migration_provider = RedisPostgresMigrationProvider(registry, pool, migration_control, metrics=metrics, audit=audit)
    attachment_ingestor = AttachmentIngestor(artifacts, adapters)
    container = ServiceContainer(
        settings=settings,
        registry=registry,
        runtimes=runtimes,
        gateway=gateway,
        metrics=metrics,
        policy=TenantPolicyEnforcer(),
        budgets=budget,
        guard=guard,
        idempotency=idempotency,
        channel_adapters=adapters,
        audit=audit,
        queue=queue,
        outbox=outbox,
        task_processor=AgentTaskProcessor(queue,
                                          gateway,
                                          outbox,
                                          consumer=settings.worker_id or "production-worker",
                                          metrics=metrics,
                                          usage_ledger=usage_ledger,
                                          registry=registry,
                                          budget=budget,
                                          attachments=attachment_ingestor),
        request_repair=RequestRepairService(requests, queue),
        delivery_worker=DeliveryWorker(outbox, adapters, metrics),
        configuration=TenantConfigurationService(registry,
                                                 invalidate=invalidate,
                                                 validate=validate_production,
                                                 audit=audit),
        artifacts=artifacts,
        knowledge=knowledge,
        approvals=PostgresApprovalStore(pool),
        migrations=MigrationCoordinator(PostgresMigrationStore(pool), migration_provider.steps),
        postgres_pool=pool,
        telemetry_provider=configure_otlp_tracing(settings.otlp_endpoint),
        secrets=SecretProviderRegistry(),
        usage_ledger=usage_ledger,
        ordering=ordering,
        customer_store=PostgresCustomerStore(pool),
        migration_control=migration_control,
        attachment_ingestor=attachment_ingestor,
    )
    if channel_adapters is None:
        resolver = container.secrets
        for tenant in await registry.list_active():
            for binding in tenant.channels:
                if not binding.enabled:
                    continue
                if binding.channel == ChannelType.WECOM:
                    if ServiceRole.WECOM not in settings.roles:
                        continue
                    secret = await resolver.resolve(binding.secret_ref) if binding.secret_ref else ""
                    bot_id = binding.external_account_id or str(binding.options.get("bot_id", ""))
                    client = AibotWeComClient(bot_id, secret)

                    async def sender(conversation_id: str, text: str, reply_to: str, *, _client=client) -> str:
                        del reply_to
                        return await _client.send_message(conversation_id, text)

                    adapter = WeComChannelAdapter(sender,
                                                  client.reply_stream,
                                                  expected_bot_id=bot_id,
                                                  downloader=client.download_file)
                    adapters[binding.binding_id] = adapter

                    async def on_frame(frame: dict[str, Any], *, _binding=binding, _adapter=adapter) -> None:
                        normalized = await _adapter.normalize(_binding.binding_id, frame, {})
                        await container.admit_channel(normalized)

                    runtime = WeComChannelRuntime(binding.binding_id, client, guard, on_frame)
                    adapter.available = lambda _runtime=runtime: _runtime.owned
                    container.channel_runtimes.append(runtime)
                elif binding.channel == ChannelType.TELEGRAM:
                    token = await resolver.resolve(binding.secret_ref) if binding.secret_ref else ""
                    webhook_secret = (await resolver.resolve(binding.webhook_secret_ref)
                                      if binding.webhook_secret_ref else "")
                    adapter = TelegramChannelAdapter(token, webhook_secret, artifacts=artifacts)
                    adapters[binding.binding_id] = adapter
    return container
