"""Composition roots for independently deployed Worker and delivery roles."""

from __future__ import annotations

import asyncio
import hmac
import logging
from collections.abc import Awaitable
from datetime import timedelta
from typing import cast

import httpx
from opentelemetry.sdk.trace import TracerProvider
from redis.asyncio import Redis
from sqlalchemy import text

from trpc_service.agent import AgentFactory, ExecutionLimits, TenantModelResolver
from trpc_service.config import Environment, Settings
from trpc_service.delivery import OutboxDispatcher, SqlChannelBindingStore
from trpc_service.delivery.contracts import DispatchState
from trpc_service.log import configure_logging
from trpc_service.projection import (
    EncryptedProjectionTextReader,
    ExplicitInstructionMemoryExtractor,
    ExtractiveWindowSummary,
    ProjectionOutcome,
    ProjectionWorker,
)
from trpc_service.reliability import ReliabilityRepository
from trpc_service.runtime.catalog import GovernedTenantCatalog, SqlActiveTenantCatalog
from trpc_service.runtime.event_store import (
    AsyncRedisClient,
    LocalEventObjectStore,
    RedisEventObjectStore,
    SqlEventObjectStore,
)
from trpc_service.runtime.supervisor import PollingSupervisor, PollTenant
from trpc_service.security import (
    EnvelopeCipher,
    EnvironmentSecretResolver,
    SecretResolutionError,
)
from trpc_service.storage import Database
from trpc_service.telemetry import configure_telemetry
from trpc_service.tenant import TenantConfigService
from trpc_service.worker import (
    ActiveTenantTurnResolver,
    EnvelopeEventCodec,
    EventObjectStore,
    TenantAgentExecutorFactory,
    WorkerOrchestrator,
    WorkerOutcome,
)

LOGGER = logging.getLogger(__name__)


class RuntimeConfigurationError(RuntimeError):
    """A process role lacks a real, safe production dependency."""


async def run_worker_role(settings: Settings, stop: asyncio.Event) -> None:
    """Build and supervise one stateless Worker process until shutdown."""

    _validate_worker_settings(settings)
    configure_logging(settings.log_level)
    telemetry = configure_telemetry(settings)
    database = Database(settings.database_url)
    redis_store: RedisEventObjectStore | None = None
    try:
        await _check_database(database)
        route_catalog = SqlActiveTenantCatalog(database.session_factory)
        # This also verifies that migrations created the public discovery table.
        await route_catalog.list_active_tenant_ids()

        secret_resolver = EnvironmentSecretResolver(settings.secret_env_allowlist)
        tenant_configs = TenantConfigService(database.session_factory)
        catalog = GovernedTenantCatalog(route_catalog, tenant_configs)
        tenant_ids = await catalog.list_active_tenant_ids()
        await _validate_active_worker_routes(
            tenant_ids,
            tenant_configs,
            settings,
            secret_resolver,
        )
        model_resolver = TenantModelResolver(
            provider=settings.model_provider,
            secret_resolver=secret_resolver,
            default_api_key=settings.model_api_key,
            base_url=str(settings.model_base_url) if settings.model_base_url is not None else None,
        )
        agent_factory = AgentFactory(model_resolver=model_resolver)
        executor_factory = TenantAgentExecutorFactory(
            agent_factory=agent_factory,
            limits=ExecutionLimits(
                max_llm_calls=settings.max_llm_calls,
                max_iterations=settings.max_iterations,
            ),
        )

        event_cipher = EnvelopeCipher(_derive_event_root(settings))
        event_store: EventObjectStore
        if settings.event_store_backend == "sql":
            event_store = SqlEventObjectStore(
                database,
                max_object_bytes=settings.event_object_max_bytes,
            )
        elif settings.event_store_backend == "redis":
            raw_redis = Redis.from_url(settings.redis_url, decode_responses=False)
            await cast(Awaitable[object], raw_redis.ping())
            redis_store = RedisEventObjectStore(
                cast(AsyncRedisClient, raw_redis),
                max_object_bytes=settings.event_object_max_bytes,
            )
            event_store = redis_store
        else:
            event_store = LocalEventObjectStore(
                settings.event_store_path,
                max_object_bytes=settings.event_object_max_bytes,
            )

        repository = ReliabilityRepository(database.session_factory)
        orchestrator = WorkerOrchestrator(
            port=repository,
            event_codec=EnvelopeEventCodec(cipher=event_cipher, store=event_store),
            tenant_resolver=ActiveTenantTurnResolver(tenant_configs),
            executor_factory=executor_factory,
            lease_ttl=timedelta(seconds=settings.lease_seconds),
            heartbeat_interval=timedelta(seconds=settings.heartbeat_seconds),
            max_attempts=settings.worker_max_attempts,
        )

        async def poll_tenant(tenant_id: str) -> bool:
            result = await orchestrator.run_once(
                tenant_id=tenant_id,
                worker_id=settings.worker_id,
            )
            LOGGER.info(
                "worker_poll_completed",
                extra={
                    "worker_outcome": result.outcome.value,
                    "run_id": result.run_id,
                    "inbox_id": result.inbox_id,
                    "attempt_no": result.attempt_no,
                    "error_type": result.error_type,
                },
            )
            if result.outcome is WorkerOutcome.IDLE:
                return False
            # A claim-backend failure has no run and must not create a hot loop.
            return result.run_id is not None

        supervisor = _supervisor(settings, "worker", catalog, poll_tenant)
        await supervisor.run(stop)
    finally:
        if redis_store is not None:
            await redis_store.aclose()
        await database.dispose()
        _shutdown_telemetry(telemetry)


async def run_dispatcher_role(settings: Settings, stop: asyncio.Event) -> None:
    """Build and supervise one independent durable Outbox delivery process."""

    configure_logging(settings.log_level)
    telemetry = configure_telemetry(settings)
    database = Database(settings.database_url)
    client = httpx.AsyncClient(
        follow_redirects=False,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    try:
        await _check_database(database)
        route_catalog = SqlActiveTenantCatalog(database.session_factory)
        await route_catalog.list_active_tenant_ids()
        secret_resolver = EnvironmentSecretResolver(settings.secret_env_allowlist)
        tenant_configs = TenantConfigService(database.session_factory)
        catalog = GovernedTenantCatalog(route_catalog, tenant_configs)
        tenant_ids = await catalog.list_active_tenant_ids()
        await _validate_active_dispatcher_routes(
            tenant_ids,
            tenant_configs,
            secret_resolver,
        )
        repository = ReliabilityRepository(database.session_factory)
        dispatcher = OutboxDispatcher(
            repository,
            SqlChannelBindingStore(database.session_factory),
            secret_resolver,
            EnvelopeCipher(settings.secret_key.get_secret_value()),
            client,
            dispatcher_id=settings.dispatcher_id,
            telegram_transport_attempts=settings.telegram_transport_attempts,
        )

        async def poll_tenant(tenant_id: str) -> bool:
            report = await dispatcher.dispatch_once(tenant_id)
            LOGGER.info(
                "delivery_poll_completed",
                extra={
                    "delivery_state": report.state.value,
                    "outbox_id": report.outbox_id,
                    "delivery_outcome": (
                        report.outcome.value if report.outcome is not None else None
                    ),
                    "error_type": report.error_type,
                    "persisted": report.persisted,
                },
            )
            return report.state is not DispatchState.NO_WORK

        supervisor = _supervisor(settings, "dispatcher", catalog, poll_tenant)
        await supervisor.run(stop)
    finally:
        await client.aclose()
        await database.dispose()
        _shutdown_telemetry(telemetry)


async def run_projector_role(settings: Settings, stop: asyncio.Event) -> None:
    """Build and supervise the durable Summary/Memory projection process."""

    _validate_projector_settings(settings)
    configure_logging(settings.log_level)
    telemetry = configure_telemetry(settings)
    database = Database(settings.database_url)
    redis_store: RedisEventObjectStore | None = None
    try:
        await _check_database(database)
        route_catalog = SqlActiveTenantCatalog(database.session_factory)
        await route_catalog.list_active_tenant_ids()
        tenant_configs = TenantConfigService(database.session_factory)
        catalog = GovernedTenantCatalog(route_catalog, tenant_configs)

        event_store: EventObjectStore
        if settings.event_store_backend == "sql":
            event_store = SqlEventObjectStore(
                database,
                max_object_bytes=settings.event_object_max_bytes,
            )
        elif settings.event_store_backend == "redis":
            raw_redis = Redis.from_url(settings.redis_url, decode_responses=False)
            await cast(Awaitable[object], raw_redis.ping())
            redis_store = RedisEventObjectStore(
                cast(AsyncRedisClient, raw_redis),
                max_object_bytes=settings.event_object_max_bytes,
            )
            event_store = redis_store
        else:
            event_store = LocalEventObjectStore(
                settings.event_store_path,
                max_object_bytes=settings.event_object_max_bytes,
            )

        codec = EnvelopeEventCodec(
            cipher=EnvelopeCipher(_derive_event_root(settings)),
            store=event_store,
        )
        reader = EncryptedProjectionTextReader(codec)
        projector = ProjectionWorker(
            port=ReliabilityRepository(database.session_factory),
            summarizer=ExtractiveWindowSummary(reader),
            memory_extractor=ExplicitInstructionMemoryExtractor(reader),
            lease_ttl=timedelta(seconds=settings.lease_seconds),
            heartbeat_interval=timedelta(seconds=settings.heartbeat_seconds),
            max_attempts=settings.worker_max_attempts,
        )

        async def poll_tenant(tenant_id: str) -> bool:
            report = await projector.process_once(tenant_id, settings.projector_id)
            LOGGER.info(
                "projection_poll_completed",
                extra={
                    "projection_outcome": report.outcome.value,
                    "job_id": report.job_id,
                    "attempt_no": report.attempt_no,
                    "error_type": report.error_type,
                },
            )
            return report.outcome is not ProjectionOutcome.IDLE

        supervisor = _supervisor(settings, "projector", catalog, poll_tenant)
        await supervisor.run(stop)
    finally:
        if redis_store is not None:
            await redis_store.aclose()
        await database.dispose()
        _shutdown_telemetry(telemetry)


def _supervisor(
    settings: Settings,
    role: str,
    catalog: GovernedTenantCatalog,
    poll_tenant: PollTenant,
) -> PollingSupervisor:
    return PollingSupervisor(
        role=role,
        catalog=catalog,
        poll_tenant=poll_tenant,
        catalog_refresh_seconds=settings.catalog_refresh_seconds,
        idle_backoff_initial_seconds=settings.idle_backoff_initial_seconds,
        idle_backoff_max_seconds=settings.idle_backoff_max_seconds,
    )


def _validate_worker_settings(settings: Settings) -> None:
    if settings.model_provider == "mock":
        raise RuntimeConfigurationError(
            "Worker requires an explicit real model provider; mock is test-only"
        )
    if settings.model_provider == "openai-compatible" and settings.model_base_url is None:
        raise RuntimeConfigurationError("openai-compatible Worker requires an HTTPS model_base_url")
    if settings.env is Environment.PRODUCTION and settings.event_store_backend != "sql":
        raise RuntimeConfigurationError(
            "production Worker requires the authoritative SQL encrypted event store"
        )


def _validate_projector_settings(settings: Settings) -> None:
    if settings.env is Environment.PRODUCTION and settings.event_store_backend != "sql":
        raise RuntimeConfigurationError(
            "production Projector requires the authoritative SQL encrypted event store"
        )


async def _validate_active_worker_routes(
    tenant_ids: tuple[str, ...],
    tenant_configs: TenantConfigService,
    settings: Settings,
    secrets: EnvironmentSecretResolver,
) -> None:
    """Reject current active routes that cannot resolve a real model credential."""

    for tenant_id in tenant_ids:
        try:
            spec = await tenant_configs.load_active(tenant_id)
        except Exception:
            raise RuntimeConfigurationError(
                "an active tenant route has no readable configuration"
            ) from None
        if spec.status != "active":
            continue
        for app in spec.apps:
            if app.model.provider.strip().casefold() != settings.model_provider:
                raise RuntimeConfigurationError(
                    "an active app requests a model provider outside the Worker route"
                )
            if app.model.api_key_ref is None:
                if not settings.model_api_key.get_secret_value():
                    raise RuntimeConfigurationError("an active app has no model credential")
                continue
            try:
                secrets.resolve(app.model.api_key_ref)
            except SecretResolutionError:
                raise RuntimeConfigurationError(
                    "an active app model credential is unavailable"
                ) from None


async def _validate_active_dispatcher_routes(
    tenant_ids: tuple[str, ...],
    tenant_configs: TenantConfigService,
    secrets: EnvironmentSecretResolver,
) -> None:
    """Resolve every active Telegram sender credential before polling Outbox."""

    for tenant_id in tenant_ids:
        try:
            spec = await tenant_configs.load_active(tenant_id)
        except Exception:
            raise RuntimeConfigurationError(
                "an active tenant route has no readable configuration"
            ) from None
        for channel in spec.channels:
            if not channel.enabled or channel.channel.value != "telegram":
                continue
            try:
                secrets.resolve(channel.secret_refs["bot_token"])
            except (KeyError, SecretResolutionError):
                raise RuntimeConfigurationError(
                    "an active Telegram delivery credential is unavailable"
                ) from None


async def _check_database(database: Database) -> None:
    try:
        async with database.session_factory() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        raise RuntimeConfigurationError("runtime database is unavailable") from None


def _derive_event_root(settings: Settings) -> bytes:
    """Domain-separate event envelopes from short-lived reply credentials."""

    return hmac.digest(
        settings.secret_key.get_secret_value().encode(),
        b"trpc-agent-service/sdk-event-root/v1",
        "sha256",
    )


def _shutdown_telemetry(provider: TracerProvider | None) -> None:
    if provider is not None:
        provider.shutdown()
