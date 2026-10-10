"""Single-image command line entry point."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import inspect
import json
import os
from collections.abc import Mapping
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

import typer

from trpc_service.config import (
    RUNTIME_DATABASE_ROLE,
    WORKER_DATABASE_ROLE,
    Environment,
    LocalSecretProvider,
    Role,
    SchedulerVersion,
    ServiceSettings,
)
from trpc_service.log import configure_logging
from trpc_service.version import __version__

if TYPE_CHECKING:
    from trpc_service.storage.services import (
        RegisteredTenantServiceBundle,
        TenantServiceFactory,
    )

app = typer.Typer(
    name="trpc-service",
    help="Multi-tenant production runtime for tRPC-Agent-Python.",
    no_args_is_help=True,
    invoke_without_command=True,
)


# Roles that run a process-wide queue/recovery loop must not use the tenant
# runtime account.  The SQL functions are SECURITY DEFINER, but the caller
# still needs an explicit EXECUTE grant on the dedicated worker role.
_WORKER_DATABASE_ROLES = frozenset(
    {
        Role.WORKER,
        Role.OUTBOX_DISPATCHER,
        Role.CHANNEL_DISPATCHER,
        Role.POST_TURN_PROJECTOR,
        Role.WECOM_CONNECTOR,
        Role.SESSION_RECOVERY,
        Role.ARTIFACT_GC,
    }
)
_GLOBAL_WORKER_FUNCTIONS = (
    "public.list_channel_bindings(text)",
    "public.claim_outbox_events(text,text,integer,integer)",
    "public.sweep_expired_session_leases(integer)",
    "public.schedule_session_mailbox_retries(integer)",
    "public.reconcile_session_mailboxes(integer)",
    "public.reconcile_session_mailboxes_v2(integer,integer)",
)
_DATABASE_FUNCTIONS: dict[Role, tuple[str, ...]] = {
    Role.GATEWAY: ("public.resolve_channel_binding(text)",),
    # Worker is the dedicated process-wide identity.  It receives the full
    # audited function set even when a particular worker binary only uses a
    # subset, keeping role grants stable across rolling releases.
    Role.WORKER: (
        "public.resolve_channel_binding(text)",
        *_GLOBAL_WORKER_FUNCTIONS,
    ),
    Role.OUTBOX_DISPATCHER: ("public.claim_outbox_events(text,text,integer,integer)",),
    Role.CHANNEL_DISPATCHER: (
        "public.claim_outbox_events(text,text,integer,integer)",
        "public.resolve_channel_binding(text)",
    ),
    Role.POST_TURN_PROJECTOR: ("public.claim_outbox_events(text,text,integer,integer)",),
    Role.WECOM_CONNECTOR: (
        "public.list_channel_bindings(text)",
        "public.resolve_channel_binding(text)",
    ),
    Role.SESSION_RECOVERY: (
        "public.sweep_expired_session_leases(integer)",
        "public.schedule_session_mailbox_retries(integer)",
        "public.reconcile_session_mailboxes(integer)",
        "public.reconcile_session_mailboxes_v2(integer,integer)",
    ),
    Role.ARTIFACT_GC: (),
}
_WORKER_TABLES = (
    "tenants",
    "agent_apps",
    "config_revisions",
    "storage_profiles",
    "tenant_policies",
    "admin_idempotency",
    "channel_bindings",
    "channel_identities",
    "inbound_messages",
    "outbound_messages",
    "delivery_attempts",
    "sessions",
    "session_turns",
    "turn_intents",
    "session_events",
    "session_summaries",
    "memories",
    "artifacts",
    "knowledge_items",
    "knowledge_embeddings",
    "outbox_events",
    "dead_letters",
    "tool_executions",
    "confirmation_challenges",
    "audit_logs",
    "tenant_budget_usage",
    "fault_stage_controls",
    "session_mailboxes",
    "session_mailbox_items",
    "wecom_connection_state",
    "im_acceptance_evidence_events",
)
_WORKER_TABLE_PRIVILEGES = {table: "SELECT,INSERT,UPDATE,DELETE" for table in _WORKER_TABLES}
_WORKER_TABLE_PRIVILEGES["wecom_connection_state"] = "SELECT,INSERT,UPDATE"
_WORKER_TABLE_PRIVILEGES["im_acceptance_evidence_events"] = "SELECT,INSERT"


@app.callback()
def main(
    version_flag: bool = typer.Option(
        False, "--version", help="Print the service version and exit.", is_eager=True
    ),
) -> None:
    if version_flag:
        typer.echo(__version__)
        raise typer.Exit()


@app.command()
def serve(
    role: Annotated[Role, typer.Option(case_sensitive=False)],
) -> None:
    """Run one stateless service role."""

    settings = ServiceSettings()
    configure_logging(settings.log_level)
    asyncio.run(_serve(role, settings))


@app.command("migrate")
def migrate_database(
    revision: Annotated[str, typer.Option()] = "head",
    check: Annotated[
        bool,
        typer.Option("--check", help="Exit successfully only when the database is at head."),
    ] = False,
) -> None:
    """Apply expand-contract schema migrations using the migration account."""

    from alembic import command
    from alembic.config import Config

    settings = ServiceSettings()
    secrets = _secret_provider(settings)
    dsn = _migration_database_dsn(settings, secrets)
    if check:
        if not asyncio.run(_database_is_at_alembic_head(dsn)):
            raise typer.Exit(code=1)
        typer.echo("database schema is at the Alembic head")
        return
    previous = os.environ.get("TRPC_MIGRATION_DATABASE_DSN")
    os.environ["TRPC_MIGRATION_DATABASE_DSN"] = dsn
    try:
        command.upgrade(Config(str(_alembic_config_path())), revision)
    finally:
        if previous is None:
            os.environ.pop("TRPC_MIGRATION_DATABASE_DSN", None)
        else:
            os.environ["TRPC_MIGRATION_DATABASE_DSN"] = previous


@app.command()
def doctor(
    output: Annotated[Path | None, typer.Option(help="Optional JSON result path.")] = None,
) -> None:
    """Validate the locked SDK public contract without external API calls."""

    from trpc_agent_sdk.context import AgentContext
    from trpc_agent_sdk.runners import Runner
    from trpc_agent_sdk.tools.safety import ToolSafetyFilter, ToolSafetyGuard

    sdk_version = version("trpc-agent-py")
    signature = inspect.signature(Runner.run_async)
    checks = {
        "sdk_version": sdk_version,
        "sdk_locked_1_1_19": sdk_version == "1.1.19",
        "runner_agent_context": "agent_context" in signature.parameters,
        "agent_context_metadata": hasattr(AgentContext, "with_metadata"),
        "tool_safety_guard": ToolSafetyGuard is not None and ToolSafetyFilter is not None,
        "openclaw_security_floors": (
            version("nanobot-ai") == "0.3.0"
            and version("dulwich") == "1.2.12"
            and version("pypdf") == "6.16.1"
        ),
    }
    result = {
        "baseline": "trpc-agent-py>=1.1.17,<1.2",
        "candidate": f"trpc-agent-py=={sdk_version}",
        "case_deltas": checks,
        "gate": "pass"
        if all(value for key, value in checks.items() if key != "sdk_version")
        else "fail",
        "rejection_reasons": [key for key, value in checks.items() if value is False],
    }
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")
    typer.echo(rendered)
    if result["gate"] != "pass":
        raise typer.Exit(code=1)


@app.command()
def probe(
    role: Annotated[Role, typer.Option(case_sensitive=False)],
    liveness: Annotated[
        bool,
        typer.Option("--liveness", help="Check the local event-loop heartbeat only."),
    ] = False,
) -> None:
    """Check local liveness or readiness dependencies for one role."""

    settings = ServiceSettings()
    if liveness:
        from trpc_service.lifecycle import is_process_live

        if not is_process_live(
            role.value,
            settings.runtime_state_dir,
            max_age_seconds=settings.liveness_max_age_seconds,
        ):
            raise typer.Exit(code=1)
        return
    if not asyncio.run(_probe_dependencies(role, settings)):
        raise typer.Exit(code=1)


@app.command()
def drain(role: Annotated[Role, typer.Option(case_sensitive=False)]) -> None:
    """Request cooperative drain for a role in this container."""

    from trpc_service.lifecycle import request_drain

    settings = ServiceSettings()
    request_drain(role.value, settings.runtime_state_dir)


async def _serve(role: Role, settings: ServiceSettings) -> None:
    import redis.asyncio as redis_async

    from trpc_service.lifecycle import ProcessLifecycle
    from trpc_service.metrics.setup import configure_tracing
    from trpc_service.storage.postgres import PostgresRuntimeRepository

    secrets = _secret_provider(settings)
    lifecycle = ProcessLifecycle(
        role.value,
        settings.runtime_state_dir,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
    )
    await lifecycle.start()
    configure_tracing(
        service_name=f"trpc-agent-service-{role.value}",
        endpoint=settings.otlp_endpoint,
        capture_content=settings.capture_content,
    )
    repository: PostgresRuntimeRepository | None = None
    redis: Any | None = None
    try:
        repository = await PostgresRuntimeRepository.create(
            _database_dsn_for_role(role, settings, secrets),
            min_size=settings.database_pool_min_size,
            max_size=settings.database_pool_max_size,
            ready_replay_cooldown_seconds=settings.recovery_ready_replay_cooldown_seconds,
        )
        await _validate_database_identity(repository, role)
        if _role_uses_redis(role):
            redis = redis_async.from_url(
                _redis_url(settings, secrets),
                decode_responses=False,
                socket_connect_timeout=settings.redis_socket_connect_timeout_seconds,
                socket_timeout=settings.redis_socket_timeout_seconds,
                retry_on_timeout=False,
            )
        if role == Role.GATEWAY:
            await _serve_gateway(settings, secrets, repository, redis, lifecycle.stop_event)
        elif role == Role.ADMIN:
            await _serve_admin(settings, secrets, repository, lifecycle.stop_event)
        elif role == Role.WORKER:
            await _serve_worker(settings, secrets, repository, redis, lifecycle.stop_event)
        elif role == Role.OUTBOX_DISPATCHER:
            await _serve_outbox(settings, secrets, repository, redis, lifecycle.stop_event)
        elif role == Role.CHANNEL_DISPATCHER:
            await _serve_channel_dispatcher(settings, secrets, repository, lifecycle.stop_event)
        elif role == Role.POST_TURN_PROJECTOR:
            await _serve_projector(settings, repository, redis, lifecycle.stop_event)
        elif role == Role.WECOM_CONNECTOR:
            await _serve_wecom(settings, secrets, repository, redis, lifecycle.stop_event)
        elif role == Role.SESSION_RECOVERY:
            await _serve_session_recovery(settings, repository, lifecycle.stop_event)
        elif role == Role.ARTIFACT_GC:
            await _serve_artifact_gc(settings, secrets, repository, lifecycle.stop_event)
    finally:
        await lifecycle.close()
        if redis is not None:
            await redis.aclose()
        if repository is not None:
            await repository.close()


async def _serve_gateway(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    repository: Any,
    redis: Any,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    import uvicorn

    from trpc_service.channels.feishu import FeishuAdapter
    from trpc_service.runtime import TenantRuntime
    from trpc_service.web.app import create_base_app
    from trpc_service.web.feishu_gateway import (
        FeishuGatewayService,
        create_feishu_gateway_router,
    )

    runtime = TenantRuntime(
        repository,
        routing_key=_secret_bytes(secrets.resolve(settings.session_hmac_ref)),
        scheduler_version=settings.scheduler_version,
    )
    emergency = _emergency_queue(settings, secrets, redis)
    feishu = FeishuAdapter(
        secrets,
        max_callback_bytes=settings.callback_body_limit_bytes,
    )
    feishu_service = FeishuGatewayService(
        repository,
        runtime,
        feishu,
        emergency_queue=emergency,
        max_body_bytes=settings.callback_body_limit_bytes,
        allow_stale_binding_cache=settings.feishu_allow_stale_binding_cache,
    )

    async def readiness() -> bool:
        return not stop_event.is_set() and await _dependencies_ready(repository, redis)

    web = create_base_app(title="tRPC Agent Gateway", readiness=readiness)
    web.include_router(create_feishu_gateway_router(feishu_service))
    try:
        server = uvicorn.Server(
            uvicorn.Config(web, host=settings.host, port=settings.port, log_config=None)
        )
        await _serve_uvicorn(server, stop_event)
    finally:
        await feishu.close()


async def _serve_admin(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    repository: Any,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    import uvicorn

    from trpc_service.tenant.auth import Authorizer, DevelopmentAuthorizer, OidcAuthorizer
    from trpc_service.tenant.control import PostgresControlPlaneRepository
    from trpc_service.web.admin import create_admin_router
    from trpc_service.web.app import create_base_app

    if settings.environment == Environment.PRODUCTION:
        assert settings.oidc_issuer and settings.oidc_audience
        authorizer: Authorizer = OidcAuthorizer(
            issuer=settings.oidc_issuer,
            audience=settings.oidc_audience,
            algorithms=settings.oidc_algorithms,
            jwks_ttl_seconds=settings.oidc_jwks_ttl_seconds,
        )
    else:
        authorizer = DevelopmentAuthorizer(
            secrets,
            settings.development_token_ref,
            enabled=settings.allow_development_token,
        )

    async def readiness() -> bool:
        return not stop_event.is_set() and await repository.ready()

    web = create_base_app(title="tRPC Agent Admin API", readiness=readiness)
    web.include_router(
        create_admin_router(PostgresControlPlaneRepository(repository.pool), authorizer)
    )
    port = 8081 if settings.port == 8080 else settings.port
    server = uvicorn.Server(uvicorn.Config(web, host=settings.host, port=port, log_config=None))
    await _serve_uvicorn(server, stop_event)


def _worker_tenant_service_factory(
    repository: Any,
    artifact_objects: Any | None,
    *,
    registered_profiles: Mapping[tuple[str, str], RegisteredTenantServiceBundle] | None = None,
) -> TenantServiceFactory:
    """Build the production default plus exact tenant/profile overrides.

    The optional registry file can only bind the process's existing built-in
    storage bundle.  Programmatic registrations are the extension point for
    separately constructed physical backends.
    """

    from trpc_service.storage.services import (
        CompositeTenantServiceFactory,
        PostgresTenantServiceFactory,
        TenantStorageProfileRegistry,
    )

    default_factory = PostgresTenantServiceFactory(
        repository.pool,
        repository=repository,
        artifact_objects=artifact_objects,
    )
    registry_path = os.getenv("TRPC_SERVICE_STORAGE_PROFILE_REGISTRY_FILE", "").strip()
    if registered_profiles is not None and registry_path:
        raise ValueError("tenant storage profile registry source is ambiguous")
    if registered_profiles is None and registry_path:
        from trpc_service.storage.profile_registry import load_default_profile_registrations

        registrations = load_default_profile_registrations(registry_path, default_factory)
    else:
        registrations = dict(registered_profiles or {})
    return CompositeTenantServiceFactory(
        TenantStorageProfileRegistry(registrations),
        default_factory,
    )


async def _serve_worker(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    repository: Any,
    redis: Any,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    from trpc_service.agent.factory import DevelopmentAgentLoader, ProductionAgentLoader
    from trpc_service.agent.mailbox_runtime import MailboxClaimExecutor, MailboxReadyClaimer
    from trpc_service.agent.worker import AgentWorker
    from trpc_service.channels.feishu import FeishuAdapter
    from trpc_service.channels.media_locator import WeComMediaLocatorCipher
    from trpc_service.channels.wecom import WeComMediaDownloader
    from trpc_service.tenant.models import Channel
    from trpc_service.tool.confirmation import ConfirmationTokenService
    from trpc_service.tool.execution import ToolExecutor
    from trpc_service.tool.governance import GovernancePipeline, SdkToolSafetyScanner
    from trpc_service.tool.postgres import (
        PostgresBudgetLedger,
        PostgresConfirmationLedger,
        PostgresExecutionLedger,
        PostgresGovernanceAuditSink,
    )
    from trpc_service.tool.test_tool import build_fault_stage_test_tools
    from trpc_service.workspace import WorkspaceManager

    runtime_repository, fault_stages = _worker_fault_stage_runtime(settings, secrets, repository)
    worker_id = f"worker-{os.getenv('HOSTNAME') or uuid4()}"
    root_key = _secret_bytes(secrets.resolve(settings.session_hmac_ref))
    confirmations = ConfirmationTokenService(
        _derive_key(root_key, b"tool-confirmation"),
        PostgresConfirmationLedger(runtime_repository.pool),
    )
    governance = GovernancePipeline(
        PostgresBudgetLedger(runtime_repository.pool),
        SdkToolSafetyScanner(),
        confirmations,
        PostgresGovernanceAuditSink(runtime_repository.pool),
    )
    tool_options: dict[str, Any] = {}
    if fault_stages is not None:
        tool_options = {"fault_stages": fault_stages, "worker_id": worker_id}
    test_tools = build_fault_stage_test_tools(
        environment=settings.environment,
        fault_injection_enabled=settings.fault_injection_enabled,
    )
    production_loader = ProductionAgentLoader(
        secrets,
        tools=test_tools,
        governance=governance,
        tool_executor=ToolExecutor(
            _derive_key(root_key, b"tool-execution"),
            PostgresExecutionLedger(runtime_repository.pool),
            fault_stage_delay_seconds=(
                settings.offline_agent_delay_seconds if fault_stages is not None else 0.0
            ),
            **tool_options,
        ),
        allowed_model_hosts=settings.model_endpoint_hosts,
    )
    agent_loader = (
        production_loader
        if settings.environment == Environment.PRODUCTION
        else DevelopmentAgentLoader(
            production_loader,
            delay_seconds=settings.offline_agent_delay_seconds,
            environment=settings.environment,
            fault_injection_enabled=settings.fault_injection_enabled,
            deterministic_tool_call=(
                settings.environment == Environment.TEST and settings.fault_injection_enabled
            ),
        )
    )
    artifact_objects = _s3_artifact_store(settings, secrets)
    feishu = FeishuAdapter(
        secrets,
        max_media_bytes=settings.media_download_max_bytes,
        media_timeout_seconds=settings.media_download_timeout_seconds,
    )
    wecom_media = WeComMediaDownloader(
        secrets,
        locator_cipher=WeComMediaLocatorCipher(_derive_key(root_key, b"wecom-media-locator")),
        max_media_bytes=settings.media_download_max_bytes,
        media_timeout_seconds=settings.media_download_timeout_seconds,
    )
    try:
        worker = AgentWorker(
            runtime_repository,
            worker_id=worker_id,
            agent_loader=agent_loader,
            lease_for=timedelta(seconds=settings.lease_seconds),
            service_factory=_worker_tenant_service_factory(
                runtime_repository,
                artifact_objects,
            ),
            media_downloaders={
                Channel.FEISHU: feishu,
                Channel.WECOM_AI_BOT: wecom_media,
            },
            workspace_manager=WorkspaceManager(
                settings.workspace_root,
                key=_derive_key(root_key, b"workspace-path"),
            ),
        )
        if settings.scheduler_version == SchedulerVersion.V2:
            from trpc_service.queue.session_ready import (
                SessionReadyQueue,
                SessionReadyReclaimer,
            )
            from trpc_service.queue.session_worker_consumer import SessionWorkerConsumer

            ready_queue = SessionReadyQueue(
                redis,
                stream=settings.redis_stream,
                group=settings.redis_consumer_group,
            )
            await ready_queue.ensure_group()
            reclaimer = SessionReadyReclaimer(
                redis,
                consumer=f"{worker_id}-reclaimer",
                stream=settings.redis_stream,
                group=settings.redis_consumer_group,
                min_idle_ms=settings.redis_reclaim_after_ms,
                count=1,
                poll_seconds=settings.worker_poll_seconds,
            )
            await SessionWorkerConsumer(
                ready_queue,
                reclaimer,
                MailboxReadyClaimer(
                    runtime_repository,
                    owner_id=worker_id,
                    lease_for=timedelta(seconds=settings.lease_seconds),
                ),
                MailboxClaimExecutor(worker),
                consumer_id=worker_id,
                concurrency=settings.worker_concurrency,
                receive_block_ms=round(settings.worker_poll_seconds * 1000),
                reclaimer_poll_seconds=settings.worker_poll_seconds,
                ack_timeout_seconds=settings.redis_ack_timeout_seconds,
                shutdown_grace_seconds=settings.shutdown_grace_seconds,
                fault_stages=fault_stages,
                fault_injection_enabled=settings.fault_injection_enabled,
                test_environment=settings.environment == Environment.TEST,
            ).run(stop_event)
        else:
            from trpc_service.queue.redis_streams import RedisStreamQueue
            from trpc_service.queue.worker_consumer import WorkerConsumer

            queue = RedisStreamQueue(
                redis,
                stream=settings.redis_stream,
                group=settings.redis_consumer_group,
                reclaim_after_ms=settings.redis_reclaim_after_ms,
            )
            consumer_options: dict[str, Any] = {}
            if fault_stages is not None:
                consumer_options["fault_stages"] = fault_stages
            await WorkerConsumer(
                runtime_repository,
                queue,
                worker,
                consumer_id=worker_id,
                concurrency=settings.worker_concurrency,
                shutdown_grace_seconds=settings.shutdown_grace_seconds,
                **consumer_options,
            ).run(stop_event=stop_event)
    finally:
        await wecom_media.close()
        await feishu.close()


def _worker_fault_stage_runtime(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    repository: Any,
) -> tuple[Any, Any | None]:
    """Build the opt-in worker fault controller and fenced repository view.

    The helper is only called from the worker role.  Keeping secret resolution
    here prevents gateway/admin/dispatcher processes from ever resolving the
    fault-injection token, while an enabled worker fails before it can start
    consuming messages if its run identity or token is invalid.
    """

    if not settings.fault_injection_enabled:
        return repository, None

    run_id = settings.fault_injection_run_id
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("fault injection run_id is required when fault injection is enabled")
    run_token = secrets.resolve(settings.fault_injection_run_token_ref)
    from trpc_service.faults import PostgresFaultStageController
    from trpc_service.storage.postgres import PostgresRuntimeRepository

    controller = PostgresFaultStageController(
        repository.pool,
        run_id=run_id,
        run_token=run_token,
    )
    return (
        PostgresRuntimeRepository(
            repository.pool,
            fault_stages=controller,
            ready_replay_cooldown_seconds=settings.recovery_ready_replay_cooldown_seconds,
        ),
        controller,
    )


async def _serve_outbox(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    repository: Any,
    redis: Any,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    from trpc_service.queue.dispatcher import OutboxDispatcher
    from trpc_service.queue.emergency import EmergencyQueueDrainer

    owner_id = f"outbox-{uuid4()}"
    if settings.scheduler_version == SchedulerVersion.V2:
        from trpc_service.queue.session_ready import SessionReadyQueue
        from trpc_service.queue.session_ready_outbox import (
            SESSION_READY_EVENT_V2,
            SessionReadyOutboxQueue,
        )

        queue: Any = SessionReadyOutboxQueue(
            SessionReadyQueue(
                redis,
                stream=settings.redis_stream,
                group=settings.redis_consumer_group,
            )
        )
        event_type = SESSION_READY_EVENT_V2
    else:
        from trpc_service.queue.redis_streams import RedisStreamQueue

        queue = RedisStreamQueue(
            redis,
            stream=settings.redis_stream,
            group=settings.redis_consumer_group,
            reclaim_after_ms=settings.redis_reclaim_after_ms,
        )
        event_type = "inbound.accepted"
    emergency = _emergency_queue(settings, secrets, redis)
    await asyncio.gather(
        OutboxDispatcher(
            repository,
            queue,
            owner_id=owner_id,
            event_type=event_type,
        ).run(stop_event=stop_event),
        EmergencyQueueDrainer(
            repository,
            emergency,
            consumer_id=owner_id,
            scheduler_version=settings.scheduler_version,
        ).run(stop_event=stop_event),
    )


async def _serve_channel_dispatcher(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    repository: Any,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    from trpc_service.channels.dispatcher import ChannelDispatcher
    from trpc_service.channels.feishu import FeishuAdapter
    from trpc_service.tenant.models import Channel

    feishu = FeishuAdapter(secrets, api_root=settings.feishu_send_api_root)
    owner = f"channel-{uuid4()}"
    try:
        await ChannelDispatcher(
            repository,
            {Channel.FEISHU: feishu},
            owner_id=f"{owner}-feishu",
            event_type="outbound.feishu.ready",
        ).run(stop_event=stop_event)
    finally:
        await feishu.close()


async def _serve_projector(
    settings: ServiceSettings,
    repository: Any,
    redis: Any,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    from trpc_service.storage.projector import PostTurnProjector
    from trpc_service.storage.redis_projection import RedisProjectionStore
    from trpc_service.storage.services import PostgresSessionStore

    await PostTurnProjector(
        repository,
        RedisProjectionStore(redis),
        owner_id=f"projector-{uuid4()}",
        session_store=PostgresSessionStore(repository),
    ).run(stop_event=stop_event)


async def _serve_wecom(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    repository: Any,
    redis: Any = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    from trpc_service.agent.wecom_manager import WeComConnectionManager
    from trpc_service.channels.dispatcher import ChannelDispatcher
    from trpc_service.channels.media_locator import WeComMediaLocatorCipher
    from trpc_service.channels.wecom import WeComConnector
    from trpc_service.runtime import TenantRuntime
    from trpc_service.storage.models import BindingRoute
    from trpc_service.storage.postgres import PostgresBindingLease
    from trpc_service.tenant.models import Channel

    owner = f"wecom-{uuid4()}"
    root_key = _secret_bytes(secrets.resolve(settings.session_hmac_ref))
    connector = WeComConnector(
        secrets,
        PostgresBindingLease(repository.pool),
        owner_id=owner,
        locator_cipher=WeComMediaLocatorCipher(_derive_key(root_key, b"wecom-media-locator")),
        max_media_bytes=settings.media_download_max_bytes,
        media_timeout_seconds=settings.media_download_timeout_seconds,
    )
    runtime = TenantRuntime(
        repository,
        routing_key=root_key,
        scheduler_version=settings.scheduler_version,
    )

    async def sink(binding_id: str, envelope: Any) -> None:
        await runtime.accept(binding_id, envelope)

    emergency = _emergency_queue(settings, secrets, redis)

    async def emergency_sink(route: BindingRoute, envelope: Any) -> None:
        await emergency.enqueue(runtime.prepare(route, envelope))

    manager = WeComConnectionManager(
        repository,
        connector,
        sink,
        emergency_sink=emergency_sink,
    )
    dispatcher = ChannelDispatcher(
        repository,
        {Channel.WECOM_AI_BOT: connector},
        owner_id=owner,
        event_type="outbound.wecom_ai_bot.ready",
        binding_ready=connector.ready_for_delivery,
    )
    await asyncio.gather(
        manager.run(stop_event=stop_event),
        dispatcher.run(stop_event=stop_event),
    )


async def _serve_session_recovery(
    settings: ServiceSettings,
    repository: Any,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run PostgreSQL-backed mailbox recovery without a Redis dependency."""

    stop_event = stop_event or asyncio.Event()

    from trpc_service.agent.session_recovery import SessionRecoveryService

    service = SessionRecoveryService(
        repository,
        owner_id=f"session-recovery-{os.getenv('HOSTNAME') or uuid4()}",
        batch_size=settings.recovery_batch_size,
        poll_seconds=settings.recovery_poll_seconds,
    )
    task = asyncio.create_task(service.run(), name="session-recovery")
    stop_task = asyncio.create_task(stop_event.wait(), name="session-recovery-drain")
    try:
        done, _ = await asyncio.wait({task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
    finally:
        stop_event.set()
        service.stop()
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        if not task.done():
            await asyncio.wait_for(task, timeout=settings.shutdown_grace_seconds)


async def _serve_artifact_gc(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    repository: Any,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run bounded PostgreSQL-authoritative staged artifact cleanup."""

    from trpc_service.storage.artifact_gc import ArtifactGarbageCollector

    objects = _s3_artifact_store(settings, secrets)
    if objects is None:
        raise ValueError("artifact-gc requires an S3 endpoint")
    stop_event = stop_event or asyncio.Event()
    collector = ArtifactGarbageCollector(
        repository.pool,
        objects,
        ttl_seconds=settings.artifact_staging_ttl_seconds,
        batch_size=settings.artifact_gc_batch_size,
        poll_seconds=settings.artifact_gc_poll_seconds,
    )
    await collector.run(stop_event)


async def _serve_uvicorn(server: Any, stop_event: asyncio.Event) -> None:
    """Bridge the shared drain event into Uvicorn's graceful shutdown."""

    server_task = asyncio.create_task(server.serve(), name="uvicorn-server")
    stop_task = asyncio.create_task(stop_event.wait(), name="uvicorn-drain")
    try:
        done, _ = await asyncio.wait({server_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if stop_task in done:
            server.should_exit = True
        await server_task
    finally:
        stop_event.set()
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)


def _database_dsn(settings: ServiceSettings, secrets: LocalSecretProvider) -> str:
    return _resolve_database_dsn(
        settings.database_dsn_ref,
        settings.database_password_ref,
        secrets,
    )


def _worker_database_dsn(settings: ServiceSettings, secrets: LocalSecretProvider) -> str:
    if settings.worker_database_dsn_ref is None:
        raise ValueError("worker database DSN reference is required for cross-tenant runtime roles")
    return _resolve_database_dsn(
        settings.worker_database_dsn_ref,
        settings.worker_database_password_ref,
        secrets,
    )


def _resolve_database_dsn(
    dsn_ref: Any,
    password_ref: Any,
    secrets: LocalSecretProvider,
) -> str:
    dsn = secrets.resolve(dsn_ref).replace("postgresql+asyncpg://", "postgresql://")
    if password_ref:
        dsn = _url_password(dsn, secrets.resolve(password_ref))
    return dsn


def _database_dsn_for_role(
    role: Role,
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
) -> str:
    """Resolve the only DSN allowed for a process role and pin its username."""

    if role in _WORKER_DATABASE_ROLES:
        dsn = _worker_database_dsn(settings, secrets)
        expected_role = WORKER_DATABASE_ROLE
    else:
        if settings.worker_database_dsn_ref is not None:
            raise ValueError("worker database DSN is forbidden for tenant runtime roles")
        dsn = _database_dsn(settings, secrets)
        expected_role = RUNTIME_DATABASE_ROLE
    username = urlsplit(dsn).username
    if username != expected_role:
        raise ValueError(f"database DSN username must be {expected_role}")
    return dsn


async def _validate_database_identity(repository: Any, role: Role) -> None:
    """Fail closed unless PostgreSQL authenticated the process as its pinned role."""

    pool = getattr(repository, "pool", None)
    # Lightweight unit doubles do not expose an asyncpg pool.  Real runtime
    # repositories always do; retaining this narrow escape keeps role wiring
    # tests independent from PostgreSQL while the production path is strict.
    if not callable(getattr(pool, "acquire", None)):
        return
    assert pool is not None

    expected_role = (
        WORKER_DATABASE_ROLE if role in _WORKER_DATABASE_ROLES else RUNTIME_DATABASE_ROLE
    )
    required_functions = _DATABASE_FUNCTIONS.get(role, ())
    async with pool.acquire() as connection:
        identity = await connection.fetchrow(
            """
                   SELECT current_user::text AS current_user,
                   session_user::text AS session_user,
                   r.rolsuper AS is_superuser,
                   r.rolbypassrls AS bypasses_rls,
                   r.rolcanlogin,
                   (SELECT count(*)
                      FROM pg_class AS c
                      JOIN pg_namespace AS n ON n.oid = c.relnamespace
                     WHERE n.nspname = 'public' AND c.relrowsecurity
                       AND pg_get_userbyid(c.relowner) = current_user)
                       AS owned_rls_table_count,
                   has_schema_privilege(current_user, 'public', 'USAGE') AS schema_usage
              FROM pg_roles AS r
             WHERE r.rolname = current_user
            """
        )
        if identity is None:
            raise RuntimeError("database identity check returned no authenticated role")
        current_user = str(identity["current_user"])
        session_user = str(identity["session_user"])
        if current_user != expected_role or session_user != expected_role:
            raise RuntimeError(
                f"database identity mismatch for {role.value}: expected {expected_role}"
            )
        if bool(identity["is_superuser"]):
            raise RuntimeError(f"database role {expected_role} must not be a superuser")
        if not bool(identity["rolcanlogin"]):
            raise RuntimeError(f"database role {expected_role} must be login-enabled")
        if int(identity["owned_rls_table_count"]) != 0:
            raise RuntimeError(f"database role {expected_role} must not own RLS tables")
        bypasses_rls = bool(identity["bypasses_rls"])
        if role in _WORKER_DATABASE_ROLES and not bypasses_rls:
            raise RuntimeError(f"database role {expected_role} must bypass row-level security")
        if role not in _WORKER_DATABASE_ROLES and bypasses_rls:
            raise RuntimeError(f"database role {expected_role} must not bypass row-level security")
        if not bool(identity["schema_usage"]):
            raise RuntimeError(f"database role {expected_role} lacks public schema usage")
        for signature in required_functions:
            granted = await connection.fetchval(
                "SELECT has_function_privilege(current_user, $1::regprocedure, 'EXECUTE')",
                signature,
            )
            if not granted:
                raise RuntimeError(f"database role {expected_role} lacks EXECUTE on {signature}")
        if role in _WORKER_DATABASE_ROLES:
            for table, privileges in _WORKER_TABLE_PRIVILEGES.items():
                granted = await connection.fetchval(
                    """
                    SELECT has_table_privilege(
                        current_user, $1, $2
                    )
                    """,
                    f"public.{table}",
                    privileges,
                )
                if not granted:
                    raise RuntimeError(
                        f"database role {expected_role} lacks tenant table privileges on {table}"
                    )


def _secret_provider(settings: ServiceSettings) -> LocalSecretProvider:
    """Build one resolver whose tenant-facing path is explicitly constrained."""

    return LocalSecretProvider(
        allow_literal=settings.environment != Environment.PRODUCTION,
        secret_root=settings.tenant_secret_root,
        allowed_env_names=settings.tenant_secret_env_names,
    )


def _emergency_queue(
    settings: ServiceSettings,
    secrets: LocalSecretProvider,
    redis: Any,
) -> Any:
    from trpc_service.queue.emergency import EmergencyQueue

    previous_keys = {
        key_version: _secret_bytes(secrets.resolve(secret_ref), exact=32)
        for key_version, secret_ref in settings.emergency_queue_previous_key_refs.items()
    }
    return EmergencyQueue(
        redis,
        _secret_bytes(secrets.resolve(settings.emergency_queue_key_ref), exact=32),
        scheduler_version=settings.scheduler_version,
        key_version=settings.emergency_queue_key_version,
        previous_keys=previous_keys,
    )


def _migration_database_dsn(settings: ServiceSettings, secrets: LocalSecretProvider) -> str:
    dsn = _database_dsn(settings, secrets)
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    return dsn


async def _database_is_at_alembic_head(dsn: str) -> bool:
    """Compare the durable Alembic revision set with this checkout's heads."""

    import asyncpg
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    asyncpg_dsn = dsn.replace("postgresql+psycopg://", "postgresql://", 1).replace(
        "postgresql+asyncpg://", "postgresql://", 1
    )
    expected = set(ScriptDirectory.from_config(Config(str(_alembic_config_path()))).get_heads())
    connection = None
    try:
        connection = await asyncpg.connect(dsn=asyncpg_dsn, timeout=10, command_timeout=10)
        rows = await connection.fetch("SELECT version_num FROM alembic_version")
    except (asyncpg.PostgresError, OSError, TimeoutError):
        return False
    finally:
        if connection is not None:
            await connection.close()
    return {str(row["version_num"]) for row in rows} == expected


def _alembic_config_path() -> Path:
    configured = os.getenv("TRPC_SERVICE_ALEMBIC_CONFIG")
    candidates = (
        Path(configured) if configured else None,
        Path.cwd() / "alembic.ini",
        Path(__file__).resolve().parents[1] / "alembic.ini",
    )
    for candidate in candidates:
        if candidate is not None and candidate.is_file():
            return candidate
    raise RuntimeError("alembic.ini was not found; set TRPC_SERVICE_ALEMBIC_CONFIG")


def _redis_url(settings: ServiceSettings, secrets: LocalSecretProvider) -> str:
    url = secrets.resolve(settings.redis_url_ref)
    if settings.redis_password_ref:
        url = _url_password(url, secrets.resolve(settings.redis_password_ref))
    return url


def _s3_artifact_store(settings: ServiceSettings, secrets: LocalSecretProvider) -> Any | None:
    if not settings.s3_endpoint:
        return None
    if not settings.s3_access_key or settings.s3_secret_key_ref is None:
        raise ValueError("S3 endpoint requires access key and secret reference")

    import boto3
    from botocore.config import Config

    from trpc_service.storage.artifacts import S3ArtifactStore

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=secrets.resolve(settings.s3_secret_key_ref),
        region_name="us-east-1",
        config=Config(
            connect_timeout=5,
            read_timeout=settings.media_download_timeout_seconds,
            max_pool_connections=8,
            retries={"max_attempts": 3, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )
    return S3ArtifactStore(
        client,
        bucket=settings.s3_bucket,
        max_size_bytes=settings.media_download_max_bytes,
    )


def _url_password(url: str, password: str) -> str:
    parts = urlsplit(url)
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    user = quote(parts.username or "", safe="")
    credentials = f"{user}:{quote(password, safe='')}@" if user else f":{quote(password, safe='')}@"
    return urlunsplit(
        (parts.scheme, credentials + host + port, parts.path, parts.query, parts.fragment)
    )


def _secret_bytes(value: str, *, exact: int | None = None) -> bytes:
    raw = value.encode()
    for decoder in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            candidate = decoder(value + "=" * (-len(value) % 4))
        except ValueError:
            continue
        if exact is not None and len(candidate) == exact:
            return candidate
        if exact is None and len(candidate) >= 32:
            return candidate
    if exact is not None and len(raw) != exact:
        raise ValueError(f"secret must decode to exactly {exact} bytes")
    if exact is None and len(raw) < 32:
        raise ValueError("secret must contain at least 32 bytes")
    return raw


def _derive_key(root_key: bytes, purpose: bytes) -> bytes:
    """Derive independent fixed-length keys from the configured runtime root."""

    if len(root_key) < 32:
        raise ValueError("root key must contain at least 32 bytes")
    return hmac.new(root_key, b"trpc-agent-service:v1:" + purpose, hashlib.sha256).digest()


async def _dependencies_ready(repository: Any, redis: Any | None = None) -> bool:
    if not await repository.ready():
        return False
    if redis is None:
        return True
    try:
        return bool(await redis.ping())
    except Exception:
        return False


def _role_uses_redis(role: Role) -> bool:
    return role in {
        Role.GATEWAY,
        Role.WORKER,
        Role.OUTBOX_DISPATCHER,
        Role.POST_TURN_PROJECTOR,
        Role.WECOM_CONNECTOR,
    }


async def _probe_dependencies(role: Role, settings: ServiceSettings) -> bool:
    import redis.asyncio as redis_async

    from trpc_service.storage.postgres import PostgresRuntimeRepository

    secrets = _secret_provider(settings)
    from trpc_service.lifecycle import is_process_ready

    if not is_process_ready(role.value, settings.runtime_state_dir):
        return False
    repository: PostgresRuntimeRepository | None = None
    redis: Any | None = None
    try:
        repository = await PostgresRuntimeRepository.create(
            _database_dsn_for_role(role, settings, secrets), min_size=1, max_size=1
        )
        await _validate_database_identity(repository, role)
        if _role_uses_redis(role):
            redis = redis_async.from_url(
                _redis_url(settings, secrets),
                decode_responses=False,
                socket_connect_timeout=settings.redis_socket_connect_timeout_seconds,
                socket_timeout=settings.redis_socket_timeout_seconds,
                retry_on_timeout=False,
            )
        return await _dependencies_ready(repository, redis)
    except Exception:
        return False
    finally:
        if redis is not None:
            await redis.aclose()
        if repository is not None:
            await repository.close()


if __name__ == "__main__":
    app()
