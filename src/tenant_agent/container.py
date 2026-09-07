"""Application composition root."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from tenant_agent.agent.deterministic import DeterministicEngine
from tenant_agent.agent.tools import REGISTERED_TOOL_NAMES
from tenant_agent.agent.trpc import EngineSelector, TrpcAgentEngine
from tenant_agent.channels.registry import ChannelRegistry
from tenant_agent.channels.wecom import validate_wecom_credentials
from tenant_agent.channels.wecom_bot import validate_bot_credentials
from tenant_agent.governance.policies import ConfirmationManager, GovernanceService
from tenant_agent.ids import IdentityDeriver
from tenant_agent.security import CompositeSecretResolver, Redactor, SecretRegistry
from tenant_agent.services.audit import AuditMaintenanceWorker
from tenant_agent.services.broker import InlineBroker, JobBroker, RedisStreamsBroker
from tenant_agent.services.config import TenantConfigService
from tenant_agent.services.dispatcher import GatewayRouter, TurnDispatcher
from tenant_agent.services.outbox import OutboxWorker
from tenant_agent.services.wecom_bot import WeComBotManager
from tenant_agent.services.worker import AgentWorker
from tenant_agent.settings import ServiceRole, Settings
from tenant_agent.storage.memory import InMemoryPlane
from tenant_agent.storage.router import StorageRouter
from tenant_agent.storage.sql import SqlPlane


@dataclass(slots=True)
class ApplicationContainer:
    settings: Settings
    redactor: Redactor
    secrets: CompositeSecretResolver
    control: object
    storage: StorageRouter
    configs: TenantConfigService
    identities: IdentityDeriver
    governance: GovernanceService
    engines: EngineSelector
    channels: ChannelRegistry
    broker: JobBroker
    gateway: GatewayRouter
    dispatcher: TurnDispatcher
    worker: AgentWorker
    outbox: OutboxWorker
    audit_maintenance: AuditMaintenanceWorker
    wecom_bot: WeComBotManager

    @classmethod
    def build(cls, settings: Settings) -> ApplicationContainer:
        identity_key = settings.session_hmac_key.get_secret_value().encode()
        registry = SecretRegistry()
        registry.register(settings.session_hmac_key.get_secret_value())
        registry.register(settings.admin_bearer_token.get_secret_value())
        registry.register(settings.internal_bearer_token.get_secret_value())
        if settings.redis_url:
            registry.register(settings.redis_url.get_secret_value())
        if settings.otlp_headers:
            registry.register(settings.otlp_headers.get_secret_value())
        registry.register(settings.control_database_url.get_secret_value())
        redactor = Redactor(registry=registry)
        secrets = CompositeSecretResolver(
            file_root=settings.secret_file_root,
            registry=registry,
            cache_ttl_seconds=settings.secret_cache_ttl_seconds,
        )
        control_url = settings.control_database_url.get_secret_value()
        if control_url == "inmemory://":
            control: object = InMemoryPlane()
        else:
            control = SqlPlane(control_url, create_schema=settings.auto_create_schema)
        storage = StorageRouter(
            settings=settings,
            control=control,  # type: ignore[arg-type]
            control_plane=control,
            secrets=secrets,
        )
        configs = TenantConfigService(control)  # type: ignore[arg-type]
        identities = IdentityDeriver(identity_key)
        governance = GovernanceService(redactor)
        confirmations = ConfirmationManager(identity_key, control)  # type: ignore[arg-type]
        trpc_engine = TrpcAgentEngine(
            settings=settings,
            secrets=secrets,
            governance=governance,
            confirmations=confirmations,
        )
        engines = EngineSelector(DeterministicEngine(), trpc_engine)
        channels = ChannelRegistry(delivery_timeout_seconds=settings.delivery_timeout_seconds)
        if settings.broker_mode == "redis-streams":
            if not settings.redis_url:
                raise ValueError("redis-streams broker mode requires TAP_REDIS_URL")
            broker: JobBroker = RedisStreamsBroker(
                url=settings.redis_url.get_secret_value(),
                stream=settings.redis_stream,
                group=settings.redis_consumer_group,
                consumer=settings.node_id,
                claim_idle_ms=settings.worker_claim_idle_ms,
                max_attempts=settings.worker_max_attempts,
                global_queue_limit=settings.broker_global_queue_limit,
                tenant_queue_limit=settings.broker_tenant_queue_limit,
                cluster=settings.redis_cluster,
            )
        else:
            broker = InlineBroker(max_queue_size=settings.broker_global_queue_limit)
        gateway = GatewayRouter(identities)
        dispatcher = TurnDispatcher(
            settings=settings,
            identities=identities,
            storage=storage,
            governance=governance,
            engines=engines,
            redactor=redactor,
        )
        worker = AgentWorker(
            settings=settings,
            broker=broker,
            configs=configs,
            dispatcher=dispatcher,
        )
        outbox = OutboxWorker(
            settings=settings,
            repository=control,  # type: ignore[arg-type]
            storage=storage,
            configs=configs,
            channels=channels,
            secrets=secrets,
            redactor=redactor,
        )
        audit_maintenance = AuditMaintenanceWorker(
            settings=settings,
            configs=configs,
            storage=storage,
            secrets=secrets,
            redactor=redactor,
        )
        wecom_bot = WeComBotManager(
            settings=settings,
            configs=configs,
            repository=control,  # type: ignore[arg-type]
            leases=control,  # type: ignore[arg-type]
            broker=broker,
            gateway=gateway,
            storage=storage,
            outbox_repository=control,  # type: ignore[arg-type]
            secrets=secrets,
            redactor=redactor,
        )
        return cls(
            settings=settings,
            redactor=redactor,
            secrets=secrets,
            control=control,
            storage=storage,
            configs=configs,
            identities=identities,
            governance=governance,
            engines=engines,
            channels=channels,
            broker=broker,
            gateway=gateway,
            dispatcher=dispatcher,
            worker=worker,
            outbox=outbox,
            audit_maintenance=audit_maintenance,
            wecom_bot=wecom_bot,
        )

    async def initialize(self) -> None:
        if self.settings.environment == "production":
            weak = {
                "development-only-change-me",
                "development-admin-token",
                "development-internal-token",
            }
            configured = {
                "session": self.settings.session_hmac_key.get_secret_value(),
                "admin": self.settings.admin_bearer_token.get_secret_value(),
                "internal": self.settings.internal_bearer_token.get_secret_value(),
            }
            required_by_role = {
                ServiceRole.ALL: {"session", "admin", "internal"},
                ServiceRole.CHANNEL: {"session", "internal"},
                ServiceRole.GATEWAY: {"session", "internal"},
                ServiceRole.WORKER: {"session"},
                ServiceRole.ADMIN: {"admin"},
                ServiceRole.OUTBOX: set(),
            }
            required = {configured[name] for name in required_by_role[self.settings.service_role]}
            if weak & required or any(
                value.casefold().startswith(("replace", "change-me")) for value in required
            ):
                raise RuntimeError("development secrets are forbidden in production")
            if any(len(value.encode()) < 32 for value in required):
                raise RuntimeError("production platform secrets must be at least 32 bytes")
            if self.settings.broker_mode != "redis-streams":
                raise RuntimeError("production requires the Redis Streams broker")
            if self.settings.service_role is ServiceRole.CHANNEL and not self.settings.gateway_internal_url:
                raise RuntimeError("production Channel Adapter requires TAP_GATEWAY_INTERNAL_URL")
            if self.settings.auto_create_schema:
                raise RuntimeError("production schema changes must run through Alembic")
            control_scheme = self.settings.control_database_url.get_secret_value().split(":", 1)[0]
            if control_scheme in {"inmemory", "sqlite", "sqlite+aiosqlite"}:
                raise RuntimeError("production control plane requires a shared SQL database")
        await self.storage.initialize()
        if self.settings.environment == "production" and isinstance(self.control, SqlPlane):
            await self.control.assert_runtime_role_unprivileged()
        await self.broker.initialize()
        await self.configs.bootstrap(
            self.settings.bootstrap_config_path,
            preflight=self.preflight_tenant,
        )

    async def close(self) -> None:
        await self.engines.close()
        await self.broker.close()
        await self.storage.close()

    async def healthcheck(self) -> bool:
        control_health = getattr(self.control, "healthcheck", None)
        if control_health is None or not await control_health():
            return False
        return await self.broker.healthcheck()

    async def preflight_tenant(self, tenant: object) -> None:
        from tenant_agent.models import ChannelType, TenantConfig

        if not isinstance(tenant, TenantConfig):
            raise TypeError("tenant preflight requires TenantConfig")
        configured_tools = set(tenant.governance.tools.allow)
        for app in tenant.apps.values():
            configured_tools.update(app.allowed_tools)
        unknown_tools = configured_tools - REGISTERED_TOOL_NAMES
        if unknown_tools:
            raise ValueError("tenant configuration references an unregistered tool")
        if self.settings.environment == "production" and any(
            binding.enabled and binding.channel is ChannelType.WEB for binding in tenant.channels
        ):
            raise ValueError("the browser fallback channel cannot be activated in production")
        try:
            max(2, int(tenant.metadata.get("summary_every_events", "20")))
        except ValueError as exc:
            raise ValueError("summary_every_events must be an integer") from exc
        references = [
            reference for binding in tenant.channels for reference in binding.credential_refs.values()
        ]
        references.extend(
            profile.api_key_ref for profile in tenant.models.values() if profile.api_key_ref is not None
        )
        if tenant.audit.export_auth_ref is not None:
            references.append(tenant.audit.export_auth_ref)
        async with asyncio.timeout(self.settings.config_preflight_timeout_seconds):
            await asyncio.gather(*(self.secrets.resolve(reference) for reference in references))
            for binding in tenant.channels:
                if binding.enabled and binding.channel is ChannelType.WECOM_BOT:
                    bot_id = await self.secrets.resolve(binding.credential_refs["bot_id"])
                    bot_secret = await self.secrets.resolve(binding.credential_refs["bot_secret"])
                    validate_bot_credentials(bot_id, bot_secret)
                if binding.enabled and binding.channel is ChannelType.WECOM:
                    values = {
                        name: await self.secrets.resolve(binding.credential_refs[name])
                        for name in (
                            "callback_token",
                            "encoding_aes_key",
                            "corp_id",
                            "corp_secret",
                            "agent_id",
                        )
                    }
                    validate_wecom_credentials(**values)
            await self.storage.preflight_tenant(tenant)
            await self.engines.trpc.preflight_tenant(tenant)
