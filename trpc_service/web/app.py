"""FastAPI gateway and an in-process runtime factory."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from opentelemetry import trace

from ..agent import AgentService, EchoExecutor
from ..agent.factory import create_executor_router_from_env
from ..channels import (
    ChannelDispatcher,
    FeishuAdapter,
    FeishuLongConnection,
    FeishuVerificationError,
    TelegramAdapter,
    WeComAdapter,
    WeComLongConnection,
    WeComVerificationError,
    make_session_id,
)
from ..channels.feishu import log_feishu_event
from ..config import ServiceSettings, resolve_secret
from ..metrics import configure_fastapi_instrumentation, configure_telemetry, exposition, new_trace_id
from ..metrics.prometheus import observe_operation, record_inbound
from ..queue import OutboxDispatcher, RedisStreamQueue
from ..storage import (
    InboundMediaMaterializer,
    OpenAIEmbedder,
    PgVectorStore,
    PostgresRepository,
    ProjectionCoordinator,
    RedisProjectionStore,
    RedisStateStore,
    S3ArtifactStore,
    StorageRouter,
)
from ..tenant import (
    AgentApp,
    AuditStore,
    ChannelBinding,
    InMemoryRateLimiter,
    MemoryStore,
    RateLimitExceeded,
    RedisRateLimiter,
    SessionStore,
    StorageProfile,
    TenantConfig,
    TenantPolicy,
    TenantRegistry,
)
from .admin import etag, expected_version, parse_config, public_config, require_role, validate_secret_references


class ServiceRuntime:
    def __init__(
        self,
        settings: ServiceSettings,
        registry: TenantRegistry,
        service: AgentService,
        feishu_connections: list[FeishuLongConnection] | None = None,
        wecom_connections: list[WeComLongConnection] | None = None,
        repository: PostgresRepository | None = None,
        outbox_dispatcher: OutboxDispatcher | None = None,
        storage_router: StorageRouter | None = None,
        rate_limiter: object | None = None,
    ) -> None:
        self.settings = settings
        self.registry = registry
        self.service = service
        self.feishu_connections = feishu_connections or []
        self.wecom_connections = wecom_connections or []
        self.repository = repository
        self.outbox_dispatcher = outbox_dispatcher
        self.storage_router = storage_router
        self.rate_limiter = rate_limiter or InMemoryRateLimiter()

    async def ready(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": "ok",
            "queue_depth": self.service.queue.qsize(),
            "feishu_connections": [connection.status for connection in self.feishu_connections],
            "wecom_connections": [connection.status for connection in self.wecom_connections],
        }
        if self.repository is not None:
            try:
                result["postgres"] = "ok" if await self.repository.ping() else "failed"
                if result["postgres"] != "ok":
                    result["status"] = "degraded"
                if self.settings.role in {"all", "gateway", "worker"}:
                    result["redis"] = "ok" if await self.service.queue.client.ping() else "failed"
                    if result["redis"] != "ok":
                        result["status"] = "degraded"
                    stats = getattr(self.service.queue, "stats", None)
                    if stats is not None:
                        result["queue"] = await stats()
            except Exception as exc:  # noqa: BLE001 - readiness must report dependency outages without leaking DSNs.
                result.update(status="degraded", dependency_error=type(exc).__name__)
        if any(connection.status == "failed" for connection in self.feishu_connections):
            result["status"] = "degraded"
        return result

    async def active_config(self, tenant_id: str) -> TenantConfig:
        if self.repository is None:
            return self.registry.get(tenant_id)
        config = await self.repository.get_active_tenant_config(tenant_id)
        validate_secret_references(config, production=self.settings.environment == "production")
        if self.storage_router is not None:
            self.storage_router.validate(config)
        return self.registry.activate(config)

    async def tenant_configs(self) -> list[TenantConfig]:
        if self.repository is None:
            return self.registry.list_tenants()
        values = await self.repository.list_tenant_configs()
        configs = [parse_config(value, version=int(value.get("version", 1))) for value in values]
        for config in configs:
            validate_secret_references(config, production=self.settings.environment == "production")
            if self.storage_router is not None:
                self.storage_router.validate(config)
            self.registry.activate(config)
        return configs

    async def close(self) -> None:
        await self.service.drain_projections()
        await self.service.queue.close()
        if self.repository is not None:
            await self.repository.close()

    async def ingest(
        self,
        tenant_id: str,
        channel: str,
        payload: Any,
        headers: dict[str, str],
        raw_body: bytes | None = None,
    ) -> bool | dict[str, Any]:
        payload_mapping = payload if isinstance(payload, Mapping) else {}
        header = payload_mapping.get("header")
        account_id = str(
            payload_mapping.get("account_id")
            or payload_mapping.get("bot_id")
            or (header.get("app_id") if isinstance(header, dict) else "")
            or headers.get("x-channel-account", "")
        )
        adapter = self.service.dispatcher.adapters[channel]
        config = await self.active_config(tenant_id)
        if config.status != "active":
            raise PermissionError("tenant is disabled")
        binding = config.channels.get(channel)
        if binding is None or not binding.enabled:
            raise KeyError(f"binding {tenant_id}/{channel}")
        if account_id and binding.account_id != account_id:
            raise KeyError(f"binding {tenant_id}/{channel}/{account_id}")
        if not account_id and not isinstance(adapter, (FeishuAdapter, WeComAdapter)):
            raise KeyError(f"binding account is missing for {tenant_id}/{channel}")
        candidate_trace_id = headers.get("x-trace-id", "").strip().lower()
        try:
            valid_trace_id = len(candidate_trace_id) == 32 and int(candidate_trace_id, 16) != 0
        except ValueError:
            valid_trace_id = False
        current_span = trace.get_current_span().get_span_context()
        trace_id = (
            f"{current_span.trace_id:032x}" if current_span.is_valid
            else candidate_trace_id if valid_trace_id else new_trace_id()
        )
        body = raw_body if raw_body is not None else json.dumps(payload, ensure_ascii=False).encode()
        if isinstance(adapter, FeishuAdapter):
            callback = adapter.handle_callback(tenant_id, binding, headers, body, trace_id)
            if callback.challenge is not None:
                return {"challenge": callback.challenge}
            if callback.ignored or callback.message is None:
                return {"accepted": True, "ignored": True}
            message = callback.message
        elif isinstance(adapter, WeComAdapter):
            message = adapter.handle_callback(tenant_id, binding, headers, body, trace_id)
        else:
            if not adapter.verify(binding, headers, body):
                raise PermissionError("invalid channel signature")
            message = adapter.parse(tenant_id, binding, payload, trace_id)
        return await self._enqueue(config, message)

    async def ingest_feishu_long_connection(self, tenant_id: str, payload: Mapping[str, Any]) -> bool | dict[str, Any]:
        config = await self.active_config(tenant_id)
        binding = config.channels.get("feishu")
        if binding is None or not binding.enabled:
            raise KeyError(f"binding {tenant_id}/feishu")
        adapter = self.service.dispatcher.adapters.get("feishu")
        if not isinstance(adapter, FeishuAdapter):
            raise KeyError("adapter feishu")
        callback = adapter.handle_long_connection_event(tenant_id, binding, payload, new_trace_id())
        if callback.ignored or callback.message is None:
            return {"accepted": True, "ignored": True}
        return await self._enqueue(config, callback.message)

    async def ingest_wecom_long_connection(
        self, tenant_id: str, frame: Mapping[str, Any], connection: WeComLongConnection
    ) -> bool | dict[str, Any]:
        if frame.get("cmd") != "aibot_msg_callback":
            return {"accepted": True, "ignored": True}
        body = frame.get("body")
        if not isinstance(body, Mapping):
            return {"accepted": True, "ignored": True}
        config = await self.active_config(tenant_id)
        binding = config.channels.get("wecom")
        adapter = self.service.dispatcher.adapters.get("wecom")
        if binding is None or not binding.enabled or not isinstance(adapter, WeComAdapter):
            raise KeyError(f"binding {tenant_id}/wecom")
        bot_id = str(body.get("aibotid") or body.get("bot_id") or "")
        if bot_id and bot_id != binding.account_id:
            raise PermissionError("WeCom bot id does not match tenant binding")
        msg_id = str(body.get("msgid") or "")
        if not msg_id:
            return {"accepted": True, "ignored": True}
        sender = body.get("from") if isinstance(body.get("from"), Mapping) else {}
        text_body = body.get("text") if isinstance(body.get("text"), Mapping) else {}
        payload = {
            "MsgId": msg_id,
            "FromUserName": str(sender.get("userid") or ""),
            "ChatId": str(body.get("chatid") or sender.get("userid") or ""),
            "chat_type": "group" if body.get("chattype") == "group" else "direct",
            "Content": str(text_body.get("content") or ""),
            "MsgType": str(body.get("msgtype") or "text"),
            "aibotid": bot_id,
        }
        adapter.register_bot_reply(msg_id, connection.client, frame)
        message = adapter.parse(tenant_id, binding, payload, new_trace_id())
        message = replace(message, raw={**message.raw, "reply_context": {"wecom_bot": True}})
        return await self._enqueue(config, message)

    async def _enqueue(self, config: TenantConfig, message: Any) -> bool:
        if not message.external_message_id or not message.external_user_id or not message.chat_id:
            raise ValueError("channel message identity is incomplete")
        session_id = make_session_id(
            message.tenant_id,
            message.channel,
            message.chat_id,
            message.chat_type,
            self.settings.session_hmac_key,
        )
        message = replace(message, session_id=session_id, config_version=config.version)
        # Do not trust an app_id supplied by an external platform unless it is
        # present in the tenant revision.
        if message.app_id not in config.apps:
            message = replace(message, app_id=next(iter(config.apps)))
        if await self.service.already_received(message):
            self.service.metrics.inc("inbound_duplicate", tenant=message.tenant_id, channel=message.channel)
            record_inbound(message.tenant_id, message.channel, "duplicate")
            if message.channel == "feishu":
                log_feishu_event(
                    "enqueued",
                    tenant_id=message.tenant_id,
                    account_id=message.account_id,
                    trace_id=message.trace_id,
                    message_id=message.external_message_id,
                    session_id=message.session_id,
                    result="duplicate",
                )
            return False
        request_key = self.service.idempotency_key(message)
        if not await self.rate_limiter.allow(
            config.tenant_id,
            config.policy.requests_per_minute,
            request_key=request_key,
        ):
            raise RateLimitExceeded("tenant request rate exceeded")
        accepted = await self.service.enqueue(message)
        if message.channel == "feishu":
            log_feishu_event(
                "enqueued",
                tenant_id=message.tenant_id,
                account_id=message.account_id,
                trace_id=message.trace_id,
                message_id=message.external_message_id,
                session_id=message.session_id,
                result="accepted" if accepted else "duplicate",
            )
        return accepted


def build_demo_runtime(settings: ServiceSettings | None = None, *, local_demo: bool = False) -> ServiceRuntime:
    settings = settings or ServiceSettings.from_env()
    if settings.role not in {"all", "gateway", "worker", "admin"}:
        raise ValueError("TRPC_SERVICE_ROLE must be all, gateway, worker, or admin")
    if settings.environment == "production":
        if settings.backend != "postgres-redis":
            raise ValueError("production requires TRPC_SERVICE_BACKEND=postgres-redis")
        if settings.auto_migrate:
            raise ValueError("production migrations must run through the dedicated migrate role")
        if settings.role in {"all", "admin"} and not settings.admin_token:
            raise ValueError("production requires TRPC_SERVICE_ADMIN_TOKEN_REF")
        if settings.role in {"all", "gateway", "admin"} and not settings.control_database_url:
            raise ValueError("production requires TRPC_SERVICE_CONTROL_DATABASE_URL")
        if settings.role in {"all", "gateway", "worker"} and (
            settings.session_hmac_key == "development-only-change-me" or len(settings.session_hmac_key) < 32
        ):
            raise ValueError("production requires a strong TRPC_SERVICE_SESSION_HMAC_KEY")
        if settings.role in {"all", "worker"} and not (
            os.getenv("TRPC_AGENT_API_KEY") and os.getenv("TRPC_AGENT_MODEL_NAME")
        ):
            raise ValueError("production worker requires TRPC_AGENT_API_KEY and TRPC_AGENT_MODEL_NAME")
        for name in ("TRPC_AGENT_BASE_URL", "TRPC_AGENT_FALLBACK_BASE_URL"):
            endpoint = os.getenv(name)
            if endpoint and not endpoint.startswith("https://"):
                raise ValueError(f"production model endpoint {name} must use HTTPS")
        process_secret_refs = {
            name: os.getenv(name)
            for name in (
                "TRPC_SERVICE_ADMIN_TOKEN_REF",
                "TRPC_SERVICE_ADMIN_OPERATOR_TOKEN_REF",
                "TRPC_SERVICE_ADMIN_VIEWER_TOKEN_REF",
                "TRPC_SERVICE_FEISHU_APP_SECRET_REF",
                "TRPC_SERVICE_FEISHU_VERIFICATION_TOKEN_REF",
                "TRPC_SERVICE_FEISHU_ENCRYPT_KEY_REF",
                "TRPC_SERVICE_TELEGRAM_BOT_TOKEN_REF",
                "TRPC_SERVICE_WECOM_SECRET_REF",
                "TRPC_SERVICE_WECOM_ENCODING_AES_KEY_REF",
                "TRPC_SERVICE_WECOM_BOT_SECRET_REF",
            )
        }
        invalid_ref = next(
            (
                name
                for name, reference in process_secret_refs.items()
                if reference and not reference.startswith(("env://", "file://"))
            ),
            None,
        )
        if invalid_ref:
            raise ValueError(f"production secret {invalid_ref} must use env:// or file://")
        role_tokens = settings.admin_role_tokens()
        if len(role_tokens.values()) != len(set(role_tokens.values())):
            raise ValueError("production admin role tokens must be distinct")
    common_policy = TenantPolicy(
        allowed_tools=frozenset({"weather", "delete_ticket"}),
        dangerous_tools=frozenset({"delete_ticket"}),
    )
    default_storage = (
        StorageProfile(session="postgres", memory="postgres", audit="postgres")
        if settings.backend == "postgres-redis"
        else StorageProfile()
    )
    demo_configs = [
        TenantConfig(
            tenant_id="acme",
            name="Acme 客服",
            apps={"default": AgentApp("default", "acme-assistant")},
            channels={
                "wecom": ChannelBinding("wecom", "acme-bot", "acme-dev-token"),
                "telegram": ChannelBinding("telegram", "acme-telegram", "acme-telegram-secret"),
            },
            policy=common_policy,
            storage=default_storage,
        ),
        TenantConfig(
            tenant_id="globex",
            name="Globex 研发",
            apps={"default": AgentApp("default", "globex-assistant")},
            channels={"telegram": ChannelBinding("telegram", "globex-telegram", "globex-telegram-secret")},
            policy=TenantPolicy(allowed_tools=frozenset()),
            storage=default_storage,
        ),
    ]
    configs = demo_configs if settings.environment != "production" else []
    receives_callbacks = settings.role in {"all", "gateway"}
    feishu_mode = (
        "webhook" if local_demo else os.getenv("TRPC_SERVICE_FEISHU_CONNECTION_MODE", "webhook").strip().lower()
    )
    if feishu_mode not in {"webhook", "websocket", "both"}:
        raise ValueError("TRPC_SERVICE_FEISHU_CONNECTION_MODE must be webhook, websocket, or both")
    feishu_app_id = None if local_demo else os.getenv("TRPC_SERVICE_FEISHU_APP_ID")
    feishu_app_secret_ref = None if local_demo else os.getenv("TRPC_SERVICE_FEISHU_APP_SECRET_REF")
    feishu_verification_token_ref = None if local_demo else os.getenv("TRPC_SERVICE_FEISHU_VERIFICATION_TOKEN_REF")
    if receives_callbacks and feishu_mode in {"websocket", "both"} and not feishu_app_id:
        raise ValueError("TRPC_SERVICE_FEISHU_APP_ID is required in websocket mode")
    if receives_callbacks and feishu_app_id:
        if not feishu_app_secret_ref:
            raise ValueError("TRPC_SERVICE_FEISHU_APP_SECRET_REF is required")
        if feishu_mode in {"webhook", "both"} and not feishu_verification_token_ref:
            raise ValueError("TRPC_SERVICE_FEISHU_VERIFICATION_TOKEN_REF is required in webhook mode")
        feishu_tenant_id = os.getenv("TRPC_SERVICE_FEISHU_TENANT_ID", "acme")
        feishu_tenant = next((config for config in configs if config.tenant_id == feishu_tenant_id), None)
        if feishu_tenant is None and settings.environment != "production":
            raise ValueError(f"unknown Feishu tenant: {feishu_tenant_id}")
        if feishu_tenant is not None:
            feishu_tenant.channels["feishu"] = ChannelBinding(
                channel="feishu",
                account_id=feishu_app_id,
                verify_token=feishu_verification_token_ref,
                secret_ref=feishu_app_secret_ref,
                encrypt_key_ref=os.getenv("TRPC_SERVICE_FEISHU_ENCRYPT_KEY_REF"),
                api_base_url=os.getenv("TRPC_SERVICE_FEISHU_API_BASE_URL"),
            )
    if settings.environment == "production":
        references = (
            value
            for config in configs
            for binding in config.channels.values()
            for value in (binding.verify_token, binding.secret_ref, binding.encrypt_key_ref)
        )
        if any(value and value.startswith("literal://") for value in references):
            raise ValueError("literal:// secrets are forbidden in production")
    registry = TenantRegistry(configs)
    telegram_token_ref = None if local_demo else os.getenv("TRPC_SERVICE_TELEGRAM_BOT_TOKEN_REF")
    wecom_secret_ref = None if local_demo else os.getenv("TRPC_SERVICE_WECOM_SECRET_REF")
    if telegram_token_ref and configs:
        configs[0].channels["telegram"] = replace(
            configs[0].channels["telegram"],
            secret_ref=telegram_token_ref,
            api_base_url=os.getenv("TRPC_SERVICE_TELEGRAM_API_BASE_URL"),
        )
    if wecom_secret_ref and configs:
        configs[0].channels["wecom"] = replace(
            configs[0].channels["wecom"],
            secret_ref=wecom_secret_ref,
            encrypt_key_ref=os.getenv("TRPC_SERVICE_WECOM_ENCODING_AES_KEY_REF"),
            corp_id=os.getenv("TRPC_SERVICE_WECOM_CORP_ID"),
            agent_id=os.getenv("TRPC_SERVICE_WECOM_AGENT_ID"),
            api_base_url=os.getenv("TRPC_SERVICE_WECOM_API_BASE_URL"),
        )
    dry_run_default = settings.backend == "inmemory" and not (telegram_token_ref or wecom_secret_ref)
    dry_run = local_demo or os.getenv("TRPC_SERVICE_IM_DRY_RUN", str(dry_run_default)).lower() in {
        "1",
        "true",
        "yes",
    }
    if settings.environment == "production" and dry_run:
        raise ValueError("TRPC_SERVICE_IM_DRY_RUN is forbidden in production")
    adapters = {
        "wecom": WeComAdapter(dry_run=dry_run),
        "telegram": TelegramAdapter(dry_run=dry_run),
        "feishu": FeishuAdapter(),
    }
    dispatcher = ChannelDispatcher(
        adapters,
        binding_resolver=lambda tenant_id, channel, account_id: registry.resolve_binding(
            tenant_id, channel, account_id
        )[1],
    )
    repository = None
    outbox_dispatcher = None
    storage_router = StorageRouter()
    rate_limiter: object = InMemoryRateLimiter()
    if settings.backend == "inmemory":
        session_store = SessionStore()
        memory_store = MemoryStore()
        audit_store = AuditStore()
        storage_router.register("inmemory", session_store, kind="session")
        storage_router.register("inmemory", memory_store, kind="memory")
        storage_router.register("inmemory", audit_store, kind="audit")
        artifact_bucket = None if local_demo else os.getenv("TRPC_SERVICE_ARTIFACT_BUCKET")
        if artifact_bucket:
            storage_router.register(
                "s3",
                S3ArtifactStore(artifact_bucket, endpoint_url=os.getenv("TRPC_SERVICE_ARTIFACT_ENDPOINT_URL")),
                kind="object_store",
            )
        projector = ProjectionCoordinator(storage_router)
        media_materializer = InboundMediaMaterializer(storage_router)
        service = AgentService(
            registry,
            dispatcher,
            executor=(
                EchoExecutor()
                if local_demo
                else create_executor_router_from_env(configs, storage_router=storage_router)
            ),
            queue_size=settings.max_queue_size,
            sessions=session_store,
            memories=memory_store,
            audits=audit_store,
            max_delivery_attempts=settings.max_delivery_attempts,
            storage_router=storage_router,
            media_materializer=media_materializer,
            projector=projector,
        )
    elif settings.backend == "postgres-redis" and settings.role == "admin":
        control_url = settings.control_database_url or settings.database_url
        if not control_url:
            raise ValueError("admin role requires TRPC_SERVICE_CONTROL_DATABASE_URL")
        repository = PostgresRepository.from_dsn(control_url)
        storage_router.register("postgres", repository, kind="session")
        storage_router.register("postgres", repository, kind="memory")
        storage_router.register("postgres", repository, kind="audit")
        storage_router.register("redis", object(), kind="session")
        storage_router.register("redis", object(), kind="memory")
        storage_router.register("pgvector", object(), kind="vector")
        storage_router.register("s3", object(), kind="object_store")
        service = AgentService(
            registry,
            dispatcher,
            executor=EchoExecutor(),
            sessions=repository,
            memories=repository,
            audits=repository,
            idempotency=repository,
            storage_router=storage_router,
        )
    elif settings.backend == "postgres-redis":
        if not settings.database_url or not settings.redis_url:
            raise ValueError("gateway and worker roles require TRPC_SERVICE_DATABASE_URL and TRPC_SERVICE_REDIS_URL")
        repository = PostgresRepository.from_dsn(settings.database_url, control_dsn=settings.control_database_url)
        queue = RedisStreamQueue(
            settings.redis_url,
            reclaim_after_ms=settings.queue_reclaim_ms,
            stream_maxlen=settings.queue_stream_maxlen,
            dlq_maxlen=settings.queue_dlq_maxlen,
        )
        rate_limiter = RedisRateLimiter(queue.client)
        redis_state = RedisStateStore(settings.redis_url, client=queue.client, projection_sink=repository)
        storage_router.register("postgres", repository, kind="session")
        storage_router.register("postgres", repository, kind="memory")
        storage_router.register("postgres", repository, kind="audit")
        storage_router.register("redis", redis_state, kind="session")
        storage_router.register("redis", redis_state, kind="memory")
        vector_dimensions = int(os.getenv("TRPC_SERVICE_VECTOR_DIMENSIONS", "1536"))
        embedding_model = os.getenv("TRPC_AGENT_EMBEDDING_MODEL")
        embedding_key = os.getenv("TRPC_AGENT_API_KEY")
        if embedding_model and embedding_key:
            storage_router.register("pgvector", PgVectorStore(repository, vector_dimensions), kind="vector")
        artifact_bucket = os.getenv("TRPC_SERVICE_ARTIFACT_BUCKET")
        if artifact_bucket:
            storage_router.register(
                "s3",
                S3ArtifactStore(artifact_bucket, endpoint_url=os.getenv("TRPC_SERVICE_ARTIFACT_ENDPOINT_URL")),
                kind="object_store",
            )
        embed = (
            OpenAIEmbedder(
                embedding_model,
                embedding_key,
                base_url=os.getenv("TRPC_AGENT_BASE_URL"),
                dimensions=vector_dimensions,
            )
            if embedding_model and embedding_key
            else None
        )
        projector = ProjectionCoordinator(
            storage_router,
            embed=embed,
            cache_store=RedisProjectionStore(settings.redis_url, client=queue.client),
        )
        media_materializer = InboundMediaMaterializer(storage_router, repository=repository)
        service = AgentService(
            registry,
            dispatcher,
            executor=(
                create_executor_router_from_env(
                    configs,
                    storage_router=storage_router,
                    repository=repository,
                )
                if settings.role in {"all", "worker"}
                else EchoExecutor()
            ),
            sessions=repository,
            memories=repository,
            audits=repository,
            idempotency=repository,
            budgets=repository,
            config_loader=repository.get_tenant_config,
            queue=queue,
            max_delivery_attempts=settings.max_delivery_attempts,
            storage_router=storage_router,
            media_materializer=media_materializer,
            projector=projector,
            knowledge_retriever=projector,
        )
        dispatcher.delivery_store = repository

        async def project_outbox(record: Any) -> None:
            payload = record.payload
            config = await repository.get_tenant_config(record.tenant_id, int(payload["config_version"]))
            await projector.project(
                config,
                record.tenant_id,
                str(payload["session_id"]),
                int(payload["source_version"]),
            )

        async def index_knowledge_outbox(record: Any) -> None:
            payload = record.payload
            config = await repository.get_tenant_config(record.tenant_id, int(payload["config_version"]))
            await projector.index_knowledge(
                config,
                record.tenant_id,
                str(payload["collection"]),
                str(payload["item_id"]),
                str(payload["content"]),
                int(payload["source_version"]),
            )
            await repository.mark_knowledge_indexed(
                record.tenant_id,
                str(payload["collection"]),
                str(payload["item_id"]),
                int(payload["source_version"]),
            )

        outbox_dispatcher = OutboxDispatcher(
            repository,
            queue,
            f"outbox-{os.getpid()}",
            handlers={
                "session.project": project_outbox,
                "knowledge.index": index_knowledge_outbox,
            },
        )
        service.projector = None
    else:
        raise ValueError("TRPC_SERVICE_BACKEND must be inmemory or postgres-redis")
    runtime = ServiceRuntime(
        settings,
        registry,
        service,
        repository=repository,
        outbox_dispatcher=outbox_dispatcher,
        storage_router=storage_router,
        rate_limiter=rate_limiter,
    )
    wecom_bot_id = None if local_demo else os.getenv("TRPC_SERVICE_WECOM_BOT_ID", "").strip() or None
    wecom_bot_secret_ref = None if local_demo else os.getenv("TRPC_SERVICE_WECOM_BOT_SECRET_REF")
    wecom_bot_tenant_id = os.getenv("TRPC_SERVICE_WECOM_BOT_TENANT_ID", "acme")
    if receives_callbacks and wecom_bot_id:
        if settings.role != "all":
            raise ValueError(
                "WeCom BotID/Secret mode currently requires TRPC_SERVICE_ROLE=all so the same process can receive and reply"
            )
        if not wecom_bot_secret_ref:
            raise ValueError("TRPC_SERVICE_WECOM_BOT_SECRET_REF is required with TRPC_SERVICE_WECOM_BOT_ID")
        tenant = next((config for config in configs if config.tenant_id == wecom_bot_tenant_id), None)
        if tenant is not None:
            tenant.channels["wecom"] = replace(
                tenant.channels["wecom"],
                account_id=wecom_bot_id,
                secret_ref=wecom_bot_secret_ref,
                verify_token=None,
            )
    wecom_connections: list[WeComLongConnection] = []
    if receives_callbacks and wecom_bot_id:
        bot_secret = resolve_secret(wecom_bot_secret_ref)
        if not bot_secret:
            raise ValueError("WeCom bot Secret is empty")
        wecom_connections.append(
            WeComLongConnection(
                wecom_bot_id,
                bot_secret,
                wecom_bot_tenant_id,
                runtime.ingest_wecom_long_connection,
                ws_url=os.getenv("TRPC_SERVICE_WECOM_BOT_WS_URL", "wss://openws.work.weixin.qq.com"),
            )
        )
    runtime.wecom_connections = wecom_connections
    if wecom_connections:
        adapters["wecom"].dry_run = False
    if receives_callbacks and feishu_app_id and feishu_mode in {"websocket", "both"}:
        app_secret = resolve_secret(feishu_app_secret_ref)
        if not app_secret:
            raise ValueError("Feishu App Secret is empty")
        runtime.feishu_connections.append(
            FeishuLongConnection(
                feishu_app_id,
                app_secret,
                feishu_tenant_id,
                runtime.ingest_feishu_long_connection,
                domain=os.getenv("TRPC_SERVICE_FEISHU_API_BASE_URL", "https://open.feishu.cn"),
                event_buffer_size=settings.max_queue_size,
                log_level=os.getenv("TRPC_SERVICE_FEISHU_LOG_LEVEL", "WARNING"),
            )
        )
    return runtime


def create_app(runtime: ServiceRuntime | None = None) -> Any:
    """Create the HTTP gateway and its background Agent workers."""
    runtime = runtime or build_demo_runtime()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        import asyncio

        stop = asyncio.Event()
        if runtime.repository is not None and runtime.settings.auto_migrate:
            await runtime.repository.migrate()
        if runtime.repository is not None and runtime.settings.role in {"all", "gateway", "admin"}:
            stored_configs = await runtime.repository.list_tenant_configs()
            if stored_configs:
                loaded = [parse_config(value, version=int(value.get("version", 1))) for value in stored_configs]
                for config in loaded:
                    validate_secret_references(config, production=runtime.settings.environment == "production")
                    if runtime.storage_router is not None:
                        runtime.storage_router.validate(config)
                runtime.registry.replace_all(loaded)
            else:
                for config in runtime.registry.list_tenants():
                    await runtime.repository.save_tenant_config(config)
        background = []
        if runtime.outbox_dispatcher is not None and runtime.settings.role in {"all", "gateway"}:
            background.append(asyncio.create_task(runtime.outbox_dispatcher.run(stop), name="outbox-dispatcher"))
        workers = []
        if runtime.settings.role in {"all", "worker"}:
            workers = [
                asyncio.create_task(runtime.service.worker_loop(stop), name=f"agent-worker-{index}")
                for index in range(runtime.settings.worker_count)
            ]
        loop = asyncio.get_running_loop()
        if runtime.settings.role in {"all", "gateway"}:
            for connection in runtime.feishu_connections:
                connection.start(loop)
        wecom_tasks = []
        if runtime.settings.role in {"all", "gateway"}:
            wecom_tasks = [
                asyncio.create_task(connection.run(), name=f"wecom-ws-{connection.tenant_id}")
                for connection in runtime.wecom_connections
            ]
        try:
            yield
        finally:
            for connection in runtime.feishu_connections:
                await asyncio.to_thread(connection.stop)
            for connection in runtime.wecom_connections:
                await connection.stop()
            stop.set()
            await asyncio.gather(*workers, *background, *wecom_tasks, return_exceptions=True)
            await runtime.close()

    app = FastAPI(
        title="tRPC-Agent multi-tenant service",
        version="0.1.0",
        lifespan=lifespan,
    )
    configure_telemetry(runtime.settings.service_name, runtime.settings.otlp_endpoint)
    configure_fastapi_instrumentation(app)

    def validate_activation(config: TenantConfig) -> None:
        validate_secret_references(config, production=runtime.settings.environment == "production")
        if runtime.storage_router is not None:
            try:
                runtime.storage_router.validate(config)
            except KeyError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

    def require_admin_role() -> None:
        if runtime.settings.role not in {"all", "admin"}:
            raise HTTPException(status_code=503, detail="admin API is disabled on this role")

    def authorize(request: Request, roles: set[str]) -> str:
        return require_role(
            request,
            runtime.settings.admin_token,
            roles,
            runtime.settings.admin_role_tokens(),
        )

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> Any:
        from fastapi.responses import JSONResponse

        result = await runtime.ready()
        if result["status"] != "ok":
            return JSONResponse(result, status_code=503)
        return result

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        payload, content_type = exposition()
        return Response(payload, media_type=content_type)

    @app.post("/webhook/{tenant_id}/{channel}")
    async def webhook(tenant_id: str, channel: str, request: Request) -> dict[str, Any]:
        if runtime.settings.role not in {"all", "gateway"}:
            raise HTTPException(status_code=503, detail="webhook is disabled on this role")
        raw = await request.body()
        if len(raw) > 2 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="callback body is too large")
        try:
            payload = raw if channel == "wecom" and raw.lstrip().startswith(b"<") else json.loads(raw.decode())
        except Exception as exc:
            raise HTTPException(status_code=400, detail="payload must be valid JSON or WeCom XML") from exc
        headers = {key.lower(): value for key, value in request.headers.items()}
        headers.update({key.lower(): value for key, value in request.query_params.items()})
        try:
            with observe_operation("callback", tenant_id, channel):
                result = await runtime.ingest(tenant_id, channel, payload, headers, raw_body=raw)
        except PermissionError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        except FeishuVerificationError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        except WeComVerificationError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        except RateLimitExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc), headers={"Retry-After": "60"}) from exc
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if isinstance(result, dict):
            return result
        return {"accepted": result, "duplicate": not result}

    @app.get("/webhook/{tenant_id}/wecom")
    async def wecom_verify(tenant_id: str, request: Request) -> Any:
        from fastapi.responses import PlainTextResponse

        if runtime.settings.role not in {"all", "gateway"}:
            raise HTTPException(status_code=503, detail="webhook is disabled on this role")
        config = await runtime.active_config(tenant_id)
        binding = config.channels.get("wecom")
        if binding is None:
            raise HTTPException(status_code=404, detail="WeCom binding not found")
        adapter = runtime.service.dispatcher.adapters.get("wecom")
        if not isinstance(adapter, WeComAdapter):
            raise HTTPException(status_code=404, detail="WeCom adapter not found")
        headers = {key.lower(): value for key, value in request.query_params.items()}
        echo = request.query_params.get("echostr", "")
        try:
            value = adapter.verify_url(binding, headers, echo)
        except WeComVerificationError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        return PlainTextResponse(value)

    @app.get("/admin/tenants")
    async def tenants(request: Request) -> list[dict[str, Any]]:
        require_admin_role()
        authorize(request, {"viewer", "operator", "admin"})
        return [public_config(item) for item in await runtime.tenant_configs()]

    @app.get("/admin/tenants/{tenant_id}")
    async def tenant(tenant_id: str, request: Request, response: Response) -> dict[str, Any]:
        require_admin_role()
        authorize(request, {"viewer", "operator", "admin"})
        config = await runtime.active_config(tenant_id)
        response.headers["ETag"] = etag(config)
        return public_config(config)

    @app.post("/admin/tenants", status_code=201)
    async def create_tenant(request: Request, response: Response) -> dict[str, Any]:
        require_admin_role()
        authorize(request, {"admin"})
        config = parse_config(await request.json())
        validate_activation(config)
        if runtime.repository is not None:
            if not await runtime.repository.create_tenant_config(config):
                raise HTTPException(status_code=409, detail="tenant already exists")
            runtime.registry.activate(config)
        else:
            try:
                runtime.registry.get(config.tenant_id)
            except KeyError:
                runtime.registry.register(config)
            else:
                raise HTTPException(status_code=409, detail="tenant already exists")
        response.headers["ETag"] = etag(config)
        return public_config(config)

    @app.put("/admin/tenants/{tenant_id}")
    async def update_tenant(tenant_id: str, request: Request, response: Response) -> dict[str, Any]:
        require_admin_role()
        authorize(request, {"admin"})
        version = expected_version(request)
        payload = await request.json()
        payload["tenant_id"] = tenant_id
        try:
            current = await runtime.active_config(tenant_id)
            candidate = parse_config(payload, version=version, existing=current)
            validate_activation(candidate)
            next_version = (
                await runtime.repository.next_tenant_config_version(tenant_id)
                if runtime.repository is not None
                else version + 1
            )
            config = replace(candidate, version=next_version)
        except ValueError as exc:
            raise HTTPException(status_code=412, detail=str(exc)) from exc
        if runtime.repository is not None:
            if not await runtime.repository.publish_tenant_config(config, version):
                raise HTTPException(status_code=412, detail="configuration conflict")
            runtime.registry.activate(config)
        else:
            try:
                config = runtime.registry.publish(candidate, expected_version=version)
            except ValueError as exc:
                raise HTTPException(status_code=412, detail=str(exc)) from exc
        response.headers["ETag"] = etag(config)
        return public_config(config)

    @app.post("/admin/tenants/{tenant_id}/rollback/{version}")
    async def rollback_tenant(tenant_id: str, version: int, request: Request, response: Response) -> dict[str, Any]:
        require_admin_role()
        authorize(request, {"operator", "admin"})
        current_version = expected_version(request)
        try:
            current = await runtime.active_config(tenant_id)
            if current.version != current_version:
                raise ValueError("configuration conflict")
            candidate = (
                await runtime.repository.get_tenant_config(tenant_id, version)
                if runtime.repository is not None
                else runtime.registry.get(tenant_id, version)
            )
            validate_activation(candidate)
            if runtime.repository is not None:
                if not await runtime.repository.activate_tenant_revision(tenant_id, version, current_version):
                    raise ValueError("configuration conflict")
                config = runtime.registry.activate(candidate)
            else:
                config = runtime.registry.rollback(tenant_id, version)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="tenant revision not found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=412, detail=str(exc)) from exc
        response.headers["ETag"] = etag(config)
        return public_config(config)

    @app.delete("/admin/tenants/{tenant_id}", status_code=204)
    async def delete_tenant(tenant_id: str, request: Request) -> Response:
        require_admin_role()
        authorize(request, {"admin"})
        version = expected_version(request)
        await runtime.active_config(tenant_id)
        if runtime.repository is not None and not await runtime.repository.delete_tenant_config(tenant_id, version):
            raise HTTPException(status_code=412, detail="configuration conflict")
        try:
            runtime.registry.delete(tenant_id, version)
        except ValueError as exc:
            raise HTTPException(status_code=412, detail=str(exc)) from exc
        return Response(status_code=204)

    @app.get("/admin/audit/{tenant_id}")
    async def audit(tenant_id: str, request: Request, limit: int = 100) -> list[dict[str, Any]]:
        require_admin_role()
        authorize(request, {"viewer", "operator", "admin"})
        config = await runtime.active_config(tenant_id)
        if not config.audit.allow_export:
            raise HTTPException(status_code=403, detail="tenant audit export is disabled")
        if runtime.repository is not None:
            return await runtime.repository.query_audit(tenant_id, limit)
        return [record for record in runtime.service.audits.records if record.get("tenant_id") == tenant_id][
            -max(1, min(limit, 1000)) :
        ]

    @app.post("/admin/audit/{tenant_id}/purge")
    async def purge_audit(tenant_id: str, request: Request) -> dict[str, Any]:
        require_admin_role()
        authorize(request, {"operator", "admin"})
        config = await runtime.active_config(tenant_id)
        if runtime.repository is None:
            raise HTTPException(status_code=503, detail="durable audit retention requires PostgreSQL")
        deleted = await runtime.repository.purge_expired_audit(tenant_id, config.audit.retention_days)
        return {"deleted": deleted, "retention_days": config.audit.retention_days}

    @app.get("/admin/outbox/{tenant_id}/dead-letters")
    async def outbox_dead_letters(tenant_id: str, request: Request, limit: int = 100) -> list[dict[str, Any]]:
        require_admin_role()
        authorize(request, {"viewer", "operator", "admin"})
        if runtime.repository is None:
            return []
        return await runtime.repository.query_outbox_dead_letters(tenant_id, limit)

    @app.post("/admin/outbox/{tenant_id}/dead-letters/{outbox_id}/replay", status_code=202)
    async def replay_outbox(tenant_id: str, outbox_id: str, request: Request) -> dict[str, Any]:
        require_admin_role()
        authorize(request, {"operator", "admin"})
        if runtime.repository is None or not await runtime.repository.replay_outbox(tenant_id, outbox_id):
            raise HTTPException(status_code=404, detail="outbox dead letter not found")
        return {"accepted": True, "outbox_id": outbox_id}

    @app.get("/admin/inbox/{tenant_id}/failed")
    async def failed_inbound(tenant_id: str, request: Request, limit: int = 100) -> list[dict[str, Any]]:
        require_admin_role()
        authorize(request, {"viewer", "operator", "admin"})
        if runtime.repository is None:
            return []
        return await runtime.repository.query_failed_inbound(tenant_id, limit)

    @app.post("/admin/inbox/{tenant_id}/{inbox_key:path}/replay", status_code=202)
    async def replay_inbound(tenant_id: str, inbox_key: str, request: Request) -> dict[str, Any]:
        require_admin_role()
        authorize(request, {"operator", "admin"})
        if runtime.repository is None or not await runtime.repository.replay_inbound(tenant_id, inbox_key):
            raise HTTPException(status_code=404, detail="failed Inbox message not found")
        return {"accepted": True, "inbox_key": inbox_key}

    @app.put("/admin/knowledge/{tenant_id}/{collection}/{item_id}", status_code=202)
    async def put_knowledge(tenant_id: str, collection: str, item_id: str, request: Request) -> dict[str, Any]:
        require_admin_role()
        authorize(request, {"operator", "admin"})
        config = await runtime.active_config(tenant_id)
        allowed = set().union(*(app.knowledge_collections for app in config.apps.values()))
        if collection not in allowed:
            raise HTTPException(status_code=422, detail="knowledge collection is not enabled by the tenant")
        payload = await request.json()
        content = str(payload.get("content", "")).strip()
        if not item_id or len(item_id) > 256 or not content or len(content) > 1_000_000:
            raise HTTPException(status_code=422, detail="knowledge item id or content is invalid")
        if runtime.repository is None:
            raise HTTPException(status_code=503, detail="durable knowledge indexing requires PostgreSQL")
        source_version = await runtime.repository.upsert_knowledge_document(
            tenant_id,
            collection,
            item_id,
            content,
            config.version,
            new_trace_id(),
        )
        return {
            "accepted": True,
            "tenant_id": tenant_id,
            "collection": collection,
            "item_id": item_id,
            "source_version": source_version,
        }

    @app.get("/admin/knowledge/{tenant_id}/{collection}")
    async def list_knowledge(
        tenant_id: str, collection: str, request: Request, limit: int = 100
    ) -> list[dict[str, Any]]:
        require_admin_role()
        authorize(request, {"viewer", "operator", "admin"})
        await runtime.active_config(tenant_id)
        if runtime.repository is None:
            return []
        return await runtime.repository.list_knowledge_documents(tenant_id, collection, limit)

    return app
