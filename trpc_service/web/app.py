"""FastAPI gateway, chat endpoint, and basic authenticated Admin API."""

from __future__ import annotations

import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy.exc import IntegrityError

from trpc_service.agent.execution import (
    AgentAppUnavailableError,
    AgentExecutionService,
    RunAgentCommand,
    TenantUnavailableError,
)
from trpc_service.agent.runtime import RunnerProvider, TenantRunnerFactory
from trpc_service.bus import (
    ExecutionBus,
    InlineExecutionBus,
    RedisExecutionBus,
    RemoteExecutionError,
)
from trpc_service.channels import ChannelMessage, ChannelProcessor, TelegramAdapter, WeComAdapter
from trpc_service.config.models import (
    AgentAppRecord,
    ChannelBindingRecord,
    ChannelMode,
    ChannelType,
    TenantRecord,
)
from trpc_service.config.secrets import SecretResolutionError, SecretResolver
from trpc_service.config.settings import (
    AppEnvironment,
    ExecutionBackend,
    ServiceSettings,
    get_settings,
)
from trpc_service.governance import InputRejectedError
from trpc_service.metrics import ServiceMetrics
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import (
    AgentAppRepository,
    ChannelBindingRepository,
    TenantRepository,
)
from trpc_service.storage.router import TenantStorageRouter
from trpc_service.telemetry import configure_telemetry
from trpc_service.tenant.session_id import SessionIdFactory
from trpc_service.version import __version__
from trpc_service.web.schemas import (
    AgentAppCreate,
    ChannelBindingCreate,
    ChatRequest,
    ChatResponse,
    TenantCreate,
    TenantStorageUpdate,
    ToolEventResponse,
)
from trpc_service.worker import WorkerService


def create_app(
    settings: ServiceSettings | None = None,
    database: Database | None = None,
    runner_provider: RunnerProvider | None = None,
    secret_resolver: SecretResolver | None = None,
    telegram_adapter: TelegramAdapter | None = None,
    wecom_adapter: WeComAdapter | None = None,
    metrics: ServiceMetrics | None = None,
) -> FastAPI:
    service_settings = settings or get_settings()
    service_database = database or Database(service_settings.database_url)
    secrets = secret_resolver or SecretResolver()
    service_metrics = metrics or ServiceMetrics()
    storage_router = TenantStorageRouter(
        service_database, secrets, artifact_root=service_settings.artifact_root
    )
    runners = runner_provider or TenantRunnerFactory(
        service_settings,
        database=service_database,
        secrets=secrets,
        storage_router=storage_router,
    )
    execution = AgentExecutionService(
        service_database,
        runners,
        storage_router=storage_router,
        metrics=service_metrics,
        timeout_seconds=service_settings.agent_timeout_seconds,
    )
    if service_settings.execution_backend == ExecutionBackend.REDIS:
        assert service_settings.queue_redis_url_ref is not None
        bus: ExecutionBus = RedisExecutionBus(
            service_database,
            secrets.resolve(service_settings.queue_redis_url_ref),
            stream=service_settings.queue_stream,
            result_timeout_seconds=service_settings.queue_result_timeout_seconds,
        )
    else:
        bus = InlineExecutionBus(WorkerService(execution))
    telegram = telegram_adapter or TelegramAdapter()
    wecom = wecom_adapter or WeComAdapter()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        configure_telemetry("trpc-agent-service", service_settings.otel_console_exporter)
        await service_database.initialize()
        if service_settings.app_env == AppEnvironment.TEST:
            session_secret = (
                secrets.resolve(service_settings.session_hmac_key_ref)
                if service_settings.session_hmac_key_ref
                else "test-only-session-hmac-key"
            )
        else:
            required_references = (
                service_settings.model_api_key_ref,
                service_settings.admin_api_key_ref,
                service_settings.session_hmac_key_ref,
            )
            resolved = [
                secrets.resolve(reference) for reference in required_references if reference
            ]
            session_secret = resolved[-1]

        application.state.settings = service_settings
        application.state.database = service_database
        application.state.session_ids = SessionIdFactory(session_secret)
        application.state.execution_bus = bus
        application.state.metrics = service_metrics
        application.state.channel_processor = ChannelProcessor(
            service_database,
            bus,
            application.state.session_ids,
            service_metrics,
        )
        try:
            yield
        finally:
            await telegram.close()
            await wecom.close()
            if isinstance(runners, TenantRunnerFactory):
                await runners.close()
            if isinstance(bus, RedisExecutionBus):
                await bus.close()
            await storage_router.close()
            await service_database.dispose()

    application = FastAPI(
        title="tRPC Agent Service",
        version=__version__,
        lifespan=lifespan,
    )

    async def require_admin_key(
        x_admin_api_key: Annotated[str | None, Header(alias="X-Admin-API-Key")] = None,
    ) -> None:
        reference = service_settings.admin_api_key_ref
        if reference is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="admin API key is not configured",
            )
        try:
            expected = secrets.resolve(reference)
        except SecretResolutionError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="admin API key is unavailable",
            ) from exc
        if x_admin_api_key is None or not hmac.compare_digest(x_admin_api_key, expected):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid API key")

    @application.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        return {
            "status": "ok",
            "version": __version__,
            "environment": service_settings.app_env.value,
        }

    @application.get("/ready", tags=["system"])
    async def ready() -> JSONResponse:
        database_ready = await service_database.ping()
        queue_ready = True
        if isinstance(bus, RedisExecutionBus):
            try:
                queue_ready = await bus.ping()
            except Exception:
                queue_ready = False
        is_ready = database_ready and queue_ready
        response_status = status.HTTP_200_OK if is_ready else status.HTTP_503_SERVICE_UNAVAILABLE
        return JSONResponse(
            {
                "status": "ready" if is_ready else "not_ready",
                "database": database_ready,
                "execution_queue": queue_ready,
            },
            status_code=response_status,
        )

    @application.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        return Response(service_metrics.render(), media_type=service_metrics.content_type)

    @application.post(
        "/admin/tenants",
        response_model=TenantRecord,
        status_code=status.HTTP_201_CREATED,
        tags=["admin"],
        dependencies=[Depends(require_admin_key)],
    )
    async def create_tenant(payload: TenantCreate) -> TenantRecord:
        try:
            record = TenantRecord(**payload.model_dump())
            return await TenantRepository(service_database).create(record)
        except IntegrityError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="tenant exists",
            ) from exc

    @application.get(
        "/admin/tenants",
        response_model=list[TenantRecord],
        tags=["admin"],
        dependencies=[Depends(require_admin_key)],
    )
    async def list_tenants() -> list[TenantRecord]:
        return await TenantRepository(service_database).list()

    @application.put(
        "/admin/tenants/{tenant_id}/storage",
        response_model=TenantRecord,
        tags=["admin"],
        dependencies=[Depends(require_admin_key)],
    )
    async def update_tenant_storage(
        tenant_id: str,
        payload: TenantStorageUpdate,
    ) -> TenantRecord:
        tenant = await TenantRepository(service_database).update_storage(
            tenant_id,
            payload.storage_config.model_dump(mode="json"),
        )
        if tenant is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="tenant not found")
        return tenant

    @application.post(
        "/admin/tenants/{tenant_id}/apps",
        response_model=AgentAppRecord,
        status_code=status.HTTP_201_CREATED,
        tags=["admin"],
        dependencies=[Depends(require_admin_key)],
    )
    async def create_agent_app(
        tenant_id: str,
        payload: AgentAppCreate,
    ) -> AgentAppRecord:
        await _require_tenant(service_database, tenant_id)
        provider = str(
            payload.model_config_data.get("provider", service_settings.model_provider)
        ).casefold()
        if service_settings.app_env != AppEnvironment.TEST and provider in {
            "test",
            "fake",
            "mock",
        }:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail="non-test environments require a real model provider",
            )
        record = AgentAppRecord(tenant_id=tenant_id, **payload.model_dump())
        try:
            return await AgentAppRepository(service_database).create(record)
        except IntegrityError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="agent app exists",
            ) from exc

    @application.get(
        "/admin/tenants/{tenant_id}/apps",
        response_model=list[AgentAppRecord],
        tags=["admin"],
        dependencies=[Depends(require_admin_key)],
    )
    async def list_agent_apps(tenant_id: str) -> list[AgentAppRecord]:
        await _require_tenant(service_database, tenant_id)
        return await AgentAppRepository(service_database).list_for_tenant(tenant_id)

    @application.post(
        "/admin/tenants/{tenant_id}/bindings",
        response_model=ChannelBindingRecord,
        status_code=status.HTTP_201_CREATED,
        tags=["admin"],
        dependencies=[Depends(require_admin_key)],
    )
    async def create_channel_binding(
        tenant_id: str,
        payload: ChannelBindingCreate,
    ) -> ChannelBindingRecord:
        await _require_tenant(service_database, tenant_id)
        if await AgentAppRepository(service_database).get(tenant_id, payload.app_id) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="agent app not found")
        record = ChannelBindingRecord(tenant_id=tenant_id, **payload.model_dump())
        try:
            return await ChannelBindingRepository(service_database).create(record)
        except IntegrityError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="channel binding exists",
            ) from exc

    @application.get(
        "/admin/tenants/{tenant_id}/bindings",
        response_model=list[ChannelBindingRecord],
        tags=["admin"],
        dependencies=[Depends(require_admin_key)],
    )
    async def list_channel_bindings(tenant_id: str) -> list[ChannelBindingRecord]:
        await _require_tenant(service_database, tenant_id)
        return await ChannelBindingRepository(service_database).list_for_tenant(tenant_id)

    @application.post("/v1/chat", response_model=ChatResponse, tags=["chat"])
    async def chat(
        payload: ChatRequest,
        x_tenant_id: Annotated[str | None, Header(alias="X-Tenant-ID")] = None,
        x_tenant_api_key: Annotated[str | None, Header(alias="X-Tenant-API-Key")] = None,
    ) -> ChatResponse:
        if not x_tenant_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="X-Tenant-ID required",
            )
        if service_settings.app_env == AppEnvironment.PRODUCTION:
            tenant = await _require_tenant(service_database, x_tenant_id)
            reference = tenant.audit_policy.get("http_api_key_ref")
            if not isinstance(reference, str):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="tenant HTTP API key is not configured",
                )
            try:
                expected = secrets.resolve(reference)
            except SecretResolutionError as exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="tenant HTTP API key is unavailable",
                ) from exc
            if x_tenant_api_key is None or not hmac.compare_digest(x_tenant_api_key, expected):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="invalid tenant API key",
                )

        session_id = application.state.session_ids.create(
            tenant_id=x_tenant_id,
            app_id=payload.app_id,
            channel=ChannelType.HTTP,
            account_id="http-api",
            principal_id=payload.user_id,
            conversation_id=payload.session_id,
        )
        command = RunAgentCommand(
            tenant_id=x_tenant_id,
            app_id=payload.app_id,
            user_id=payload.user_id,
            session_id=session_id,
            message=payload.message,
        )
        try:
            reply = await application.state.execution_bus.submit(command)
        except TenantUnavailableError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except AgentAppUnavailableError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except (SecretResolutionError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="model runtime is not configured",
            ) from exc
        except TimeoutError as exc:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="agent execution timed out",
            ) from exc
        except RemoteExecutionError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(exc),
            ) from exc

        return ChatResponse(
            tenant_id=reply.tenant_id,
            app_id=reply.app_id,
            user_id=reply.user_id,
            session_id=reply.session_id,
            trace_id=reply.trace_id,
            reply=reply.text,
            tool_events=[
                ToolEventResponse(type=item.type, name=item.name, data=item.data)
                for item in reply.tool_events
            ],
        )

    async def execute_channel_message(
        binding: ChannelBindingRecord,
        message: ChannelMessage,
    ):
        try:
            return await application.state.channel_processor.process(binding, message)
        except InputRejectedError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
            ) from exc

    @application.post("/webhooks/telegram/{account_id}", tags=["webhooks"])
    async def telegram_webhook(
        account_id: str,
        payload: Annotated[dict[str, object], Body()],
        webhook_secret: Annotated[
            str | None, Header(alias="X-Telegram-Bot-Api-Secret-Token")
        ] = None,
    ) -> dict[str, str]:
        binding = await ChannelBindingRepository(service_database).find_active(
            ChannelType.TELEGRAM, account_id, ChannelMode.WEBHOOK
        )
        if binding is None or not binding.token_ref or not binding.secret_ref:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="binding not found")
        expected_secret = secrets.resolve(binding.secret_ref)
        if not telegram.verify_secret(webhook_secret, expected_secret):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid signature"
            )
        message = telegram.parse(account_id, payload)
        if message is None:
            return {"status": "ignored"}
        result = await execute_channel_message(binding, message)
        if result.duplicate:
            return {"status": "duplicate"}
        assert result.reply is not None and result.inbound is not None
        token = secrets.resolve(binding.token_ref)
        try:
            await telegram.send(token, message, result.reply.text)
            await application.state.channel_processor.mark_delivery(
                result.inbound.inbound_id, "telegram", True
            )
        except Exception:
            await application.state.channel_processor.mark_delivery(
                result.inbound.inbound_id, "telegram", False
            )
            raise
        return {"status": "completed", "trace_id": result.reply.trace_id}

    @application.get("/webhooks/wecom/{account_id}", tags=["webhooks"])
    async def verify_wecom_webhook(
        account_id: str,
        msg_signature: Annotated[str | None, Query()] = None,
        timestamp: Annotated[str, Query()] = "",
        nonce: Annotated[str, Query()] = "",
        echostr: Annotated[str, Query()] = "",
    ) -> PlainTextResponse:
        binding = await ChannelBindingRepository(service_database).find_active(
            ChannelType.WECOM, account_id, ChannelMode.WEBHOOK
        )
        if binding is None or not binding.token_ref or not binding.aes_key_ref:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="binding not found")
        callback_token = secrets.resolve(binding.token_ref)
        if not wecom.verify_signature(msg_signature, callback_token, timestamp, nonce, echostr):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid signature"
            )
        plaintext = wecom.decrypt(echostr, secrets.resolve(binding.aes_key_ref), account_id)
        return PlainTextResponse(plaintext.decode("utf-8"))

    @application.post("/webhooks/wecom/{account_id}", tags=["webhooks"])
    async def wecom_webhook(
        account_id: str,
        request: Request,
        msg_signature: Annotated[str | None, Query()] = None,
        timestamp: Annotated[str, Query()] = "",
        nonce: Annotated[str, Query()] = "",
    ) -> PlainTextResponse:
        binding = await ChannelBindingRepository(service_database).find_active(
            ChannelType.WECOM, account_id, ChannelMode.WEBHOOK
        )
        if (
            binding is None
            or not binding.token_ref
            or not binding.secret_ref
            or not binding.aes_key_ref
        ):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="binding not found")
        body = await request.body()
        encrypted = wecom.extract_encrypted(body)
        callback_token = secrets.resolve(binding.token_ref)
        if not wecom.verify_signature(msg_signature, callback_token, timestamp, nonce, encrypted):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid signature"
            )
        plaintext = wecom.decrypt(encrypted, secrets.resolve(binding.aes_key_ref), account_id)
        message = wecom.parse(account_id, plaintext)
        if message is None:
            return PlainTextResponse("success")
        result = await execute_channel_message(binding, message)
        if not result.duplicate:
            assert result.reply is not None and result.inbound is not None
            try:
                await wecom.send(secrets.resolve(binding.secret_ref), message, result.reply.text)
                await application.state.channel_processor.mark_delivery(
                    result.inbound.inbound_id, "wecom", True
                )
            except Exception:
                await application.state.channel_processor.mark_delivery(
                    result.inbound.inbound_id, "wecom", False
                )
                raise
        return PlainTextResponse("success")

    @application.exception_handler(Exception)
    async def unhandled_error(_: Request, __: Exception) -> JSONResponse:
        return JSONResponse(
            {"error": "internal_server_error"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    return application


async def _require_tenant(database: Database, tenant_id: str) -> TenantRecord:
    tenant = await TenantRepository(database).get(tenant_id)
    if tenant is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="tenant not found")
    return tenant


app = create_app()


__all__ = ["app", "create_app"]
