"""Optional FastAPI application for Admin, Webhook, and debug endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from base64 import b64decode
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import Request as UrlRequest
from urllib.request import urlopen
from uuid import uuid4

from trpc_service.admin.auth import AdminAuthenticationError, authenticate, authorize
from trpc_service.admin.service import AdminService
from trpc_service.agent.bridge import build_runtime_workers
from trpc_service.channels import default_channel_adapters
from trpc_service.channels.base import (
    Attachment,
    ChannelVerificationError,
    OutboundMessage,
    SendResult,
    WebhookPayloadError,
    parse_webhook_body,
    sanitize_event_payload,
)
from trpc_service.channels.outbound import build_outbound_messages, split_outbound_messages, visible_answer
from trpc_service.channels.outbound_queue import OutboundDeliveryQueue
from trpc_service.channels.reliable import ChannelRateLimiter, persist_dead_letter, send_with_retry
from trpc_service.channels.wecom_ai_bot import WeComAIBotConnector
from trpc_service.gateway import AgentGateway
from trpc_service.gateway.session_id import build_idempotency_key
from trpc_service.gateway.worker_queue import DurableWebhookQueue, WorkerQueue
from trpc_service.policy.quota import QuotaEnforcer, QuotaExceeded
from trpc_service.policy.tenant_filter import PolicyDenied, TenantPolicy
from trpc_service.security.secrets import SecretManager, redact_secret_data, redact_secret_text
from trpc_service.security.ssrf import validate_outbound_url
from trpc_service.storage.base import AuditRecord
from trpc_service.storage.compensation import replay_compensations
from trpc_service.storage.factory import create_storage
from trpc_service.storage.manager import TenantStorageManager
from trpc_service.telemetry.metrics import CONTENT_TYPE_LATEST, generate_latest, observe_delivery, observe_error
from trpc_service.telemetry.tracing import TraceRecorder
from trpc_service.tenant.models import TenantConfig, TenantContext
from trpc_service.tenant.repository import (
    TenantNotFound,
    TenantRepositoryError,
    persistent_demo_repository,
)
from trpc_service.tenant.service import TenantConfigConflict, TenantService

try:
    from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, Response
    from fastapi.responses import HTMLResponse, PlainTextResponse
except ImportError:  # pragma: no cover
    BackgroundTasks = Depends = FastAPI = HTTPException = Request = Response = None
    HTMLResponse = PlainTextResponse = None


logger = logging.getLogger(__name__)


def create_runtime() -> tuple[AgentGateway, AdminService]:
    repository = persistent_demo_repository(os.getenv("TENANT_DB_PATH", "data/tenant_config.sqlite3"))
    tenants = TenantService(repository)
    storage = create_storage()
    worker_queue = None
    if os.getenv("WORKER_QUEUE_URL") and os.getenv("WORKER_REMOTE", "1") == "1":
        worker_queue = WorkerQueue(os.getenv("WORKER_QUEUE_URL"))
    telemetry = TraceRecorder()
    gateway = AgentGateway(
        tenants,
        storage,
        workers=build_runtime_workers(storage, telemetry),
        telemetry=telemetry,
        storage_manager=TenantStorageManager(),
        worker_queue=worker_queue,
        quota=QuotaEnforcer(os.getenv("REDIS_URL")),
    )
    return gateway, AdminService(tenants)


def create_app():
    if FastAPI is None:
        raise RuntimeError("FastAPI is optional. Install requirements.txt to run the HTTP service.")

    previous_app = globals().get("app")
    if previous_app is not None and not getattr(previous_app.state, "runtime_started", False):
        previous_close = getattr(previous_app.state, "close_runtime_resources", None)
        if previous_close is not None:
            previous_close()

    gateway, admin = create_runtime()
    adapters = default_channel_adapters()
    channel_limiter = ChannelRateLimiter()
    webhook_queue = None
    webhook_queue_error = None
    outbound_queue = None
    outbound_queue_error = None
    wecom_ai_bot_connector = None
    runtime_closed = False
    if os.getenv("WECOM_AI_BOT_ENABLED", "0").strip().lower() in {"1", "true", "yes"}:
        wecom_ai_bot_connector = WeComAIBotConnector()
        ai_bot_adapter = adapters.get("wecom_ai_bot")
        if ai_bot_adapter is not None:
            ai_bot_adapter.connector = wecom_ai_bot_connector
    logger.info(
        "WeCom AI Bot startup config: enabled=%s, account_id_length=%d, connector=%s",
        os.getenv("WECOM_AI_BOT_ENABLED", "0").strip().lower() in {"1", "true", "yes"},
        len(os.getenv("WECOM_AI_BOT_ACCOUNT_ID", "").strip()),
        wecom_ai_bot_connector is not None,
    )

    def get_channel_adapter(channel: str):
        try:
            return adapters[channel]
        except KeyError as exc:
            if channel == "wecom":
                raise KeyError(
                    "wecom is a legacy Enterprise WeChat callback adapter and is disabled by default; "
                    "use channel=wecom_ai_bot for BotID/BotSecret long-connection acceptance, "
                    "or set ENABLE_LEGACY_WECOM=1 only for legacy callback validation"
                ) from exc
            raise

    def close_runtime_resources() -> None:
        nonlocal runtime_closed
        if runtime_closed:
            return
        runtime_closed = True
        if gateway.storage_manager is not None:
            gateway.storage_manager.close()
        gateway.storage.close()
        gateway.telemetry.close()
        if webhook_queue is not None:
            webhook_queue.close()
        if outbound_queue is not None:
            outbound_queue.close()
        close = getattr(admin.tenants.repository, "close", None)
        if close:
            close()

    if os.getenv("REDIS_URL", "").strip() and os.getenv("WEBHOOK_DURABLE_QUEUE", "1") == "1":
        try:
            webhook_queue = DurableWebhookQueue(os.getenv("REDIS_URL"))
        except Exception as exc:
            observe_error("platform", "webhook_queue", type(exc).__name__)
            webhook_queue_error = type(exc).__name__
            if os.getenv("REQUIRE_DURABLE_WEBHOOK", "0").lower() in {"1", "true", "yes"}:
                close_runtime_resources()
                raise RuntimeError("durable webhook queue is required but unavailable") from exc
            webhook_queue = None

    if os.getenv("OUTBOUND_QUEUE_URL", "").strip() and os.getenv("OUTBOUND_QUEUE_ENABLED", "0").lower() in {
        "1",
        "true",
        "yes",
    }:
        try:
            outbound_queue = OutboundDeliveryQueue(os.getenv("OUTBOUND_QUEUE_URL"))
        except Exception as exc:
            observe_error("platform", "outbound_queue", type(exc).__name__)
            outbound_queue_error = type(exc).__name__
            if os.getenv("REQUIRE_OUTBOUND_QUEUE", "0").lower() in {"1", "true", "yes"}:
                close_runtime_resources()
                raise RuntimeError("outbound delivery queue is required but unavailable") from exc

    @asynccontextmanager
    async def lifespan(app_obj):
        app_obj.state.runtime_started = True
        app_obj.state.webhook_consumer_stop = threading.Event()
        app_obj.state.webhook_consumer_thread = None
        app_obj.state.compensation_stop = threading.Event()
        app_obj.state.compensation_thread = None
        app_obj.state.wecom_ai_bot_stop = asyncio.Event()
        app_obj.state.wecom_ai_bot_tasks = []
        if webhook_queue is not None:

            def handle(item: dict[str, Any]) -> None:
                payload = dict(item.get("payload", {}))
                if item.get("traceparent"):
                    payload["_traceparent"] = item["traceparent"]
                build_ui_result(item["channel"], item["account_id"], payload, True)

            def consume_loop() -> None:
                while not app_obj.state.webhook_consumer_stop.is_set():
                    try:
                        webhook_queue.consume_once(handle, timeout=1)
                    except Exception as exc:
                        observe_error("platform", "webhook", type(exc).__name__)

            thread = threading.Thread(
                target=consume_loop,
                name="trpc-webhook-consumer",
                daemon=True,
            )
            app_obj.state.webhook_consumer_thread = thread
            thread.start()
        if wecom_ai_bot_connector is not None:
            active_tenants = getattr(admin.tenants.repository, "all_active", list)()
            ai_bindings = [
                binding
                for tenant in active_tenants
                for binding in tenant.channel_bindings
                if binding.enabled and binding.channel == "wecom_ai_bot"
            ]
            logger.info("WeCom AI Bot active bindings: count=%d", len(ai_bindings))

            async def ai_bot_sink(inbound, binding) -> None:
                payload = {
                    "message_id": inbound.external_message_id,
                    "from_user_id": inbound.external_user_id,
                    "chat_id": inbound.group_id,
                    "text": inbound.text,
                    "attachments": [
                        {
                            "kind": item.kind,
                            "url": item.url,
                            "name": item.name,
                            "content_type": item.content_type,
                            "metadata": dict(item.metadata),
                        }
                        for item in inbound.attachments
                    ],
                    "raw_event": dict(inbound.raw_event),
                }
                if webhook_queue is not None:
                    webhook_queue.submit(
                        binding.channel,
                        binding.account_id,
                        payload,
                        None,
                        tenant_id=binding.tenant_id,
                    )
                    return
                await asyncio.to_thread(
                    build_ui_result,
                    binding.channel,
                    binding.account_id,
                    payload,
                    False,
                )

            for tenant in active_tenants:
                for binding in tenant.channel_bindings:
                    if binding.enabled and binding.channel == "wecom_ai_bot":
                        app_obj.state.wecom_ai_bot_tasks.append(
                            asyncio.create_task(
                                wecom_ai_bot_connector.run(
                                    binding,
                                    ai_bot_sink,
                                    app_obj.state.wecom_ai_bot_stop,
                                )
                            )
                        )
        if outbound_queue is not None and os.getenv("OUTBOUND_QUEUE_CONSUMER", "0").lower() in {"1", "true", "yes"}:

            def outbound_loop() -> None:
                while not app_obj.state.webhook_consumer_stop.is_set():
                    try:
                        outbound_queue.consume_once(
                            handle_outbound,
                            on_dead_letter=handle_outbound_dead_letter,
                            timeout=1,
                        )
                    except Exception as exc:
                        observe_error("platform", "outbound", type(exc).__name__)

            outbound_thread = threading.Thread(
                target=outbound_loop,
                name="trpc-outbound-consumer",
                daemon=True,
            )
            app_obj.state.outbound_consumer_thread = outbound_thread
            outbound_thread.start()
        if os.getenv("COMPENSATION_WORKER", "1").strip().lower() in {"1", "true", "yes"}:

            def compensation_loop() -> None:
                interval = float(os.getenv("COMPENSATION_INTERVAL_SECONDS", "5"))
                limit = int(os.getenv("COMPENSATION_REPLAY_LIMIT", "50"))
                while not app_obj.state.compensation_stop.is_set():
                    try:
                        replay_all_compensations(limit)
                    except Exception as exc:
                        observe_error("platform", "compensation", type(exc).__name__)
                    app_obj.state.compensation_stop.wait(interval)

            compensation_thread = threading.Thread(
                target=compensation_loop,
                name="trpc-compensation-worker",
                daemon=True,
            )
            app_obj.state.compensation_thread = compensation_thread
            compensation_thread.start()
        try:
            yield
        finally:
            app_obj.state.webhook_consumer_stop.set()
            app_obj.state.compensation_stop.set()
            app_obj.state.wecom_ai_bot_stop.set()
            for thread_name in (
                "webhook_consumer_thread",
                "outbound_consumer_thread",
                "compensation_thread",
            ):
                thread = getattr(app_obj.state, thread_name, None)
                if thread is not None and thread is not threading.current_thread():
                    thread.join(timeout=10)
            for binding_id in tuple(getattr(wecom_ai_bot_connector, "_stop_events", {})):
                wecom_ai_bot_connector.stop(binding_id)
            ai_bot_tasks = getattr(app_obj.state, "wecom_ai_bot_tasks", [])
            if ai_bot_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*ai_bot_tasks, return_exceptions=True),
                        timeout=10,
                    )
                except TimeoutError:
                    for task in ai_bot_tasks:
                        task.cancel()
                    await asyncio.gather(*ai_bot_tasks, return_exceptions=True)
            close_runtime_resources()
            app_obj.state.runtime_started = False

    app = FastAPI(title="tRPC-Agent Multi-Tenant Service", version="0.1.0", lifespan=lifespan)
    app.state.runtime_started = False
    app.state.close_runtime_resources = close_runtime_resources
    app.state.gateway = gateway
    app.state.admin = admin
    app.state.webhook_queue = webhook_queue
    if webhook_queue_error is not None:
        app.state.webhook_queue_error = webhook_queue_error
    app.state.outbound_queue = outbound_queue
    if outbound_queue_error is not None:
        app.state.outbound_queue_error = outbound_queue_error

    def get_storage_for_tenant(tenant_id: str):
        tenant = gateway.tenants.get_tenant(tenant_id)
        return gateway.storage_manager.get(tenant) if gateway.storage_manager else gateway.storage

    def public_audit_record(record: AuditRecord) -> dict[str, Any]:
        """Serialize an audit record without allowing provider secrets to escape."""
        return {
            "audit_id": record.audit_id,
            "tenant_id": record.tenant_id,
            "decision": record.decision,
            "trace_id": record.trace_id,
            "channel": record.channel,
            "user_id": record.user_id,
            "session_id": record.session_id,
            "agent_name": record.agent_name,
            "tool_name": record.tool_name,
            "latency_ms": record.latency_ms,
            "error_type": redact_secret_text(record.error_type) if record.error_type else None,
            "token_usage": record.token_usage,
            "cost": record.cost,
            "metadata": redact_secret_data(record.metadata),
            "created_at": record.created_at.isoformat(),
        }

    def expected_config_version(request: Request) -> int | None:
        raw = request.headers.get("if-match")
        if not raw:
            if os.getenv("REQUIRE_ETAG", "0").strip().lower() in {"1", "true", "yes"}:
                raise HTTPException(status_code=428, detail="If-Match header is required")
            return None
        raw = raw.strip()
        if raw == "*":
            return None
        if raw.startswith("W/"):
            raw = raw[2:].strip()
        raw = raw.strip('"')
        try:
            return int(raw)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="If-Match must contain a config version") from exc

    def set_config_etag(response: Response, payload: dict[str, Any]) -> None:
        version = payload.get("config_version")
        if version is not None:
            response.headers["ETag"] = f'"{version}"'

    def replay_all_compensations(limit: int = 50) -> dict[str, int]:
        counts: dict[str, int] = {}
        tenants = getattr(admin.tenants.repository, "all_active", list)()
        for tenant in tenants:
            storage = gateway.storage_manager.get(tenant) if gateway.storage_manager else gateway.storage
            counts[tenant.tenant_id] = replay_compensations(
                storage,
                limit=limit,
                tenant_id=tenant.tenant_id,
            )
        return counts

    def _decode_outbound_messages(
        raw_messages: list[dict[str, Any]],
    ) -> list[OutboundMessage]:
        return [
            OutboundMessage(
                channel=item["channel"],
                account_id=item["account_id"],
                session_id=item["session_id"],
                external_user_id=item["external_user_id"],
                text=item["text"],
                group_id=item.get("group_id"),
                attachments=[Attachment(**attachment) for attachment in item.get("attachments", [])],
                metadata=dict(item.get("metadata", {})),
            )
            for item in raw_messages
        ]

    def handle_outbound(item: dict[str, Any]) -> None:
        """Deliver a queued response and finalize inbound delivery idempotency."""

        binding = gateway.tenants.resolve_binding(item["channel"], item["account_id"])
        storage = get_storage_for_tenant(item["tenant_id"])
        messages = _decode_outbound_messages(item.get("messages", []))
        completed = {int(index) for index in item.get("completed_parts", [])}
        results = list(item.get("results", []))
        for index, message in enumerate(messages):
            if index in completed:
                continue
            channel_limiter.acquire(
                f"{message.channel}:{message.account_id}",
                int(item.get("send_qps_limit", 20)),
            )
            result = send_with_retry(
                lambda message=message: adapters[message.channel].send(message=message, binding=binding),
                attempts=int(item.get("send_attempts", 3)),
            )
            if not result.ok:
                raise RuntimeError(result.error or "channel delivery failed")
            completed.add(index)
            results.append(result.metadata)
            item["completed_parts"] = sorted(completed)
            item["results"] = results
            if outbound_queue is not None:
                outbound_queue.update(item)
        reply = {
            "parts": len(messages),
            "message_types": [message.metadata.get("message_type", "text") for message in messages],
            "results": results,
        }
        storage.idempotency.complete(
            item["tenant_id"],
            item["idempotency_key"],
            item["response_ref"],
            {"text": item.get("answer", ""), "reply": reply, "delivered": True},
        )
        observe_delivery(item["channel"], "success", item["tenant_id"])
        storage.audit.append(
            AuditRecord(
                audit_id=str(uuid4()),
                tenant_id=item["tenant_id"],
                channel=item["channel"],
                user_id=item.get("external_user_id"),
                session_id=item.get("session_id"),
                agent_name=item.get("agent_name"),
                decision="delivered",
                latency_ms=int((monotonic() - float(item.get("queued_at", monotonic()))) * 1000),
                trace_id=item["trace_id"],
                metadata={"parts": len(messages), "queued": True},
            )
        )

    def handle_outbound_dead_letter(item: dict[str, Any], error: str) -> None:
        try:
            storage = get_storage_for_tenant(item["tenant_id"])
            storage.idempotency.release_delivery(item["tenant_id"], item["idempotency_key"])
            observe_delivery(item["channel"], "error", item["tenant_id"])
            storage.audit.append(
                AuditRecord(
                    audit_id=str(uuid4()),
                    tenant_id=item["tenant_id"],
                    channel=item["channel"],
                    user_id=item.get("external_user_id"),
                    session_id=item.get("session_id"),
                    agent_name=item.get("agent_name"),
                    decision="delivery_failed",
                    error_type="outbound_dead_letter",
                    trace_id=item["trace_id"],
                    metadata={"error": error, "queued": True},
                )
            )
        except Exception as exc:
            observe_error("platform", "outbound_dead_letter", type(exc).__name__)

    def resolve_attachment_token(binding, *fields: str) -> str | None:
        manager = SecretManager()
        for field in fields:
            reference = binding.config.get(field) if field != "token_ref" else binding.token_ref
            if reference:
                return manager.resolve(reference)
        return None

    def read_limited_response(response, max_bytes: int) -> bytes:
        return response.read(max_bytes + 1)

    def response_content_type(response, fallback: str | None) -> str:
        headers = getattr(response, "headers", None)
        if headers is not None:
            if hasattr(headers, "get_content_type"):
                content_type = headers.get_content_type()
                if content_type:
                    return str(content_type)
            content_type = headers.get("Content-Type")
            if content_type:
                return str(content_type).split(";", 1)[0]
        return fallback or "application/octet-stream"

    def download_allowed_url(url: str, allowed_hosts: set[str], max_bytes: int) -> tuple[bytes, str | None]:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("attachment URL must use http or https")
        if parsed.hostname is None or parsed.hostname.lower() not in allowed_hosts:
            raise ValueError("attachment host is not in the tenant allowlist")
        request = UrlRequest(validate_outbound_url(url), headers={"User-Agent": "trpc-agent-service"})
        with urlopen(request, timeout=15) as response:
            return read_limited_response(response, max_bytes), response_content_type(response, None)

    def download_telegram_file(binding, file_id: str, max_bytes: int) -> tuple[bytes | None, str | None, str | None]:
        token = resolve_attachment_token(binding, "token_ref")
        if not token:
            return None, None, None
        metadata_url = f"https://api.telegram.org/bot{token}/getFile?{urlencode({'file_id': file_id})}"
        with urlopen(
            UrlRequest(validate_outbound_url(metadata_url, trusted_hosts={"api.telegram.org"})),
            timeout=15,
        ) as response:
            body = json.loads(response.read(max_bytes + 1).decode("utf-8"))
        if not body.get("ok") or not body.get("result", {}).get("file_path"):
            raise ValueError("Telegram getFile did not return a downloadable file_path")
        file_path = str(body["result"]["file_path"]).lstrip("/")
        file_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
        with urlopen(
            UrlRequest(validate_outbound_url(file_url, trusted_hosts={"api.telegram.org"})),
            timeout=15,
        ) as response:
            content = read_limited_response(response, max_bytes)
            return content, response_content_type(response, None), file_path

    def download_wechat_media(binding, media_id: str, max_bytes: int) -> tuple[bytes | None, str | None]:
        token = resolve_attachment_token(binding, "access_token_ref", "token_ref")
        if not token:
            return None, None
        api_base = str(
            binding.config.get("api_base_url")
            or ("https://qyapi.weixin.qq.com" if binding.channel == "wecom" else "https://api.weixin.qq.com")
        ).rstrip("/")
        query = urlencode({"access_token": token, "media_id": media_id})
        request = UrlRequest(validate_outbound_url(f"{api_base}/cgi-bin/media/get?{query}"))
        with urlopen(request, timeout=15) as response:
            content = read_limited_response(response, max_bytes)
            content_type = response_content_type(response, None)
        if content_type == "application/json" or content.lstrip().startswith(b"{"):
            try:
                payload = json.loads(content.decode("utf-8"))
            except ValueError as exc:
                raise ValueError("WeChat media download returned an invalid JSON error") from exc
            if payload.get("errcode"):
                raise ValueError("WeChat media download failed")
        return content, content_type

    def persist_inbound_attachments(inbound, storage, tenant_id: str, binding) -> None:
        """Materialize provider attachments before the Worker starts.

        The Worker receives tenant-scoped artifact references rather than
        provider URLs, so later retries do not depend on a short-lived IM URL.
        """
        max_bytes = int(os.getenv("MAX_ATTACHMENT_BYTES", str(10 * 1024 * 1024)))
        allowed_hosts = {str(host).lower() for host in binding.config.get("attachment_host_allowlist", [])}
        for attachment in inbound.attachments:
            content: bytes | None = None
            materialized_from: str | None = None
            downloaded_content_type: str | None = None
            if attachment.metadata.get("content_base64"):
                content = b64decode(str(attachment.metadata["content_base64"]))
                materialized_from = "inline_base64"
            elif attachment.metadata.get("file_id") and binding.channel == "telegram":
                content, downloaded_content_type, file_path = download_telegram_file(
                    binding, str(attachment.metadata["file_id"]), max_bytes
                )
                if content is not None:
                    materialized_from = "telegram_file_id"
                    attachment.metadata["telegram_file_path"] = file_path
            elif attachment.metadata.get("resource_key") and binding.channel == "feishu":
                adapter = adapters.get(binding.channel)
                downloader = getattr(adapter, "download_media", None)
                if downloader:
                    content, downloaded_content_type, downloaded_filename = downloader(
                        binding,
                        str(attachment.metadata.get("message_id", "")),
                        str(attachment.metadata["resource_key"]),
                        resource_type=str(attachment.metadata.get("resource_type", "file")),
                        max_bytes=max_bytes,
                    )
                    if content is not None:
                        materialized_from = "feishu_resource"
                        if downloaded_filename and not attachment.name:
                            attachment.name = downloaded_filename
            elif (
                binding.channel == "wecom_ai_bot"
                and attachment.metadata.get("provider_url")
                and attachment.metadata.get("aes_key")
            ):
                adapter = adapters.get(binding.channel)
                downloader = getattr(adapter, "download_media", None)
                if downloader:
                    content, downloaded_content_type, downloaded_filename = downloader(
                        attachment,
                        max_bytes=max_bytes,
                    )
                    if content is not None:
                        materialized_from = "wecom_ai_bot_media"
                        if downloaded_filename and not attachment.name:
                            attachment.name = downloaded_filename
            elif attachment.metadata.get("media_id") and binding.channel in {
                "wecom",
                "wechat_official_account",
                "wechat_customer_service",
            }:
                content, downloaded_content_type = download_wechat_media(
                    binding, str(attachment.metadata["media_id"]), max_bytes
                )
                if content is not None:
                    materialized_from = "wechat_media_id"
            elif attachment.url:
                content, downloaded_content_type = download_allowed_url(attachment.url, allowed_hosts, max_bytes)
                materialized_from = "url"
            if content is None:
                continue
            if len(content) > max_bytes:
                raise ValueError("attachment exceeds MAX_ATTACHMENT_BYTES")
            stored = storage.artifacts.put(
                tenant_id,
                content,
                downloaded_content_type or attachment.content_type or "application/octet-stream",
            )
            attachment.metadata.update(
                {
                    "artifact_id": stored.object_id,
                    "size": stored.size,
                    "content_type": stored.content_type,
                    "materialized_from": materialized_from,
                }
            )
            attachment.metadata.pop("content_base64", None)
            attachment.metadata.pop("provider_url", None)
            attachment.metadata.pop("aes_key", None)
            attachment.url = None

    def build_ui_result(channel: str, account_id: str, payload: dict[str, Any], verify: bool = True) -> dict[str, Any]:
        binding = gateway.tenants.resolve_binding(channel, account_id)
        adapter = get_channel_adapter(channel)
        if verify and not payload.get("callback_verified"):
            adapter.verify_callback(payload, binding)
        inbound = adapter.parse_event(payload, binding)
        internal_user_id = binding.resolve_user_id(inbound.external_user_id)
        TenantPolicy(gateway.tenants.get_tenant(binding.tenant_id), binding.agent_app_id).check_im_user(
            binding, inbound.external_user_id, internal_user_id
        )
        storage = get_storage_for_tenant(binding.tenant_id)
        persist_inbound_attachments(inbound, storage, binding.tenant_id, binding)
        trace_id = str(inbound.raw_event.get("trace_id") or uuid4())
        traceparent = inbound.raw_event.get("_traceparent") or inbound.raw_event.get("traceparent")
        callback_context = TenantContext(
            binding.tenant_id,
            binding.agent_app_id,
            gateway.tenants.get_tenant(binding.tenant_id).config_version,
            trace_id,
            None,
            channel,
            internal_user_id,
            traceparent,
        )
        with gateway.telemetry.span("im.callback", callback_context):
            callback_traceparent = gateway.telemetry.inject_traceparent() or traceparent
            session_id, events, response_ref = gateway.dispatch(
                inbound,
                trace_id=trace_id,
                traceparent=callback_traceparent,
            )
        if any(event.metadata.get("queued") for event in events):
            return {
                "ok": True,
                "accepted": True,
                "durable": True,
                "queued": True,
                "session_id": session_id,
                "response_ref": response_ref,
                "answer": "",
                "reply": {"queued": True},
            }
        outbound_messages = build_outbound_messages(
            events,
            channel=channel,
            account_id=account_id,
            session_id=session_id,
            external_user_id=inbound.external_user_id,
            group_id=inbound.group_id,
        )
        if inbound.is_revoke:
            return {
                "ok": True,
                "revoked": True,
                "session_id": session_id,
                "response_ref": response_ref,
                "answer": "",
                "reply": {},
            }
        context = TenantContext(
            binding.tenant_id,
            binding.agent_app_id,
            gateway.tenants.get_tenant(binding.tenant_id).config_version,
            trace_id,
            session_id,
            channel,
            internal_user_id,
            callback_traceparent,
        )
        configured_max_length = int(binding.config.get("max_message_length", 4096))
        max_length = min(configured_max_length, int(adapter.capabilities.max_text_length))
        if max_length <= 0:
            raise ValueError("channel max_message_length must be positive")
        rate_limit = int(binding.config.get("send_qps_limit", 20))
        delivery_key = build_idempotency_key(
            binding.tenant_id,
            channel,
            account_id,
            inbound.external_message_id,
        )
        parts = split_outbound_messages(outbound_messages, max_length, idempotency_key=delivery_key)
        answer_text = visible_answer(outbound_messages)
        results: list[SendResult] = []
        delivery_started = monotonic()
        with gateway.telemetry.span("im.reply", context):
            if not storage.idempotency.claim_delivery(
                binding.tenant_id,
                delivery_key,
            ):
                existing = storage.idempotency.get(
                    binding.tenant_id,
                    delivery_key,
                )
                return {
                    "ok": True,
                    "duplicate": True,
                    "session_id": session_id,
                    "response_ref": response_ref,
                    "answer": (existing.result or {}).get("text", "") if existing else "",
                    "reply": (existing.result or {}).get("reply", {}) if existing else {},
                }
            if (
                outbound_queue is not None
                and channel != "web"
                and not AgentGateway._durable_inbox_enabled()
            ):
                try:
                    task_id = outbound_queue.enqueue(
                        {
                            "tenant_id": binding.tenant_id,
                            "channel": channel,
                            "account_id": account_id,
                            "external_user_id": inbound.external_user_id,
                            "session_id": session_id,
                            "group_id": inbound.group_id,
                            "agent_name": gateway.tenants.get_tenant(binding.tenant_id)
                            .app(binding.agent_app_id)
                            .agent_name,
                            "messages": [asdict(message) for message in parts],
                            "answer": answer_text,
                            "response_ref": response_ref,
                            "idempotency_key": delivery_key,
                            "trace_id": trace_id,
                            "queued_at": time.time(),
                            "send_qps_limit": rate_limit,
                        },
                        task_id=f"{delivery_key}:outbound",
                    )
                except Exception:
                    storage.idempotency.release_delivery(binding.tenant_id, delivery_key)
                    raise
                return {
                    "ok": True,
                    "accepted": True,
                    "durable": True,
                    "queued": True,
                    "task_id": task_id,
                    "session_id": session_id,
                    "response_ref": response_ref,
                    "answer": answer_text,
                    "reply": {"parts": len(parts), "queued": True},
                }
            try:
                for part_message in parts:
                    channel_limiter.acquire(f"{channel}:{account_id}", rate_limit)
                    result = send_with_retry(
                        lambda message=part_message: adapter.send(message=message, binding=binding),
                        dead_letter=lambda result, message=part_message: persist_dead_letter(
                            channel, account_id, message, result
                        )
                    )
                    results.append(result)
                    if not result.ok:
                        # Do not send later fragments after a failed fragment
                        # or report a partial delivery as a full success.
                        break
            except Exception as exc:
                storage.idempotency.release_delivery(binding.tenant_id, delivery_key)
                storage.audit.append(
                    AuditRecord(
                        audit_id=str(uuid4()),
                        tenant_id=binding.tenant_id,
                        channel=channel,
                        user_id=internal_user_id,
                        session_id=session_id,
                        agent_name=gateway.tenants.get_tenant(binding.tenant_id).app(binding.agent_app_id).agent_name,
                        decision="delivery_error",
                        latency_ms=int((monotonic() - delivery_started) * 1000),
                        error_type=type(exc).__name__,
                        trace_id=trace_id,
                    )
                )
                raise
        outbound = results[-1] if results else SendResult(False, "", "no outbound message")
        outbound.metadata = {
            "parts": len(parts),
            "message_types": [message.metadata.get("message_type", "text") for message in parts],
            "results": [result.metadata for result in results],
        }
        observe_delivery(channel, "success" if outbound.ok else "error", binding.tenant_id)
        if outbound.ok:
            storage.idempotency.complete(
                binding.tenant_id,
                delivery_key,
                response_ref,
                {"text": answer_text, "reply": outbound.metadata, "delivered": True},
            )
        else:
            storage.idempotency.release_delivery(
                binding.tenant_id,
                delivery_key,
            )
        storage.audit.append(
            AuditRecord(
                audit_id=str(uuid4()),
                tenant_id=binding.tenant_id,
                channel=channel,
                user_id=internal_user_id,
                session_id=session_id,
                agent_name=gateway.tenants.get_tenant(binding.tenant_id).app(binding.agent_app_id).agent_name,
                decision="delivered" if outbound.ok else "delivery_failed",
                latency_ms=int((monotonic() - delivery_started) * 1000),
                error_type=None if outbound.ok else "channel_delivery_failed",
                trace_id=trace_id,
                metadata={
                    "parts": len(parts),
                    "message_types": [message.metadata.get("message_type", "text") for message in parts],
                    "response_ref": outbound.response_ref,
                },
            )
        )
        return {
            "ok": outbound.ok,
            "session_id": session_id,
            "response_ref": response_ref,
            "answer": answer_text,
            "reply": outbound.metadata,
        }

    def validate_webhook_payload(channel: str, account_id: str, payload: dict[str, Any]):
        """Validate a callback before it can enter the durable queue."""
        binding = gateway.tenants.resolve_binding(channel, account_id)
        adapter = get_channel_adapter(channel)
        adapter.verify_callback(payload, binding)
        if getattr(adapter, "is_noop", lambda *_: False)(payload, binding):
            return binding
        inbound = adapter.parse_event(payload, binding)
        TenantPolicy(gateway.tenants.get_tenant(binding.tenant_id), binding.agent_app_id).check_im_user(
            binding,
            inbound.external_user_id,
            binding.resolve_user_id(inbound.external_user_id),
        )
        return binding

    def principal(request: Request):
        try:
            return authenticate(request.headers.get("X-Admin-API-Key"), request.headers.get("Authorization"))
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc

    def public_surface_principal(request: Request):
        """Protect operational/demo surfaces when the process is not local-only."""

        configured = os.getenv("PUBLIC_SURFACE_AUTH_REQUIRED")
        if configured is None:
            runtime_mode = os.getenv("TRPC_AGENT_RUNTIME_MODE", "trpc").strip().lower()
            required = runtime_mode not in {"local", "test", "demo"}
        else:
            required = configured.strip().lower() in {"1", "true", "yes", "on"}
        if not required:
            return None
        return principal(request)

    @app.get("/health")
    def health(response: Response) -> dict[str, Any]:
        """Report readiness for the dependencies this process needs to serve traffic."""

        dependencies: dict[str, str] = {}
        errors: dict[str, str] = {}

        def check(name: str, operation) -> None:
            try:
                operation()
                dependencies[name] = "ok"
            except Exception as exc:
                dependencies[name] = "error"
                errors[name] = type(exc).__name__

        repository = admin.tenants.repository
        active_tenants: list[TenantConfig] = []

        def load_active_tenants() -> None:
            nonlocal active_tenants
            active_tenants = list(getattr(repository, "all_active", list)())

        check("tenant_repository", load_active_tenants)
        if gateway.worker_queue is not None:
            check("worker_queue", gateway.worker_queue.client.ping)
        if webhook_queue is not None:
            check("webhook_queue", webhook_queue.client.ping)
        if outbound_queue is not None:
            check("outbound_queue", outbound_queue.client.ping)
        for tenant in active_tenants:
            check(
                f"session_backend:{tenant.tenant_id}",
                lambda tenant=tenant: get_storage_for_tenant(tenant.tenant_id).session.load_state(
                    tenant.tenant_id,
                    "__healthcheck__",
                ),
            )

        healthy = not errors
        if not healthy:
            response.status_code = 503
        return {
            "status": "ok" if healthy else "degraded",
            "worker_mode": "remote" if gateway.worker_queue is not None else "local",
            "local_workers": len(gateway.workers),
            "active_tenants": len(active_tenants),
            "dependencies": dependencies,
            "errors": errors,
        }

    @app.get("/livez")
    def livez() -> dict[str, str]:
        """Process liveness must not depend on external services."""
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz(response: Response) -> dict[str, Any]:
        """Readiness uses the same dependency checks as the public health view."""
        return health(response)

    @app.get("/metrics")
    def metrics(_current=Depends(public_surface_principal)):
        from fastapi.responses import Response

        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/ui", response_class=HTMLResponse)
    def ui(_current=Depends(public_surface_principal)) -> str:
        return HTMLResponse(
            content=Path(__file__).with_name("ui.html").read_text(encoding="utf-8"),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
            },
        )

    @app.post("/ui/api/chat")
    async def ui_chat(payload: dict[str, Any], current=Depends(public_surface_principal)) -> dict[str, Any]:
        channel = payload.get("channel", "web")
        account_id = payload.get("account_id", "web_demo")
        inbound_payload = {
            "message_id": payload.get("message_id", f"ui-{uuid4()}"),
            "from_user_id": payload.get("user_id", "ui-user"),
            "text": payload.get("text", ""),
            "attachments": payload.get("attachments", []),
        }
        if payload.get("group_id"):
            inbound_payload["chat_id"] = payload["group_id"]
        binding = gateway.tenants.resolve_binding(channel, account_id)
        if current is not None:
            try:
                authorize(current, binding.tenant_id, {"viewer", "operator"})
            except AdminAuthenticationError as exc:
                raise HTTPException(status_code=403, detail=str(exc)) from exc
        result = build_ui_result(channel, account_id, inbound_payload, verify=False)
        app_config = gateway.tenants.get_tenant(binding.tenant_id).app(binding.agent_app_id)
        use_cli = os.getenv("CPA_USE_CODEX_CLI", "0").strip().lower() in {"1", "true", "yes"}
        try:
            configured_key = (
                SecretManager().resolve(app_config.model_config.api_key_ref).strip()
                if app_config.model_config.api_key_ref
                else os.getenv(app_config.model_config.api_key_env, "").strip()
            )
        except Exception:
            configured_key = ""
        configured_base_url = app_config.model_config.base_url.strip() or os.getenv(
            "CPA_BASE_URL",
            "",
        ).strip()
        has_model_credentials = bool(configured_key and configured_base_url)
        if use_cli:
            result["model_mode"] = "codex_cli"
        elif has_model_credentials:
            result["model_mode"] = app_config.model_config.wire_api
        else:
            result["model_mode"] = "fallback"
        return result

    @app.post("/admin/v1/tenants")
    def create_tenant(payload: dict[str, Any], current=Depends(principal)) -> dict[str, Any]:
        try:
            if current.role not in {"superadmin", "platform_admin"}:
                raise HTTPException(status_code=403, detail="insufficient admin role")
            return admin.create_tenant(payload)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/admin/v1/tenants/{tenant_id}")
    def get_tenant(
        tenant_id: str,
        version: int | None = None,
        response: Response = None,
        current=Depends(principal),
    ) -> dict[str, Any]:
        try:
            authorize(current, tenant_id, {"viewer", "operator"})
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        try:
            result = admin.get_tenant(tenant_id, version)
            set_config_etag(response, result)
            return result
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.put("/admin/v1/tenants/{tenant_id}/config")
    def update_config(
        tenant_id: str,
        payload: dict[str, Any],
        request: Request,
        response: Response,
        current=Depends(principal),
    ) -> dict[str, Any]:
        try:
            authorize(current, tenant_id, {"operator"})
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        try:
            result = admin.update_config(
                tenant_id,
                payload,
                expected_version=expected_config_version(request),
            )
            set_config_etag(response, result)
            return result
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TenantConfigConflict as exc:
            raise HTTPException(status_code=412, detail=str(exc)) from exc
        except (ValueError, KeyError, TenantRepositoryError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/admin/v1/tenants/{tenant_id}/publish")
    def publish(
        tenant_id: str,
        payload: dict[str, Any],
        request: Request,
        response: Response,
        current=Depends(principal),
    ) -> dict[str, Any]:
        try:
            authorize(current, tenant_id, {"operator"})
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        try:
            result = admin.publish(
                tenant_id,
                int(payload["version"]),
                expected_version=expected_config_version(request),
            )
            set_config_etag(response, result)
            return result
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TenantConfigConflict as exc:
            raise HTTPException(status_code=412, detail=str(exc)) from exc
        except (ValueError, KeyError, TenantRepositoryError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/admin/v1/tenants/{tenant_id}/rollback")
    def rollback(
        tenant_id: str,
        payload: dict[str, Any],
        request: Request,
        response: Response,
        current=Depends(principal),
    ) -> dict[str, Any]:
        try:
            authorize(current, tenant_id, {"operator"})
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        try:
            result = admin.rollback(
                tenant_id,
                int(payload["version"]),
                expected_version=expected_config_version(request),
            )
            set_config_etag(response, result)
            return result
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TenantConfigConflict as exc:
            raise HTTPException(status_code=412, detail=str(exc)) from exc
        except (ValueError, KeyError, TenantRepositoryError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/admin/v1/tenants/{tenant_id}/gray-release")
    def configure_gray_release(
        tenant_id: str,
        payload: dict[str, Any],
        request: Request,
        response: Response,
        current=Depends(principal),
    ) -> dict[str, Any]:
        try:
            authorize(current, tenant_id, {"operator"})
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        try:
            result = admin.configure_gray_release(
                tenant_id,
                payload,
                expected_version=expected_config_version(request),
            )
            set_config_etag(response, result)
            return result
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TenantConfigConflict as exc:
            raise HTTPException(status_code=412, detail=str(exc)) from exc
        except (ValueError, KeyError, TenantRepositoryError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/admin/v1/tenants/{tenant_id}/channels")
    def add_channel(
        tenant_id: str,
        payload: dict[str, Any],
        request: Request,
        response: Response,
        current=Depends(principal),
    ) -> dict[str, Any]:
        try:
            authorize(current, tenant_id, {"operator"})
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        try:
            result = admin.add_channel(
                tenant_id,
                payload,
                expected_version=expected_config_version(request),
            )
            set_config_etag(response, result)
            return result
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TenantConfigConflict as exc:
            raise HTTPException(status_code=412, detail=str(exc)) from exc
        except (ValueError, KeyError, TenantRepositoryError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/admin/v1/tenants/{tenant_id}/health")
    def tenant_health(tenant_id: str, current=Depends(principal)) -> dict[str, Any]:
        try:
            authorize(current, tenant_id, {"viewer", "operator"})
            config = gateway.tenants.get_tenant(tenant_id)
            storage = get_storage_for_tenant(tenant_id)
            backend_health = {
                "session_backend": {
                    "status": "ok",
                    "backend": getattr(storage.session, "backend_name", type(storage.session).__name__),
                },
                "memory_backend": {
                    "status": "ok",
                    "backend": getattr(storage.memory, "backend_name", type(storage.memory).__name__),
                },
                "summary_backend": {
                    "status": "ok",
                    "backend": getattr(storage.summary, "backend_name", type(storage.summary).__name__),
                },
                "knowledge_backend": {
                    "status": "ok",
                    "backend": getattr(storage.knowledge, "backend_name", type(storage.knowledge).__name__),
                },
                "artifact_backend": {
                    "status": "ok",
                    "backend": getattr(storage.artifacts, "backend_name", type(storage.artifacts).__name__),
                },
                "audit_backend": {
                    "status": "ok",
                    "backend": getattr(storage.audit, "backend_name", type(storage.audit).__name__),
                },
                "compensation_backend": {
                    "status": "ok",
                    "backend": getattr(storage.compensation, "backend_name", type(storage.compensation).__name__),
                },
            }
            return {
                "tenant_id": tenant_id,
                "status": config.status.value,
                "config_version": config.config_version,
                "storage_profile": config.storage_profile.to_public_dict(),
                "backends": backend_health,
            }
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/admin/v1/tenants/{tenant_id}/audit")
    def tenant_audit(
        tenant_id: str,
        limit: int = 100,
        decision: str | None = None,
        current=Depends(principal),
    ) -> dict[str, Any]:
        """Provide a bounded, tenant-authorized audit view for operators."""
        try:
            authorize(current, tenant_id, {"viewer", "operator"})
            if limit < 1 or limit > 500:
                raise HTTPException(status_code=400, detail="limit must be between 1 and 500")
            storage = get_storage_for_tenant(tenant_id)
            records = storage.audit.list_by_tenant(tenant_id, limit=500)
            if decision:
                records = [record for record in records if record.decision == decision]
            records = records[:limit]
            return {
                "tenant_id": tenant_id,
                "count": len(records),
                "items": [public_audit_record(record) for record in records],
            }
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except HTTPException:
            raise
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/admin/v1/tenants/{tenant_id}/compensations/replay")
    def replay_tenant_compensations(
        tenant_id: str, payload: dict[str, Any] | None = None, current=Depends(principal)
    ) -> dict[str, Any]:
        try:
            authorize(current, tenant_id, {"operator"})
            config = gateway.tenants.get_tenant(tenant_id)
            storage = gateway.storage_manager.get(config) if gateway.storage_manager else gateway.storage
            limit = int((payload or {}).get("limit", 100))
            return {"tenant_id": tenant_id, "processed": replay_compensations(storage, limit=limit)}
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get(
        "/webhooks/{channel}/{account_id}",
        operation_id="verify_webhook_callback",
        response_class=PlainTextResponse,
    )
    async def verify_webhook(channel: str, account_id: str, request: Request):
        try:
            binding = gateway.tenants.resolve_binding(channel, account_id)
            adapter = get_channel_adapter(channel)
            payload = {key: value for key, value in request.query_params.items()}
            handshake = getattr(adapter, "verify_handshake", None)
            if not handshake:
                return PlainTextResponse("ok")
            return PlainTextResponse(handshake(payload, binding))
        except QuotaExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        except ChannelVerificationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except PolicyDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TenantRepositoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="webhook verification failed") from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail="webhook processing failed") from exc

    @app.post("/webhooks/{channel}/{account_id}", operation_id="receive_webhook_callback")
    async def receive_webhook(
        channel: str, account_id: str, request: Request, background_tasks: BackgroundTasks
    ) -> dict[str, Any]:
        try:
            max_body = int(os.getenv("WEBHOOK_MAX_BODY_BYTES", str(1024 * 1024)))
            content_length = request.headers.get("content-length")
            if content_length:
                try:
                    if int(content_length) < 0 or int(content_length) > max_body:
                        raise WebhookPayloadError("webhook body exceeds configured limit")
                except ValueError:
                    raise WebhookPayloadError("content-length header is invalid") from None
            body = await request.body()
            content_type = request.headers.get("content-type", "")
            payload = parse_webhook_body(body, content_type)
            payload = {**{key: value for key, value in request.query_params.items()}, **payload}
            if channel == "telegram":
                payload["_callback_secret_token"] = request.headers.get(
                    "X-Telegram-Bot-Api-Secret-Token",
                    "",
                )
            if channel == "feishu":
                payload["_raw_body"] = body.decode("utf-8")
                payload["_headers"] = dict(request.headers)
            adapter = get_channel_adapter(channel)
            binding = validate_webhook_payload(channel, account_id, payload)
            if getattr(adapter, "is_noop", lambda *_: False)(payload, binding):
                return getattr(adapter, "webhook_ack", lambda *_: {"ok": True})(payload, binding)
            # Verification has completed; only this sanitized form may cross
            # the process boundary into a durable queue.
            payload = sanitize_event_payload({**payload, "callback_verified": True})
            traceparent = request.headers.get("traceparent")
            if webhook_queue is not None:
                task_id = webhook_queue.submit(
                    channel,
                    account_id,
                    payload,
                    traceparent,
                    tenant_id=binding.tenant_id,
                )
                return {
                    "ok": True,
                    "accepted": True,
                    "durable": True,
                    "task_id": task_id,
                    "channel": channel,
                    "account_id": account_id,
                }
            background_tasks.add_task(build_ui_result, channel, account_id, payload, True)
            return {"ok": True, "accepted": True, "durable": False, "channel": channel, "account_id": account_id}
        except QuotaExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        except ChannelVerificationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except WebhookPayloadError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except PolicyDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except TenantNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TenantRepositoryError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/admin/v1/webhook-tasks/{task_id}")
    def webhook_task_status(task_id: str, current=Depends(principal)) -> dict[str, Any]:
        """Expose bounded asynchronous webhook state for acceptance probes."""

        if webhook_queue is None:
            raise HTTPException(status_code=503, detail="durable webhook queue is unavailable")
        status = webhook_queue.status(task_id)
        if status is None:
            raise HTTPException(status_code=404, detail="webhook task was not found")
        tenant_id = str(status.get("tenant_id") or "")
        if not tenant_id:
            raise HTTPException(status_code=404, detail="webhook task has no tenant binding")
        try:
            authorize(current, tenant_id, {"viewer", "operator"})
        except AdminAuthenticationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        return status

    return app


if os.getenv("TRPC_AGENT_NO_AUTO_APP", "0") == "1":
    app = None
else:
    try:
        app = create_app()
    except RuntimeError:
        app = None
