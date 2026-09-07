"""Browser/UI adapter used when no external IM credentials are available."""

from __future__ import annotations

import hmac
import json
import uuid
from datetime import UTC, datetime

from tenant_agent.channels.base import (
    DeliveryResult,
    ParsedWebhook,
    SignatureError,
    UnsupportedMessage,
    WebhookAck,
    WebhookRequest,
)
from tenant_agent.models import (
    ChannelBindingConfig,
    ChannelType,
    ChatType,
    InboundEnvelope,
    OutboundMessage,
    TenantConfig,
)
from tenant_agent.security import CompositeSecretResolver


class WebAdapter:
    name = "web"

    async def parse(
        self,
        request: WebhookRequest,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> ParsedWebhook:
        token_ref = binding.credential_refs.get("webhook_token")
        if token_ref:
            expected = await secrets.resolve(token_ref)
            presented = request.headers.get("x-webhook-token", "")
            if not hmac.compare_digest(presented.encode(), expected.encode()):
                raise SignatureError("invalid web channel token")
        try:
            payload = json.loads(request.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UnsupportedMessage("invalid web channel JSON") from exc
        if not isinstance(payload, dict):
            raise UnsupportedMessage("web channel payload must be an object")
        user_id = str(payload.get("user_id") or "")
        chat_id = str(payload.get("conversation_id") or user_id)
        if not user_id or not chat_id:
            raise UnsupportedMessage("user_id and conversation_id are required")
        try:
            chat_type = ChatType(payload.get("chat_type", "direct"))
        except ValueError as exc:
            raise UnsupportedMessage("invalid web chat_type") from exc
        envelope = InboundEnvelope(
            message_id=str(payload.get("message_id") or uuid.uuid4().hex),
            tenant_id=tenant.tenant_id,
            app_id=binding.app_id,
            binding_id=binding.binding_id,
            channel=ChannelType.WEB,
            external_account_id=binding.external_account_id,
            external_user_id=user_id,
            external_chat_id=chat_id,
            chat_type=chat_type,
            thread_id=str(payload["thread_id"]) if payload.get("thread_id") else None,
            text=str(payload.get("text") or ""),
            occurred_at=datetime.now(UTC),
            metadata={"synchronous": bool(payload.get("synchronous", True))},
        )
        return ParsedWebhook(
            (envelope,), WebhookAck(status_code=202, body=b'{"accepted":true}', media_type="application/json")
        )

    async def deliver(
        self,
        message: OutboundMessage,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> DeliveryResult:
        del tenant, binding, secrets
        return DeliveryResult((message.stream_key or uuid.uuid4().hex,))
