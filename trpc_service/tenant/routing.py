"""Trusted tenant, user and session routing at the Agent Gateway boundary."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Callable
from collections.abc import Iterable

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from trpc_service.channels import ChannelType
from trpc_service.channels import ConversationType
from trpc_service.channels import CallbackVerificationError
from trpc_service.channels import ChannelAdapter
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.channels import verify_and_parse_callback

from .models import ChannelBindingConfig
from .models import TenantConfig
from .models import TenantStatus


class RouteNotFoundError(LookupError):
    """Raised when a callback cannot be mapped to an active tenant binding."""


class InboundRouteRequest(BaseModel):
    """Identity fields needed to route one verified callback."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    channel: ChannelType
    webhook_public_id: str = Field(min_length=8, max_length=128)
    external_user_id: str = Field(min_length=1, max_length=255)
    conversation_type: ConversationType
    external_chat_id: str | None = Field(default=None, max_length=255)
    thread_id: str | None = Field(default=None, max_length=255)

    @model_validator(mode="after")
    def validate_conversation_identity(self) -> "InboundRouteRequest":
        if self.conversation_type in {ConversationType.GROUP, ConversationType.THREAD} and not self.external_chat_id:
            raise ValueError("group and thread conversations require external_chat_id")
        if self.conversation_type is ConversationType.THREAD and not self.thread_id:
            raise ValueError("thread conversations require thread_id")
        return self


class ResolvedRoute(BaseModel):
    """Trusted route consumed by Gateway queues and Agent Workers."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    config_version: int
    storage_revision: int
    agent_app_id: str
    channel_binding_id: str
    actor_id: str
    internal_user_id: str
    session_id: str
    partition_key: str


class ChannelBindingRegistry:
    """Read-only index from public callback identity to tenant configuration."""

    def __init__(self, tenants: Iterable[TenantConfig]) -> None:
        self._tenants: dict[str, TenantConfig] = {}
        self._bindings: dict[tuple[ChannelType, str], tuple[TenantConfig, ChannelBindingConfig]] = {}

        for tenant in tenants:
            if tenant.tenant_id in self._tenants:
                raise ValueError(f"duplicate tenant_id: {tenant.tenant_id}")
            self._tenants[tenant.tenant_id] = tenant
            for binding in tenant.channel_bindings:
                key = (binding.channel, binding.webhook_public_id)
                if key in self._bindings:
                    raise ValueError(f"duplicate public callback identity: {key}")
                self._bindings[key] = (tenant, binding)

    def resolve(self, channel: ChannelType, webhook_public_id: str) -> tuple[TenantConfig, ChannelBindingConfig]:
        result = self._bindings.get((channel, webhook_public_id))
        if result is None:
            raise RouteNotFoundError("channel binding not found")
        tenant, binding = result
        if tenant.status is not TenantStatus.ACTIVE or not binding.enabled:
            raise RouteNotFoundError("channel binding is not active")
        application = next(app for app in tenant.applications if app.app_id == binding.agent_app_id)
        if not application.enabled:
            raise RouteNotFoundError("agent application is not active")
        return tenant, binding


class SessionIdentityFactory:
    """Build stable HMAC identities without exposing raw IM identifiers."""

    def __init__(self, tenant_key_resolver: Callable[[str], bytes]) -> None:
        self._tenant_key_resolver = tenant_key_resolver

    def _digest(self, tenant_id: str, purpose: str, *parts: str) -> str:
        key = self._tenant_key_resolver(tenant_id)
        if len(key) < 32:
            raise ValueError("tenant session key must contain at least 32 bytes")
        payload = json.dumps([2, purpose, tenant_id, *parts], ensure_ascii=False,
                             separators=(",", ":")).encode("utf-8")
        return hmac.new(key, payload, hashlib.sha256).hexdigest()

    def internal_user_id(self, tenant_id: str, binding_id: str, external_user_id: str) -> str:
        return f"usr_{self._digest(tenant_id, 'user', binding_id, external_user_id)[:32]}"

    def scope_parts(self, binding: ChannelBindingConfig, request: InboundRouteRequest) -> tuple[str, ...]:
        direct = request.conversation_type is ConversationType.DIRECT
        return (binding.channel.value, binding.binding_id, binding.agent_app_id,
                request.conversation_type.value,
                request.external_user_id if direct else request.external_chat_id or "",
                request.thread_id or "",
                request.external_user_id if direct or binding.group_mode == "per_user" else "shared",
                str(binding.conversation_epoch))

    def storage_user_id(self, tenant_id: str, binding: ChannelBindingConfig, request: InboundRouteRequest) -> str:
        """SDK user scope includes the chat/topic; actor identity remains separate."""
        return "scp_" + self._digest(tenant_id, "storage_user", *self.scope_parts(binding, request))[:40]

    def session_id(self, tenant_id: str, binding: ChannelBindingConfig, request: InboundRouteRequest) -> str:
        return "ses_" + self._digest(tenant_id, "session", *self.scope_parts(binding, request))[:40]


class MessageRouter:
    """Resolve a verified callback to the correct tenant, app, user and session."""

    def __init__(self, registry: ChannelBindingRegistry, identities: SessionIdentityFactory) -> None:
        self._registry = registry
        self._identities = identities

    def route(self, request: InboundRouteRequest) -> ResolvedRoute:
        tenant, binding = self._registry.resolve(request.channel, request.webhook_public_id)
        return self._build_route(tenant, binding, request)

    def route_message(
        self,
        message: NormalizedInboundMessage,
        *,
        expected_binding: ChannelBindingConfig | None = None,
    ) -> ResolvedRoute:
        """Route a normalized message while retaining the verified Binding identity."""

        tenant, binding = self._registry.resolve(message.channel, message.webhook_public_id)
        if expected_binding is not None and binding != expected_binding:
            raise RouteNotFoundError("verified channel binding does not match routed binding")
        request = InboundRouteRequest(
            channel=message.channel,
            webhook_public_id=message.webhook_public_id,
            external_user_id=message.external_user_id,
            conversation_type=message.conversation_type,
            external_chat_id=message.external_chat_id,
            thread_id=message.thread_id,
        )
        return self._build_route(tenant, binding, request)

    def _build_route(
        self,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        request: InboundRouteRequest,
    ) -> ResolvedRoute:
        actor_id = self._identities.internal_user_id(tenant.tenant_id, binding.binding_id, request.external_user_id)
        user_id = self._identities.storage_user_id(tenant.tenant_id, binding, request)
        session_id = self._identities.session_id(tenant.tenant_id, binding, request)
        return ResolvedRoute(
            tenant_id=tenant.tenant_id,
            config_version=tenant.config_version,
            storage_revision=tenant.storage_revision,
            agent_app_id=binding.agent_app_id,
            channel_binding_id=binding.binding_id,
            actor_id=actor_id,
            internal_user_id=user_id,
            session_id=session_id,
            partition_key=f"{tenant.tenant_id}:{session_id}",
        )


async def verify_parse_and_route_callback(
    *,
    adapter: ChannelAdapter,
    raw_request: object,
    channel: ChannelType,
    webhook_public_id: str,
    registry: ChannelBindingRegistry,
    router: MessageRouter,
) -> tuple[NormalizedInboundMessage, ResolvedRoute]:
    """Resolve a Binding, verify the raw callback, then parse and route it."""

    _, binding = registry.resolve(channel, webhook_public_id)
    message = await verify_and_parse_callback(adapter, raw_request, binding)
    if message.channel is not channel or message.webhook_public_id != webhook_public_id:
        raise CallbackVerificationError("parsed message does not match the verified channel binding")
    return message, router.route_message(message, expected_binding=binding)
