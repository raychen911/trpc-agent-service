"""Authenticated IM ingress with durable-before-acknowledge semantics."""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.channels.contracts import (
    CallbackKind,
    CallbackRequest,
    Channel,
    TrustedBindingContext,
)
from trpc_service.channels.session import ChannelIdentityDeriver
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.wecom import WeComAdapter, WeComCrypto
from trpc_service.config import Settings
from trpc_service.metrics import METRICS
from trpc_service.reliability import (
    InboxAcceptance,
    InboxEnvelope,
    ReliabilityRepository,
    ReplyCredentialData,
)
from trpc_service.security import EnvelopeCipher, EnvironmentSecretResolver, SecretResolver
from trpc_service.storage.models import ChannelBinding, ChannelIngressRoute

LOGGER = logging.getLogger(__name__)
_PUBLIC_CALLBACK_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class IngressError(RuntimeError):
    """Base class for public callback failures safe to classify at the HTTP edge."""


class IngressRouteNotFoundError(IngressError):
    """The public callback ID does not resolve to an active binding."""


class IngressConfigurationError(IngressError):
    """A trusted binding cannot be instantiated because its configuration is invalid."""


class ReliabilityPort(Protocol):
    """Narrow durable acceptance port used by the HTTP ingress."""

    async def accept_inbox(self, envelope: InboxEnvelope) -> InboxAcceptance:
        """Commit a normalized message and optional encrypted credential."""


@dataclass(frozen=True, slots=True)
class ResolvedBinding:
    """A server-resolved binding plus secret references and app revision."""

    context: TrustedBindingContext
    secret_refs: dict[str, str]


@dataclass(frozen=True, slots=True)
class IngressResult:
    """Log-safe result returned after callback processing."""

    kind: CallbackKind
    channel: Channel
    tenant_id: str
    disposition: str
    acceptance: InboxAcceptance | None = None


class ChannelBindingResolver:
    """Resolve the minimal public route before entering the tenant RLS scope."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def resolve(self, public_callback_id: str, channel: Channel) -> ResolvedBinding:
        if _PUBLIC_CALLBACK_ID.fullmatch(public_callback_id) is None:
            raise IngressRouteNotFoundError("callback route not found")

        async with self._session_factory() as database, database.begin():
            route = await database.scalar(
                select(ChannelIngressRoute).where(
                    ChannelIngressRoute.public_callback_id == public_callback_id,
                )
            )
            if route is None or route.status != "active" or route.channel_type != channel.value:
                raise IngressRouteNotFoundError("callback route not found")
            if database.get_bind().dialect.name == "postgresql":
                await database.execute(
                    text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
                    {"tenant_id": route.tenant_id},
                )
            binding = await database.scalar(
                select(ChannelBinding).where(
                    ChannelBinding.tenant_id == route.tenant_id,
                    ChannelBinding.binding_id == route.binding_id,
                )
            )
            if (
                binding is None
                or binding.status != "active"
                or binding.channel_type != route.channel_type
                or binding.public_callback_id != public_callback_id
                or binding.config_revision != route.config_revision
            ):
                raise IngressRouteNotFoundError("callback route not found")
            return ResolvedBinding(
                context=TrustedBindingContext(
                    tenant_id=binding.tenant_id,
                    app_id=binding.app_id,
                    app_revision=binding.app_revision,
                    binding_id=binding.binding_id,
                    binding_revision=binding.config_revision,
                    channel=channel,
                    external_account_id=binding.external_account_id,
                    enabled=True,
                ),
                secret_refs=dict(binding.secret_refs),
            )


class ChannelIngressService:
    """Authenticate, normalize, encrypt reply routes, and durably accept callbacks."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        reliability: ReliabilityPort | None = None,
        secret_resolver: SecretResolver | None = None,
    ) -> None:
        root_key = settings.secret_key.get_secret_value()
        self._resolver = ChannelBindingResolver(session_factory)
        self._reliability = reliability or ReliabilityRepository(session_factory)
        self._secrets = secret_resolver or EnvironmentSecretResolver(settings.secret_env_allowlist)
        self._identities = ChannelIdentityDeriver(root_key)
        self._cipher = EnvelopeCipher(root_key)
        self._fingerprint_key = hashlib.sha256(
            b"trpc-agent-service/reply-route-fingerprint/v1\x00" + root_key.encode()
        ).digest()

    async def adapter_for(
        self,
        public_callback_id: str,
        channel: Channel,
    ) -> tuple[ResolvedBinding, WeComAdapter | TelegramAdapter]:
        resolved = await self._resolver.resolve(public_callback_id, channel)
        refs = resolved.secret_refs
        try:
            if channel is Channel.WECOM:
                adapter: WeComAdapter | TelegramAdapter = WeComAdapter(
                    WeComCrypto(
                        self._resolve(refs, "token"),
                        self._resolve(refs, "aes_key"),
                    ),
                    self._identities,
                )
            else:
                adapter = TelegramAdapter(
                    self._resolve(refs, "webhook_secret"),
                    self._identities,
                )
        except (KeyError, ValueError) as exc:
            raise IngressConfigurationError("channel binding is unavailable") from exc
        return resolved, adapter

    async def accept(
        self,
        *,
        public_callback_id: str,
        channel: Channel,
        callback_request: CallbackRequest,
    ) -> IngressResult:
        resolved, adapter = await self.adapter_for(public_callback_id, channel)
        return await self.accept_resolved(
            resolved=resolved,
            adapter=adapter,
            callback_request=callback_request,
        )

    async def accept_resolved(
        self,
        *,
        resolved: ResolvedBinding,
        adapter: WeComAdapter | TelegramAdapter,
        callback_request: CallbackRequest,
    ) -> IngressResult:
        """Accept using one immutable binding snapshot, avoiding a routing race."""

        channel = resolved.context.channel
        if adapter.channel is not channel:
            raise IngressConfigurationError("binding and adapter channel differ")
        verified, sensitive_route = adapter.verify_decrypt_and_normalize(
            callback_request,
            resolved.context,
        )
        if verified.kind is not CallbackKind.USER_MESSAGE:
            outcome = (
                "control_refresh"
                if verified.kind is CallbackKind.CONTROL_REFRESH
                else "channel_event"
            )
            METRICS.inbound_total.labels(
                resolved.context.tenant_id,
                channel.value,
                outcome,
            ).inc()
            return IngressResult(
                kind=verified.kind,
                channel=channel,
                tenant_id=resolved.context.tenant_id,
                disposition=outcome,
            )

        inbound = verified.inbound
        if inbound is None or sensitive_route is None:
            raise IngressConfigurationError("user callback has no durable reply route")
        if sensitive_route.route_key != inbound.reply_route_key:
            raise IngressConfigurationError("reply route does not match normalized input")

        context = {
            "tenant_id": inbound.tenant_id,
            "binding_id": inbound.binding_id,
            "delivery_id": inbound.delivery_id,
            "credential_kind": sensitive_route.kind.value,
        }
        plaintext = sensitive_route.value.get_secret_value()
        ciphertext = self._cipher.encrypt(plaintext, context=context)
        credential = ReplyCredentialData(
            credential_kind=sensitive_route.kind.value,
            ciphertext=ciphertext,
            # Keyed and deterministic, so a legitimate at-least-once callback
            # matches its first ciphertext despite AES-GCM's random nonce.
            ciphertext_hash=hmac.new(
                self._fingerprint_key,
                plaintext.encode(),
                hashlib.sha256,
            ).hexdigest(),
            expires_at=sensitive_route.expires_at,
        )
        payload = inbound.model_dump(mode="json", exclude_none=True)
        acceptance = await self._reliability.accept_inbox(
            InboxEnvelope(
                tenant_id=inbound.tenant_id,
                binding_id=inbound.binding_id,
                session_id=inbound.session_id,
                app_id=inbound.app_id,
                app_revision=resolved.context.app_revision,
                config_revision=resolved.context.binding_revision,
                scope=inbound.conversation_kind.value,
                principal_id=inbound.principal_id,
                external_delivery_id=inbound.delivery_id,
                payload=payload,
                payload_hash=inbound.payload_sha256,
                request_id=inbound.request_id,
                trace_id=inbound.trace_id,
                reply_credential=credential,
            )
        )
        METRICS.inbound_total.labels(
            inbound.tenant_id,
            channel.value,
            acceptance.disposition.value,
        ).inc()
        return IngressResult(
            kind=verified.kind,
            channel=channel,
            tenant_id=inbound.tenant_id,
            disposition=acceptance.disposition.value,
            acceptance=acceptance,
        )

    def _resolve(self, references: dict[str, str], name: str) -> str:
        reference = references[name]
        return self._secrets.resolve(reference).get_secret_value()


def make_callback_request(
    *,
    binding_id: str,
    headers: tuple[tuple[str, str], ...],
    query: tuple[tuple[str, str], ...],
    body: bytes,
    received_at: datetime,
    request_id: str,
    trace_id: str,
) -> CallbackRequest:
    """Create one timestamped immutable callback request at the HTTP boundary."""

    return CallbackRequest(
        path_binding_id=binding_id,
        headers=headers,
        query=query,
        body=body,
        received_at=received_at,
        request_id=request_id,
        trace_id=trace_id,
    )
