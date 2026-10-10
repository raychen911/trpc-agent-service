"""Durable inbound acceptance and tenant routing."""

from __future__ import annotations

import secrets
from uuid import uuid4

from opentelemetry import trace

from trpc_service.channels.envelopes import InboundEnvelope
from trpc_service.config.settings import SchedulerVersion
from trpc_service.metrics.privacy import inject_trace_headers
from trpc_service.storage.models import Acceptance, BindingRoute, PreparedInbound
from trpc_service.storage.protocols import RuntimeRepository
from trpc_service.tenant.models import TenantContext
from trpc_service.tenant.session_id import (
    make_principal_id,
    make_session_id,
    rollout_bucket,
    select_config_version,
)


class UnknownBinding(LookupError):
    pass


class BindingMismatch(ValueError):
    pass


class TenantRuntime:
    """Resolve authenticated bindings and atomically accept an inbound message."""

    def __init__(
        self,
        repository: RuntimeRepository,
        *,
        routing_key: bytes,
        scheduler_version: SchedulerVersion = SchedulerVersion.V1,
    ) -> None:
        if len(routing_key) < 32:
            raise ValueError("routing HMAC key must contain at least 32 bytes")
        self._repository = repository
        self._routing_key = routing_key
        self._scheduler_version = scheduler_version

    async def accept(self, binding_id: str, envelope: InboundEnvelope) -> Acceptance:
        route = await self._repository.resolve_binding(binding_id)
        if route is None or not route.binding.enabled or not route.tenant_active:
            raise UnknownBinding("channel binding is unavailable")
        return await self.accept_prepared(self.prepare(route, envelope))

    def prepare(self, route: BindingRoute, envelope: InboundEnvelope) -> PreparedInbound:
        """Freeze authenticated routing before a potentially unavailable database write."""

        binding = route.binding
        if not binding.enabled or not route.tenant_active:
            raise UnknownBinding("channel binding is unavailable")
        if envelope.channel != binding.channel or envelope.account_id != binding.account_id:
            raise BindingMismatch("verified callback does not match its channel binding")

        session_id = make_session_id(
            self._routing_key,
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            kind=envelope.conversation_kind,
            external_user_id=envelope.external_user_id,
            external_conversation_id=envelope.external_conversation_id,
        )
        bucket = rollout_bucket(
            self._routing_key,
            tenant_id=binding.tenant_id,
            app_id=binding.app_id,
            session_id=session_id,
        )
        config_version = select_config_version(
            active_version=route.active_config_version,
            candidate_version=route.candidate_config_version,
            candidate_percent=route.candidate_percent,
            bucket=bucket,
        )
        span_context = trace.get_current_span().get_span_context()
        trace_id = (
            f"{span_context.trace_id:032x}" if span_context.is_valid else secrets.token_hex(16)
        )
        context = TenantContext(
            tenant_id=binding.tenant_id,
            app_id=binding.app_id,
            config_version=config_version,
            channel_binding_id=binding.binding_id,
            principal_id=make_principal_id(
                self._routing_key,
                tenant_id=binding.tenant_id,
                binding_id=binding.binding_id,
                external_user_id=envelope.external_user_id,
            ),
            session_id=session_id,
            request_id=str(uuid4()),
            trace_id=trace_id,
        )
        trace_headers: dict[str, str] = {}
        inject_trace_headers(trace_headers)
        return PreparedInbound(
            context=context,
            envelope=envelope,
            trace_headers=trace_headers,
        )

    async def accept_prepared(self, prepared: PreparedInbound) -> Acceptance:
        """Persist a previously verified and revision-pinned inbound message."""

        context = prepared.context
        await self._repository.get_config(
            context.tenant_id,
            context.app_id,
            context.config_version,
        )
        if self._scheduler_version == SchedulerVersion.V2:
            return await self._repository.accept_inbound_v2(
                context=prepared.context,
                envelope=prepared.envelope,
                trace_headers=prepared.trace_headers,
            )
        return await self._repository.accept_inbound(
            context=prepared.context,
            envelope=prepared.envelope,
            trace_headers=prepared.trace_headers,
        )


__all__ = ["BindingMismatch", "TenantRuntime", "UnknownBinding"]
