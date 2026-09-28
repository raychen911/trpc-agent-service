"""Feishu ingress orchestration over the shared durable Agent queue."""

import json
from collections.abc import Mapping
from dataclasses import replace
import logging

from trpc_service.agent.contracts import AgentExecutionRequest
from trpc_service.agent.ports import AgentTaskQueue
from trpc_service.channels.adapters.feishu import FeishuChannelAdapter
from trpc_service.channels.approval import ApprovalCommandProcessor
from trpc_service.channels.contracts import (
    ChannelBindingConfig,
    IncomingEnvelope,
)
from trpc_service.channels.identity import PostgreSQLChannelIdentityService
from trpc_service.metrics import PlatformTelemetry
from trpc_service.tenant.context import TenantContext

logger = logging.getLogger(__name__)


class FeishuMessageService:
    """Normalize one official SDK event and enqueue it idempotently."""

    def __init__(
        self,
        adapter: FeishuChannelAdapter,
        queue: AgentTaskQueue,
        telemetry: PlatformTelemetry,
        approval_commands: ApprovalCommandProcessor,
        identity_service: PostgreSQLChannelIdentityService,
    ) -> None:
        self._adapter = adapter
        self._queue = queue
        self._telemetry = telemetry
        self._approval_commands = approval_commands
        self._identity_service = identity_service

    async def submit(
        self,
        frame: Mapping[str, object],
        binding: ChannelBindingConfig,
        tenant: TenantContext,
    ) -> str:
        envelope = IncomingEnvelope(
            binding_public_id=str(binding.binding_id),
            body=json.dumps(frame, ensure_ascii=False).encode(),
        )
        incoming = await self._adapter.decode(envelope, binding)
        session_id = f"{binding.binding_id}:{incoming.conversation_id}"
        identity = await self._identity_service.resolve(binding, incoming)
        incoming = replace(incoming, principal_id=str(identity.principal_id))
        session_id = identity.session_id
        incoming = await self._approval_commands.process(incoming, binding, session_id)
        approved_version = incoming.attributes.get("approval_config_version")
        if (incoming.attributes.get("approval_verified") is True
                and incoming.attributes.get("approval_decision") == "approve"
                and isinstance(approved_version, int)):
            tenant = tenant.model_copy(update={"config_version": approved_version})
        await self._queue.enqueue(
            AgentExecutionRequest(
                tenant=tenant,
                session_id=session_id,
                incoming=incoming,
                channel=binding,
                trace_context=self._telemetry.inject_context(),
            ))
        await self._adapter.acknowledge(envelope, binding)
        try:
            # This is a user-visible receipt, separate from the transport ACK.
            # Failure must not discard or duplicate the already durable task.
            await self._adapter.send_progress(incoming, binding)
        except Exception as error:
            logger.warning(
                "Feishu progress reply failed binding_id=%s error_type=%s",
                binding.binding_id,
                type(error).__name__,
            )
        return incoming.external_message_id
