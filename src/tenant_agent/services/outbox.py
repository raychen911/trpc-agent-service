"""Reliable IM delivery worker with bounded retry and dead-letter handling."""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

from tenant_agent.channels.base import PermanentDeliveryError, RateLimited
from tenant_agent.channels.planning import plan_outbound
from tenant_agent.channels.registry import ChannelRegistry
from tenant_agent.governance.policies import GovernanceService
from tenant_agent.models import AuditRecord, OutboundMessage
from tenant_agent.observability import (
    AUXILIARY_REPAIRS,
    DELIVERIES,
    ERRORS,
    extracted_trace_context,
    trace_id,
    traced,
)
from tenant_agent.security import CompositeSecretResolver, Redactor
from tenant_agent.services.config import TenantConfigService
from tenant_agent.services.repair import AuxiliaryRepairService
from tenant_agent.settings import Settings
from tenant_agent.storage.base import OutboxItem, OutboxRepository
from tenant_agent.storage.router import StorageRouter

logger = logging.getLogger(__name__)


class OutboxWorker:
    def __init__(
        self,
        *,
        settings: Settings,
        repository: OutboxRepository,
        storage: StorageRouter,
        configs: TenantConfigService,
        channels: ChannelRegistry,
        secrets: CompositeSecretResolver,
        redactor: Redactor,
        kinds: tuple[str, ...] = ("im-delivery", "auxiliary-repair"),
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.storage = storage
        self.configs = configs
        self.channels = channels
        self.secrets = secrets
        self.redactor = redactor
        self.kinds = kinds
        self.repairs = AuxiliaryRepairService(storage, GovernanceService(redactor))
        self.owner = f"outbox:{settings.node_id}:{uuid.uuid4().hex[:8]}"

    async def run_once(self, *, limit: int = 1) -> int:
        items = await self.repository.claim_outbox(
            self.owner, limit=limit, now=datetime.now(UTC), kinds=self.kinds
        )
        for item in items:
            if item.kind == "auxiliary-repair":
                await self._repair(item)
                continue
            started = time.perf_counter()
            channel_name = "unknown"
            try:
                revision = int(item.payload["config_revision"])
                app_id = str(item.payload["app_id"])
                current = await self.configs.repository.get_active_tenant(item.tenant_id)
                if current is None:
                    raise PermanentDeliveryError("tenant is no longer active")
                tenant = await self.configs.exact_revision(item.tenant_id, revision)
                message = OutboundMessage.model_validate(item.payload["message"])
                channel_name = message.channel.value
                if not any(
                    candidate.enabled
                    and candidate.channel == message.channel
                    and candidate.binding_id == message.binding_id
                    for candidate in current.channels
                ):
                    raise PermanentDeliveryError("channel binding is no longer active")
                binding = next(
                    binding
                    for binding in tenant.channels
                    if binding.binding_id == message.binding_id and binding.channel == message.channel
                )
                adapter = self.channels.get(message.channel)
                with extracted_trace_context(item.payload.get("trace_context", {})):
                    raw_segments = item.payload.get("segments")
                    segments = (
                        tuple(OutboundMessage.model_validate(raw) for raw in raw_segments)
                        if raw_segments
                        else plan_outbound(message)
                    )
                    next_segment = int(item.payload.get("next_segment", 0))
                    delivered_ids = list(item.payload.get("delivered_external_ids", []))
                    for segment_index in range(next_segment, len(segments)):
                        segment = segments[segment_index]
                        with traced(
                            "im.reply.segment",
                            {
                                "tenant.id": tenant.tenant_id,
                                "messaging.system": message.channel.value,
                                "messaging.destination": message.binding_id,
                                "messaging.segment.index": segment_index,
                                "messaging.segment.count": len(segments),
                            },
                            redactor=self.redactor,
                        ):
                            delivery_result = await adapter.deliver(
                                segment,
                                tenant=tenant,
                                binding=binding,
                                secrets=self.secrets,
                            )
                        delivered_ids.extend(delivery_result.external_message_ids)
                        checkpoint = dict(item.payload)
                        checkpoint["segments"] = [planned.model_dump(mode="json") for planned in segments]
                        checkpoint["delivered_external_ids"] = delivered_ids
                        checkpoint["next_segment"] = segment_index + 1
                        await self.repository.checkpoint_outbox(
                            item.outbox_id,
                            self.owner,
                            payload=checkpoint,
                        )
                    await self.repository.complete_outbox(item.outbox_id, self.owner)
                    DELIVERIES.labels(item.tenant_id, channel_name, "success").inc()
            except Exception as exc:
                error_type = exc.__class__.__name__
                terminal = isinstance(exc, PermanentDeliveryError) or (
                    item.attempts >= self.settings.outbox_max_attempts
                )
                if isinstance(exc, RateLimited):
                    delay = exc.retry_after_seconds
                else:
                    cap = min(300.0, 2 ** min(item.attempts, 8))
                    delay = random.uniform(0.5, cap)  # noqa: S311 - retry jitter only
                await self.repository.retry_outbox(
                    item.outbox_id,
                    self.owner,
                    error_type=error_type,
                    available_at=datetime.now(UTC) + timedelta(seconds=delay),
                    terminal=terminal,
                )
                DELIVERIES.labels(item.tenant_id, channel_name, "dead" if terminal else "retry").inc()
                ERRORS.labels(item.tenant_id, "im_delivery", error_type).inc()
                logger.warning("IM delivery failed with %s (terminal=%s)", error_type, terminal)
                continue

            try:
                with extracted_trace_context(item.payload.get("trace_context", {})):
                    audit = await self.storage.audit_for_tenant(tenant)
                    await tenant_audit_delivery(
                        tenant,
                        message,
                        repository=audit,
                        app_id=app_id,
                        decision="delivered",
                        latency_ms=(time.perf_counter() - started) * 1_000,
                    )
            except Exception as exc:
                error_type = exc.__class__.__name__
                ERRORS.labels(item.tenant_id, "delivery_audit", error_type).inc()
                logger.warning(
                    "Delivery audit failed after outbox completion with %s",
                    error_type,
                )
        return len(items)

    async def _repair(self, item: OutboxItem) -> None:
        resource = str(item.payload.get("resource", "unknown"))
        try:
            tenant = await self.configs.exact_revision(
                item.tenant_id,
                int(item.payload["config_revision"]),
            )
            with extracted_trace_context(item.payload.get("trace_context", {})):
                await self.repairs.repair(tenant, item.payload)
            await self.repository.complete_outbox(item.outbox_id, self.owner)
            AUXILIARY_REPAIRS.labels(item.tenant_id, resource, "success").inc()
        except Exception as exc:
            error_type = exc.__class__.__name__
            terminal = isinstance(exc, (ValueError, KeyError)) or (
                item.attempts >= self.settings.outbox_max_attempts
            )
            cap = min(300.0, 2 ** min(item.attempts, 8))
            delay = random.uniform(0.5, cap)  # noqa: S311 - retry jitter only
            await self.repository.retry_outbox(
                item.outbox_id,
                self.owner,
                error_type=error_type,
                available_at=datetime.now(UTC) + timedelta(seconds=delay),
                terminal=terminal,
            )
            AUXILIARY_REPAIRS.labels(
                item.tenant_id,
                resource,
                "dead" if terminal else "retry",
            ).inc()
            ERRORS.labels(item.tenant_id, "auxiliary_repair", error_type).inc()
            logger.warning("Auxiliary repair failed with %s (terminal=%s)", error_type, terminal)

    async def run_forever(self, stop: asyncio.Event) -> None:
        async def delivery_slot(slot: int) -> None:
            while not stop.is_set():
                try:
                    count = await self.run_once(limit=1)
                except Exception as exc:
                    error_type = exc.__class__.__name__
                    ERRORS.labels("_system", "outbox_loop", error_type).inc()
                    logger.warning("Outbox slot %s failed with %s", slot, error_type)
                    count = 0
                if count == 0:
                    try:
                        await asyncio.wait_for(
                            stop.wait(),
                            timeout=self.settings.outbox_poll_seconds,
                        )
                    except TimeoutError:
                        pass

        tasks = [
            asyncio.create_task(delivery_slot(index), name=f"outbox-slot:{index}")
            for index in range(self.settings.outbox_concurrency)
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def tenant_audit_delivery(
    tenant: object,
    message: OutboundMessage,
    *,
    repository: object,
    app_id: str,
    decision: str,
    latency_ms: float,
) -> None:
    from tenant_agent.models import TenantConfig

    if not isinstance(tenant, TenantConfig) or not tenant.audit.enabled:
        return
    if not hasattr(repository, "append_audit"):
        return
    await repository.append_audit(
        AuditRecord(
            audit_id=f"delivery-{uuid.uuid4().hex}",
            tenant_id=tenant.tenant_id,
            channel=message.channel.value,
            user_id=str(message.metadata.get("internal_user_id", "system")),
            session_id=str(message.metadata.get("internal_session_id", "unknown")),
            agent_name=tenant.apps[app_id].agent_name,
            decision=decision,
            latency_ms=latency_ms,
            trace_id=trace_id(),
            details={"binding_id": message.binding_id},
        )
    )
