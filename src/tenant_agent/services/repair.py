"""Replay eventually consistent Summary and Memory projections from Session events."""

from __future__ import annotations

from typing import Any

from tenant_agent.governance.policies import GovernanceService
from tenant_agent.models import MemoryRecord, SummaryRecord, TenantConfig
from tenant_agent.storage.router import StorageRouter


class AuxiliaryRepairService:
    def __init__(self, storage: StorageRouter, governance: GovernanceService) -> None:
        self.storage = storage
        self.governance = governance

    def _event_text(self, tenant: TenantConfig, event: Any) -> str:
        if "effective_text" in event.payload:
            return str(event.payload["effective_text"])
        if event.kind == "user_message":
            return self.governance.effective_input(
                tenant,
                str(event.payload.get("text", "")),
                event.payload.get("attachments", ()),
            )
        return str(event.payload.get("text", ""))

    async def repair(self, tenant: TenantConfig, payload: dict[str, Any]) -> str:
        resource = str(payload["resource"])
        if resource == "summary":
            await self._repair_summary(tenant, payload)
        elif resource == "memory":
            await self._repair_memory(tenant, payload)
        else:
            raise ValueError("unsupported auxiliary repair resource")
        return resource

    async def _repair_summary(self, tenant: TenantConfig, payload: dict[str, Any]) -> None:
        sessions = await self.storage.session_for_tenant(tenant)
        summaries = await self.storage.summary_for_tenant(tenant)
        session_id = str(payload["session_id"])
        snapshot = await sessions.get_session(tenant.tenant_id, session_id)
        if snapshot is None:
            raise KeyError("repair session no longer exists")
        previous = await summaries.get_summary(tenant.tenant_id, session_id)
        through = previous.through_event_sequence if previous else 0
        threshold = max(2, int(payload["summary_every_events"]))
        if snapshot.last_event_sequence - through < threshold:
            return
        events = await sessions.list_events(
            tenant.tenant_id,
            session_id,
            after_sequence=through,
        )
        lines = [
            f"{event.kind}: " + self._event_text(tenant, event)[:800]
            for event in events
            if event.kind in {"user_message", "assistant_message"}
        ]
        summary = SummaryRecord(
            tenant_id=tenant.tenant_id,
            session_id=session_id,
            version=(previous.version + 1) if previous else 1,
            through_event_sequence=snapshot.last_event_sequence,
            content=((previous.content + "\n") if previous else "") + "\n".join(lines),
        )
        await summaries.put_summary(summary.model_copy(update={"content": summary.content[-8_000:]}))

    async def _repair_memory(self, tenant: TenantConfig, payload: dict[str, Any]) -> None:
        sessions = await self.storage.session_for_tenant(tenant)
        memories = await self.storage.memory_for_tenant(tenant)
        session_id = str(payload["session_id"])
        inbound = await sessions.get_event(
            tenant.tenant_id,
            session_id,
            str(payload["inbound_event_id"]),
        )
        outbound = await sessions.get_event(
            tenant.tenant_id,
            session_id,
            str(payload["outbound_event_id"]),
        )
        if inbound is None or outbound is None:
            raise KeyError("repair source events are incomplete")
        effective_text = self._event_text(tenant, inbound)
        await memories.put_memory(
            MemoryRecord(
                memory_id=str(payload["memory_id"]),
                tenant_id=tenant.tenant_id,
                user_id=str(payload["user_id"]),
                content=(f"User: {effective_text}\nAssistant: {outbound.payload.get('text', '')!s}"),
                metadata={
                    "session_id": session_id,
                    "message_id": str(payload["message_id"]),
                    "through_event_sequence": outbound.sequence,
                },
            )
        )
