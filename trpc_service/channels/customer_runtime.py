"""Customer-service sync worker. Cursor commit precedes admission to Agent."""
import hashlib

from trpc_service.channels.base import UnsupportedMessageError
from trpc_service.gateway.models import TraceContext
from trpc_service.log import AuditEvent
from trpc_service.log.audit import register_secret
from trpc_service.metrics import platform_span, current_trace_context
from .customer_service import CustomerServiceError


class CustomerServiceRuntime:

    def __init__(self, adapter, store, admit, audit=None, tenant_id=""):
        self.adapter, self.store, self.admit = adapter, store, admit
        self.binding = adapter.binding
        self.audit, self.tenant_id = audit, tenant_id

    async def notification(self, body, signature, timestamp, nonce, trace=None):
        if not self.adapter.crypto:
            raise ValueError("customer-service callback crypto is not configured")
        value = self.adapter.crypto.notification(body, signature, timestamp, nonce)
        if value.get("OpenKfId") != self.binding.open_kfid:
            from .base import ChannelAuthenticationError
            raise ChannelAuthenticationError("customer-service callback account mismatch")
        register_secret(value["Token"])
        with platform_span("customer_service.callback", trace):
            await self.store.notify(self.binding.binding_id, value["Token"],
                                    hashlib.sha256(body).hexdigest(),
                                    current_trace_context().model_dump())

    async def step(self):
        binding_id = self.binding.binding_id
        claim = await self.store.claim_sync(binding_id)
        if claim:
            try:
                with platform_span("customer_service.sync", TraceContext(**(claim.get("trace") or {}))):
                    page = await self.adapter.client.sync_messages(self.binding.open_kfid, claim["cursor"],
                                                                   claim["token"])
                    claim["trace"] = current_trace_context().model_dump()
                    await self.store.save_page(binding_id, claim, page, self.binding.open_kfid)
            except Exception:
                await self.store.release_sync(binding_id, claim)
                raise
        item = await self.store.claim_message(binding_id)
        if item:
            try:
                message = await self.adapter.normalize(binding_id, item["raw"], {})
                message.trace = TraceContext(**item.get("trace", {}))
                user = message.external_user_id
                if await self.adapter.client.service_state(self.binding.open_kfid, user) not in {0, 1}:
                    await self.store.finish_message(binding_id, item, "human_or_closed")
                    await self._audit("customer_message", "deny")
                else:
                    await self.admit(message)
                    await self.store.finish_message(binding_id, item, "admitted")
                    await self._audit("customer_message", "allow")
            except UnsupportedMessageError:
                await self.store.finish_message(binding_id, item, "unsupported")
            except CustomerServiceError as error:
                if not error.retryable:
                    await self.store.finish_message(binding_id, item, "failed")
                raise
        return bool(claim or item)

    async def _audit(self, action, decision):
        if self.audit:
            await self.audit.write(
                AuditEvent(tenant_id=self.tenant_id,
                           channel="wecom_kf",
                           user_id="",
                           session_id="",
                           agent_name=self.binding.app_id,
                           action=action,
                           decision=decision))
