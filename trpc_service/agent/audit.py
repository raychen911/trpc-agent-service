"""Runtime Agent audit records routed through tenant-selected AuditStore ports."""

from collections.abc import Mapping
from datetime import datetime, timezone
import hashlib

from trpc_service.agent.contracts import AgentExecutionRequest, AgentRuntimeConfig
from trpc_service.agent.governance import AgentAuditRecorder
from trpc_service.log import SensitiveDataRedactor
from trpc_service.storage.router import BackendProfile, StorageRouter
from trpc_service.storage.types import AuditRecord


class StorageAuditRecorder(AgentAuditRecorder):
    """Append complete redacted runtime facts to the configured Audit backend."""

    def __init__(self, router: StorageRouter, redactor: SensitiveDataRedactor) -> None:
        self._router = router
        self._redactor = redactor

    @staticmethod
    def _audit_required(config: AgentRuntimeConfig) -> bool:
        raw_audit = config.policy.get("audit", {})
        if not isinstance(raw_audit, Mapping):
            raise ValueError("audit policy must be an object")
        required = raw_audit.get("required", True)
        if not isinstance(required, bool):
            raise ValueError("audit required flag must be a boolean")
        return required

    async def record(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        *,
        action: str,
        decision: str,
        reason_code: str,
        latency_ms: float,
        error_type: str | None = None,
        tool_name: str | None = None,
        cost_amount: float = 0,
        details: Mapping[str, object] | None = None,
    ) -> None:
        """Write one append-only event or fail closed when audit is mandatory."""

        storage = self._router.resolve(BackendProfile.from_mapping(config.backends))
        if storage.audit is None:
            if self._audit_required(config):
                raise RuntimeError("tenant policy requires an Audit backend")
            return
        raw_name = config.application.get("name", str(request.tenant.agent_app_id))
        agent_name = self._redactor.redact_text(str(raw_name), redact_pii=False)
        resource_id_hash = hashlib.sha256(
            (f"{request.tenant.tenant_id}:{request.tenant.agent_app_id}:"
             f"{request.session_id}").encode()).hexdigest()
        source_ip_hash = request.incoming.attributes.get("source_ip_hash")
        if source_ip_hash is not None and not isinstance(source_ip_hash, str):
            raise ValueError("source_ip_hash must be a string when supplied by an adapter")
        attributes: dict[str, object] = {
            "binding_id":
            request.channel.binding_id,
            "channel":
            request.channel.channel_type,
            "principal_id":
            request.incoming.principal_id,
            "user_id":
            request.incoming.principal_id,
            "session_id":
            request.session_id,
            "agent_name":
            agent_name,
            "tool_name": (None if tool_name is None else self._redactor.redact_text(
                tool_name,
                redact_pii=True,
            )),
            "latency_ms":
            max(latency_ms, 0),
            "error_type":
            error_type,
            "cost_amount":
            max(cost_amount, 0),
            "trace_id":
            request.tenant.trace_id,
            "request_id":
            request.tenant.request_id,
            "policy_version":
            str(config.config_version),
            "config_version":
            config.config_version,
            "reason_code":
            reason_code,
            "actor_type":
            "channel_principal",
            "resource_type":
            "tool" if tool_name is not None else "agent_execution",
            "resource_id_hash":
            resource_id_hash,
            "source_ip_hash":
            source_ip_hash,
            "details_redacted":
            self._redactor.redact_mapping(
                details or {},
                redact_pii=True,
            ),
        }
        await storage.audit.append(
            request.tenant,
            AuditRecord(
                action=action,
                decision=decision,
                occurred_at=datetime.now(timezone.utc),
                attributes=attributes,
            ),
        )
