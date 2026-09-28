import pytest

from tests.test_agent_task_queue import _request
from tests.test_storage_router import StubSessionStore
from trpc_service.agent.audit import StorageAuditRecorder
from trpc_service.agent.contracts import AgentRuntimeConfig
from trpc_service.log import SensitiveDataRedactor
from trpc_service.storage import (
    AuditRecord,
    AuditStore,
    StorageBackend,
    StorageBackendRegistry,
    StorageRouter,
)


class RecordingAuditStore(AuditStore):

    def __init__(self) -> None:
        self.records: list[AuditRecord] = []

    async def append(self, context, record):  # type: ignore[no-untyped-def]
        del context
        self.records.append(record)


@pytest.mark.anyio
async def test_runtime_audit_contains_required_redacted_dimensions() -> None:
    request = _request()
    session = StubSessionStore()
    audit = RecordingAuditStore()
    registry = StorageBackendRegistry()
    registry.register(StorageBackend("facts", session=session, outbox=session))
    registry.register(StorageBackend("audit", audit=audit))
    recorder = StorageAuditRecorder(StorageRouter(registry), SensitiveDataRedactor())
    config = AgentRuntimeConfig(
        config_version=request.tenant.config_version,
        runner_name="trpc_agent",
        application={"name": "Support Agent"},
        policy={"audit": {
            "required": True
        }},
        backends={
            "session": "facts",
            "audit": "audit"
        },
    )

    await recorder.record(
        request,
        config,
        action="agent.execute",
        decision="allow",
        reason_code="COMPLETED",
        latency_ms=125.5,
        cost_amount=0.02,
        tool_name="search-sk-example-secret-123456",
        details={"authorization": "Bearer audit-secret-value"},
    )

    record = audit.records[0]
    assert record.action == "agent.execute"
    assert record.decision == "allow"
    assert record.attributes["binding_id"] == request.channel.binding_id
    assert record.attributes["channel"] == "wecom"
    assert record.attributes["principal_id"] == request.incoming.principal_id
    assert record.attributes["session_id"] == request.session_id
    assert record.attributes["agent_name"] == "Support Agent"
    assert record.attributes["latency_ms"] == 125.5
    assert record.attributes["cost_amount"] == 0.02
    assert record.attributes["tool_name"] == "search-[REDACTED_SECRET]"
    assert record.attributes["actor_type"] == "channel_principal"
    assert record.attributes["resource_type"] == "tool"
    assert len(str(record.attributes["resource_id_hash"])) == 64
    assert record.attributes["source_ip_hash"] is None
    assert record.attributes["details_redacted"] == {"authorization": "[REDACTED]"}
