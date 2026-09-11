"""Sensitive-safe audit/tool ledgers and monotonic derived data."""

import pytest

from trpc_service.persistence import Database
from trpc_service.reliability import AuditRecord
from trpc_service.reliability import AuditWriter
from trpc_service.reliability import DurablePostTurnWorker
from trpc_service.reliability import PostTurnRepository
from trpc_service.reliability import SummaryRepository
from trpc_service.reliability import ToolInvocationRepository
from trpc_service.storage import ConflictError


@pytest.fixture
def database(tmp_path):
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'derived.db'}")
    database.create_schema()
    yield database
    database.close()


def test_audit_rejects_sensitive_body_fields_and_persists_required_context(database: Database) -> None:
    writer = AuditWriter(database)
    audit_id = writer.write(
        AuditRecord(
            tenant_id="tenant_acme",
            decision="tool_allowed",
            channel="wecom",
            user_id="usr_1",
            session_id="ses_1",
            agent_name="support_agent",
            tool_name="search_order",
            latency_ms=12.5,
            trace_id="trace-1",
            request_id="req-1",
            config_version=3,
            policy_version=2,
            metadata={"execution_id": "exec-1"},
        ))
    assert writer.get(audit_id, tenant_id="tenant_acme").metadata == {"execution_id": "exec-1"}
    with pytest.raises(LookupError):
        writer.get(audit_id, tenant_id="tenant_other")

    with pytest.raises(ValueError, match="sensitive"):
        writer.write(AuditRecord(tenant_id="tenant_acme", decision="blocked", metadata={"api_key": "plain"}))


@pytest.mark.parametrize("metadata", [{"context": [{"token": "private"}]},
                                      {"details": "arbitrary prompt body"},
                                      {"execution_id": {"text": "private"}}])
def test_audit_rejects_unstructured_or_nested_bodies(database, metadata):
    with pytest.raises(ValueError):
        AuditWriter(database).write(AuditRecord(tenant_id="tenant_acme", decision="blocked", metadata=metadata))


def test_tool_ledger_stores_hashes_and_reuses_completed_result(database: Database) -> None:
    tools = ToolInvocationRepository(database)
    invocation = tools.begin("tenant_acme", "exec-1", "call-1", "charge", {"card": "4111"})
    assert invocation.idempotency_key == "exec-1:call-1"
    assert "4111" not in repr(invocation)
    tools.complete("exec-1", "call-1", {"operation_id": "op-1"}, external_operation_id="op-1")
    repeated = tools.begin("tenant_acme", "exec-1", "call-1", "charge", {"card": "4111"})
    assert repeated.status == "succeeded"
    assert repeated.external_operation_id == "op-1"

    with pytest.raises(ConflictError):
        tools.begin("tenant_acme", "exec-1", "call-1", "charge", {"card": "changed"})


def test_summary_watermark_is_monotonic_and_task_survives_repository_restart(database: Database) -> None:
    tasks = PostTurnRepository(database)
    task_id = tasks.enqueue("tenant_acme", "ses-1", "summary", 8)
    restarted = PostTurnRepository(database)
    assert restarted.claim(limit=1, worker_id="worker-b", lease_seconds=10)[0].task_id == task_id

    summaries = SummaryRepository(database)
    assert summaries.commit("tenant_acme", "ses-1", 8, "summary 8", "model-v1") == 1
    assert summaries.commit("tenant_acme", "ses-1", 8, "summary 8", "model-v1") == 1
    with pytest.raises(ConflictError, match="watermark"):
        summaries.commit("tenant_acme", "ses-1", 7, "stale", "model-v1")
    assert summaries.latest("tenant_acme", "ses-1").covered_event_seq == 8


@pytest.mark.asyncio
async def test_durable_post_turn_worker_completes_claimed_task(database: Database) -> None:
    repository = PostTurnRepository(database)
    repository.enqueue("tenant_acme", "ses-1", "memory", 9)
    processed = []

    async def process(task):
        processed.append((task.tenant_id, task.source_event_seq))

    worker = DurablePostTurnWorker(repository, process, worker_id="post-worker")
    assert await worker.run_once()
    assert processed == [("tenant_acme", 9)]
    assert not await worker.run_once()
