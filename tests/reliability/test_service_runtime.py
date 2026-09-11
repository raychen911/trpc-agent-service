"""The deployable simulation uses the real SDK, including native recovery."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import delete, select

from trpc_service.channels import NormalizedInboundMessage
from trpc_service.persistence import Database
from trpc_service.persistence.models import InboundMessageRow, OutboxMessageRow, SessionEventRow, PostTurnTaskRow
from trpc_service.service_runtime import ServiceRuntime


@pytest.mark.asyncio
async def test_native_history_recovery_memory_and_summary(tmp_path, monkeypatch):
    tenants = json.loads(Path("deploy/simulation-tenants.json").read_text(encoding="utf-8"))
    for role in ("session", "summary"):
        tenants[0]["data_backends"][role].update(kind="sql", profile_id="sql_runtime")
    config = tmp_path / "tenants.json"
    config.write_text(json.dumps(tenants), encoding="utf-8")
    monkeypatch.setenv("TRPC_RUNTIME_MODE", "simulation")
    monkeypatch.setenv("TRPC_TENANT_CONFIG", str(config))
    monkeypatch.setenv("TRPC_IDENTITY_KEY", "test-identity-key-" * 4)
    monkeypatch.setenv("TRPC_SESSION_DATABASE_URL", f"sqlite:///{tmp_path / 'native.db'}")
    db = Database(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    db.create_schema()
    runtime = ServiceRuntime(db)
    await runtime.start()
    try:
        for index, text in enumerate(("remember lighthouse", "what did I say?", "continue")):
            message = NormalizedInboundMessage(
                channel="wecom", webhook_public_id="callback_acme",
                external_message_id=f"native-{index}", external_user_id="user-1", conversation_type="direct",
                text=text, received_at=datetime.now(timezone.utc), request_id=f"request-{index}")
            route = runtime.router.route_message(message)
            receipt = runtime.pipeline.ingest(message, route, trace_id="trace")
            work = runtime.inbox.claim(worker_id="worker", lease_seconds=30)
            outputs = await runtime.pipeline.execute(work, runtime.registry, worker_id="worker")
            assert "lighthouse" in outputs[-1].text
        # Remove only platform projections to reproduce native-final/platform gap.
        with db.sessions.begin() as session:
            source_seq = session.scalar(select(SessionEventRow.seq_no).where(
                SessionEventRow.execution_id == receipt.execution_id))
            session.execute(delete(PostTurnTaskRow).where(PostTurnTaskRow.source_event_seq == source_seq))
            session.execute(delete(OutboxMessageRow).where(
                OutboxMessageRow.inbound_message_id == receipt.inbound_message_id))
            session.execute(delete(SessionEventRow).where(SessionEventRow.execution_id == receipt.execution_id))
            row = session.get(InboundMessageRow, receipt.inbound_message_id)
            row.status = "retry"
        recovered = runtime.inbox.claim(worker_id="recovered", lease_seconds=30)
        service = runtime.sessions[(route.tenant_id, route.agent_app_id, route.config_version)]
        before = await service.get_session(app_name=service.app_name, user_id=route.internal_user_id,
                                           session_id=route.session_id)
        await runtime.pipeline.execute(recovered, runtime.registry, worker_id="recovered")
        after = await service.get_session(app_name=service.app_name, user_id=route.internal_user_id,
                                          session_id=route.session_id)
        assert [event.id for event in before.events] == [event.id for event in after.events]
        tasks = runtime.post.claim(limit=100, worker_id="post", lease_seconds=60)
        for task in sorted(tasks, key=lambda item: item.task_type):
            await runtime.process_post_turn(task)
            runtime.post.complete(task.task_id, "post")
        assert (await runtime.memory.search_memory(after.save_key, "lighthouse")).memories
        summary_session = await service.get_session(app_name=service.app_name, user_id=route.internal_user_id,
                                                    session_id=route.session_id)
        anchors = [event for event in summary_session.events if event.is_summary_event()]
        assert anchors and anchors[0].custom_metadata["source_native_event_ids"]
        with db.sessions() as session:
            assert set(session.scalars(select(InboundMessageRow.status))) == {"succeeded"}
    finally:
        await runtime.close()
        db.close()


def test_production_is_rejected_without_atomic_sdk_contract(tmp_path, monkeypatch):
    monkeypatch.delenv("TRPC_RUNTIME_MODE", raising=False)
    db = Database(f"sqlite:///{tmp_path / 'gate.db'}")
    try:
        with pytest.raises(ValueError, match="atomic"):
            ServiceRuntime(db)
    finally:
        db.close()
