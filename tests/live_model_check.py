"""Opt-in acceptance against a running local service, real models and Workers.

Run with PYTHONPATH=. .venv/bin/python tests/live_model_check.py --help.
This is deliberately outside pytest discovery: it uses provider quota.
"""

import argparse
import asyncio
from datetime import datetime, timezone
import json
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from time import monotonic
from uuid import UUID, uuid4

import httpx
from sqlalchemy import func, select, update

from trpc_service.agent.contracts import AgentExecutionRequest
from trpc_service.agent.queue import PostgreSQLAgentTaskQueue
from trpc_service.channels.contracts import IncomingMessage, MessageKind
from trpc_service.channels.models import ChannelBinding
from trpc_service.config import Settings
from trpc_service.storage.database import build_engine, build_session_factory
from trpc_service.storage.runtime_orm import (
    AgentTaskRow,
    OutboxMessageRow,
    ToolCallLedgerRow,
    UsageLedgerRow,
)
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import Tenant


def report(**fields: object) -> None:
    """Print only synthetic test replies and safe runtime counters."""
    print(json.dumps(fields, ensure_ascii=False), flush=True)


async def wait_for_test_tasks(sessions, tenant_id: UUID, *, timeout: float = 30) -> None:
    """Wait for final commits, without revoking a live Worker's execution lease."""
    async with asyncio.timeout(timeout):
        while True:
            async with sessions() as database:
                count = await database.scalar(
                    select(func.count()).select_from(AgentTaskRow).where(
                        AgentTaskRow.tenant_id == tenant_id,
                        AgentTaskRow.status.in_(("queued", "running", "retryable_failed"))))
            if not count:
                return
            await asyncio.sleep(.25)


def finish_cleanup(failures: list[tuple[str, Exception]],
                   primary_error: BaseException | None) -> None:
    """Preserve the original verdict and make incomplete cleanup explicit."""
    if not failures:
        report(cleanup="complete")
        return
    labels = [f"{label}: {type(error).__name__}" for label, error in failures]
    report(cleanup="incomplete",
           failures=labels,
           note="Test resources or unfinished tasks may remain; inspect this run's tenant.")
    if primary_error is not None:
        primary_error.add_note("Acceptance cleanup incomplete: " + "; ".join(labels))
    else:
        raise RuntimeError("Acceptance cleanup incomplete: " + "; ".join(labels))


async def cleanup_steps(
    operations: list[tuple[str, Callable[[], Awaitable[object]]]],
    primary_error: BaseException | None,
) -> None:
    """Try every scoped cleanup step, including drain and final resource close."""
    failures = []
    for label, operation in operations:
        try:
            await operation()
        except Exception as error:
            failures.append((label, error))
    finish_cleanup(failures, primary_error)


async def run_check(args: argparse.Namespace) -> None:
    settings = Settings(database_url=args.database_url,
                        database_password_file=args.database_password_file)
    engine = build_engine(settings)
    sessions = build_session_factory(engine)
    queue = PostgreSQLAgentTaskQueue(sessions)
    tenant_id = agent_id = profile_id = document_id = None
    stamp = uuid4().hex[:10]
    token = args.admin_token_file.read_text().strip()
    headers = {"Authorization": f"Bearer {token}", "X-Support-Reason": "real model acceptance"}
    async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{settings.port}{settings.api_prefix}",
            headers=headers,
            trust_env=False,
            follow_redirects=False,
            timeout=150,
    ) as client:

        async def api(method: str, path: str, **kwargs: object):
            response = await client.request(method, path, **kwargs)
            if not response.is_success:
                # Do not reflect arbitrary proxy/provider response bodies.
                raise RuntimeError(f"acceptance API returned HTTP {response.status_code}: {path}")
            return response.json() if response.content else None

        try:
            catalog = None
            offset = 0
            while catalog is None:
                page = await api("GET",
                                 "/admin/model-catalog",
                                 params={
                                     "offset": offset,
                                     "limit": 100
                                 })
                catalog = next((item for item in page["items"]
                                if item["model_catalog_id"] == str(args.model_catalog_id)), None)
                offset += len(page["items"])
                if catalog is None and (offset >= page["total"] or not page["items"]):
                    raise ValueError("model catalog entry not found")
            assert catalog["status"] == "active", "select an active model catalog entry"
            tenant_id = UUID((await api("POST", "/tenants", json={
                "name": f"live-check-{stamp}",
            }))["tenant_id"])
            async with sessions() as database:
                assert await database.get(Tenant, tenant_id), "API and database do not match"
            profile_id = (await api("POST",
                                    f"/tenants/{tenant_id}/model-profiles",
                                    json={
                                        "name": "live-check",
                                        "model_catalog_id": str(args.model_catalog_id),
                                        "credential_id": str(args.credential_id),
                                        "parameter_config": {
                                            "temperature":
                                            0,
                                            "max_output_tokens":
                                            2048,
                                            "timeout_seconds":
                                            120,
                                            **({
                                                "enable_thinking": False
                                            } if args.disable_thinking else {})
                                        },
                                    }))["model_profile_id"]
            agent_id = UUID((await api("POST",
                                       f"/tenants/{tenant_id}/agents",
                                       json={
                                           "name": "live-check",
                                           "model_profile_id": profile_id,
                                           "application_config": {
                                               "instruction": "你是验收助手。准确、简短地回答。用户要求工具时必须调用工具。"
                                           },
                                           "tool_permissions": {
                                               "allowlist": ["calculate", "knowledge.search"]
                                           },
                                           "knowledge_config": {
                                               "knowledge_base_names": ["live-check"]
                                           },
                                       }))["agent_app_id"])
            async with sessions.begin() as database:
                binding = ChannelBinding(
                    tenant_id=tenant_id,
                    agent_app_id=agent_id,
                    channel_type="live_check",
                    external_account_hash=stamp,
                )
                database.add(binding)
                await database.flush()
                channel = binding.to_config()
            report(tenant=str(tenant_id),
                   agent=str(agent_id),
                   model=catalog["model_name"],
                   embedding=settings.embedding.model_name)

            async def turn(label: str,
                           prompt: str,
                           *,
                           conversation: str = "conversation",
                           expected: str,
                           tool: str | None = None) -> str:
                request_id = f"live-{uuid4().hex}"
                request = AgentExecutionRequest(
                    tenant=TenantContext(tenant_id=tenant_id,
                                         agent_app_id=agent_id,
                                         config_version=1,
                                         request_id=request_id,
                                         trace_id=uuid4().hex),
                    session_id=conversation,
                    channel=channel,
                    incoming=IncomingMessage(
                        external_message_id=request_id,
                        principal_id="live-tester",
                        conversation_id=conversation,
                        kind=MessageKind.TEXT,
                        occurred_at=datetime.now(timezone.utc),
                        text=prompt,
                    ),
                )
                task_id = await queue.enqueue(request)
                assert await queue.enqueue(request) == task_id, "enqueue is not idempotent"
                started = monotonic()
                async with asyncio.timeout(160):
                    while True:
                        async with sessions() as database:
                            task = await database.get(AgentTaskRow, UUID(task_id))
                            if task.status in {"permanent_failed", "retryable_failed"}:
                                raise RuntimeError(
                                    f"{label}: {task.status}, {task.last_error_code}")
                            if task.status == "succeeded":
                                replies = (await database.scalars(
                                    select(OutboxMessageRow).where(
                                        OutboxMessageRow.tenant_id == tenant_id,
                                        OutboxMessageRow.request_id == request_id,
                                    ))).all()
                                usage = await database.scalar(
                                    select(UsageLedgerRow).where(
                                        UsageLedgerRow.tenant_id == tenant_id,
                                        UsageLedgerRow.request_id == request_id,
                                    ))
                                calls = (await database.scalars(
                                    select(ToolCallLedgerRow).where(
                                        ToolCallLedgerRow.tenant_id == tenant_id,
                                        ToolCallLedgerRow.request_id == request_id,
                                    ))).all()
                                assert replies and all(
                                    row.status == "PENDING" and row.destination == "live_check"
                                    for row in replies)
                                answer = "\n".join(
                                    str(row.payload.get("text", "")) for row in replies)
                                assert expected in answer, f"{label}: expected answer missing"
                                assert len(answer) < 600, f"{label}: unexpectedly verbose reply"
                                assert usage and usage.status == "completed"
                                assert usage.total_tokens > 0
                                assert task.attempt_count == 1, "unexpected retry"
                                if tool:
                                    assert any(call.name == tool and call.status == "SUCCEEDED"
                                               for call in calls), "real tool was not executed"
                                report(test=label,
                                       status="passed",
                                       seconds=round(monotonic() - started, 2),
                                       answer=answer,
                                       input_tokens=usage.input_tokens,
                                       output_tokens=usage.output_tokens,
                                       tools=[call.name for call in calls])
                                break
                        await asyncio.sleep(.5)
                assert await queue.enqueue(request) == task_id, "completed task was duplicated"
                async with sessions() as database:
                    count = await database.scalar(
                        select(func.count()).select_from(UsageLedgerRow).where(
                            UsageLedgerRow.tenant_id == tenant_id,
                            UsageLedgerRow.request_id == request_id))
                    assert count == 1, "duplicate usage fact"
                return answer

            await turn("basic", "只回复：真实模型连接成功。", expected="真实模型连接成功")
            await turn("remember", "记住验收代号是蓝鹭7529，只回复已记住。", expected="已记住")
            await turn("recall", "刚才告诉你的验收代号是什么？", expected="蓝鹭7529")
            await turn("tool",
                       "必须使用 calculate 工具计算 (173 * 29) + 41。",
                       expected="5058",
                       tool="calculate")
            route = f"/tenants/{tenant_id}/knowledge-bases/live-check/documents"
            params = {"agent_app_id": str(agent_id), "filename": "live-check.txt"}
            document = await api("POST",
                                 route,
                                 params=params,
                                 headers={"Content-Type": "text/plain"},
                                 content="云帆项目的专属通行口令为松石-8362。验收负责人称号为白鹭工程师。".encode())
            document_id = document["document_id"]
            assert document["status"] == "READY" and document["chunk_count"] > 0
            report(test="embedding-upload", status="passed", chunks=document["chunk_count"])
            answer = await turn(
                "rag",
                "必须调用 knowledge_search 工具检索 live-check 知识库，云帆项目的通行口令和负责人称号是什么？引用来源。",
                conversation="rag-v1",
                expected="8362",
                tool="knowledge.search")
            assert "白鹭" in answer
            assert "live-check.txt" in answer or "[知识1]" in answer
            replacement = await api("PUT",
                                    f"{route}/{document_id}",
                                    params=params,
                                    headers={"Content-Type": "text/plain"},
                                    content="云帆项目的专属通行口令已更新为琥珀-4917。".encode())
            document_id = replacement["document_id"]
            assert replacement["status"] == "READY" and replacement["version"] == 2
            answer = await turn("rag-update",
                                "检索 live-check 知识库，云帆项目目前的通行口令是什么？",
                                conversation="rag-v2",
                                expected="4917")
            assert "8362" not in answer, "superseded document is still visible"
            await api("DELETE", f"{route}/{document_id}", params={"agent_app_id": str(agent_id)})
            document_id = None
            remaining = await api("GET", route, params={"agent_app_id": str(agent_id)})
            assert remaining["total"] == 0
            report(test="knowledge-delete", status="passed")
        finally:
            primary_error = sys.exception()
            operations: list[tuple[str, Callable[[], Awaitable[object]]]] = []

            if tenant_id:

                async def disable_binding() -> None:
                    async with sessions.begin() as database:
                        await database.execute(
                            update(ChannelBinding).where(
                                ChannelBinding.tenant_id == tenant_id).values(status="disabled"))

                async def cancel_replies() -> None:
                    async with sessions.begin() as database:
                        await database.execute(
                            update(OutboxMessageRow).where(
                                OutboxMessageRow.tenant_id == tenant_id,
                                OutboxMessageRow.status == "PENDING").values(status="CANCELLED"))

                operations.append(("binding", disable_binding))
                if document_id and agent_id:
                    operations.append(
                        ("document",
                         lambda: api("DELETE", f"/tenants/{tenant_id}/knowledge-bases/live-check/"
                                     f"documents/{document_id}",
                                     params={"agent_app_id": str(agent_id)})))
                if agent_id:
                    operations.append(
                        ("agent", lambda: api("DELETE", f"/tenants/{tenant_id}/agents/{agent_id}")))
                if profile_id:
                    operations.append((
                        "profile",
                        lambda: api("DELETE", f"/tenants/{tenant_id}/model-profiles/{profile_id}")))
                operations.append(("tenant", lambda: api("DELETE", f"/tenants/{tenant_id}")))
                # Polling timeout does not cancel a Worker. Wait until this run's
                # tasks settle before the final Outbox sweep; report any leftovers.
                operations.append(("task drain", lambda: wait_for_test_tasks(sessions, tenant_id)))
                operations.append(("outbox", cancel_replies))
            operations.append(("database close", engine.dispose))
            await cleanup_steps(operations, primary_error)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url",
                        required=True,
                        help="Same local database used by start.sh")
    parser.add_argument("--database-password-file",
                        type=Path,
                        default=Path(".secrets/postgres_password"))
    parser.add_argument("--admin-token-file",
                        type=Path,
                        default=Path(".secrets/admin_bootstrap_token"))
    parser.add_argument("--model-catalog-id", required=True, type=UUID)
    parser.add_argument("--credential-id", required=True, type=UUID)
    parser.add_argument("--disable-thinking",
                        action="store_true",
                        help="Request fast replies on models supporting enable_thinking")
    asyncio.run(run_check(parser.parse_args()))
