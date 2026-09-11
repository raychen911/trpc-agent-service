"""Official SDK wire/tool/SQL/summary chain; provider and object transport are local fixtures."""

from decimal import Decimal
import json
import os
from types import SimpleNamespace

from cryptography.fernet import Fernet
import httpx
import pytest
from sqlalchemy import select
from telegram import Bot
from trpc_agent_sdk.abc import ArtifactId
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tests.integration.test_im_transports import ProtocolRequest, update
from tests.integration.test_openrouter_accounting import provider_response
from tests.storage.test_artifact_knowledge import FakeMinio
from trpc_service.channels.acceptance import AcceptanceRuntime, test_tenant as build_tenant
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.persistence.models import ToolInvocationRow, ToolResultRow, InboundMessageRow
from trpc_service.storage import S3ArtifactService, GuardedMemoryService, TenantBoundaryError
from trpc_service.telemetry import runtime as telemetry


def tool_response(index, *, missing_cost=False):
    value = provider_response(None if missing_cost else "0.00001234")
    if index < 2:
        name = "write_artifact" if index == 0 else "read_artifact"
        args = {"filename": "acceptance.txt"}
        if index == 0:
            args["text"] = "lighthouse saved content"
        value["choices"][0].update(finish_reason="tool_calls",
                                   message={
                                       "role":
                                       "assistant",
                                       "content":
                                       None,
                                       "tool_calls": [{
                                           "index": 0,
                                           "id": "artifact-call-" + str(index),
                                           "type": "function",
                                           "function": {
                                               "name": name,
                                               "arguments": json.dumps(args)
                                           }
                                       }]
                                   })
    if index >= 3:  # Summary uses the official non-streaming request path.
        return httpx.Response(200, json=value)
    value["object"] = "chat.completion.chunk"
    value["choices"][0]["delta"] = value["choices"][0].pop("message")
    return httpx.Response(200,
                          content="data: " + json.dumps(value) + "\n\ndata: [DONE]\n\n",
                          headers={"content-type": "text/event-stream"})


@pytest.mark.asyncio
@pytest.mark.parametrize("cost_gap", [None, 0, 3], ids=["complete", "tool-cost-missing", "summary-cost-missing"])
async def test_budgeted_native_tools_post_turn_and_trace(tmp_path, cost_gap, request):
    config = build_tenant("telegram", "123456").model_dump(mode="json")
    config["applications"][0].update(max_tool_iterations=2,
                                     tool_policy={
                                         "allow": ["write_artifact", "read_artifact"],
                                         "max_calls_per_run": 2
                                     })
    from trpc_service.tenant import TenantConfig
    tenant = TenantConfig.model_validate(config)
    runtime = AcceptanceRuntime(tmp_path / "isolated", tenant, b"k" * 32, [Fernet.generate_key()])
    real_objects = request.config.getoption("--backend-mode") == "real"
    if real_objects:
        from minio import Minio
        from tests.e2e.conftest import required_env
        client = Minio(required_env("TRPC_TOOL_TEST_MINIO_ENDPOINT"),
                       access_key=required_env("TRPC_TOOL_TEST_MINIO_ACCESS_KEY"),
                       secret_key=required_env("TRPC_TOOL_TEST_MINIO_SECRET_KEY"),
                       secure=os.environ.get("TRPC_TOOL_TEST_MINIO_SECURE", "true") != "false")
    else:
        client = FakeMinio()
    bucket = "tool-test-" + tenant.tenant_id.replace("_", "-")
    artifact = S3ArtifactService(tenant_id=tenant.tenant_id, bucket=bucket, client=client, database=runtime.database)
    resources = SimpleNamespace(artifact=artifact, knowledge=None, index_version=1)
    exporter = InMemorySpanExporter()
    telemetry.configure("tool-acceptance", span_exporter=exporter)
    calls = []

    def handle(request):
        calls.append(json.loads(request.content))
        return tool_response(len(calls) - 1, missing_cost=cost_gap == len(calls) - 1)

    try:
        await runtime.start("synthetic-api-key", 163840, transport=httpx.MockTransport(handle), resources=resources)
        protocol = ProtocolRequest()
        async with Bot("123456:synthetic-token", request=protocol) as bot:
            adapter = TelegramAdapter(bot)
            challenge = "save lighthouse and read the saved file"
            event = adapter.normalize(update(text=challenge), runtime.binding)
            route = runtime.router.route_message(event.message)
            from trpc_agent_sdk.events import Event
            from trpc_agent_sdk.types import Content, Part
            seed = await runtime.session.create_session(app_name=runtime.session.app_name,
                                                        user_id=route.internal_user_id,
                                                        session_id="past-session")
            await runtime.session.append_event(
                seed,
                Event(author="user",
                      invocation_id="seed",
                      content=Content(role="user", parts=[Part.from_text(text="lighthouse memory anchor")])))
            await runtime.memory.store_session(seed)
            if cost_gap == 3:
                with pytest.raises(RuntimeError, match="summary was not produced"):
                    await runtime.execute(event, adapter, challenge)
                report = runtime.report()
                assert report["input_statuses"] == ["succeeded"]
                assert report["post_turn"] == "not_executed"
                assert report["post_turn_statuses"] == {"memory": "succeeded", "summary": "failed"}
                assert [row["status"] for row in report["model_attempts"]].count("pending_reconciliation") == 1
                assert len(calls) == 4 and [name for name, _ in protocol.sent].count("sendMessage") == 1
                return
            report = await runtime.execute(event, adapter, challenge)
            if cost_gap == 0:
                assert len(calls) == 1 and not list(client.list_objects(bucket, prefix="", recursive=True))
                assert report["model_attempts"][0]["status"] == "pending_reconciliation"
                return
            assert report["input_statuses"] == ["succeeded"] and report["post_turn"] == "executed"
            assert report["production_ready"] is False
            assert len(calls) == 4
            assert "lighthouse memory anchor" in json.dumps(calls[0])
            assert all(row["status"] == "settled" for row in report["model_attempts"])
            assert sum(Decimal(row["actual_amount"]) for row in report["model_attempts"]) == Decimal("0.00004936")
            assert {tool["function"]["name"] for tool in calls[0]["tools"]} == {"write_artifact", "read_artifact"}
            assert any(message["role"] == "tool" and "lighthouse saved content" in message["content"]
                       for message in calls[2]["messages"])
            assert "tools" not in calls[3]
            assert [name for name, _ in protocol.sent].count("sendMessage") == 1
            identity = ArtifactId(app_name=runtime.session.app_name,
                                  user_id=route.internal_user_id,
                                  session_id=route.session_id,
                                  filename="acceptance.txt")
            entry = await artifact.load_artifact(artifact_id=identity)
            assert entry.data.text == "lighthouse saved content" and entry.version.version == 0
            with pytest.raises(TenantBoundaryError):
                await artifact.load_artifact(artifact_id=identity.model_copy(update={"app_name": "other:app"}))
            with runtime.database.sessions() as session:
                inbound = session.scalar(select(InboundMessageRow))
                assert len(list(session.scalars(select(ToolResultRow)))) == 2
                assert list(session.scalars(select(ToolInvocationRow.status))) == ["succeeded", "succeeded"]
                parent = inbound.payload_json["trace_context"]
                trace_id = inbound.trace_id
            memory = GuardedMemoryService(runtime.memory, tenant.tenant_id, route.agent_app_id)
            with telemetry.operation("memory.verify", parent=parent):
                assert (await memory.search_memory(f"{runtime.session.app_name}/{route.internal_user_id}",
                                                   "lighthouse")).memories
            with pytest.raises(TenantBoundaryError):
                await memory.search_memory("other:app/user", "lighthouse")
            native = await runtime.session.get_session(app_name=runtime.session.app_name,
                                                       user_id=route.internal_user_id,
                                                       session_id=route.session_id)
            assert native.historical_events and any(item.is_summary_event() for item in native.events)
            for provider in telemetry._providers:
                provider.force_flush()
            chain = [span for span in exporter.get_finished_spans() if f"{span.context.trace_id:032x}" == trace_id]
            by_id = {span.context.span_id: span for span in chain}

            def within_worker(span):
                while span.parent and span.parent.span_id in by_id:
                    span = by_id[span.parent.span_id]
                    if span.name == "worker.execute":
                        return True
                return False

            assert any(span.name == "memory.search" and within_worker(span) for span in chain)
            assert {
                "im.callback", "worker.execute", "session.append", "model.provider_response", "tool.invoke",
                "object.upload", "object.read", "memory.store", "memory.search", "summary.generate", "session.update",
                "im.reply"
            } <= {span.name
                  for span in chain}
            assert all(challenge not in str(span.attributes) and "synthetic-api-key" not in str(span.attributes)
                       for span in exporter.get_finished_spans())
    finally:
        await artifact.close()
        await runtime.close()
        telemetry.shutdown()
        if real_objects:
            # Only this run's newly created random bucket is removed.
            for item in client.list_objects(bucket, recursive=True):
                client.remove_object(bucket, item.object_name)
            client.remove_bucket(bucket)
