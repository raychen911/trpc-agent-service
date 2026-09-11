"""The deployable runtime uses all six real backends and official SDK tools."""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.types import Content, Part

from trpc_service.channels import NormalizedInboundMessage
from trpc_service.management import ConfigurationStore, ProfileRegistration, ConfigDraft
from trpc_service.persistence.models import ToolResultRow, ToolInvocationRow, AuditLogRow
from trpc_service.service_runtime import ServiceRuntime, SimulationModel


@pytest.mark.asyncio
async def test_real_runtime_tools_artifacts_knowledge_audit(platform_database, vector_backend, monkeypatch, request):
    if request.config.getoption("--backend-mode") != "real":
        pytest.skip("six-backend deployment requires real mode")
    database = platform_database
    tenant_id = "tenant_" + uuid.uuid4().hex[:12]
    monkeypatch.setenv("TRPC_CONFIG_SOURCE", "database")
    monkeypatch.setenv("TRPC_RUNTIME_MODE", "simulation")
    monkeypatch.setenv("TRPC_IDENTITY_KEY", "resource-test-identity-key-" * 3)
    with database.sessions() as session:
        schema = session.scalar(text("select current_schema()"))
    audit_url = make_url(database.url).update_query_dict({"options": "-csearch_path=" + schema})
    monkeypatch.setenv("RESOURCE_AUDIT_URL", audit_url.render_as_string(hide_password=False))
    store = ConfigurationStore(database)
    profiles = json.loads(Path("deploy/management-profiles.json").read_text(encoding="utf-8"))
    for profile in profiles:
        if profile["kind"] == "object":
            profile["options"] = {"secure": False, "bucket": "trpc-runtime-test"}
        if profile["kind"] == "vector":
            profile["options"] = {"collection": vector_backend._collection, "index_version": 1}
        if profile["profile_id"] == "sql_audit":
            profile["secret_ref"] = {"uri": "env://RESOURCE_AUDIT_URL"}
        store.register_profile(ProfileRegistration.model_validate(profile), "test")
    config = json.loads(Path("deploy/simulation-tenants.json").read_text(encoding="utf-8"))[0]
    config["tenant_id"] = tenant_id
    for ref in config["data_backends"].values():
        ref["namespace"] = tenant_id
    config["channel_bindings"][0]["tenant_id"] = tenant_id
    config["applications"][0]["tool_policy"]["allow"] = [
        "read_artifact", "write_artifact", "search_knowledge"]
    config["applications"][0]["knowledge_base_ids"] = ["kb_allowed"]
    store.save_draft(ConfigDraft(config=config, profile_revisions={
        role: 1 for role in config["data_backends"]}), "test")
    store.publish(tenant_id, config["config_version"], 0, "test")
    vector_backend.upsert(tenant_id, "kb_allowed", 1, "doc-1", "chunk-1", [1.0, 0.0], "scoped knowledge", {})
    vector_backend.upsert("tenant_other", "kb_allowed", 1, "doc-secret", "chunk-1", [1.0, 0.0], "other tenant", {})

    class ToolModel(SimulationModel):
        async def _generate_async_impl(self, request, stream=False, ctx=None):
            responses = [part.function_response for content in request.contents for part in content.parts
                         if part.function_response]
            commands = [("write_artifact", {"filename": "answer.txt", "text": "saved content"}),
                        ("read_artifact", {"filename": "answer.txt"}),
                        ("search_knowledge", {"knowledge_base_id": "kb_allowed", "vector": [1.0, 0.0]})]
            if len(responses) < len(commands):
                name, args = commands[len(responses)]
                part = Part.from_function_call(name=name, args=args)
                part.function_call.id = "call-" + str(len(responses))
                yield LlmResponse(content=Content(role="model", parts=[part]))
            else:
                yield LlmResponse(content=Content(role="model", parts=[Part.from_text(text="tools completed")]))

    monkeypatch.setattr("trpc_service.service_runtime.SimulationModel", ToolModel)
    runtime = ServiceRuntime(database)
    try:
        await runtime.start()
        message = NormalizedInboundMessage(
            channel="wecom", webhook_public_id="callback_acme",
            external_message_id=uuid.uuid4().hex, external_user_id="real-tool-user", conversation_type="direct",
            text="run tools", received_at=datetime.now(timezone.utc), request_id="test-request")
        route = store.route_message(message)
        runtime.pipeline.ingest(message, route, trace_id="real-tool-trace")
        work = runtime.inbox.claim(worker_id="resource-worker", lease_seconds=90)
        outputs = await runtime.pipeline.execute(work, runtime.registry, worker_id="resource-worker")
        assert outputs[-1].text == "tools completed"
        with database.sessions() as session:
            rows = list(session.scalars(select(ToolResultRow).where(ToolResultRow.execution_id == work.execution_id)))
            calls = list(session.scalars(select(ToolInvocationRow).where(
                ToolInvocationRow.execution_id == work.execution_id)))
            assert len(rows) == len(calls) == 3
            assert all(row.status == "succeeded" for row in calls)
            values = [row.result_json["value"] for row in rows]
            assert {"found": True, "text": "saved content"} in values
            assert {"matches": [{"document_id": "doc-1", "text": "scoped knowledge"}]} in values
            assert session.scalar(select(AuditLogRow).where(
                AuditLogRow.tenant_id == tenant_id, AuditLogRow.decision == "execution_storage_ready"))
        for task in runtime.post.claim(limit=10, worker_id="post", lease_seconds=90):
            await runtime.process_post_turn(task)
            runtime.post.complete(task.task_id, "post")
    finally:
        await runtime.close()
