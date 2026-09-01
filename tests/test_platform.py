import base64
import asyncio
import tempfile
import unittest
import json
import threading
import sys
import types
import os
import socket
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.request import Request
from urllib.error import HTTPError
from hashlib import sha1

for key in (
    "WORKER_QUEUE_URL",
    "WORKER_REMOTE",
    "REDIS_URL",
    "POSTGRES_DSN",
    "TENANT_DB_DSN",
    "POSTGRES_RLS_ENABLED",
    "POSTGRES_AUTO_CREATE_SCHEMA",
    "POSTGRES_SCHEMA_DSN",
    "POSTGRES_RLS_APP_ROLE",
    "POSTGRES_RLS_ADMIN_ROLE",
    "POSTGRES_RLS_APP_PASSWORD",
    "POSTGRES_RLS_ADMIN_PASSWORD",
    "DEFAULT_SESSION_BACKEND",
    "DEFAULT_MEMORY_BACKEND",
    "DEFAULT_SUMMARY_BACKEND",
    "DEFAULT_KNOWLEDGE_BACKEND",
    "DEFAULT_ARTIFACT_BACKEND",
    "DEFAULT_AUDIT_BACKEND",
    "SYNC_DEMO_DEFAULT_STORAGE",
    "REQUIRE_SHARED_COORDINATION",
):
    os.environ.pop(key, None)
os.environ.setdefault("CPA_MODEL", "test-model")
# The test suite intentionally exercises the no-credential demo runtime.
# Production defaults remain fail-fast tRPC-Agent-Python mode.
os.environ.setdefault("TRPC_AGENT_RUNTIME_MODE", "local")

from trpc_service.agent.model_client import (
    ModelResponse,
    ModelToolCall,
    ResponsesModelClient,
    extract_response_text,
)
from trpc_service.admin.auth import AdminAuthenticationError, AdminPrincipal, authenticate, authorize
from trpc_service.channels import InboundMessage, default_channel_adapters
from trpc_service.channels.base import OutboundMessage
from trpc_service.channels.base import Attachment
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.wecom import WeComAdapter
from trpc_service.channels.wechat_customer_service import WeChatCustomerServiceAdapter
from trpc_service.channels.wechat_official_account import WeChatOfficialAccountAdapter
from trpc_service.gateway.router import AgentWorker
from trpc_service.gateway import AgentGateway, build_idempotency_key, build_session_id
from trpc_service.policy.quota import QuotaEnforcer, QuotaExceeded
from trpc_service.storage.factory import create_storage
from trpc_service.storage.base import MemoryItem, Summary
from trpc_service.storage.compensation import replay_compensations
from trpc_service.storage.remote_vector import RemoteVectorStore
from trpc_service.storage.vector_store import KnowledgeChunk
from trpc_service.telemetry.tracing import TraceRecorder
from trpc_service.agent import RuntimeBridgeSpec, build_runtime_worker
from trpc_service.tenant.models import (
    AgentEvent,
    ChannelBinding,
    ModelConfig,
    StorageProfile,
    TenantContext,
    ToolPolicy,
    RunRequest,
    UserInput,
    QuotaPolicy,
    default_demo_config,
)
from trpc_service.tenant.repository import InMemoryTenantRepository
from trpc_service.tenant.repository import SQLiteTenantRepository
from trpc_service.tenant.service import TenantService, TenantValidationError
from trpc_service.tool.runtime import ToolRegistry
from trpc_service.channels.reliable import send_with_retry
from trpc_service.channels.outbound_queue import OutboundDeliveryQueue
from trpc_service.channels.outbound import (
    build_outbound_messages,
    split_outbound_messages,
    visible_answer,
)
from trpc_service.web.app import create_runtime
from trpc_service.migrate import cutover_plan, export_tenant, import_tenant, verify_tenant
from trpc_service.security.secrets import redact_secret_data, redact_secret_text
from trpc_service.tool.runtime import ToolResult
from trpc_service.workspace.policy import WorkspacePolicy


class PlatformTests(unittest.TestCase):
    def setUp(self):
        runtime = create_runtime()
        self.gateway = runtime[0]
        self.tenants = runtime[1].tenants

    def test_session_is_tenant_and_conversation_scoped(self):
        first = build_session_id("t1", "app", "telegram", "a", "user")
        same = build_session_id("t1", "app", "telegram", "a", "user")
        other_tenant = build_session_id("t2", "app", "telegram", "a", "user")
        group = build_session_id("t1", "app", "telegram", "a", "user", "group")
        self.assertEqual(first, same)
        self.assertNotEqual(first, other_tenant)
        self.assertNotEqual(first, group)

    def test_channel_binding_maps_external_identity_to_internal_user(self):
        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="web:account",
            channel="web",
            account_id="account",
            agent_app_id="app_support",
            config={"identity_mapping": {"external_to_internal": {"openid-1": "user-1001"}}},
        )
        inbound = default_channel_adapters()["web"].parse_event(
            {"message_id": "m1", "user_id": "openid-1", "text": "hello"},
            binding,
        )
        self.assertEqual(inbound.external_user_id, "openid-1")
        self.assertEqual(inbound.internal_user_id, "user-1001")
        self.assertEqual(inbound.effective_user_id, "user-1001")

    def test_workspace_policy_scopes_root_to_tenant_component(self):
        policy = WorkspacePolicy(root="data/workspaces")
        self.assertEqual(policy.tenant_root("tenant_demo"), "data/workspaces/tenant_demo")
        self.assertEqual(policy.tenant_root("tenant-two"), "data/workspaces/tenant-two")
        self.assertNotEqual(policy.tenant_root("tenant_a"), policy.tenant_root("tenant_b"))

    def test_webhook_dispatch_and_idempotency(self):
        message = InboundMessage(
            channel="telegram",
            account_id="corp_account_1",
            external_message_id="message-1",
            external_user_id="user-1",
            text="hello",
        )
        session_id, events, response_ref = self.gateway.dispatch(message)
        self.assertTrue(session_id.startswith("sess_user_"))
        self.assertEqual(events[-1].event_type, "message_end")
        self.assertTrue(response_ref)
        worker_count = self.gateway.workers[0].executions

        _, duplicate_events, duplicate_ref = self.gateway.dispatch(message)
        self.assertEqual(duplicate_ref, response_ref)
        self.assertEqual(duplicate_events[-1].content, events[-1].content)
        self.assertEqual(self.gateway.workers[0].executions, worker_count)

    def test_memory_search_is_session_scoped(self):
        storage = create_storage()
        storage.memory.put(MemoryItem("tenant_demo", "a", "session-a", "private alpha"))
        storage.memory.put(MemoryItem("tenant_demo", "b", "session-b", "private alpha"))
        result = storage.memory.search("tenant_demo", "private alpha", scope_keys=("session-a",))
        self.assertEqual([item.memory_id for item in result], ["a"])
        storage.close()

    def test_processing_idempotency_lease_can_be_reclaimed(self):
        storage = create_storage()
        first = storage.idempotency.start("tenant_demo", "lease-key", "trace-one", lease_seconds=1)
        storage.idempotency._records[("tenant_demo", "lease-key")].updated_at = first.updated_at - timedelta(seconds=2)
        reclaimed = storage.idempotency.start("tenant_demo", "lease-key", "trace-two", lease_seconds=1)
        self.assertEqual(reclaimed.trace_id, "trace-two")
        self.assertEqual(reclaimed.attempt, 2)
        storage.close()

    def test_duplicate_channel_binding_across_tenants_is_rejected(self):
        from copy import deepcopy

        with patch.dict("os.environ", {"CPA_MODEL": "test-model"}, clear=False):
            first = default_demo_config()
            repository = InMemoryTenantRepository()
            service = TenantService(repository)
            service.create_tenant(first)
            second = deepcopy(first)
            second.tenant_id = "tenant_two"
            for binding in second.channel_bindings:
                binding.tenant_id = second.tenant_id
            with self.assertRaises(ValueError):
                service.create_tenant(second)

    def test_two_workers_restore_shared_state(self):
        from trpc_service.gateway.router import AgentWorker

        storage = create_storage()
        repository = InMemoryTenantRepository()
        repository.create(self.gateway.tenants.repository.get("tenant_demo"))
        service = TenantService(repository)
        gateway = AgentGateway(
            service,
            storage,
            workers=[AgentWorker(storage), AgentWorker(storage)],
        )
        first = InboundMessage(
            channel="telegram",
            account_id="corp_account_1",
            external_message_id="message-2",
            external_user_id="user-1",
            text="first",
        )
        second = InboundMessage(
            channel="telegram",
            account_id="corp_account_1",
            external_message_id="message-3",
            external_user_id="user-1",
            text="second",
        )
        gateway.dispatch(first)
        _, events, _ = gateway.dispatch(second)
        self.assertIn("已恢复", events[-1].content)

    def test_sqlite_backend(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = create_storage(
                profile=StorageProfile(session_backend="sql"),
                data_dir=Path(directory),
            )
            self.assertEqual(storage.structured.backend_name, "sql")
            self.assertEqual(build_idempotency_key("t", "c", "a", "m"), build_idempotency_key("t", "c", "a", "m"))
            storage.structured.close()

    def test_channel_registry(self):
        adapters = default_channel_adapters()
        self.assertEqual(
            set(adapters),
            {"web", "wecom", "wechat_customer_service", "wechat_official_account", "telegram"},
        )

    def test_gateway_package_imports_in_a_clean_interpreter(self):
        import subprocess

        result = subprocess.run(
            [sys.executable, "-c", "from trpc_service.gateway import AgentGateway"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_web_ui_channel_is_available_by_default(self):
        config = self.tenants.get_tenant("tenant_demo")
        self.assertIn("web", {binding.channel for binding in config.channel_bindings})
        self.assertIn(
            "web_demo", {binding.account_id for binding in config.channel_bindings if binding.channel == "web"}
        )

    def test_default_demo_storage_profile_reads_deployment_backends(self):
        with patch.dict(
            "os.environ",
            {
                "DEFAULT_SESSION_BACKEND": "redis",
                "DEFAULT_MEMORY_BACKEND": "postgres",
                "DEFAULT_SUMMARY_BACKEND": "postgres",
                "DEFAULT_KNOWLEDGE_BACKEND": "postgres",
                "DEFAULT_ARTIFACT_BACKEND": "object",
                "DEFAULT_AUDIT_BACKEND": "postgres",
            },
            clear=False,
        ):
            profile = default_demo_config().storage_profile
        self.assertEqual(profile.session_backend, "redis")
        self.assertEqual(profile.memory_backend, "postgres")
        self.assertEqual(profile.summary_backend, "postgres")
        self.assertEqual(profile.knowledge_backend, "postgres")
        self.assertEqual(profile.audit_backend, "postgres")

    def test_responses_client_request_shape(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"model":"actual-test-model","output_text":"real model reply"}'

        captured = {}

        def fake_urlopen(request: Request, timeout: float):
            captured["url"] = request.full_url
            captured["authorization"] = request.get_header("Authorization")
            captured["body"] = request.data.decode("utf-8")
            captured["timeout"] = timeout
            return FakeResponse()

        client = ResponsesModelClient(
            base_url="https://example.test/v1",
            api_key="test-key",
            timeout_ms=5_000,
        )
        with patch("trpc_service.agent.model_client.urlopen", fake_urlopen):
            reply = client.generate(
                model="test-model",
                system_prompt="Be concise.",
                conversation=[{"role": "user", "content": "hello"}],
            )

        self.assertEqual(reply, "real model reply")
        self.assertEqual(captured["url"], "https://example.test/v1/responses")
        self.assertEqual(captured["authorization"], "Bearer test-key")
        self.assertIn('"model": "test-model"', captured["body"])
        self.assertEqual(captured["timeout"], 5.0)

    def test_responses_client_returns_actual_model_and_redacts_timeout_error(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"model":"actual-test-model","output_text":"ok"}'

        client = ResponsesModelClient(
            base_url="https://example.test/v1",
            api_key="test-secret",
            timeout_ms=25,
            max_retries=0,
        )
        with patch(
            "trpc_service.agent.model_client.urlopen",
            side_effect=socket.timeout("timed out with token=test-secret"),
        ):
            with self.assertRaisesRegex(Exception, "connection failed") as raised:
                client.generate_with_usage(
                    model="test-model",
                    system_prompt="Be concise.",
                    conversation=[{"role": "user", "content": "hello"}],
                )
            self.assertNotIn("test-secret", str(raised.exception))

        with patch("trpc_service.agent.model_client.urlopen", return_value=FakeResponse()):
            response = client.generate_with_usage(
                model="test-model",
                system_prompt="Be concise.",
                conversation=[{"role": "user", "content": "hello"}],
            )
        self.assertEqual(response.model, "actual-test-model")

    def test_chat_completions_client_request_shape(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return (
                    b'{"choices":[{"message":{"content":"chat reply"}}],'
                    b'"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}'
                )

        captured = {}

        def fake_urlopen(request: Request, timeout: float):
            captured["url"] = request.full_url
            captured["authorization"] = request.get_header("Authorization")
            captured["body"] = request.data.decode("utf-8")
            captured["timeout"] = timeout
            return FakeResponse()

        client = ResponsesModelClient(
            base_url="https://example.test/v1",
            api_key="test-key",
            timeout_ms=5_000,
            wire_api="chat_completions",
        )
        with patch("trpc_service.agent.model_client.urlopen", fake_urlopen):
            reply = client.generate(
                model="test-model",
                system_prompt="Be concise.",
                conversation=[{"role": "user", "content": "hello"}],
            )

        self.assertEqual(reply, "chat reply")
        self.assertEqual(captured["url"], "https://example.test/v1/chat/completions")
        self.assertEqual(captured["authorization"], "Bearer test-key")
        self.assertIn('"messages"', captured["body"])
        self.assertEqual(captured["timeout"], 5.0)

    def test_responses_output_parser_compatible_shape(self):
        self.assertEqual(
            extract_response_text({"output": [{"content": [{"type": "output_text", "text": "hello"}]}]}),
            "hello",
        )

    def test_model_tool_call_parsers_accept_empty_text_and_preserve_arguments(self):
        from trpc_service.agent.model_client import extract_chat_tool_calls, extract_response_tool_calls

        response_calls = extract_response_tool_calls(
            {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "resp-call-1",
                        "name": "search_knowledge",
                        "arguments": '{"query":"redis"}',
                    }
                ]
            }
        )
        chat_calls = extract_chat_tool_calls(
            {
                "tool_calls": [
                    {
                        "id": "chat-call-1",
                        "type": "function",
                        "function": {
                            "name": "search_knowledge",
                            "arguments": '{"query":"postgres"}',
                        },
                    }
                ]
            }
        )
        self.assertEqual(response_calls[0].arguments, {"query": "redis"})
        self.assertEqual(chat_calls[0].arguments, {"query": "postgres"})

    def test_tool_registry_exposes_custom_function_schema(self):
        registry = ToolRegistry()

        def lookup(query: str, limit: int = 3, idempotency_key: str = ""):
            """Look up tenant data."""
            del query, limit, idempotency_key
            return ToolResult("lookup", "ok", {})

        registry.register("lookup", lookup)
        schema = registry.tool_schemas({"lookup"})[0]["function"]
        self.assertEqual(schema["name"], "lookup")
        self.assertEqual(schema["parameters"]["properties"]["query"]["type"], "string")
        self.assertIn("query", schema["parameters"]["required"])
        self.assertNotIn("idempotency_key", schema["parameters"]["properties"])

    def test_worker_completes_native_chat_tool_loop(self):
        storage = create_storage()
        config = self.tenants.get_tenant("tenant_demo")
        config.apps[0].tool_policy = ToolPolicy(allowlist=["lookup"])
        calls = []

        def lookup(query: str, idempotency_key: str = ""):
            calls.append((query, idempotency_key))
            return ToolResult("lookup", f"found:{query}", {})

        class FakeModel:
            def __init__(self):
                self.requests = []

            def generate_with_usage(self, **kwargs):
                self.requests.append(kwargs)
                if len(self.requests) == 1:
                    return ModelResponse(
                        "",
                        2,
                        1,
                        3,
                        tool_calls=[ModelToolCall("call-1", "lookup", {"query": "alpha"})],
                    )
                conversation = kwargs["conversation"]
                self.assert_tool_history(conversation)
                return ModelResponse("answer from tool", 5, 3, 8)

            @staticmethod
            def assert_tool_history(conversation):
                assert any(item.get("role") == "assistant" and item.get("tool_calls") for item in conversation)
                assert any(item.get("role") == "tool" and item.get("tool_call_id") == "call-1" for item in conversation)

        model = FakeModel()
        worker = AgentWorker(storage, model_client=model)
        worker.tools.register("lookup", lookup)
        request = RunRequest(
            TenantContext(
                "tenant_demo",
                "app_support",
                config.config_version,
                "trace-tool-loop",
                "session-tool-loop",
                "web",
                "user-tool",
            ),
            UserInput("find alpha"),
            "tool-loop-request",
        )
        events = worker.run(request, config, storage)
        self.assertEqual(events[-1].content, "answer from tool")
        self.assertEqual(calls[0][0], "alpha")
        self.assertEqual(len(model.requests), 2)
        storage.close()

    def test_sdk_runtime_builds_sdk_function_tools_for_allowed_tools(self):
        from trpc_service.agent.trpc_runtime import TrpcAgentWorker
        from trpc_agent_sdk.tools import FunctionTool

        worker = TrpcAgentWorker(create_storage(), TraceRecorder())

        def lookup(query: str):
            return ToolResult("lookup", query, {})

        worker.tools.register("lookup", lookup)
        tools = worker._build_sdk_tools(FunctionTool, {"lookup"})
        self.assertEqual([tool.name for tool in tools], ["lookup"])
        declaration = tools[0]._get_declaration()
        self.assertEqual(declaration.parameters.type.value, "OBJECT")
        self.assertIn("query", declaration.parameters.properties)
        self.assertEqual(declaration.parameters.properties["query"].type.value, "STRING")
        self.assertEqual(declaration.parameters.required, ["query"])
        self.assertIsNone(declaration.parameters_json_schema)
        self.assertIn("query", str(tools[0].func.__signature__))
        self.assertIn("tool_context", str(tools[0].func.__signature__))

    def test_sdk_runtime_uses_sdk_path_when_tools_are_declared(self):
        from trpc_service.agent.trpc_runtime import TrpcAgentWorker

        storage = create_storage()
        config = self.tenants.get_tenant("tenant_demo")

        class CapturingWorker(TrpcAgentWorker):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.captured_tool_names = None

            async def _run_sdk_async(self, app, conversation, tool_names=None):
                del app, conversation
                self.captured_tool_names = set(tool_names or set())
                return ModelResponse("sdk answer", 1, 1, 2)

        worker = CapturingWorker(storage, TraceRecorder())
        response = worker._generate_answer(
            "find alpha",
            config.app("app_support"),
            [],
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "lookup",
                        "description": "lookup",
                        "parameters": {"type": "object", "properties": {}},
                    },
                }
            ],
        )
        self.assertEqual(response.text, "sdk answer")
        self.assertEqual(worker.captured_tool_names, {"lookup"})
        storage.close()

    def test_sdk_tool_adapter_executes_platform_tool_with_audit(self):
        from trpc_service.agent.trpc_runtime import TrpcAgentWorker
        from trpc_service.policy.tenant_filter import TenantPolicy

        storage = create_storage()
        config = self.tenants.get_tenant("tenant_demo")
        config.apps[0].tool_policy = ToolPolicy(allowlist=["lookup"])
        worker = TrpcAgentWorker(storage, TraceRecorder())
        calls = []

        def lookup(query: str, idempotency_key: str = ""):
            calls.append((query, idempotency_key))
            return ToolResult("lookup", f"found:{query}", {"source": "test"})

        worker.tools.register("lookup", lookup)
        request = RunRequest(
            TenantContext(
                "tenant_demo",
                "app_support",
                config.config_version,
                "trace-sdk-tool",
                "session-sdk-tool",
                "web",
                "user-sdk-tool",
            ),
            UserInput("find alpha"),
            "sdk-tool-request",
        )
        app = config.app("app_support")
        tokens = [
            (worker._active_request_var, worker._active_request_var.set(request)),
            (worker._active_app_var, worker._active_app_var.set(app)),
            (worker._active_policy_var, worker._active_policy_var.set(TenantPolicy(config, "app_support"))),
            (worker._active_storage_var, worker._active_storage_var.set(storage)),
            (worker._sdk_tool_events_var, worker._sdk_tool_events_var.set([])),
        ]
        try:
            tool = worker._build_sdk_tools(None, {"lookup"})[0]
            result = asyncio.run(
                tool._run_async_impl(
                    tool_context=types.SimpleNamespace(function_call_id="sdk-call-1"),
                    args={"query": "alpha"},
                )
            )
            sdk_events = worker._sdk_tool_events_var.get()
        finally:
            for var, token in reversed(tokens):
                var.reset(token)

        self.assertEqual(result["content"], "found:alpha")
        self.assertEqual(calls, [("alpha", "sdk-tool-request:sdk:sdk-call-1")])
        self.assertTrue(any(event.event_type == "tool_call" for event in sdk_events))
        audit = storage.audit.list_by_tenant("tenant_demo")
        self.assertTrue(any(record.tool_name == "lookup" and record.decision == "allow" for record in audit))
        storage.close()

    def test_sdk_tool_adapter_surfaces_platform_approval_request(self):
        from trpc_service.agent.trpc_runtime import TrpcAgentWorker, _ApprovalRequired
        from trpc_service.policy.tenant_filter import TenantPolicy

        storage = create_storage()
        config = self.tenants.get_tenant("tenant_demo")
        config.apps[0].tool_policy = ToolPolicy(
            allowlist=["send_external_message"],
            approval_rules=["send_external_message"],
        )
        worker = TrpcAgentWorker(storage, TraceRecorder())
        executed = []

        def send_external_message(target: str):
            executed.append(target)
            return ToolResult("send_external_message", "sent", {})

        worker.tools.register("send_external_message", send_external_message)
        request = RunRequest(
            TenantContext(
                "tenant_demo",
                "app_support",
                config.config_version,
                "trace-sdk-approval",
                "session-sdk-approval",
                "telegram",
                "user-sdk-approval",
            ),
            UserInput("send"),
            "sdk-approval-request",
        )
        app = config.app("app_support")
        tokens = [
            (worker._active_request_var, worker._active_request_var.set(request)),
            (worker._active_app_var, worker._active_app_var.set(app)),
            (worker._active_policy_var, worker._active_policy_var.set(TenantPolicy(config, "app_support"))),
            (worker._active_storage_var, worker._active_storage_var.set(storage)),
            (worker._sdk_tool_events_var, worker._sdk_tool_events_var.set([])),
        ]
        try:
            tool = worker._build_sdk_tools(None, {"send_external_message"})[0]
            with self.assertRaises(_ApprovalRequired):
                asyncio.run(
                    tool._run_async_impl(
                        tool_context=types.SimpleNamespace(function_call_id="sdk-call-approval"),
                        args={"target": "ops"},
                    )
                )
            sdk_events = worker._sdk_tool_events_var.get()
        finally:
            for var, token in reversed(tokens):
                var.reset(token)

        self.assertEqual(executed, [])
        self.assertEqual(sdk_events[-1].event_type, "approval_required")
        self.assertTrue(
            any(
                event.event_type == "tool_approval_requested"
                for event in storage.session.load_events("tenant_demo", "session-sdk-approval")
            )
        )
        storage.close()

    def test_model_retries_429_and_opens_circuit_after_failures(self):
        calls = []

        def always_429(request, timeout):
            calls.append(1)
            raise HTTPError(request.full_url, 429, "rate limited", {}, None)

        client = ResponsesModelClient(
            base_url="https://example.test/v1",
            api_key="test-key",
            timeout_ms=10,
            max_retries=1,
        )
        with patch("trpc_service.agent.model_client.urlopen", always_429), patch(
            "trpc_service.agent.model_client.sleep"
        ):
            for _ in range(5):
                with self.assertRaises(Exception):
                    client.generate("model", "system", [{"role": "user", "content": "hi"}])
        self.assertGreaterEqual(len(calls), 5)
        with self.assertRaises(Exception):
            client.generate("model", "system", [{"role": "user", "content": "hi"}])

    def test_model_client_resolves_tenant_scoped_api_key_reference(self):
        config_a = ModelConfig(
            provider="openai-compatible",
            model="tenant-a-model",
            base_url="https://example.test/a",
            api_key_ref="secret://tenant_a/model/api_key",
        )
        config_b = ModelConfig(
            provider="openai-compatible",
            model="tenant-b-model",
            base_url="https://example.test/b",
            api_key_ref="secret://tenant_b/model/api_key",
        )
        with patch.dict(
            "os.environ",
            {
                "SECRET_TENANT_A_MODEL_API_KEY": "tenant-a-key",
                "SECRET_TENANT_B_MODEL_API_KEY": "tenant-b-key",
            },
            clear=False,
        ):
            client_a = ResponsesModelClient.from_config(config_a)
            client_b = ResponsesModelClient.from_config(config_b)
        self.assertIsNotNone(client_a)
        self.assertIsNotNone(client_b)
        self.assertEqual(client_a.api_key, "tenant-a-key")
        self.assertEqual(client_b.api_key, "tenant-b-key")
        self.assertNotEqual(client_a.api_key, client_b.api_key)
        self.assertNotIn("tenant-a-key", redact_secret_text("model provider failed"))
        self.assertNotIn("tenant-b-key", redact_secret_text("model provider failed"))

    def test_admin_api_key_rbac(self):
        with patch.dict("os.environ", {"ADMIN_API_KEY": "secret-admin"}, clear=False):
            principal = authenticate(api_key="secret-admin")
        self.assertEqual(principal.role, "superadmin")
        authorize(principal, "tenant_demo", {"operator"})
        scoped = AdminPrincipal("tenant-admin", "operator", frozenset({"tenant_a"}))
        authorize(scoped, "tenant_a", {"operator"})
        with self.assertRaises(AdminAuthenticationError):
            authorize(scoped, "tenant_b", {"operator"})
        with patch.dict("os.environ", {"ADMIN_API_KEY": "secret-admin"}, clear=False):
            with self.assertRaises(AdminAuthenticationError):
                authenticate(api_key="wrong")
        with self.assertRaises(AdminAuthenticationError):
            authorize(AdminPrincipal("operator", "operator"), "tenant_demo", {"operator"})

    def test_quota_enforcer_rejects_qps_and_daily_usage(self):
        with patch.dict("os.environ", {"REDIS_URL": ""}, clear=False):
            quota = QuotaEnforcer()
            policy = QuotaPolicy(qps_limit=1, daily_token_limit=3, daily_cost_limit=1.0)
            quota.check("tenant_demo", policy)
            with self.assertRaises(QuotaExceeded):
                quota.check("tenant_demo", policy)
            quota.record("tenant_tokens", 4, 0.0)
            with self.assertRaises(QuotaExceeded):
                quota.check("tenant_tokens", policy)
            with self.assertRaises(QuotaExceeded):
                QuotaEnforcer().check("tenant_budget", policy, requested_tokens=4)

    def test_quota_release_returns_failed_reservation(self):
        quota = QuotaEnforcer(redis_url="")
        policy = QuotaPolicy(qps_limit=20, daily_token_limit=5, daily_cost_limit=1.0)
        quota.reserve("tenant_release", policy, requested_tokens=5, requested_cost=0.5)
        quota.release("tenant_release", 5, 0.5)
        quota.reserve("tenant_release", policy, requested_tokens=5, requested_cost=0.5)

    def test_gateway_preserves_inbound_traceparent_without_otel_exporter(self):
        repository = InMemoryTenantRepository()
        repository.create(self.tenants.get_tenant("tenant_demo"))
        service = TenantService(repository)
        storage = create_storage()
        captured = {}

        class CaptureWorker:
            def run(self, request, config, storage):
                captured["traceparent"] = request.tenant_context.traceparent
                return [AgentEvent("message_end", "captured")]

        gateway = AgentGateway(
            service,
            storage,
            workers=[CaptureWorker()],
            telemetry=TraceRecorder(),
        )
        gateway.dispatch(
            InboundMessage(
                channel="telegram",
                account_id="corp_account_1",
                external_message_id="trace-message",
                external_user_id="trace-user",
                text="hello",
            ),
            traceparent="00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        )
        self.assertEqual(
            captured["traceparent"],
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        )

    def test_qdrant_list_by_tenant_scans_real_collections(self):
        store = RemoteVectorStore(
            "https://qdrant.example.test",
            collection_prefix="trpc-agent",
        )
        calls = []
        scroll_count = {}

        def fake_request(method, path, payload=None):
            calls.append((method, path))
            if method == "GET" and path == "/collections":
                return {
                    "result": {
                        "collections": [
                            {"name": "trpc-agent_default"},
                            {"name": "trpc-agent_other"},
                        ]
                    }
                }
            scroll_count[path] = scroll_count.get(path, 0) + 1
            if path.endswith("trpc-agent_default/points/scroll"):
                if scroll_count[path] > 1:
                    return {"result": {"points": [], "next_page_offset": None}}
                return {
                    "result": {
                        "points": [
                            {
                                "id": 1,
                                "payload": {
                                    "tenant_id": "tenant_demo",
                                    "collection": "default",
                                    "chunk_id": "chunk-1",
                                    "text": "hello",
                                    "metadata": {},
                                },
                            }
                        ],
                        "next_page_offset": None,
                    }
                }
            return {"result": {"points": [], "next_page_offset": None}}

        with patch.object(store, "_request", fake_request):
            chunks = store.list_by_tenant("tenant_demo")
        self.assertEqual([chunk.chunk_id for chunk in chunks], ["chunk-1"])
        self.assertIn(
            ("POST", "/collections/trpc-agent_default/points/scroll"),
            calls,
        )
        self.assertNotIn(
            ("POST", "/collections/trpc-agent_trpc-agent/points/scroll"),
            calls,
        )

    def test_wechat_xml_callback_parsing_and_signature(self):
        token = "wechat-token"
        timestamp = "123"
        nonce = "abc"
        signature = sha1("".join(sorted((token, timestamp, nonce))).encode()).hexdigest()
        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="wechat:account",
            channel="wechat_official_account",
            account_id="account",
            agent_app_id="app_support",
            token_ref="secret://tenant_demo/wechat/token",
        )
        payload = {
            "timestamp": timestamp,
            "nonce": nonce,
            "signature": signature,
            "raw_body": "<xml><MsgId>m1</MsgId><FromUserName>user</FromUserName><Content>hello</Content></xml>",
        }
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_WECHAT_TOKEN": token}, clear=False):
            adapter = WeChatOfficialAccountAdapter()
            adapter.verify_callback(payload, binding)
            inbound = adapter.parse_event(payload, binding)
        self.assertEqual(inbound.external_message_id, "m1")
        self.assertEqual(inbound.external_user_id, "user")
        self.assertEqual(inbound.text, "hello")

    def test_wechat_encrypted_callback_requires_signature_and_aes_key(self):
        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="wechat:encrypted",
            channel="wechat_official_account",
            account_id="encrypted",
            agent_app_id="app_support",
            token_ref="secret://tenant_demo/wechat/token",
            config={"aes_key_ref": "secret://tenant_demo/wechat/aes-key"},
        )
        payload = {
            "timestamp": "123",
            "nonce": "abc",
            "Encrypt": "encrypted-body",
        }
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_WECHAT_TOKEN": "wechat-token"}, clear=False):
            with self.assertRaises(Exception):
                WeChatOfficialAccountAdapter().verify_callback(payload, binding)

    def test_im_signature_failure_and_telegram_secret(self):
        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="wecom:account",
            channel="wecom",
            account_id="account",
            agent_app_id="app_support",
            token_ref="secret://tenant_demo/wecom/token",
        )
        from trpc_service.channels.wecom import WeComAdapter

        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_WECOM_TOKEN": "token"}, clear=False):
            with self.assertRaises(Exception):
                WeComAdapter().verify_callback({"timestamp": "1", "nonce": "2", "signature": "bad"}, binding)

        from trpc_service.channels.telegram import TelegramAdapter

        binding.secret_ref = "secret://tenant_demo/telegram/secret"
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_TELEGRAM_SECRET": "telegram-secret"}, clear=False):
            with self.assertRaises(Exception):
                TelegramAdapter().verify_callback({"secret_token": "bad"}, binding)

    def test_telegram_adapter_prefers_python_telegram_bot_sdk(self):
        class FakeMessage:
            message_id = 42

        class FakeBot:
            token = None

            def __init__(self, token):
                FakeBot.token = token

            async def send_message(self, chat_id, text):
                self.chat_id = chat_id
                self.text = text
                return FakeMessage()

            async def shutdown(self):
                return None

        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="telegram:account",
            channel="telegram",
            account_id="account",
            agent_app_id="app_support",
            token_ref="secret://tenant_demo/telegram/token",
            config={"sdk_enabled": True},
        )
        message = OutboundMessage(
            channel="telegram",
            account_id="account",
            session_id="session-telegram",
            external_user_id="user-1",
            text="hello",
        )
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_TELEGRAM_TOKEN": "telegram-token"}, clear=False):
            with patch("telegram.Bot", FakeBot):
                result = TelegramAdapter().send(message, binding)
        self.assertTrue(result.ok)
        self.assertEqual(result.response_ref, "telegram:42")
        self.assertEqual(result.metadata["sdk"], "python-telegram-bot")

    def test_telegram_adapter_sends_image_and_file_attachments(self):
        class FakeMessage:
            message_id = 77

        class FakeBot:
            instances = []

            def __init__(self, token):
                self.token = token
                self.photo = None
                self.document = None
                self.caption = None
                FakeBot.instances.append(self)

            async def send_photo(self, chat_id, photo, caption=None):
                self.chat_id = chat_id
                self.photo = photo
                self.caption = caption
                return FakeMessage()

            async def send_document(self, chat_id, document, caption=None):
                self.chat_id = chat_id
                self.document = document
                self.caption = caption
                return FakeMessage()

            async def shutdown(self):
                return None

        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="telegram:account",
            channel="telegram",
            account_id="account",
            agent_app_id="app_support",
            token_ref="secret://tenant_demo/telegram/token",
            config={"sdk_enabled": True},
        )
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_TELEGRAM_TOKEN": "telegram-token"}, clear=False):
            with patch("telegram.Bot", FakeBot):
                image_message = OutboundMessage(
                    channel="telegram",
                    account_id="account",
                    session_id="session-telegram",
                    external_user_id="user-1",
                    text="look",
                    attachments=[
                        Attachment(
                            kind="image",
                            name="preview.png",
                            content_type="image/png",
                            metadata={"content_base64": base64.b64encode(b"png-bytes").decode()},
                        )
                    ],
                    metadata={"message_type": "image"},
                )
                image_result = TelegramAdapter().send(image_message, binding)
                file_message = OutboundMessage(
                    channel="telegram",
                    account_id="account",
                    session_id="session-telegram",
                    external_user_id="user-1",
                    text="file",
                    attachments=[
                        Attachment(
                            kind="file",
                            name="report.pdf",
                            content_type="application/pdf",
                            metadata={"content_base64": base64.b64encode(b"pdf-bytes").decode()},
                        )
                    ],
                    metadata={"message_type": "file"},
                )
                file_result = TelegramAdapter().send(file_message, binding)
        self.assertTrue(image_result.ok)
        self.assertTrue(file_result.ok)
        self.assertIsNotNone(FakeBot.instances[0].photo)
        self.assertEqual(FakeBot.instances[0].caption, "look")
        self.assertIsNotNone(FakeBot.instances[1].document)
        self.assertEqual(FakeBot.instances[1].caption, "file")

    def test_wecom_adapter_prefers_wechatpy_enterprise_sdk(self):
        class FakeMessageApi:
            def send_text(self, agent_id, user_ids, content):
                self.args = (agent_id, user_ids, content)
                return {"errcode": 0, "msgid": "wecom-msg"}

            def send_image(self, agent_id, user_ids, media_id):
                self.image_args = (agent_id, user_ids, media_id)
                return {"errcode": 0, "msgid": "wecom-image"}

            def send_file(self, agent_id, user_ids, media_id):
                self.file_args = (agent_id, user_ids, media_id)
                return {"errcode": 0, "msgid": "wecom-file"}

        class FakeMediaApi:
            def upload(self, media_type, media_file):
                self.media_type = media_type
                self.media_file = media_file
                self.media_bytes = media_file.read()
                return {"media_id": f"{media_type}-media-id"}

        class FakeClient:
            instances = []

            def __init__(self, corp_id, secret):
                self.corp_id = corp_id
                self.secret = secret
                self.message = FakeMessageApi()
                self.media = FakeMediaApi()
                FakeClient.instances.append(self)

        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="wecom:account",
            channel="wecom",
            account_id="account",
            agent_app_id="app_support",
            config={
                "sdk_enabled": True,
                "corp_id": "corp-id",
                "corp_secret_ref": "secret://tenant_demo/wecom/corp-secret",
                "agent_id": 1001,
            },
        )
        message = OutboundMessage(
            channel="wecom",
            account_id="account",
            session_id="session-wecom",
            external_user_id="user-1",
            text="hello",
        )
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_WECOM_CORP_SECRET": "corp-secret"}, clear=False):
            with patch("wechatpy.enterprise.WeChatClient", FakeClient):
                result = WeComAdapter().send(message, binding)
        self.assertTrue(result.ok)
        self.assertEqual(result.metadata["sdk"], "wechatpy.enterprise")

        image_message = OutboundMessage(
            channel="wecom",
            account_id="account",
            session_id="session-wecom",
            external_user_id="user-1",
            text="preview",
            attachments=[
                Attachment(
                    kind="image",
                    name="preview.png",
                    content_type="image/png",
                    metadata={"content_base64": base64.b64encode(b"image-bytes").decode()},
                )
            ],
            metadata={"message_type": "image"},
        )
        file_message = OutboundMessage(
            channel="wecom",
            account_id="account",
            session_id="session-wecom",
            external_user_id="user-1",
            text="report",
            attachments=[
                Attachment(
                    kind="file",
                    name="report.pdf",
                    content_type="application/pdf",
                    metadata={"content_base64": base64.b64encode(b"file-bytes").decode()},
                )
            ],
            metadata={"message_type": "file"},
        )
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_WECOM_CORP_SECRET": "corp-secret"}, clear=False):
            with patch("wechatpy.enterprise.WeChatClient", FakeClient):
                image_result = WeComAdapter().send(image_message, binding)
                file_result = WeComAdapter().send(file_message, binding)
        self.assertTrue(image_result.ok)
        self.assertTrue(file_result.ok)
        self.assertEqual(image_result.metadata["message_type"], "image")
        self.assertEqual(file_result.metadata["message_type"], "file")
        self.assertEqual(FakeClient.instances[1].media.media_bytes, b"image-bytes")
        self.assertEqual(FakeClient.instances[2].media.media_bytes, b"file-bytes")

    def test_wecom_robot_webhook_url_can_be_secret_reference(self):
        captured = {}

        def fake_post_json(url, payload):
            captured["url"] = url
            captured["payload"] = payload
            return {"errcode": 0, "msgid": "robot-msg"}

        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="wecom:robot",
            channel="wecom",
            account_id="robot",
            agent_app_id="app_support",
            config={
                "sdk_enabled": False,
                "webhook_url_ref": "secret://tenant_demo/wecom/robot-webhook-url",
            },
        )
        message = OutboundMessage(
            channel="wecom",
            account_id="robot",
            session_id="session-wecom",
            external_user_id="user-1",
            text="hello",
        )
        with patch.dict(
            "os.environ",
            {
                "SECRET_TENANT_DEMO_WECOM_ROBOT_WEBHOOK_URL": (
                    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=robot-secret"
                )
            },
            clear=False,
        ):
            with patch("trpc_service.channels.wecom._post_json", fake_post_json):
                result = WeComAdapter().send(message, binding)
        self.assertTrue(result.ok)
        self.assertEqual(captured["url"], "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=robot-secret")
        self.assertEqual(captured["payload"]["msgtype"], "text")

    def test_wechat_official_account_adapter_prefers_wechatpy_sdk(self):
        class FakeMessageApi:
            def send_text(self, user_id, content):
                self.args = (user_id, content)
                return {"errcode": 0, "errmsg": "ok"}

            def send_image(self, user_id, media_id):
                self.image_args = (user_id, media_id)
                return {"errcode": 0, "errmsg": "ok"}

        class FakeMediaApi:
            def upload(self, media_type, media_file):
                self.media_type = media_type
                self.media_file = media_file
                self.media_bytes = media_file.read()
                return {"media_id": f"{media_type}-media-id"}

        class FakeClient:
            instances = []

            def __init__(self, app_id, secret):
                self.app_id = app_id
                self.secret = secret
                self.message = FakeMessageApi()
                self.media = FakeMediaApi()
                FakeClient.instances.append(self)

        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="wechat:account",
            channel="wechat_official_account",
            account_id="account",
            agent_app_id="app_support",
            config={
                "sdk_enabled": True,
                "app_id": "wx-app",
                "app_secret_ref": "secret://tenant_demo/wechat/app-secret",
            },
        )
        message = OutboundMessage(
            channel="wechat_official_account",
            account_id="account",
            session_id="session-wechat",
            external_user_id="openid-1",
            text="hello",
        )
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_WECHAT_APP_SECRET": "app-secret"}, clear=False):
            with patch("wechatpy.WeChatClient", FakeClient):
                result = WeChatOfficialAccountAdapter().send(message, binding)
        self.assertTrue(result.ok)
        self.assertEqual(result.metadata["sdk"], "wechatpy")

        image_message = OutboundMessage(
            channel="wechat_official_account",
            account_id="account",
            session_id="session-wechat-image",
            external_user_id="openid-1",
            text="preview",
            attachments=[
                Attachment(
                    kind="image",
                    name="preview.png",
                    content_type="image/png",
                    metadata={"content_base64": base64.b64encode(b"image-bytes").decode()},
                )
            ],
            metadata={"message_type": "image"},
        )
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_WECHAT_APP_SECRET": "app-secret"}, clear=False):
            with patch("wechatpy.WeChatClient", FakeClient):
                image_result = WeChatOfficialAccountAdapter().send(image_message, binding)
        self.assertTrue(image_result.ok)
        self.assertEqual(image_result.metadata["message_type"], "image")
        self.assertEqual(FakeClient.instances[1].media.media_bytes, b"image-bytes")

        file_message = OutboundMessage(
            channel="wechat_official_account",
            account_id="account",
            session_id="session-wechat-file",
            external_user_id="openid-1",
            text="report",
            attachments=[
                Attachment(
                    kind="file",
                    name="report.pdf",
                    content_type="application/pdf",
                    metadata={"content_base64": base64.b64encode(b"file-bytes").decode()},
                )
            ],
            metadata={"message_type": "file"},
        )
        with patch.dict("os.environ", {"SECRET_TENANT_DEMO_WECHAT_APP_SECRET": "app-secret"}, clear=False):
            with patch("wechatpy.WeChatClient", FakeClient):
                file_result = WeChatOfficialAccountAdapter().send(file_message, binding)
        self.assertFalse(file_result.ok)
        self.assertTrue(file_result.metadata["unsupported"])

    def test_wechat_customer_service_rejects_file_without_text_fallback(self):
        binding = ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="wechat-kf:account",
            channel="wechat_customer_service",
            account_id="account",
            agent_app_id="app_support",
        )
        message = OutboundMessage(
            channel="wechat_customer_service",
            account_id="account",
            session_id="session-wechat-kf-file",
            external_user_id="openid-1",
            text="report",
            attachments=[
                Attachment(
                    kind="file",
                    name="report.pdf",
                    content_type="application/pdf",
                    metadata={"content_base64": base64.b64encode(b"file-bytes").decode()},
                )
            ],
        )
        result = WeChatCustomerServiceAdapter().send(message, binding)
        self.assertFalse(result.ok)
        self.assertTrue(result.metadata["unsupported"])

    def test_inbound_media_parsing_normalizes_platform_payloads(self):
        telegram = TelegramAdapter().parse_event(
            {
                "update_id": 100,
                "message": {
                    "message_id": 200,
                    "from": {"id": 42},
                    "chat": {"id": 42},
                    "photo": [
                        {"file_id": "small", "file_unique_id": "small-u", "width": 64},
                        {"file_id": "large", "file_unique_id": "large-u", "width": 1024},
                    ],
                    "document": {
                        "file_id": "doc-id",
                        "file_unique_id": "doc-u",
                        "file_name": "report.pdf",
                        "mime_type": "application/pdf",
                    },
                    "voice": {"file_id": "voice-id", "file_unique_id": "voice-u"},
                },
            },
            ChannelBinding("tenant_demo", "telegram:account", "telegram", "account", "app_support"),
        )
        self.assertEqual(
            [item.metadata.get("source") for item in telegram.attachments],
            [
                "telegram.photo",
                "telegram.document",
                "telegram.voice",
            ],
        )
        self.assertEqual(telegram.attachments[0].metadata["file_id"], "large")

        wecom = WeComAdapter().parse_event(
            {
                "MsgId": "m1",
                "FromUserName": "user",
                "MsgType": "image",
                "PicUrl": "https://wecom.example/image.jpg",
                "MediaId": "media-image",
            },
            ChannelBinding("tenant_demo", "wecom:account", "wecom", "account", "app_support"),
        )
        self.assertEqual(wecom.attachments[0].kind, "image")
        self.assertEqual(wecom.attachments[0].metadata["media_id"], "media-image")

        official = WeChatOfficialAccountAdapter().parse_event(
            {
                "MsgId": "m2",
                "FromUserName": "openid",
                "MsgType": "image",
                "PicUrl": "https://mmbiz.example/image.jpg",
                "MediaId": "official-media",
            },
            ChannelBinding(
                "tenant_demo",
                "wechat:account",
                "wechat_official_account",
                "account",
                "app_support",
            ),
        )
        self.assertEqual(official.attachments[0].metadata["media_id"], "official-media")

        customer = WeChatCustomerServiceAdapter().parse_event(
            {
                "MsgId": "m3",
                "OpenId": "openid",
                "MsgType": "image",
                "MediaId": "kf-media",
            },
            ChannelBinding(
                "tenant_demo",
                "wechat-kf:account",
                "wechat_customer_service",
                "account",
                "app_support",
            ),
        )
        self.assertEqual(customer.attachments[0].metadata["media_id"], "kf-media")

    def test_worker_executes_knowledge_tool_chain(self):
        storage = create_storage()
        storage.knowledge.upsert(
            KnowledgeChunk(
                tenant_id="tenant_demo",
                collection="default",
                chunk_id="k1",
                text="pricing policy supports tenant quotas",
            )
        )
        config = self.tenants.get_tenant("tenant_demo")
        request = RunRequest(
            tenant_context=TenantContext(
                tenant_id="tenant_demo",
                agent_app_id="app_support",
                config_version=config.config_version,
                trace_id="trace-tool",
                session_id="session-tool",
                channel="telegram",
                user_id="user",
            ),
            user_input=UserInput(text="/search pricing"),
            idempotency_key="tool-message",
        )
        events = AgentWorker(storage).run(request, config, storage)
        self.assertTrue(any(event.event_type == "tool_call" for event in events))
        self.assertIn("pricing policy", "\n".join(event.content for event in events))

    def test_tenant_versions_survive_repository_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tenant.sqlite3"
            repository = SQLiteTenantRepository(path)
            original = repository.create(self.tenants.get_tenant("tenant_demo"))
            changed = repository.get("tenant_demo")
            changed.apps[0].prompt = "version two"
            version = repository.save_version(changed)
            repository.publish("tenant_demo", version.config_version)
            repository.close()

            reopened = SQLiteTenantRepository(path)
            self.assertEqual(reopened.get("tenant_demo").config_version, version.config_version)
            self.assertEqual(reopened.get("tenant_demo").apps[0].prompt, "version two")
            self.assertEqual(
                reopened.get("tenant_demo", original.config_version).apps[0].prompt, original.apps[0].prompt
            )
            reopened.close()

    def test_persistent_demo_repository_tolerates_concurrent_seed(self):
        import trpc_service.tenant.repository as tenant_repository_module

        class RacingRepository:
            def __init__(self, dsn):
                self.dsn = dsn
                self.get_calls = 0

            def get(self, tenant_id, version=None):
                del version
                self.get_calls += 1
                if self.get_calls == 1:
                    raise tenant_repository_module.TenantNotFound(tenant_id)
                return default_demo_config()

            def create(self, config):
                del config
                raise RuntimeError("duplicate key value violates unique constraint")

        with patch.dict("os.environ", {"TENANT_DB_DSN": "postgresql://test"}, clear=False):
            with patch.object(tenant_repository_module, "PostgresTenantRepository", RacingRepository):
                repository = tenant_repository_module.persistent_demo_repository("unused.sqlite3")

        self.assertIsInstance(repository, RacingRepository)
        self.assertEqual(repository.get_calls, 2)

    def test_tenant_gray_release_resolves_candidate_by_session(self):
        repository = InMemoryTenantRepository()
        config = self.tenants.get_tenant("tenant_demo")
        config.apps[0].model_config.model = config.apps[0].model_config.model or "demo-model"
        original = repository.create(config)
        candidate_config = repository.get("tenant_demo")
        candidate_config.apps[0].prompt = "gray prompt"
        candidate = repository.save_version(candidate_config)
        service = TenantService(repository)

        active = service.configure_gray_release("tenant_demo", candidate.config_version, 100)
        self.assertTrue(active.gray_release.enabled)
        self.assertEqual(active.gray_release.candidate_version, candidate.config_version)
        self.assertEqual(
            service.resolve_runtime_config("tenant_demo", "session-a").config_version,
            candidate.config_version,
        )

        active = service.configure_gray_release(
            "tenant_demo",
            candidate.config_version,
            0,
            session_overrides={"session-b": original.config_version},
        )
        self.assertEqual(
            service.resolve_runtime_config("tenant_demo", "session-a").config_version,
            active.config_version,
        )
        self.assertEqual(
            service.resolve_runtime_config("tenant_demo", "session-b").config_version,
            original.config_version,
        )

    def test_mcp_json_rpc_tool_call(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps({"jsonrpc": "2.0", "result": {"content": "mcp result"}}).encode()

        captured = {}

        def fake_urlopen(request, timeout):
            captured["body"] = json.loads(request.data.decode())
            captured["timeout"] = timeout
            return FakeResponse()

        registry = ToolRegistry()
        registry.register_mcp_server("demo", "https://mcp.example.test", ["lookup"])
        with patch("trpc_service.tool.runtime.urlopen", fake_urlopen):
            result = registry.call("lookup", arguments={"q": "hello"}, request_id="r1")
        self.assertEqual(result.content, "mcp result")
        self.assertEqual(captured["body"]["method"], "tools/call")
        self.assertEqual(captured["body"]["params"]["arguments"]["q"], "hello")

    def test_reliable_delivery_retries_and_dead_letters(self):
        attempts = []
        result = send_with_retry(
            lambda: attempts.append(1)
            or __import__("trpc_service.channels.base", fromlist=["SendResult"]).SendResult(False, "", "down"),
            attempts=2,
        )
        self.assertFalse(result.ok)
        self.assertEqual(len(attempts), 2)

    def test_outbound_queue_update_persists_progress(self):
        class FakeRedis:
            def __init__(self):
                self.hashes = {}
                self.lists = {}

            @classmethod
            def from_url(cls, *_args, **_kwargs):
                return cls()

            def hsetnx(self, key, field, value):
                bucket = self.hashes.setdefault(key, {})
                if field in bucket:
                    return False
                bucket[field] = value
                return True

            def rpush(self, key, value):
                self.lists.setdefault(key, []).append(value)
                return len(self.lists[key])

            def hset(self, key, field, value):
                self.hashes.setdefault(key, {})[field] = value
                return 1

            def hget(self, key, field):
                return self.hashes.get(key, {}).get(field)

            def close(self):
                return None

        fake_redis = types.SimpleNamespace(Redis=FakeRedis)
        with patch.dict(sys.modules, {"redis": fake_redis}):
            queue = OutboundDeliveryQueue("redis://example")
            task_id = queue.enqueue({"tenant_id": "tenant_demo", "answer": "hello"}, task_id="task-1")
            queue.update({"task_id": task_id, "tenant_id": "tenant_demo", "completed_parts": [0]})

        payload = json.loads(queue.client.hashes[queue.data_key][task_id])
        self.assertEqual(payload["completed_parts"], [0])
        self.assertEqual(payload["answer"], "hello")

    def test_outbound_queue_poll_timeout_does_not_crash_consumer(self):
        class FakeRedis:
            @classmethod
            def from_url(cls, *_args, **_kwargs):
                return cls()

            def brpoplpush(self, *_args, **_kwargs):
                raise TimeoutError("poll timed out")

        fake_redis = types.SimpleNamespace(Redis=FakeRedis)
        with patch.dict(sys.modules, {"redis": fake_redis}):
            queue = OutboundDeliveryQueue("redis://example")
            self.assertIsNone(queue._move_to_processing(timeout=1))

    def test_outbound_mapper_supports_card_file_stream_and_split_metadata(self):
        base = {
            "channel": "web",
            "account_id": "web_demo",
            "session_id": "session-outbound",
            "external_user_id": "user-1",
        }
        messages = build_outbound_messages(
            [
                AgentEvent(
                    "card",
                    "",
                    {
                        "title": "deploy approval",
                        "body": "secondary confirmation required",
                        "buttons": [{"label": "open", "url": "https://example.test/ticket"}],
                    },
                ),
                AgentEvent(
                    "file",
                    "report generated",
                    {
                        "url": "https://example.test/report.pdf",
                        "name": "report.pdf",
                        "content_type": "application/pdf",
                    },
                ),
            ],
            **base,
        )
        self.assertEqual([item.metadata["message_type"] for item in messages], ["card", "file"])
        self.assertIn("deploy approval", messages[0].text)
        self.assertEqual(messages[1].attachments[0].name, "report.pdf")
        self.assertEqual(
            visible_answer(messages),
            "deploy approval\nsecondary confirmation required\nopen: https://example.test/ticket\nreport generated",
        )

        stream = build_outbound_messages(
            [AgentEvent("stream_delta", "hello "), AgentEvent("stream_delta", "world")],
            **base,
        )
        self.assertEqual(stream[0].text, "hello world")
        self.assertEqual(stream[0].metadata["message_type"], "stream")

        terminal = build_outbound_messages(
            [AgentEvent("stream_delta", "partial"), AgentEvent("message_end", "final")],
            **base,
        )
        self.assertEqual(len(terminal), 1)
        self.assertEqual(terminal[0].text, "final")

        split = split_outbound_messages(
            build_outbound_messages([AgentEvent("message_end", "abcdef")], **base),
            2,
        )
        self.assertEqual([item.text for item in split], ["ab", "cd", "ef"])
        self.assertEqual([item.metadata["part_index"] for item in split], [0, 1, 2])
        self.assertEqual({item.metadata["part_count"] for item in split}, {3})

    def test_migration_round_trip_with_sqlite_storage(self):
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
            source = create_storage(
                StorageProfile(session_backend="sql"),
                data_dir=Path(source_dir),
            )
            target = create_storage(
                StorageProfile(session_backend="sql"),
                data_dir=Path(target_dir),
            )
            try:
                config = self.tenants.get_tenant("tenant_demo")
                worker = AgentWorker(source)
                worker.run(
                    RunRequest(
                        tenant_context=TenantContext(
                            "tenant_demo",
                            "app_support",
                            config.config_version,
                            "migration-trace",
                            "migration-session",
                            "telegram",
                            "user",
                        ),
                        user_input=UserInput("migration payload"),
                        idempotency_key="migration-message",
                    ),
                    config,
                    source,
                )
                source.idempotency.start("tenant_demo", "migration-message", "migration-trace")
                source.idempotency.complete(
                    "tenant_demo",
                    "migration-message",
                    "migration-ref",
                    {"text": "ok"},
                )
                payload = export_tenant(source, "tenant_demo")
                import_tenant(target, payload)
                self.assertEqual(
                    len(target.session.load_events("tenant_demo", "migration-session")),
                    2,
                )
                self.assertTrue(target.memory.search("tenant_demo", "migration payload"))
                self.assertIn("idempotency", payload)
                self.assertIn("migration-message", {item["key"] for item in payload["idempotency"]})
                self.assertTrue(verify_tenant(target, payload)["ok"])
            finally:
                source.close()
                target.close()

    def test_migration_exports_knowledge_artifacts_and_cutover_plan(self):
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
            source = create_storage(data_dir=Path(source_dir))
            target = create_storage(data_dir=Path(target_dir))
            try:
                source.knowledge.upsert(
                    KnowledgeChunk(
                        tenant_id="tenant_demo",
                        collection="default",
                        chunk_id="chunk-1",
                        text="artifact migration knowledge",
                    )
                )
                obj = source.artifacts.put("tenant_demo", b"artifact-bytes", "text/plain")
                source.idempotency.start("tenant_demo", "idem-1", "trace-idem")
                source.idempotency.complete("tenant_demo", "idem-1", "ref-1", {"text": "ok"})

                payload = export_tenant(source, "tenant_demo")
                self.assertEqual({item["chunk_id"] for item in payload["knowledge"]}, {"chunk-1"})
                self.assertEqual({item["object_id"] for item in payload["artifacts"]}, {obj.object_id})
                self.assertEqual({item["key"] for item in payload["idempotency"]}, {"idem-1"})

                import_tenant(target, payload)
                self.assertEqual(target.artifacts.get("tenant_demo", obj.object_id), b"artifact-bytes")
                self.assertTrue(target.knowledge.search("tenant_demo", "default", "migration knowledge"))
                self.assertTrue(verify_tenant(target, payload)["ok"])
                self.assertEqual(cutover_plan(payload)["counts"]["artifacts"], 1)
            finally:
                source.close()
                target.close()

    def test_concurrent_session_updates_preserve_tenant_scope(self):
        storage = create_storage()
        config = self.tenants.get_tenant("tenant_demo")
        worker = AgentWorker(storage)
        errors = []

        def run(index):
            try:
                worker.run(
                    RunRequest(
                        tenant_context=TenantContext(
                            "tenant_demo",
                            "app_support",
                            config.config_version,
                            f"trace-{index}",
                            "parallel",
                            "telegram",
                            "user",
                        ),
                        user_input=UserInput(f"message-{index}"),
                        idempotency_key=f"parallel-{index}",
                    ),
                    config,
                    storage,
                )
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=run, args=(index,)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertLessEqual(len(errors), 1)
        self.assertTrue(storage.session.load_events("tenant_demo", "parallel"))

    def test_http_admin_requires_credentials_when_testclient_available(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {"TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3"), "ADMIN_API_KEY": "http-test-key"},
            clear=False,
        ):
            from trpc_service.web.app import create_app

            with TestClient(create_app()) as client:
                self.assertEqual(client.get("/admin/v1/tenants/tenant_demo").status_code, 401)
                self.assertEqual(
                    client.get(
                        "/admin/v1/tenants/tenant_demo", headers={"X-Admin-API-Key": "http-test-key"}
                    ).status_code,
                    200,
                )

    def test_http_ui_chat_works_without_external_im_secrets(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {"TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3")},
            clear=False,
        ):
            from trpc_service.web.app import create_app

            with TestClient(create_app()) as client:
                health = client.get("/health")
                self.assertEqual(health.status_code, 200)
                self.assertEqual(health.json()["status"], "ok")
                resp = client.post(
                    "/ui/api/chat",
                    json={"text": "hello", "user_id": "ui-user"},
                )
                self.assertEqual(resp.status_code, 200)
                body = resp.json()
                self.assertTrue(body["ok"])
                self.assertEqual(body["session_id"][:5], "sess_")
                self.assertTrue(body["session_id"].startswith("sess_user_"))

                group_resp = client.post(
                    "/ui/api/chat",
                    json={"text": "group hello", "user_id": "ui-user", "group_id": "ui-group"},
                )
                self.assertEqual(group_resp.status_code, 200)
                self.assertTrue(group_resp.json()["session_id"].startswith("sess_group_"))

    def test_sync_delivery_does_not_report_partial_split_success(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")

        import importlib

        web_app_module = importlib.import_module("trpc_service.web.app")
        from trpc_service.channels.web import WebAdapter

        calls = []

        class FailingWebAdapter(WebAdapter):
            def send(self, message, binding):
                calls.append(message.text)
                if len(calls) <= 3:
                    from trpc_service.channels.base import SendResult

                    return SendResult(False, "", "simulated delivery failure")
                return super().send(message, binding)

        def adapters_with_failure():
            adapters = default_channel_adapters()
            adapters["web"] = FailingWebAdapter()
            return adapters

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {"TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3")},
            clear=False,
        ), patch.object(web_app_module, "default_channel_adapters", adapters_with_failure):
            with TestClient(web_app_module.create_app()) as client:
                response = client.post(
                    "/ui/api/chat",
                    json={"text": "x" * 10_000, "user_id": "partial-delivery-user"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(len(calls), 3)

    def test_http_ui_attachment_is_stored_as_artifact_reference(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {"TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3")},
            clear=False,
        ):
            from trpc_service.web.app import create_app

            application = create_app()
            with TestClient(application) as client:
                resp = client.post(
                    "/ui/api/chat",
                    json={
                        "text": "attachment",
                        "user_id": "ui-user",
                        "attachments": [
                            {
                                "kind": "file",
                                "name": "test.txt",
                                "content_type": "text/plain",
                                "metadata": {"content_base64": base64.b64encode(b"artifact-data").decode()},
                            }
                        ],
                    },
                )
                self.assertEqual(resp.status_code, 200)
                body = resp.json()
                self.assertTrue(body["ok"])
                tenant = application.state.gateway.tenants.get_tenant("tenant_demo")
                storage = application.state.gateway.storage_manager.get(tenant)
                events = storage.session.load_events(tenant.tenant_id, body["session_id"])
                user_event = next(event for event in events if event.event_type == "user_message")
                attachment = user_event.payload["metadata"]["attachments"][0]
                self.assertIn("artifact_id", attachment["metadata"])
                self.assertNotIn("content_base64", attachment["metadata"])

    def test_telegram_file_id_attachment_is_downloaded_to_artifact(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")

        class FakeResponse:
            def __init__(self, body, content_type="application/octet-stream"):
                self.body = body
                self.headers = {"Content-Type": content_type}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, *_args):
                return self.body

        requested_urls = []

        def fake_urlopen(request, timeout):
            del timeout
            url = request.full_url if hasattr(request, "full_url") else str(request)
            requested_urls.append(url)
            if "/getFile?" in url:
                return FakeResponse(
                    json.dumps({"ok": True, "result": {"file_path": "photos/file.jpg"}}).encode(), "application/json"
                )
            if "/file/bot" in url:
                return FakeResponse(b"telegram-image", "image/jpeg")
            raise AssertionError(url)

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {
                "TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3"),
                "SECRET_TENANT_DEMO_TELEGRAM_TOKEN": "telegram-token",
                "SECRET_TENANT_DEMO_TELEGRAM_SECRET": "telegram-secret",
            },
            clear=False,
        ), patch("trpc_service.web.app.urlopen", fake_urlopen):
            from trpc_service.web.app import create_app

            application = create_app()
            tenant = application.state.gateway.tenants.get_tenant("tenant_demo")
            for binding in tenant.channel_bindings:
                if binding.channel == "telegram":
                    binding.secret_ref = None
            with TestClient(application) as client:
                resp = client.post(
                    "/webhooks/telegram/corp_account_1",
                    json={
                        "update_id": "tg-media",
                        "message": {
                            "message_id": 1,
                            "from": {"id": 42},
                            "chat": {"id": 42},
                            "photo": [{"file_id": "photo-id", "file_unique_id": "p1", "width": 800}],
                        },
                        "secret_token": "telegram-secret",
                    },
                    headers={"X-Telegram-Bot-Api-Secret-Token": "telegram-secret"},
                )
                self.assertEqual(resp.status_code, 200)
                body = resp.json()
                self.assertTrue(body["accepted"])
                storage = application.state.gateway.storage_manager.get(tenant)
                session_id = build_session_id(
                    tenant.tenant_id,
                    "app_support",
                    "telegram",
                    "corp_account_1",
                    "42",
                    "42",
                )
                events = storage.session.load_events(tenant.tenant_id, session_id)
                user_event = next(event for event in events if event.event_type == "user_message")
                attachment = user_event.payload["metadata"]["attachments"][0]
        encoded = json.dumps(attachment, ensure_ascii=False)
        self.assertIn("artifact_id", attachment["metadata"])
        self.assertEqual(attachment["metadata"]["materialized_from"], "telegram_file_id")
        self.assertNotIn("content_base64", attachment["metadata"])
        self.assertNotIn("telegram-token", encoded)
        self.assertTrue(any("getFile" in url for url in requested_urls))

    def test_wechat_media_id_attachment_is_downloaded_to_artifact(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")

        class FakeResponse:
            headers = {"Content-Type": "image/jpeg"}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, *_args):
                return b"wechat-image"

        captured = []

        def fake_urlopen(request, timeout):
            del timeout
            captured.append(request.full_url if hasattr(request, "full_url") else str(request))
            return FakeResponse()

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {
                "TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3"),
                "SECRET_TENANT_DEMO_WECHAT_OFFICIAL_ACCOUNT_TOKEN": "wechat-access-token",
            },
            clear=False,
        ), patch("trpc_service.web.app.urlopen", fake_urlopen):
            from trpc_service.web.app import create_app

            application = create_app()
            tenant = application.state.gateway.tenants.get_tenant("tenant_demo")
            with TestClient(application) as client:
                resp = client.post(
                    "/webhooks/wechat_official_account/corp_account_1",
                    json={
                        "MsgId": "wx-media",
                        "FromUserName": "openid",
                        "MsgType": "image",
                        "MediaId": "media-id",
                    },
                )
                self.assertEqual(resp.status_code, 200)
                body = resp.json()
                self.assertTrue(body["accepted"])
                storage = application.state.gateway.storage_manager.get(tenant)
                session_id = build_session_id(
                    tenant.tenant_id,
                    "app_support",
                    "wechat_official_account",
                    "corp_account_1",
                    "openid",
                )
                events = storage.session.load_events(tenant.tenant_id, session_id)
                user_event = next(event for event in events if event.event_type == "user_message")
                attachment = user_event.payload["metadata"]["attachments"][0]
        encoded = json.dumps(attachment, ensure_ascii=False)
        self.assertIn("artifact_id", attachment["metadata"])
        self.assertEqual(attachment["metadata"]["materialized_from"], "wechat_media_id")
        self.assertNotIn("wechat-access-token", encoded)
        self.assertTrue(any("/cgi-bin/media/get" in url for url in captured))

    def test_strict_channel_config_requires_real_im_credentials(self):
        repository = InMemoryTenantRepository()
        config = self.tenants.get_tenant("tenant_demo")
        config.channel_bindings = [
            ChannelBinding(
                tenant_id="tenant_demo",
                binding_id="telegram:strict",
                channel="telegram",
                account_id="strict",
                agent_app_id="app_support",
                token_ref="secret://tenant_demo/telegram/token",
            )
        ]
        with patch.dict("os.environ", {"STRICT_CHANNEL_CONFIG": "1"}, clear=False):
            with self.assertRaises(TenantValidationError):
                TenantService(repository).validate(config)
        config.channel_bindings[0].secret_ref = "secret://tenant_demo/telegram/secret"
        with patch.dict("os.environ", {"STRICT_CHANNEL_CONFIG": "1"}, clear=False):
            TenantService(repository).validate(config)

    def test_runtime_bridge_loads_external_factory_when_configured(self):
        module = types.ModuleType("dummy_runtime_factory")

        class DummyWorker:
            def __init__(self, settings):
                self.settings = settings

            def run(self, request, config, storage):
                del request, config, storage
                return [AgentEvent("message_end", self.settings["label"])]

        def build_worker(*, storage, telemetry=None, model_client=None, settings=None):
            del storage, telemetry, model_client
            return DummyWorker(settings or {})

        module.build_worker = build_worker
        with patch.dict(
            sys.modules,
            {"dummy_runtime_factory": module},
            clear=False,
        ), patch.dict(
            "os.environ",
            {
                "TRPC_AGENT_RUNTIME_FACTORY": "dummy_runtime_factory:build_worker",
                "TRPC_AGENT_RUNTIME_SETTINGS_JSON": '{"label":"bridged"}',
                "TRPC_AGENT_RUNTIME_MODE": "external",
            },
            clear=False,
        ):
            storage = create_storage()
            try:
                worker = build_runtime_worker(storage, TraceRecorder(), spec=RuntimeBridgeSpec.from_env())
            finally:
                storage.close()
        self.assertEqual(worker.run(None, None, None)[0].content, "bridged")

    def test_runtime_bridge_local_mode_is_explicit(self):
        with patch.dict(
            "os.environ",
            {
                "TRPC_AGENT_RUNTIME_FACTORY": "missing_runtime_factory:build_worker",
                "TRPC_AGENT_RUNTIME_MODE": "local",
            },
            clear=False,
        ):
            storage = create_storage()
            try:
                worker = build_runtime_worker(storage, TraceRecorder())
            finally:
                storage.close()
        self.assertEqual(worker.__class__.__name__, "AgentWorker")

    def test_runtime_bridge_defaults_to_trpc_runtime(self):
        with patch.dict("os.environ", {"TRPC_AGENT_RUNTIME_MODE": "trpc"}, clear=False):
            storage = create_storage()
            try:
                worker = build_runtime_worker(storage, TraceRecorder())
            finally:
                storage.close()
        self.assertEqual(worker.__class__.__name__, "TrpcAgentWorker")

    def test_trpc_runtime_run_preserves_session_lease(self):
        from trpc_service.agent.trpc_runtime import TrpcAgentWorker

        storage = create_storage()
        config = self.tenants.get_tenant("tenant_demo")

        class StubTrpcWorker(TrpcAgentWorker):
            async def _run_sdk_async(self, app, conversation, tool_names=None):
                del app, conversation, tool_names
                return ModelResponse("trpc runtime reply", 2, 3, 5)

        worker = StubTrpcWorker(storage, TraceRecorder())
        request = RunRequest(
            TenantContext(
                "tenant_demo",
                "app_support",
                config.config_version,
                "trace-trpc-run",
                "session-trpc-run",
                "web",
                "user-trpc-run",
            ),
            UserInput("hello from trpc runtime"),
            "trpc-runtime-request",
        )
        try:
            events = worker.run(request, config, storage)
            state = storage.session.load_state("tenant_demo", "session-trpc-run")
        finally:
            storage.close()
        self.assertEqual(events[-1].content, "trpc runtime reply")
        self.assertEqual(state.latest_event_seq, 2)

    def test_runtime_bridge_external_mode_fails_without_factory(self):
        with patch.dict(
            "os.environ",
            {"TRPC_AGENT_RUNTIME_MODE": "external", "TRPC_AGENT_RUNTIME_FACTORY": ""},
            clear=False,
        ):
            storage = create_storage()
            try:
                with self.assertRaisesRegex(RuntimeError, "TRPC_AGENT_RUNTIME_FACTORY"):
                    build_runtime_worker(storage, TraceRecorder())
            finally:
                storage.close()

    def test_worker_injects_summary_and_memory_into_model_context(self):
        storage = create_storage()
        config = self.tenants.get_tenant("tenant_demo")
        storage.summary.put(
            Summary(
                tenant_id="tenant_demo",
                session_id="sess-context",
                content="The user prefers concise replies.",
                source_event_seq=2,
            )
        )
        storage.memory.put(
            MemoryItem(
                tenant_id="tenant_demo",
                memory_id="memory-context",
                scope_key="sess-context",
                content="The user's deployment uses Redis and PostgreSQL.",
            )
        )

        class CaptureModel:
            def __init__(self):
                self.conversation = None

            def generate_with_usage(self, **kwargs):
                self.conversation = kwargs["conversation"]
                return ModelResponse("captured", 3, 1, 4)

        model = CaptureModel()
        worker = AgentWorker(storage, model_client=model)
        worker.run(
            RunRequest(
                tenant_context=TenantContext(
                    "tenant_demo",
                    "app_support",
                    config.config_version,
                    "trace-context",
                    "sess-context",
                    "web",
                    "user-context",
                ),
                user_input=UserInput("deployment"),
                idempotency_key="context-request",
            ),
            config,
            storage,
        )
        combined = "\n".join(item["content"] for item in model.conversation)
        self.assertIn("Session summary:", combined)
        self.assertIn("The user prefers concise replies.", combined)
        self.assertIn("Relevant tenant memory:", combined)
        self.assertIn("Redis and PostgreSQL", combined)

    def test_secret_redaction_covers_text_and_nested_provider_data(self):
        with patch.dict(
            "os.environ",
            {"TEST_API_KEY": "api-key-value", "TEST_PASSWORD": "password-value"},
            clear=False,
        ):
            text = redact_secret_text(
                "Authorization: Bearer api-key-value; password=password-value; access_token=url-token"
            )
            data = redact_secret_data({"access_token": "api-key-value", "nested": {"message": "api-key-value"}})
        self.assertNotIn("api-key-value", text)
        self.assertNotIn("password-value", text)
        self.assertNotIn("url-token", text)
        self.assertEqual(data["access_token"], "[secret-redacted]")
        self.assertNotIn("api-key-value", str(data))

    def test_tenant_serialization_does_not_expose_direct_database_urls(self):
        config = self.tenants.get_tenant("tenant_demo")
        config.storage_profile.redis_url = "redis://:redis-password@redis:6379/0"
        config.storage_profile.sql_dsn = "postgresql://user:sql-password@sql/db"
        serialized = config.to_dict()
        public = config.to_public_dict()
        for payload in (serialized, public):
            encoded = json.dumps(payload, ensure_ascii=False)
            self.assertNotIn("redis-password", encoded)
            self.assertNotIn("sql-password", encoded)
            self.assertNotIn("redis://:redis-password", encoded)
            self.assertNotIn("postgresql://user:sql-password", encoded)

    def test_tenant_serialization_retains_non_secret_database_endpoints(self):
        config = self.tenants.get_tenant("tenant_demo")
        config.storage_profile.redis_url = "redis://redis.internal:6379/0"
        config.storage_profile.sql_dsn = "postgresql://sql.internal/trpc_agent"
        serialized = config.to_dict()["storage_profile"]
        self.assertEqual(serialized["redis_url"], "redis://redis.internal:6379/0")
        self.assertEqual(serialized["sql_dsn"], "postgresql://sql.internal/trpc_agent")

    def test_tenant_validation_rejects_plaintext_database_credentials(self):
        config = default_demo_config()
        config.storage_profile.redis_url = "redis://:redis-password@redis.internal:6379/0"
        with self.assertRaises(TenantValidationError):
            TenantService(InMemoryTenantRepository()).validate(config)

    def test_sdk_session_service_is_ephemeral_platform_context(self):
        from trpc_service.agent.trpc_runtime import _build_session_service
        from trpc_agent_sdk.sessions import InMemorySessionService

        profile = StorageProfile(
            session_backend="redis",
            redis_url_ref="secret://tenant_a/redis/url",
            sql_dsn_ref="secret://tenant_a/sql/dsn",
        )
        service = _build_session_service(profile)
        self.assertIsInstance(service, InMemorySessionService)

    def test_public_tenant_config_redacts_channel_and_model_secrets(self):
        config = default_demo_config()
        config.apps[0].model_config.api_key_ref = "secret://tenant_demo/model/api-key"
        config.channel_bindings[0].config = {
            "safe_option": "visible",
            "webhook_url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=robot-secret",
            "corp_secret_ref": "secret://tenant_demo/wecom/corp-secret",
            "nested": {"access_token": "literal-token"},
        }
        public = config.to_public_dict()
        encoded = json.dumps(public, ensure_ascii=False)
        self.assertIn("visible", encoded)
        self.assertNotIn("robot-secret", encoded)
        self.assertNotIn("literal-token", encoded)
        self.assertNotIn("secret://tenant_demo/model/api-key", encoded)
        self.assertNotIn("secret://tenant_demo/wecom/corp-secret", encoded)

    def test_tenant_validation_rejects_plaintext_channel_secrets(self):
        config = default_demo_config()
        config.channel_bindings[0].config = {
            "webhook_url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=robot-secret"
        }
        with self.assertRaises(TenantValidationError):
            TenantService(InMemoryTenantRepository()).validate(config)

    def test_unknown_admin_tenant_returns_404(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {"TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3"), "ADMIN_API_KEY": "http-test-key"},
            clear=False,
        ):
            from trpc_service.web.app import create_app

            with TestClient(create_app()) as client:
                response = client.get(
                    "/admin/v1/tenants/tenant-missing",
                    headers={"X-Admin-API-Key": "http-test-key"},
                )
                self.assertEqual(response.status_code, 404)

    def test_durable_webhook_rejects_bad_signature_before_enqueue(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")

        class FakeQueue:
            submitted = 0

            def __init__(self, *args, **kwargs):
                pass

            def submit(self, *args, **kwargs):
                type(self).submitted += 1
                return "task"

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {
                "TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3"),
                "REDIS_URL": "redis://fake",
                "ADMIN_API_KEY": "http-test-key",
                "SECRET_TENANT_DEMO_TELEGRAM_SECRET": "expected-secret",
            },
            clear=False,
        ), patch("trpc_service.web.app.DurableWebhookQueue", FakeQueue):
            from trpc_service.web.app import create_app

            with TestClient(create_app()) as client:
                response = client.post(
                    "/webhooks/telegram/corp_account_1",
                    json={
                        "update_id": "bad-signature",
                        "message": {
                            "message_id": 1,
                            "from": {"id": 42},
                            "chat": {"id": 42},
                            "text": "hello",
                        },
                    },
                )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(FakeQueue.submitted, 0)

    def test_durable_webhook_poll_timeout_is_idle(self):
        class FakeRedis:
            @classmethod
            def from_url(cls, *_args, **_kwargs):
                return cls()

            def hscan_iter(self, *_args, **_kwargs):
                return iter(())

            def lrange(self, *_args, **_kwargs):
                return []

            def brpoplpush(self, *_args, **_kwargs):
                raise TimeoutError("poll timed out")

        fake_redis = types.SimpleNamespace(Redis=FakeRedis)
        with patch.dict(sys.modules, {"redis": fake_redis}):
            from trpc_service.gateway.worker_queue import DurableWebhookQueue

            queue = DurableWebhookQueue("redis://example")
            self.assertFalse(queue.consume_once(lambda _item: None, timeout=1))

    def test_worker_queue_orphan_recovery_waits_for_grace_period(self):
        raw = json.dumps({"request_id": "req-1", "attempt": 0})

        class FakeRedis:
            @classmethod
            def from_url(cls, *_args, **_kwargs):
                return cls()

            def hscan_iter(self, *_args, **_kwargs):
                return iter(())

            def lrange(self, *_args, **_kwargs):
                return [raw]

            def lrem(self, *_args, **_kwargs):
                raise AssertionError("fresh processing items must not be recovered immediately")

        fake_redis = types.SimpleNamespace(Redis=FakeRedis)
        with patch.dict(sys.modules, {"redis": fake_redis}), patch.dict(
            "os.environ",
            {"WORKER_ORPHAN_GRACE_SECONDS": "60"},
            clear=False,
        ):
            from trpc_service.gateway.worker_queue import WorkerQueue

            queue = WorkerQueue("redis://example")
            self.assertEqual(queue._recover_orphaned(), 0)

    def test_durable_webhook_can_fail_fast_when_required(self):
        try:
            from fastapi.testclient import TestClient  # noqa: F401
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")

        class BrokenQueue:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("redis unavailable")

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {
                "TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3"),
                "REDIS_URL": "redis://fake",
                "WEBHOOK_DURABLE_QUEUE": "1",
                "REQUIRE_DURABLE_WEBHOOK": "1",
            },
            clear=False,
        ), patch("trpc_service.web.app.DurableWebhookQueue", BrokenQueue):
            from trpc_service.web.app import create_app

            with self.assertRaises(RuntimeError):
                create_app()

    def test_sqlite_compensation_round_trip_does_not_override_idempotency(self):
        from trpc_service.storage.sql_store import SQLiteStorage

        with tempfile.TemporaryDirectory() as directory:
            storage = SQLiteStorage(Path(directory) / "platform.sqlite3")
            try:
                storage.idempotency.start("tenant_demo", "idem-comp", "trace-comp")
                completed = storage.idempotency.complete(
                    "tenant_demo",
                    "idem-comp",
                    "response-ref",
                    {"text": "ok"},
                )
                self.assertEqual(completed.response_ref, "response-ref")
                self.assertEqual(storage.idempotency.get("tenant_demo", "idem-comp").result["text"], "ok")

                task = storage.compensation.enqueue(
                    "tenant_demo",
                    "memory.put",
                    {
                        "tenant_id": "tenant_demo",
                        "memory_id": "mem-comp",
                        "scope_key": "session-comp",
                        "content": "hello compensation",
                        "metadata": {},
                    },
                    task_id="task-comp",
                )
                self.assertEqual(task.status, "pending")
                claimed = storage.compensation.claim(1)
                self.assertEqual([item.task_id for item in claimed], ["task-comp"])
                storage.compensation.fail("task-comp", "token=secret", retry_after_seconds=0)
                retried = storage.compensation.claim(1)
                self.assertEqual([item.task_id for item in retried], ["task-comp"])
                storage.compensation.complete("task-comp")
                self.assertEqual(storage.compensation.claim(1), [])
            finally:
                storage.close()

    def test_worker_defers_memory_summary_and_audit_write_failures(self):
        class FailOnceStore:
            def __init__(self, target, method_name):
                self.target = target
                self.method_name = method_name
                self.failed = False

            def __getattr__(self, name):
                return getattr(self.target, name)

            def _maybe_fail(self):
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("temporary backend unavailable")

            def put(self, value):
                if self.method_name == "put":
                    self._maybe_fail()
                return self.target.put(value)

            def append(self, value):
                if self.method_name == "append":
                    self._maybe_fail()
                return self.target.append(value)

        storage = create_storage()
        storage.memory = FailOnceStore(storage.memory, "put")
        storage.summary = FailOnceStore(storage.summary, "put")
        storage.audit = FailOnceStore(storage.audit, "append")
        config = self.tenants.get_tenant("tenant_demo")
        worker = AgentWorker(
            storage,
            model_client=type(
                "FakeModel",
                (),
                {"generate_with_usage": lambda self, **kwargs: ModelResponse("ok", 1, 1, 2)},
            )(),
        )
        request = RunRequest(
            tenant_context=TenantContext(
                "tenant_demo",
                "app_support",
                config.config_version,
                "trace-defer",
                "session-defer",
                "telegram",
                "user-defer",
            ),
            user_input=UserInput("remember this"),
            idempotency_key="defer-message",
        )

        events = worker.run(request, config, storage)

        self.assertEqual(events[-1].event_type, "message_end")
        claimed = storage.compensation.claim(10)
        self.assertEqual({task.operation for task in claimed}, {"memory.put", "summary.put", "audit.append"})
        for task in claimed:
            storage.compensation.fail(task.task_id, "retry now", retry_after_seconds=0)
        self.assertEqual(replay_compensations(storage, limit=10), 3)
        self.assertTrue(storage.memory.search("tenant_demo", "remember", scope_keys=("session-defer",)))
        self.assertIsNotNone(storage.summary.latest("tenant_demo", "session-defer"))
        self.assertTrue(any(record.trace_id == "trace-defer" for record in storage.audit.list_by_tenant("tenant_demo")))

    def test_tool_approval_requires_persistent_confirmation(self):
        storage = create_storage()
        config = self.tenants.get_tenant("tenant_demo")
        config.apps[0].tool_policy = ToolPolicy(
            allowlist=["search_knowledge", "send_external_message"],
            approval_rules=["send_external_message"],
        )
        worker = AgentWorker(
            storage,
            model_client=type(
                "FakeModel",
                (),
                {"generate_with_usage": lambda self, **kwargs: ModelResponse("approved", 1, 1, 2)},
            )(),
        )
        executed = []

        def send_external_message(**kwargs):
            executed.append(kwargs)
            return ToolResult("send_external_message", "sent", {"ok": True})

        worker.tools.register("send_external_message", send_external_message)
        base_context = TenantContext(
            "tenant_demo",
            "app_support",
            config.config_version,
            "trace-approval-1",
            "session-approval",
            "telegram",
            "user-approval",
        )
        first = worker.run(
            RunRequest(
                tenant_context=base_context,
                user_input=UserInput(
                    "send",
                    {"tool_calls": [{"name": "send_external_message", "arguments": {"target": "ops"}}]},
                ),
                idempotency_key="approval-1",
            ),
            config,
            storage,
        )
        self.assertEqual(first[-1].event_type, "approval_required")
        self.assertEqual(executed, [])
        approval = first[-1].metadata["approval_id"]
        token = first[-1].metadata["approval_token"]
        self.assertTrue(
            any(
                event.event_type == "tool_approval_requested" and event.payload["approval_id"] == approval
                for event in storage.session.load_events("tenant_demo", "session-approval")
            )
        )

        second = worker.run(
            RunRequest(
                tenant_context=TenantContext(
                    "tenant_demo",
                    "app_support",
                    config.config_version,
                    "trace-approval-2",
                    "session-approval",
                    "telegram",
                    "user-approval",
                ),
                user_input=UserInput(
                    "send",
                    {
                        "tool_calls": [
                            {
                                "name": "send_external_message",
                                "arguments": {"target": "ops"},
                                "approval_id": approval,
                                "approval_token": token,
                            }
                        ]
                    },
                ),
                idempotency_key="approval-2",
            ),
            config,
            storage,
        )
        self.assertEqual(len(executed), 1)
        self.assertTrue(any(event.event_type == "tool_call" for event in second))
        self.assertEqual(second[-1].event_type, "message_end")

    def test_revoke_message_is_audited_without_worker_execution(self):
        storage = create_storage()
        repository = InMemoryTenantRepository()
        repository.create(self.tenants.get_tenant("tenant_demo"))
        gateway = AgentGateway(
            TenantService(repository),
            storage,
            workers=[AgentWorker(storage)],
            storage_manager=None,
        )
        message = InboundMessage(
            channel="telegram",
            account_id="corp_account_1",
            external_message_id="revoke-event",
            external_user_id="user-revoke",
            raw_event={"normalized_event_type": "revoke", "target_message_id": "msg-original"},
        )

        session_id, events, response_ref = gateway.dispatch(message, trace_id="trace-revoke")

        self.assertTrue(session_id)
        self.assertEqual(response_ref, "telegram:revoke-event:revoked")
        self.assertEqual(events, [AgentEvent("message_revoked", "", {"revoked": True})])
        self.assertEqual(gateway.workers[0].executions, 0)
        self.assertTrue(
            any(
                event.event_type == "message_revoked" and event.payload["target_message_id"] == "msg-original"
                for event in storage.session.load_events("tenant_demo", session_id)
            )
        )
        self.assertTrue(
            any(
                record.decision == "revoked" and record.trace_id == "trace-revoke"
                for record in storage.audit.list_by_tenant("tenant_demo")
            )
        )

    def test_revoke_outbound_messages_are_empty(self):
        messages = build_outbound_messages(
            [AgentEvent("message_revoked", "", {"revoked": True})],
            channel="telegram",
            account_id="corp_account_1",
            session_id="session-revoke",
            external_user_id="user-revoke",
        )
        self.assertEqual(messages, [])


if __name__ == "__main__":
    unittest.main()
