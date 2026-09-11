from __future__ import annotations

import base64
import hashlib
import json
import os
import struct
import unittest
from collections.abc import Mapping
from dataclasses import replace
from unittest.mock import patch
from uuid import UUID

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from fastapi.testclient import TestClient

from trpc_service.agent import EchoExecutor
from trpc_service.channels import FeishuAdapter, TelegramAdapter, WeComAdapter, WeComVerificationError
from trpc_service.channels.feishu import HTTPResponse
from trpc_service.config import ServiceSettings
from trpc_service.storage import (
    InboundMediaMaterializer,
    MigrationCheckpoint,
    MigrationCoordinator,
    MigrationPhase,
    ProjectionCoordinator,
    RedisStateStore,
    StorageRouter,
    VectorMatch,
)
from trpc_service.tenant import (
    AgentApp,
    ChannelBinding,
    InboundMessage,
    OutboundMessage,
    StorageProfile,
    TenantConfig,
)
from trpc_service.tool import ConfirmationError, ConfirmationScope, ConfirmationService, arguments_hash
from trpc_service.tool.integration import GovernedToolRuntime, ToolCatalog, ToolDefinition, active_tool_turn
from trpc_service.web.app import build_demo_runtime, create_app


class FakeHTTP:
    def __init__(self, responses: list[tuple[int, dict[str, object]]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, Mapping[str, object]]] = []

    async def post(self, url: str, *, headers: Mapping[str, str], payload: Mapping[str, object]) -> HTTPResponse:
        self.calls.append((url, payload))
        status, body = self.responses.pop(0)
        return HTTPResponse(status, {}, json.dumps(body).encode())

    async def delete(self, url: str, *, headers: Mapping[str, str]) -> HTTPResponse:
        self.calls.append((url, headers))
        status, body = self.responses.pop(0)
        return HTTPResponse(status, {}, json.dumps(body).encode())


class ProductionFeatureTest(unittest.IsolatedAsyncioTestCase):
    async def test_telegram_retries_429_and_sends_media(self) -> None:
        http = FakeHTTP(
            [
                (429, {"ok": False, "error_code": 429, "parameters": {"retry_after": 0.001}}),
                (200, {"ok": True, "result": {"message_id": 9}}),
            ]
        )
        adapter = TelegramAdapter(http, api_base_url="https://telegram.test")
        binding = ChannelBinding("telegram", "bot", "verify", secret_ref="literal://bot-token")
        message = OutboundMessage(
            "acme", "telegram", "bot", "42", "42", "caption", "7", "trace", message_type="photo", media_url="https://x/p.png"
        )

        receipt = await adapter.send(message, binding)

        self.assertTrue(receipt["ok"])
        self.assertEqual(2, len(http.calls))
        self.assertTrue(http.calls[-1][0].endswith("/sendPhoto"))
        self.assertEqual("https://x/p.png", http.calls[-1][1]["photo"])

    async def test_feishu_proactive_card_and_recall(self) -> None:
        http = FakeHTTP(
            [
                (200, {"code": 0, "tenant_access_token": "token", "expire": 7200}),
                (200, {"code": 0, "data": {"message_id": "om-new"}}),
                (200, {"code": 0, "data": {}}),
            ]
        )
        adapter = FeishuAdapter(http, api_base_url="https://feishu.test")
        binding = ChannelBinding("feishu", "cli", secret_ref="literal://secret")

        sent = await adapter.send_card(binding, "ou-user", {"elements": []})
        recalled = await adapter.recall(binding, "om-new")

        self.assertTrue(sent["ok"])
        self.assertTrue(recalled["ok"])
        self.assertEqual("interactive", http.calls[1][1]["msg_type"])
        proactive_uuid = str(http.calls[1][1]["uuid"])
        self.assertEqual(proactive_uuid, str(UUID(proactive_uuid)))
        self.assertLessEqual(len(proactive_uuid), 50)

    async def test_confirmation_token_is_scoped_and_one_time(self) -> None:
        service = ConfirmationService(b"x" * 32)
        scope = ConfirmationScope("acme", "user", "session", "delete_ticket", arguments_hash({"id": 1}))
        token = await service.issue(scope)

        await service.consume(token, scope)
        with self.assertRaises(ConfirmationError):
            await service.consume(token, scope)

    async def test_migration_rejects_invalid_cutover(self) -> None:
        class Repository:
            pool = None

        coordinator = MigrationCoordinator(Repository())
        checkpoint = MigrationCheckpoint("acme", "m1", MigrationPhase.SHADOW_READ, 2, 1)

        async def invalid(_: MigrationCheckpoint) -> MigrationCheckpoint:
            return replace(checkpoint, phase=MigrationPhase.CUTOVER)

        with self.assertRaisesRegex(ValueError, "validation"):
            await coordinator.advance(checkpoint, invalid)

    async def test_wecom_decrypts_aes_callback_and_checks_receiver(self) -> None:
        key = bytes(range(32))
        encoding_key = base64.b64encode(key).decode().rstrip("=")
        xml = b"<xml><FromUserName>u1</FromUserName><MsgId>8</MsgId><MsgType>text</MsgType><Content>hello</Content></xml>"
        plaintext = b"0" * 16 + struct.pack("!I", len(xml)) + xml + b"corp-id"
        padder = padding.PKCS7(256).padder()
        padded = padder.update(plaintext) + padder.finalize()
        encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
        encrypted = base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode()
        body = f"<xml><Encrypt><![CDATA[{encrypted}]]></Encrypt></xml>".encode()
        token, timestamp, nonce = "verify", "1", "n"
        signature = hashlib.sha1("".join(sorted((token, timestamp, nonce, encrypted))).encode()).hexdigest()
        binding = ChannelBinding(
            "wecom", "account", token, encrypt_key_ref=f"literal://{encoding_key}", corp_id="corp-id"
        )
        adapter = WeComAdapter(dry_run=True)

        message = adapter.handle_callback(
            "acme",
            binding,
            {"msg_signature": signature, "timestamp": timestamp, "nonce": nonce},
            body,
            "trace",
        )

        self.assertEqual("hello", message.text)
        with self.assertRaises(WeComVerificationError):
            adapter.decrypt(replace(binding, corp_id="other"), encrypted)

    async def test_telegram_downloads_get_file_and_bytes(self) -> None:
        class MediaHTTP(FakeHTTP):
            async def get(self, url: str, *, headers: Mapping[str, str]) -> HTTPResponse:
                self.calls.append((url, headers))
                return HTTPResponse(200, {"Content-Type": "image/png"}, b"bytes")

        http = MediaHTTP([(200, {"ok": True, "result": {"file_path": "photos/a.png"}})])
        adapter = TelegramAdapter(http, api_base_url="https://telegram.test")
        binding = ChannelBinding("telegram", "bot", secret_ref="literal://token")
        data, content_type = await adapter.download_media(binding, "file-id")
        self.assertEqual((b"bytes", "image/png"), (data, content_type))
        self.assertTrue(http.calls[0][0].endswith("/getFile"))

    async def test_storage_router_supports_kind_specific_names(self) -> None:
        router = StorageRouter()
        session, memory, audit = object(), object(), object()
        router.register("shared", session, kind="session")
        router.register("shared", memory, kind="memory")
        router.register("shared", audit, kind="audit")
        config = TenantConfig(
            "acme", "Acme", {"default": AgentApp("default", "agent")}, {},
            storage=StorageProfile(session="shared", memory="shared", audit="shared"),
        )
        routed = router.route(config)
        self.assertIs(session, routed.session)
        self.assertIs(memory, routed.memory)
        self.assertIs(audit, routed.audit)

    async def test_media_materializer_is_idempotent_and_tenant_scoped(self) -> None:
        class Store:
            def __init__(self) -> None:
                self.calls = 0
                self.values = {}

            async def put(self, tenant_id, data, content_type, artifact_id=None):
                self.calls += 1
                import hashlib

                from trpc_service.storage import Artifact
                checksum = hashlib.sha256(data).hexdigest()
                value = Artifact(tenant_id, artifact_id, f"tenants/{tenant_id}/{artifact_id}", checksum, len(data), content_type)
                self.values[artifact_id] = value
                return value

            def uri(self, artifact):
                return f"memory://{artifact.key}"

        class Adapter:
            async def download_media(self, binding, locator, *, max_bytes):
                return b"data", "image/png"

        class Repository:
            def __init__(self):
                self.values = {}

            async def get_artifact(self, tenant_id, artifact_id):
                return self.values.get((tenant_id, artifact_id))

            async def record_artifact(self, artifact):
                self.values[(artifact.tenant_id, artifact.artifact_id)] = artifact

        store = Store()
        router = StorageRouter()
        router.register("memory", object(), kind="session")
        router.register("memory", object(), kind="memory")
        router.register("memory", object(), kind="audit")
        router.register("objects", store, kind="object_store")
        config = TenantConfig(
            "acme", "Acme", {"default": AgentApp("default", "agent")},
            {"telegram": ChannelBinding("telegram", "bot")},
            storage=StorageProfile(session="memory", memory="memory", audit="memory", object_store="objects"),
        )
        message = InboundMessage(
            "acme", "telegram", "bot", "m1", "u1", "c1", "direct", "[photo]",
            raw={"normalized_media": {"type": "photo", "file_id": "f1"}},
        )
        materializer = InboundMediaMaterializer(router, repository=Repository())
        first = await materializer.materialize(config, message, Adapter(), config.channels["telegram"])
        second = await materializer.materialize(config, first, Adapter(), config.channels["telegram"])
        self.assertEqual(1, store.calls)
        self.assertEqual(first.raw["normalized_media"]["artifact"], second.raw["normalized_media"]["artifact"])

    async def test_adapter_drops_forged_internal_artifact_metadata(self) -> None:
        adapter = TelegramAdapter(dry_run=True)
        binding = ChannelBinding("telegram", "bot", "secret")
        message = adapter.parse(
            "acme",
            binding,
            {
                "normalized_media": {"artifact": {"tenant_id": "other"}},
                "_artifact_materialized": True,
                "message": {"message_id": 1, "from": {"id": 2}, "chat": {"id": 2}, "text": "hello"},
            },
            "trace",
        )
        self.assertNotIn("normalized_media", message.raw)
        self.assertNotIn("_artifact_materialized", message.raw)

    async def test_projection_coordinator_rejects_old_versions(self) -> None:
        class Session:
            async def events(self, tenant_id, session_id):
                from trpc_service.tenant import SessionEvent
                return [
                    SessionEvent(tenant_id, session_id, 1, "user_message", {"text": "hello"}, "trace"),
                    SessionEvent(tenant_id, session_id, 3, "user_message", {"text": "future"}, "trace"),
                ]

        router = StorageRouter()
        session = Session()
        router.register("s", session, kind="session")
        router.register("m", object(), kind="memory")
        router.register("a", object(), kind="audit")
        config = TenantConfig("acme", "Acme", {"default": AgentApp("default", "a")}, {}, storage=StorageProfile(session="s", memory="m", audit="a"))
        coordinator = ProjectionCoordinator(router)
        first = await coordinator.project(config, "acme", "s1", 2)
        stored = await coordinator._summary_stores[id(session)].get_summary("acme", "s1")
        second = await coordinator.project(config, "acme", "s1", 1)
        self.assertTrue(first.summary_updated)
        self.assertNotIn("future", stored.content)
        self.assertFalse(second.summary_updated)

    async def test_knowledge_index_and_recall_are_collection_scoped(self) -> None:
        class Vector:
            def __init__(self) -> None:
                self.upserts = []
                self.searches = []

            async def upsert(self, tenant_id, item_id, content, embedding, source_version, collection="default"):
                self.upserts.append((tenant_id, item_id, content, embedding, source_version, collection))

            async def search(self, tenant_id, embedding, limit=10, collections=None):
                self.searches.append((tenant_id, embedding, limit, collections))
                return [VectorMatch("faq:item-1", "tenant answer", 0.9, 1)]

        async def embed(value):
            self.assertTrue(value)
            return [1.0]

        router = StorageRouter()
        vector = Vector()
        router.register("s", object(), kind="session")
        router.register("m", object(), kind="memory")
        router.register("a", object(), kind="audit")
        router.register("v", vector, kind="vector")
        config = TenantConfig(
            "acme",
            "Acme",
            {"default": AgentApp("default", "agent", knowledge_collections=frozenset({"faq"}))},
            {},
            storage=StorageProfile(session="s", memory="m", audit="a", vector="v"),
        )
        coordinator = ProjectionCoordinator(router, embed=embed)

        await coordinator.index_knowledge(config, "acme", "faq", "item-1", "tenant answer", 1)
        values = await coordinator.recall(config, "acme", "question", frozenset({"faq"}))

        self.assertEqual("faq:item-1", vector.upserts[0][1])
        self.assertEqual("faq", vector.upserts[0][-1])
        self.assertEqual(frozenset({"faq"}), vector.searches[0][-1])
        self.assertEqual(["tenant answer"], values)

    async def test_dangerous_function_tool_requires_one_time_confirmation(self) -> None:
        calls = []

        async def delete_ticket(ticket_id: str):
            calls.append(ticket_id)
            return {"deleted": ticket_id}

        config = TenantConfig(
            "acme", "Acme", {"default": AgentApp("default", "agent", tools=frozenset({"delete_ticket"}))}, {},
            policy=__import__("trpc_service.tenant", fromlist=["TenantPolicy"]).TenantPolicy(
                allowed_tools=frozenset({"delete_ticket"}), dangerous_tools=frozenset({"delete_ticket"})
            ),
        )
        wrapped = ToolCatalog(
            {"delete_ticket": ToolDefinition("delete_ticket", delete_ticket, idempotent=False)}
        ).build(config, config.apps["default"], b"k" * 32)[0]
        declaration = wrapped._get_declaration()
        self.assertIn("confirmation_token", declaration.parameters.properties)
        runtime = GovernedToolRuntime(config, b"k" * 32)
        message = InboundMessage("acme", "telegram", "bot", "m1", "u1", "c1", "direct", "x", session_id="s")

        async def run(turn, args):
            with active_tool_turn(turn):
                return await runtime.execute(
                    "delete_ticket", args, lambda values: delete_ticket(**values), idempotent=False
                )

        first = await run(message, {"ticket_id": "T1"})
        self.assertEqual("confirmation_required", first["error"])
        automatic = await run(message, {"ticket_id": "T1", "confirmation_token": first["confirmation_token"]})
        self.assertEqual("confirmation_token_must_be_supplied_by_user", automatic["error"])
        confirmed_message = replace(message, external_message_id="m2", text=f"确认 {first['confirmation_token']}")
        second = await run(
            confirmed_message,
            {"ticket_id": "T1", "confirmation_token": first["confirmation_token"]},
        )
        self.assertEqual({"deleted": "T1"}, second)
        self.assertEqual(["T1"], calls)


class AdminApiTest(unittest.TestCase):
    def test_local_demo_never_uses_external_credentials(self) -> None:
        values = {
            "TRPC_AGENT_API_KEY": "real-key-placeholder",
            "TRPC_AGENT_MODEL_NAME": "external-model",
            "TRPC_SERVICE_TELEGRAM_BOT_TOKEN_REF": "env://TELEGRAM_BOT_TOKEN",
            "TRPC_SERVICE_WECOM_SECRET_REF": "env://WECOM_APP_SECRET",
            "TRPC_SERVICE_FEISHU_APP_ID": "cli-app",
            "TRPC_SERVICE_FEISHU_APP_SECRET_REF": "env://FEISHU_APP_SECRET",
            "TRPC_SERVICE_FEISHU_CONNECTION_MODE": "websocket",
            "TRPC_SERVICE_IM_DRY_RUN": "false",
        }
        with patch.dict(os.environ, values, clear=True):
            runtime = build_demo_runtime(ServiceSettings(), local_demo=True)

        self.assertIsInstance(runtime.service.executor, EchoExecutor)
        self.assertTrue(runtime.service.dispatcher.adapters["telegram"].dry_run)
        self.assertTrue(runtime.service.dispatcher.adapters["wecom"].dry_run)
        self.assertNotIn("feishu", runtime.registry.get("acme").channels)

    def test_production_admin_role_only_requires_control_database(self) -> None:
        environment = {
            "TRPC_SERVICE_ENV": "production",
            "TRPC_SERVICE_BACKEND": "postgres-redis",
            "TRPC_SERVICE_ROLE": "admin",
            "TRPC_SERVICE_CONTROL_DATABASE_URL": "postgresql://control",
            "TRPC_SERVICE_ADMIN_TOKEN_REF": "env://ADMIN_TOKEN",
            "ADMIN_TOKEN": "secret",
            "TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook",
        }
        with patch.dict(os.environ, environment, clear=True):
            runtime = build_demo_runtime()

        self.assertEqual("admin", runtime.settings.role)
        self.assertIsNotNone(runtime.repository)
        self.assertIsNone(runtime.settings.database_url)
        self.assertIsNone(runtime.settings.redis_url)

    def test_postgres_redis_runtime_registers_selectable_redis_state(self) -> None:
        settings = {
            "TRPC_SERVICE_BACKEND": "postgres-redis",
            "TRPC_SERVICE_ROLE": "gateway",
            "TRPC_SERVICE_DATABASE_URL": "postgresql://runtime",
            "TRPC_SERVICE_REDIS_URL": "redis://queue",
            "TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook",
        }
        with patch.dict(os.environ, settings, clear=True):
            runtime = build_demo_runtime()

        self.assertIsInstance(runtime.storage_router.get("redis", kind="session"), RedisStateStore)
        self.assertIs(
            runtime.storage_router.get("redis", kind="session"),
            runtime.storage_router.get("redis", kind="memory"),
        )

    def test_admin_crud_uses_role_and_etag(self) -> None:
        with patch.dict(os.environ, {"TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook"}, clear=True):
            runtime = build_demo_runtime()
        runtime.settings = replace(
            runtime.settings,
            admin_token="admin-secret",
            admin_viewer_token="viewer-secret",
        )
        headers = {"x-admin-token": "admin-secret"}

        with TestClient(create_app(runtime)) as client:
            current = client.get("/admin/tenants/acme", headers=headers)
            self.assertEqual(200, current.status_code)
            tenant = current.json()
            tenant["name"] = "Acme Updated"
            updated = client.put(
                "/admin/tenants/acme", headers={**headers, "if-match": current.headers["etag"]}, json=tenant
            )
            self.assertEqual(200, updated.status_code)
            self.assertEqual("Acme Updated", updated.json()["name"])
            conflict = client.put(
                "/admin/tenants/acme", headers={**headers, "if-match": current.headers["etag"]}, json=tenant
            )
            self.assertEqual(412, conflict.status_code)
            viewer_delete = client.delete(
                "/admin/tenants/acme",
                headers={
                    "x-admin-token": "viewer-secret",
                    "x-admin-role": "admin",
                    "if-match": updated.headers["etag"],
                },
            )
            self.assertEqual(403, viewer_delete.status_code)

    def test_readiness_returns_503_when_dependency_is_degraded(self) -> None:
        with patch.dict(os.environ, {"TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook"}, clear=True):
            runtime = build_demo_runtime()

        async def degraded():
            return {"status": "degraded", "dependency_error": "ConnectionError"}

        runtime.ready = degraded  # type: ignore[method-assign]
        with TestClient(create_app(runtime)) as client:
            response = client.get("/health/ready")

        self.assertEqual(503, response.status_code)
        self.assertEqual("degraded", response.json()["status"])

    def test_metrics_endpoint_exposes_prometheus_registry(self) -> None:
        with patch.dict(os.environ, {"TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook"}, clear=True):
            runtime = build_demo_runtime()
        with TestClient(create_app(runtime)) as client:
            response = client.get("/metrics")

        self.assertEqual(200, response.status_code)
        self.assertIn("trpc_inbound_messages_total", response.text)

    def test_production_worker_rejects_echo_and_dry_run(self) -> None:
        base = {
            "TRPC_SERVICE_ENV": "production",
            "TRPC_SERVICE_BACKEND": "postgres-redis",
            "TRPC_SERVICE_ROLE": "worker",
            "TRPC_SERVICE_DATABASE_URL": "postgresql://runtime",
            "TRPC_SERVICE_REDIS_URL": "redis://queue",
            "TRPC_SERVICE_SESSION_HMAC_KEY": "x" * 32,
            "TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook",
        }
        with patch.dict(os.environ, base, clear=True), self.assertRaisesRegex(ValueError, "requires TRPC_AGENT"):
            build_demo_runtime()

        configured = {
            **base,
            "TRPC_AGENT_API_KEY": "key",
            "TRPC_AGENT_MODEL_NAME": "model",
            "TRPC_SERVICE_IM_DRY_RUN": "true",
        }
        with patch.dict(os.environ, configured, clear=True), self.assertRaisesRegex(ValueError, "DRY_RUN"):
            build_demo_runtime()

    def test_production_admin_rejects_literal_secret_and_unknown_storage(self) -> None:
        with patch.dict(os.environ, {"TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook"}, clear=True):
            runtime = build_demo_runtime()
        runtime.settings = replace(runtime.settings, environment="production", admin_token="admin-secret")
        headers = {"x-admin-token": "admin-secret"}
        payload = {
            "tenant_id": "new",
            "name": "New",
            "apps": {"default": {"app_id": "default", "name": "agent"}},
            "channels": {
                "telegram": {
                    "channel": "telegram",
                    "account_id": "bot",
                    "verify_token": "literal://unsafe",
                }
            },
        }
        with TestClient(create_app(runtime)) as client:
            literal = client.post("/admin/tenants", headers=headers, json=payload)
            self.assertEqual(422, literal.status_code)
            payload["channels"]["telegram"]["verify_token"] = "env://SAFE_TOKEN"
            payload["storage"] = {"session": "missing", "memory": "inmemory", "audit": "inmemory"}
            missing = client.post("/admin/tenants", headers=headers, json=payload)
            self.assertEqual(422, missing.status_code)

    def test_gateway_role_disables_admin_api(self) -> None:
        with patch.dict(os.environ, {"TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook"}, clear=True):
            runtime = build_demo_runtime()
        runtime.settings = replace(runtime.settings, role="gateway")
        with TestClient(create_app(runtime)) as client:
            self.assertEqual(503, client.get("/admin/tenants").status_code)

    def test_audit_query_honors_tenant_export_policy(self) -> None:
        with patch.dict(os.environ, {"TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook"}, clear=True):
            runtime = build_demo_runtime()
        current = runtime.registry.get("acme")
        headers: dict[str, str] = {}
        with TestClient(create_app(runtime)) as client:
            denied = client.get("/admin/audit/acme", headers=headers)
            self.assertEqual(403, denied.status_code)

            runtime.registry.activate(replace(current, audit=replace(current.audit, allow_export=True)))
            allowed = client.get("/admin/audit/acme", headers=headers)
            self.assertEqual(200, allowed.status_code)

    def test_webhook_returns_429_after_tenant_rate_limit(self) -> None:
        with patch.dict(os.environ, {"TRPC_SERVICE_FEISHU_CONNECTION_MODE": "webhook"}, clear=True):
            runtime = build_demo_runtime()
        current = runtime.registry.get("acme")
        runtime.registry.activate(replace(current, policy=replace(current.policy, requests_per_minute=1)))
        headers = {"X-Telegram-Bot-Api-Secret-Token": "acme-telegram-secret"}

        def payload(identifier: int) -> dict[str, object]:
            return {
                "account_id": "acme-telegram",
                "update_id": identifier,
                "message": {
                    "message_id": identifier,
                    "from": {"id": 42},
                    "chat": {"id": 42, "type": "private"},
                    "text": "hello",
                },
            }

        with TestClient(create_app(runtime)) as client:
            self.assertEqual(200, client.post("/webhook/acme/telegram", headers=headers, json=payload(1)).status_code)
            duplicate = client.post("/webhook/acme/telegram", headers=headers, json=payload(1))
            self.assertEqual(200, duplicate.status_code)
            self.assertTrue(duplicate.json()["duplicate"])
            limited = client.post("/webhook/acme/telegram", headers=headers, json=payload(2))
            self.assertEqual(429, limited.status_code)
            self.assertEqual("60", limited.headers["retry-after"])


if __name__ == "__main__":
    unittest.main()
