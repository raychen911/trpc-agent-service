from __future__ import annotations

from types import SimpleNamespace

import pytest

from trpc_service.agent.bridge import _invoke_factory
from trpc_service.channels.base import Attachment, ChannelVerificationError, OutboundMessage
from trpc_service.channels.feishu import FeishuAdapter, FeishuVerificationError
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.storage.manager import TenantStorageManager
from trpc_service.storage.object_store import RedisObjectStore, S3ObjectStore
from trpc_service.telemetry import metrics
from trpc_service.telemetry.tracing import TraceRecorder
from trpc_service.tenant.models import TenantContext, default_demo_config


class SecretMap:
    def __init__(self, values):
        self.values = values

    def resolve(self, reference):
        return self.values[reference]


def channel_binding(channel, **config):
    from trpc_service.tenant.models import ChannelBinding

    token_ref = config.pop("token_ref", None)
    return ChannelBinding("tenant", f"{channel}:account", channel, "account", "app", token_ref=token_ref, config=config)


def test_storage_manager_applies_cutover_and_dual_write_profiles(monkeypatch, tmp_path):
    import trpc_service.storage.manager as manager_module

    created = []

    class Bundle:
        def close(self):
            return None

    def fake_create_storage(profile, tenant_dir):
        created.append(("single", profile, tenant_dir))
        return Bundle()

    def fake_create_mirrored_storage(primary, secondary, tenant_dir):
        created.append(("mirror", primary, secondary, tenant_dir))
        return Bundle()

    monkeypatch.setattr(manager_module, "create_storage", fake_create_storage)
    monkeypatch.setattr(manager_module, "create_mirrored_storage", fake_create_mirrored_storage)
    config = default_demo_config()
    config.tenant_id = "migration-tenant"
    state = tmp_path / "migration.json"
    monkeypatch.setenv("MIGRATION_STATE_FILE", str(state))

    state.write_text(
        '{"tenant_id":"migration-tenant","phase":"cutover","target_backend":"sqlite"}',
        encoding="utf-8",
    )
    manager = TenantStorageManager(tmp_path / "data")
    first = manager.get(config)
    assert first is manager.get(config)
    assert created[0][0] == "single"
    assert created[0][1].session_backend == "sqlite"

    manager.close()
    created.clear()
    state.write_text(
        '{"tenant_id":"migration-tenant","phase":"dual_write","target_backend":"redis"}',
        encoding="utf-8",
    )
    monkeypatch.setenv("MIGRATION_DUAL_WRITE_KNOWLEDGE_BACKEND", "redis")
    manager = TenantStorageManager(tmp_path / "data")
    manager.get(config)
    assert created[0][0] == "mirror"
    assert created[0][2].session_backend == "redis"
    manager.close()


def test_object_store_constructors_and_empty_listing_edges(monkeypatch):
    class RedisClient:
        def __init__(self):
            self.values = {}

        def hset(self, key, mapping):
            self.values[key] = mapping

        def hget(self, key, field):
            value = self.values.get(key, {}).get(field)
            if field == "content_type" and value is not None:
                return value.encode()
            return value

        def scan_iter(self, pattern):
            del pattern
            return iter(self.values)

    redis_client = RedisClient()
    monkeypatch.setattr("redis.Redis.from_url", lambda url: redis_client)
    store = RedisObjectStore("redis://example", prefix="test")
    stored = store.put("tenant", b"data", "text/plain")
    assert store.get("tenant", stored.object_id) == b"data"
    assert store.list_by_tenant("tenant")[0].content_type == "text/plain"

    class S3Client:
        def put_object(self, **kwargs):
            self.put_kwargs = kwargs

        def get_object(self, **kwargs):
            del kwargs
            return {"Body": SimpleNamespace(read=lambda: b"data")}

        def list_objects_v2(self, **kwargs):
            del kwargs
            return {}

    s3_client = S3Client()
    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: s3_client)
    s3 = S3ObjectStore("https://s3.example", "bucket", region="", prefix="/objects/")
    assert s3._key("tenant", "object") == "objects/tenant/object"
    assert s3.list_by_tenant("tenant") == []
    assert s3.close() is None


def test_metrics_noop_guards_and_trace_error_lifecycle(monkeypatch):
    names = (
        "REQUESTS",
        "TOKENS",
        "MODEL_LATENCY",
        "TOOL_CALLS",
        "TOOL_LATENCY",
        "IM_DELIVERIES",
        "COST",
        "SESSION_BACKEND_LATENCY",
        "ERRORS",
    )
    for name in names:
        monkeypatch.setattr(metrics, name, None)
    metrics.observe_request("t", "web", "ok")
    metrics.observe_tokens("t", 1)
    metrics.observe_model_latency("t", 0.1)
    metrics.observe_tool("t", "tool", "ok")
    metrics.observe_tool_latency("t", "tool", 0.1)
    metrics.observe_delivery("web", "ok", "t")
    metrics.observe_cost("t", 0)
    metrics.observe_session_backend_latency("t", "load", 0.1)
    metrics.observe_error("t", "web", "error")

    monkeypatch.setenv("TRACE_MAX_RETAINED_SPANS", "invalid")
    recorder = TraceRecorder()
    context = TenantContext("tenant", "app", 1, "trace", "session", "web", "user")
    with pytest.raises(ValueError, match="bad"):
        with recorder.span("storage.load", context, {"api_key": "token=secret"}) as span:
            assert span.attributes["api_key"] == "token=[secret-redacted]"
            raise ValueError("bad")
    assert recorder.spans[-1].error == "ValueError: bad"
    recorder._provider = SimpleNamespace(shutdown=lambda: setattr(recorder, "flushed", True))
    recorder.close()
    assert recorder.flushed


def test_bridge_factory_signature_fallbacks_and_validation():
    marker = object()
    assert _invoke_factory(lambda **kwargs: kwargs["storage"], marker, None, None, {}) is marker

    class CallableWithoutSignature:
        __signature__ = "invalid"

        def __call__(self, **kwargs):
            return kwargs["settings"]

    assert _invoke_factory(CallableWithoutSignature(), marker, None, None, {"mode": "test"}) == {"mode": "test"}

    with pytest.raises(TypeError):
        _invoke_factory(lambda required: required, marker, None, None, {})


def test_feishu_sdk_paths_and_parser_rejections(monkeypatch):
    import trpc_service.channels.feishu as module

    binding = channel_binding("feishu", app_id="app", token_ref="verify", app_secret_ref="secret")
    adapter = FeishuAdapter(SecretMap({"verify": "verify", "secret": "secret"}))

    class Handler:
        def do(self, headers, body):
            assert headers["X-Test"] == "ok" and body == "{}"

    class HandlerBuilder:
        @classmethod
        def builder(cls, token, encrypt_key):
            assert token == "verify" and encrypt_key == ""
            return cls()

        def build(self):
            return Handler()

    monkeypatch.setattr(module, "LARK_SDK_AVAILABLE", True)
    monkeypatch.setattr(module, "lark", SimpleNamespace(EventDispatcherHandler=HandlerBuilder))
    adapter.verify_callback({"_raw_body": "{}", "_headers": {"X-Test": "ok"}}, binding)

    monkeypatch.setattr(adapter, "_send_sdk", lambda *_args: (_ for _ in ()).throw(RuntimeError("sdk down")))
    monkeypatch.setattr(adapter, "_tenant_access_token", lambda *_args, **_kwargs: "token")
    monkeypatch.setattr(adapter, "_send_text", lambda *_args, **_kwargs: {"code": 2, "msg": "rejected"})
    failed = adapter.send(OutboundMessage("feishu", "account", "session", "user", "hello"), binding)
    assert not failed.ok and failed.metadata["code"] == 2
    monkeypatch.setattr(adapter, "_send_sdk", FeishuAdapter._send_sdk.__get__(adapter))

    class RequestBuilder:
        def __init__(self):
            self.values = {}

        @classmethod
        def builder(cls):
            return cls()

        def receive_id(self, value):
            self.values["receive_id"] = value
            return self

        def msg_type(self, value):
            self.values["msg_type"] = value
            return self

        def content(self, value):
            self.values["content"] = value
            return self

        def uuid(self, value):
            self.values["uuid"] = value
            return self

        def message_id(self, value):
            self.values["message_id"] = value
            return self

        def file_key(self, value):
            self.values["file_key"] = value
            return self

        def type(self, value):
            self.values["type"] = value
            return self

        def request_body(self, value):
            self.values["request_body"] = value
            return self

        def receive_id_type(self, value):
            self.values["receive_id_type"] = value
            return self

        def build(self):
            return self.values

    class Response:
        code = 99
        msg = "rejected"
        data = None
        file = None

        def __init__(self, ok, *, file=None):
            self.ok = ok
            self.file = file

        def success(self):
            return self.ok

    class MessageApi:
        def __init__(self, response):
            self.response = response

        def create(self, request):
            self.request = request
            return self.response

    class ResourceApi:
        def __init__(self, response):
            self.response = response

        def get(self, request):
            self.request = request
            return self.response

    monkeypatch.setattr(module, "CreateMessageRequestBody", RequestBuilder)
    monkeypatch.setattr(module, "CreateMessageRequest", RequestBuilder)
    monkeypatch.setattr(module, "GetMessageResourceRequest", RequestBuilder)
    client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(
                message=MessageApi(Response(True)),
                message_resource=ResourceApi(Response(True, file=b"file")),
            )
        )
    )
    monkeypatch.setattr(adapter, "_get_sdk_client", lambda _binding: client)
    sent = adapter._send_sdk(OutboundMessage("feishu", "account", "session", "user", "hello"), binding)
    assert sent.ok and sent.response_ref == "feishu:session"
    assert adapter._download_media_sdk(binding, "message", "resource", "file", 100) == (b"file", None, None)
    client.im.v1.message = MessageApi(Response(False))
    rejected = adapter._send_sdk(OutboundMessage("feishu", "account", "session", "user", "hello"), binding)
    assert not rejected.ok
    client.im.v1.message_resource = ResourceApi(Response(False))
    with pytest.raises(ValueError, match="media download"):
        adapter._download_media_sdk(binding, "message", "resource", "file", 100)

    valid = {
        "header": {"app_id": "app"},
        "event": {
            "sender": {"sender_id": {"open_id": "user"}},
            "message": {
                "message_id": "message",
                "chat_id": "chat",
                "message_type": "text",
                "content": '{"text":"hello"}',
            },
        },
    }
    invalid_payloads = (
        {},
        {"header": {"app_id": "wrong"}},
        {"header": {"app_id": "app"}, "event": None},
        {"header": {"app_id": "app"}, "event": {"sender": None, "message": {}}},
        {"header": {"app_id": "app"}, "event": {"sender": {"sender_id": None}, "message": {}}},
        {"header": {"app_id": "app"}, "event": {"sender": {"sender_id": {}}, "message": {}}},
        {"header": {"app_id": "app"}, "event": {"sender": {"sender_id": {"open_id": "u"}}, "message": {"content": "{}"}}},
        {**valid, "event": {**valid["event"], "message": {**valid["event"]["message"], "content": "{"}}},
        {**valid, "event": {**valid["event"], "message": {**valid["event"]["message"], "content": []}}},
    )
    for payload in invalid_payloads:
        with pytest.raises(FeishuVerificationError):
            adapter.parse_event(payload, binding)


def test_telegram_http_and_verification_failure_paths(monkeypatch, tmp_path):
    import trpc_service.channels.telegram as module

    adapter = TelegramAdapter()
    binding = channel_binding("telegram", token_ref="token", sdk_enabled=False)
    monkeypatch.setattr("trpc_service.channels.telegram.SecretManager.resolve", lambda *_args: "bot-token")

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return self.payload

    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: Response(b'{"ok":false,"description":"bad"}'))
    failed = adapter.send(OutboundMessage("telegram", "account", "session", "user", "hello"), binding)
    assert not failed.ok

    image = tmp_path / "image.png"
    image.write_bytes(b"image")
    monkeypatch.setattr(module, "post_multipart_json", lambda *_args, **_kwargs: {"ok": False, "description": "bad"})
    failed_media = adapter.send(
        OutboundMessage("telegram", "account", "session", "user", "hello", attachments=[Attachment("image", url=str(image))]),
        binding,
    )
    assert not failed_media.ok

    secret_binding = channel_binding("telegram", secret_ref="secret")
    secret_binding.secret_ref = "secret"
    with pytest.raises(ChannelVerificationError, match="invalid"):
        adapter.verify_callback({"_callback_secret_token": 1}, secret_binding)
    monkeypatch.setattr(
        "trpc_service.channels.telegram.SecretManager.resolve",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("secret down")),
    )
    with pytest.raises(ChannelVerificationError, match="unavailable"):
        adapter.verify_callback({}, secret_binding)
