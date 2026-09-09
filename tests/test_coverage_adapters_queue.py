from __future__ import annotations

import asyncio
import base64
import builtins
import hashlib
import json
import sqlite3
import time
from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.error import HTTPError, URLError

import pytest
from cryptography.hazmat.primitives import padding as crypto_padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.agent.bridge import RuntimeBridgeSpec, _invoke_factory, build_runtime_worker, build_runtime_workers
from trpc_service.agent.trpc_runtime import (
    TrpcAgentWorker,
    _ApprovalRequired,
    _close_async,
    _max_sdk_llm_calls,
    _max_sdk_tool_calls,
    _PlatformSdkTool,
    _render_conversation,
    _resolve_api_key,
    _run_coroutine_sync,
    _signature_proxy,
)
from trpc_service.channels.base import Attachment, ChannelVerificationError, OutboundMessage
from trpc_service.channels.feishu import FeishuAdapter, FeishuVerificationError
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.wechat_customer_service import WeChatCustomerServiceAdapter
from trpc_service.channels.wechat_official_account import WeChatOfficialAccountAdapter
from trpc_service.channels.wecom import WeComAdapter
from trpc_service.channels.wecom_ai_bot import (
    WeComAIBotAdapter,
    WeComAIBotConnector,
    _content_disposition_filename,
    _decode_aes_key,
    _frame_timestamp,
    _response_code,
    _safe_filename,
    _validate_shape,
    _validate_url,
    parse_wecom_ai_bot_frame,
    sdk_client_factory,
)
from trpc_service.gateway.worker_queue import DurableWebhookQueue, WorkerQueue, _bounded_webhook_result
from trpc_service.migrate import (
    _checksums,
    _counts,
    _dt,
    _json,
    _migration_manifest,
    _profile,
    _snapshot_fingerprint,
    cutover_plan,
    export_tenant,
    import_tenant,
    verify_tenant,
)
from trpc_service.policy.quota import QuotaEnforcer, QuotaExceeded
from trpc_service.storage.base import AuditRecord, MemoryItem, SessionEvent, Summary
from trpc_service.storage.factory import create_storage
from trpc_service.storage.vector_store import KnowledgeChunk
from trpc_service.tenant.models import (
    ChannelBinding,
    RunRequest,
    StorageProfile,
    TenantContext,
    UserInput,
    default_demo_config,
)
from trpc_service.tool.runtime import ToolRegistry, ToolResult, _handler_schema, _json_type


def bind(channel: str, **config) -> ChannelBinding:
    token_ref = config.pop("token_ref", None)
    secret_ref = config.pop("secret_ref", None)
    return ChannelBinding(
        "tenant-a", f"{channel}:account", channel, "account", "app",
        token_ref=token_ref, secret_ref=secret_ref, config=config,
    )


class SecretMap:
    def __init__(self, values=None):
        self.values = values or {}

    def resolve(self, reference):
        if reference not in self.values:
            raise KeyError(reference)
        return self.values[reference]


def message(channel="feishu", *, attachments=None, group_id=None):
    return OutboundMessage(channel, "account", "session-1", "user-1", "hello", group_id=group_id, attachments=attachments or [])


def test_feishu_protocol_helpers_and_content_matrix():
    adapter = FeishuAdapter(SecretMap({"token": "verify", "key": "encrypt", "secret": "app-secret"}))
    binding = bind("feishu", app_id="app", verification_token_ref="token")
    callback = {
        "header": {"app_id": "app", "event_type": "im.message.receive_v1", "event_id": "evt"},
        "event": {
            "sender": {"sender_id": {"open_id": "user"}},
            "message": {
                "message_id": "msg", "chat_id": "chat", "chat_type": "group",
                "message_type": "text", "content": json.dumps({"text": "@bot hello"}),
                "mentions": [{"key": "@bot"}],
            },
        },
    }
    inbound = adapter.parse_event(callback, binding)
    assert inbound.text == "hello" and inbound.group_id == "chat"
    assert not adapter.is_noop(callback, binding)
    assert adapter.webhook_ack({"type": "url_verification", "challenge": "c"}, binding) == {"challenge": "c"}
    assert adapter.webhook_ack(callback, binding) == {"msg": "success"}
    assert adapter.is_noop({"type": "url_verification", "challenge": "c"}, binding)
    assert adapter.is_noop({"event": {}}, binding)
    assert adapter.is_noop({"type": "other", "event": {"sender": {"sender_type": "bot"}}}, binding)
    assert FeishuAdapter._normalize_content("post", {"title": "T", "content": [[{"tag": "text", "text": "A"}]]}, None, "m")[0] == "T\nA"
    assert FeishuAdapter._normalize_content("image", {"image_key": "k"}, None, "m")[1][0].kind == "image"
    assert FeishuAdapter._normalize_content("file", {"file_key": "k", "file_name": "x"}, None, "m")[1][0].name == "x"
    assert FeishuAdapter._normalize_content("unknown", {}, None, "m") == (None, [])
    assert FeishuAdapter._flatten_post({"content": [{"tag": "a", "text": "link"}]}) == "link"
    assert FeishuAdapter._flatten_post({"content": []}) is None
    assert adapter._event_type({"header": {"event_type": "x"}}) == "x"
    assert adapter._header_value({"header": {"x": 2}}, "x") == "2"
    assert adapter._header_value({"x": 3}, "x") == "3"
    assert adapter._header({"X-Test": 1}, "x-test") == "1"
    assert adapter._response_code({"errcode": "0"}) == 0
    assert adapter._response_code({"code": "bad"}) == -1


def test_feishu_callback_verification_and_encryption(monkeypatch):
    secrets = SecretMap({"token": "verify", "key": "encrypt", "secret": "app-secret"})
    adapter = FeishuAdapter(secrets)
    plain = {"type": "url_verification", "token": "verify", "challenge": "c"}
    binding = bind("feishu", app_id="app", token_ref="token")
    adapter.verify_callback(plain, binding)
    assert plain["_feishu_payload"] == {"type": "url_verification", "token": "verify", "challenge": "c"}
    with pytest.raises(FeishuVerificationError, match="token"):
        adapter.verify_callback({**plain, "token": "bad"}, binding)
    with pytest.raises(FeishuVerificationError, match="challenge"):
        adapter.verify_callback({"type": "url_verification", "token": "verify"}, binding)
    encrypted_binding = bind("feishu", app_id="app", token_ref="token", encrypt_key_ref="key")
    encryptor = crypto_padding.PKCS7(128).padder()
    padded = encryptor.update(json.dumps(plain).encode()) + encryptor.finalize()
    iv = b"0123456789abcdef"
    cipher = Cipher(algorithms.AES(hashlib.sha256(b"encrypt").digest()), modes.CBC(iv)).encryptor()
    encrypted_outer = {"encrypt": base64.b64encode(iv + cipher.update(padded) + cipher.finalize()).decode()}
    decoded, verified, encrypted = adapter._decode_payload({"_raw_body": json.dumps(encrypted_outer)}, encrypted_binding)
    assert decoded == plain and not verified and encrypted
    with pytest.raises(FeishuVerificationError, match="invalid"):
        adapter._decode_payload({"_raw_body": b"{"}, binding)

    import trpc_service.channels.feishu as module
    body = json.dumps(plain, separators=(",", ":"), ensure_ascii=False).encode()
    timestamp, nonce = str(int(time.time())), "nonce"
    signature = hashlib.sha256(timestamp.encode() + nonce.encode() + b"encrypt" + body).hexdigest()
    decoded, verified, encrypted = adapter._decode_payload(
        {"_raw_body": body, "_headers": {"x-lark-request-timestamp": timestamp, "X-Lark-Request-Nonce": nonce, "X-Lark-Signature": signature}},
        encrypted_binding,
    )
    assert decoded == plain and verified and not encrypted
    with pytest.raises(FeishuVerificationError, match="invalid"):
        adapter._verify_signature(body, {"X-Lark-Request-Timestamp": "1", "X-Lark-Request-Nonce": "n", "X-Lark-Signature": "x"}, "encrypt")
    with pytest.raises(FeishuVerificationError, match="invalid"):
        adapter._verify_signature(body, {"X-Lark-Request-Timestamp": timestamp, "X-Lark-Request-Nonce": nonce, "X-Lark-Signature": "x"}, "encrypt")
    with pytest.raises(FeishuVerificationError, match="invalid"):
        adapter._decode_payload({"_raw_body": json.dumps({"encrypt": "bad"})}, binding)
    assert module.FeishuAdapter._api_root(adapter, binding).startswith("https://")


def test_feishu_http_token_send_and_download_paths(monkeypatch):
    adapter = FeishuAdapter(SecretMap({"secret": "app-secret", "token": "verify"}))
    binding = bind("feishu", app_id="app", app_secret_ref="secret", api_base_url="https://api.example")
    requests = []

    def request(method, url, **kwargs):
        requests.append((method, url, kwargs))
        if url.endswith("/internal"):
            return {"code": 0, "tenant_access_token": "token", "expire": 3600}
        if "resources" in url:
            return {"code": 0, "_bytes": b"file", "_content_type": "image/png", "_filename": "x.png"}
        return {"code": 0, "data": {"message_id": "provider-id"}}

    monkeypatch.setattr(adapter, "_request", request)
    assert adapter._tenant_access_token(binding, force_refresh=False) == "token"
    assert adapter._tenant_access_token(binding, force_refresh=False) == "token"
    sent = adapter.send(message(), binding)
    assert sent.ok and sent.response_ref == "feishu:provider-id"
    data = adapter.download_media(binding, "m/1", "key/1", resource_type="image")
    assert data == (b"file", "image/png", "x.png")
    assert any("receive_id_type=open_id" in url for _, url, _ in requests)

    responses = iter([{"code": 99991663}, {"code": 0, "tenant_access_token": "fresh", "expire": "bad"}, {"code": 0, "data": {}}])
    monkeypatch.setattr(adapter, "_request", lambda *args, **kwargs: next(responses))
    result = adapter.send(message(), binding)
    assert result.ok and result.response_ref == "feishu:session-1"
    attachment = adapter.send(message(attachments=[Attachment("file")]), binding)
    assert not attachment.ok
    real_request = FeishuAdapter._request.__get__(adapter)

    class Response:
        headers = {"Content-Type": "application/json"}
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self, _limit): return b'{"code":0}'

    monkeypatch.setattr("trpc_service.channels.feishu.urlopen", lambda *a, **k: Response())
    assert real_request("POST", "https://api.example/test", body={"x": 1})["code"] == 0
    monkeypatch.setattr("trpc_service.channels.feishu.urlopen", lambda *a, **k: (_ for _ in ()).throw(URLError("down")))
    with pytest.raises(RuntimeError, match="transport"):
        real_request("GET", "https://api.example/test")
    error = HTTPError("https://api.example", 500, "bad", {}, None)
    monkeypatch.setattr("trpc_service.channels.feishu.urlopen", lambda *a, **k: (_ for _ in ()).throw(error))
    assert real_request("GET", "https://api.example/test")["code"] == 500


def test_wecom_and_customer_service_edges(monkeypatch, tmp_path):
    secrets = SecretMap({"token": "token", "access": "access", "url": "https://hook.example"})
    wecom = WeComAdapter()
    binding = bind("wecom", token_ref="token", webhook_url="https://hook.example", sdk_enabled=False)
    monkeypatch.setattr("trpc_service.channels.wecom.SecretManager.resolve", lambda _self, _ref: "token")
    expected = hashlib.sha1("".join(sorted(("token", "1", "2"))).encode()).hexdigest()
    wecom.verify_callback({"timestamp": "1", "nonce": "2", "signature": expected}, binding)
    with pytest.raises(ChannelVerificationError):
        wecom.verify_callback({"timestamp": "1", "nonce": "2", "signature": "bad"}, binding)
    parsed = wecom.parse_event({"raw_body": "<xml><MsgType>image</MsgType><FromUserName>u</FromUserName><MsgId>m</MsgId><MediaId>media</MediaId></xml>"}, binding)
    assert parsed.attachments[0].kind == "image"
    calls = []
    monkeypatch.setattr("trpc_service.channels.wecom._post_json", lambda url, payload: calls.append(payload) or {"errcode": 0, "msgid": "id"})
    assert wecom.send(message("wecom"), binding).ok
    image = tmp_path / "x.png"
    image.write_bytes(b"png")
    assert wecom.send(message("wecom", attachments=[Attachment("image", url=str(image))]), binding).ok
    assert not wecom.send(message("wecom", attachments=[Attachment("file")]), binding).ok

    customer = WeChatCustomerServiceAdapter()
    customer_binding = bind("wechat_customer_service", token_ref="token", api_base_url="https://api.example", sdk_enabled=False)
    customer.verify_callback({}, customer_binding)
    customer_event = customer.parse_event({"MsgType": "file", "MediaId": "m", "OpenId": "u", "MsgId": "id"}, customer_binding)
    assert customer_event.attachments[0].kind == "file"
    monkeypatch.setattr("trpc_service.channels.wechat_customer_service.SecretManager.resolve", lambda _self, _ref: "access")
    replies = iter([{"errcode": 0, "media_id": "uploaded"}, {"errcode": 0}])
    monkeypatch.setattr("trpc_service.channels.wechat_customer_service._wechat_json", lambda *a, **k: next(replies))
    monkeypatch.setattr(
        "trpc_service.channels.wechat_customer_service.post_multipart_json",
        lambda *a, **k: {"media_id": "uploaded"},
    )
    assert customer.send(message("wechat_customer_service", attachments=[Attachment("image", url=str(image))]), customer_binding).ok
    assert customer.send(message("wechat_customer_service"), customer_binding).ok


def test_wecom_ai_bot_parsing_and_safe_helpers():
    binding = bind("wecom_ai_bot", bot_id="bot")
    for body, text, kind in (
        ({"msgtype": "text", "from": {"userid": "u"}, "aibotid": "bot", "text": {"content": "@bot hi"}, "atuserlist": [{"userid": "bot"}]}, "hi", None),
        ({"msgtype": "voice", "from": {"userid": "u"}, "aibotid": "bot", "voice": {"content": "voice"}}, "voice", None),
        ({"msgtype": "image", "from": {"userid": "u"}, "aibotid": "bot", "image": {"url": "https://cdn.example/x", "aeskey": base64.b64encode(b"k" * 32).decode()}}, None, "image"),
        ({"msgtype": "mixed", "from": {"userid": "u"}, "aibotid": "bot", "mixed": {"msg_item": [{"msgtype": "text", "text": {"content": "one"}}, {"msgtype": "file", "file": {"url": "https://cdn.example/f", "aeskey": base64.b64encode(b"k" * 32).decode()}}, 1]}}, "one", "file"),
    ):
        inbound = parse_wecom_ai_bot_frame({"body": body}, binding)
        assert inbound.text == text
        if kind:
            assert inbound.attachments[0].kind == kind
    group = parse_wecom_ai_bot_frame({"body": {"from": {"userid": "u"}, "aibotid": "bot", "chattype": "group", "chatid": "g", "msgtype": "event"}}, binding)
    assert group.group_id == "g" and group.external_message_id.startswith("frame_")
    with pytest.raises(ValueError): parse_wecom_ai_bot_frame({"body": {"from": {}}}, binding)
    with pytest.raises(ValueError): parse_wecom_ai_bot_frame({"body": {"from": {"userid": "u"}, "aibotid": "other"}}, binding)
    with pytest.raises(ValueError): _validate_shape({"a": {"b": {"c": {"d": {"e": {"f": {"g": {"h": {"i": {"j": {"k": 1}}}}}}}}}}})
    with pytest.raises(ValueError): _validate_shape({str(i): i for i in range(257)})
    with pytest.raises(ValueError): _validate_url("http://example.com/x")
    with pytest.raises(ValueError): _validate_url("https://127.0.0.1/x")
    with pytest.raises(ValueError): _decode_aes_key("bad key")
    assert _decode_aes_key(base64.urlsafe_b64encode(b"k" * 32).rstrip(b"=").decode()) == b"k" * 32
    assert _safe_filename('"a\\b.txt"') == "b.txt"
    assert _safe_filename("bad\x01") is None
    assert _content_disposition_filename('attachment; filename="x.txt"') == "x.txt"
    assert _content_disposition_filename(None) is None
    assert _frame_timestamp("bad").tzinfo == UTC
    assert _response_code({"code": "bad"}) is None
    assert _response_code(SimpleNamespace(errcode=0)) == 0


class BotClient:
    is_connected = True

    def __init__(self, stop_event=None):
        self.handlers = {}
        self.sent = []
        self.disconnected = False
        self.stop_event = stop_event

    def on(self, name, handler): self.handlers[name] = handler
    async def connect_async(self):
        await self.handlers["message.text"]({"body": {"msgtype": "text", "from": {"userid": "u"}, "aibotid": "bot", "text": {"content": "hi"}}})
        await self.handlers["disconnected"]()
        if self.stop_event is not None:
            self.stop_event.set()
    def disconnect(self): self.disconnected = True
    async def send_message(self, target, body): self.sent.append((target, body)); return {"msgid": "m"}


def test_wecom_ai_bot_adapter_connector_and_media(monkeypatch):
    stop = asyncio.Event()
    client = BotClient(stop)
    connector = WeComAIBotConnector(SecretMap({"secret": "secret"}), client_factory=lambda *_: client, reconnect_delay_seconds=0)
    binding = bind("wecom_ai_bot", bot_id="bot", bot_secret_ref="secret")
    seen = []
    async def sink(inbound, _binding):
        seen.append(inbound)

    asyncio.run(connector.run(binding, sink, stop))
    assert seen[0].text == "hi" and client.disconnected
    result = WeComAIBotAdapter(connector=connector).send(message("wecom_ai_bot"), binding)
    assert not result.ok
    real_import = builtins.__import__

    def missing_wecom_sdk(name, *args, **kwargs):
        if name == "wecom_aibot_sdk":
            raise ImportError("simulated missing optional SDK")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_wecom_sdk)
    with pytest.raises(RuntimeError):
        sdk_client_factory("b", "s")
    with pytest.raises(ValueError): WeComAIBotAdapter().download_media(Attachment("image"))
    connector._clients[binding.binding_id] = client
    assert asyncio.run(connector.send(message("wecom_ai_bot"), binding))["msgid"] == "m"
    connector.stop(binding.binding_id)
    assert _bounded_webhook_result({"answer": "secret", "ok": True}) == {"ok": True, "answer_length": 6}


def test_wecom_ai_bot_connector_reconnects_after_client_failure():
    stop = asyncio.Event()
    client = BotClient(stop)
    attempts = []

    def factory(*args):
        attempts.append(args)
        if len(attempts) == 1:
            raise RuntimeError("initial connect failed")
        return client

    connector = WeComAIBotConnector(
        SecretMap({"secret": "secret"}),
        client_factory=factory,
        reconnect_delay_seconds=0,
    )
    binding = bind("wecom_ai_bot", bot_id="bot", bot_secret_ref="secret")
    asyncio.run(connector.run(binding, lambda *_: asyncio.sleep(0), stop))
    assert len(attempts) == 2
    assert client.disconnected


def test_wecom_ai_bot_media_download_decrypts_and_validates(monkeypatch):
    import trpc_service.channels.wecom_ai_bot as module

    key = b"k" * 32
    plain = b"media-content"
    pad = 16 - (len(plain) % 16)
    padded = plain + bytes([pad]) * pad
    cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    ciphertext = cipher.update(padded) + cipher.finalize()

    class Response:
        headers = {
            "Content-Length": str(len(ciphertext)),
            "Content-Type": "application/octet-stream",
            "Content-Disposition": "attachment; filename*=UTF-8''report%20one.bin",
        }

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self, _limit):
            return ciphertext

    monkeypatch.setattr(module, "urlopen", lambda *args, **kwargs: Response())
    connector = WeComAIBotConnector(max_media_bytes=64)
    attachment = Attachment(
        "file",
        metadata={
            "provider_url": "https://media.example.test/item",
            "aes_key": base64.urlsafe_b64encode(key).rstrip(b"=").decode(),
        },
    )
    assert connector.download_media(attachment) == (
        plain,
        "application/octet-stream",
        "report one.bin",
    )
    with pytest.raises(ValueError):
        connector.download_media(
            Attachment("file", metadata={"provider_url": "http://media.example.test/item", "aes_key": "bad"})
        )


class QueueRedis:
    def __init__(self):
        self.lists = defaultdict(list)
        self.hashes = defaultdict(dict)
        self.values = {}
        self.eval_result = None
        self.closed = False

    def rpush(self, key, value): self.lists[key].append(value)
    def lpush(self, key, value): self.lists[key].insert(0, value)
    def lrange(self, key, start, end): return self.lists[key][start:] if end == -1 else self.lists[key][start:end + 1]
    def lrem(self, key, _count, value):
        try: self.lists[key].remove(value); return 1
        except ValueError: return 0
    def hscan_iter(self, key): return iter(list(self.hashes[key].items()))
    def hset(self, key, field, value): self.hashes[key][field] = value
    def hget(self, key, field): return self.hashes[key].get(field)
    def hdel(self, key, field): self.hashes[key].pop(field, None)
    def setex(self, key, _ttl, value): self.values[key] = value
    def get(self, key): return self.values.get(key)
    def delete(self, key): self.values.pop(key, None)
    def eval(self, *_args):
        if isinstance(self.eval_result, Exception): raise self.eval_result
        return self.eval_result
    def brpoplpush(self, source, destination, timeout=0):
        if not self.lists[source]: return None
        item = self.lists[source].pop()
        self.lists[destination].insert(0, item)
        return item
    def close(self): self.closed = True


def queue_instance(cls, client):
    queue = cls.__new__(cls)
    queue.client = client
    queue.prefix = "test"
    queue.queue_key = "test:queue"
    queue.processing_key = "test:processing"
    queue.processing_meta_key = "test:meta"
    queue.dead_letter_key = "test:dead"
    queue.status_prefix = "test:status:"
    queue.max_attempts = 2
    queue.visibility_timeout = 0
    queue.orphan_grace_seconds = 0
    queue._orphan_seen_at = {}
    queue.streams = None
    return queue


def test_queue_claim_fallback_and_recovery_paths():
    client = QueueRedis()
    worker = queue_instance(WorkerQueue, client)
    raw = json.dumps({"request_id": "r", "attempt": 0})
    client.lists[worker.queue_key].append(raw)
    client.eval_result = TypeError("fake")
    assert worker._claim(0) == raw
    assert "r" in client.hashes[worker.processing_meta_key]
    client.hashes[worker.processing_meta_key]["bad"] = "{"
    client.lists[worker.processing_key].append("{bad")
    assert worker.requeue_stale() >= 1
    assert client.lists[worker.dead_letter_key]
    client.hashes[worker.processing_meta_key]["r"] = json.dumps({"raw": raw, "claimed_at": 0})
    client.lists[worker.processing_key].append(raw)
    assert worker.requeue_stale() >= 1
    webhook = queue_instance(DurableWebhookQueue, client)
    webhook._claim = lambda _timeout: None
    assert webhook.consume_once(lambda _: None) is False
    task = webhook.submit("web", "account", {"text": "hi"}, None, "tenant")
    assert webhook.status(task)["status"] == "accepted"
    webhook._claim = lambda _timeout: "{bad"
    assert webhook.consume_once(lambda _: None, timeout=0)
    assert client.lists[webhook.dead_letter_key]
    worker.close()
    webhook.close()
    assert client.closed


def test_webhook_queue_retries_dead_letters_and_status_decode():
    client = QueueRedis()
    queue = queue_instance(DurableWebhookQueue, client)
    queue.requeue_stale = lambda: 0
    queue._start_heartbeat = lambda _task: SimpleNamespace(set=lambda: None)
    task = queue.submit("web", "a", {}, None, "tenant")
    raw = client.lists[queue.queue_key].pop()
    queue._claim = lambda _timeout: raw
    assert queue.consume_once(lambda _: (_ for _ in ()).throw(RuntimeError("token=secret")), timeout=0)
    assert queue.status(task)["status"] == "retrying"
    raw = client.lists[queue.queue_key].pop()
    queue._claim = lambda _timeout: raw
    assert queue.consume_once(lambda _: (_ for _ in ()).throw(RuntimeError("permanent")), timeout=0)
    assert queue.status(task)["status"] == "dead"
    client.values[queue.status_prefix + "bad"] = "[]"
    assert queue.status("bad")["status"] == "unknown"
    queue._claim = lambda _timeout: None
    client.eval_result = ConnectionError("down")
    queue._claim = lambda _timeout: (_ for _ in ()).throw(ConnectionError("down"))
    assert queue.consume_once(lambda _: None, timeout=0) is False


def test_list_worker_submit_success_and_consume_retry_then_dead_letter():
    client = QueueRedis()
    worker = queue_instance(WorkerQueue, client)

    original_rpush = client.rpush

    def rpush_with_result(key, value):
        original_rpush(key, value)
        if key == worker.queue_key:
            payload = json.loads(value)
            client.values[f"{worker.prefix}:worker:result:{payload['request_id']}"] = json.dumps(
                {"events": []}
            )

    client.rpush = rpush_with_result
    context = TenantContext("tenant-a", "app", 1, "trace", "session", "web", "user")
    request = RunRequest(context, UserInput(text="hello", metadata={}), "request-key")
    assert worker.submit(request, default_demo_config(), timeout=0.1) == []
    assert client.lists[worker.queue_key]

    raw_payload = json.loads(client.lists[worker.queue_key].pop())
    raw_payload["request_id"] = "consume-success"
    raw = json.dumps(raw_payload)
    client.lists[worker.queue_key].append(raw)

    def claim_or_stop(_timeout):
        if not client.lists[worker.queue_key]:
            raise StopIteration
        return client.lists[worker.queue_key].pop()

    worker._claim = claim_or_stop
    worker._start_heartbeat = lambda _request_id: SimpleNamespace(set=lambda: None)
    with pytest.raises(StopIteration):
        worker.consume(lambda _request, _config: [], timeout=0)
    assert client.values["test:worker:result:consume-success"]

    failing_payload = json.loads(raw)
    failing_payload["request_id"] = "consume-failing"
    failing_raw = json.dumps(failing_payload)
    client.lists[worker.queue_key].append(failing_raw)
    worker._claim = claim_or_stop
    with pytest.raises(StopIteration):
        worker.consume(lambda _request, _config: (_ for _ in ()).throw(RuntimeError("token=secret")), timeout=0)
    assert client.lists[worker.dead_letter_key]
    assert "token=secret" not in client.lists[worker.dead_letter_key][-1]


def test_queue_helpers_and_constructor_transport_modes(monkeypatch):
    from trpc_service.gateway import worker_queue as module

    assert module._is_transient_redis_error(TimeoutError())
    assert module._is_transient_redis_error(ConnectionError())
    assert module._is_transient_redis_error(type("BusyLoadingError", (), {})())
    assert not module._is_transient_redis_error(RuntimeError())
    assert module._bounded_webhook_result("ignored") == {}
    assert module._bounded_webhook_result({"answer": None, "task_id": "t", "secret": "x"}) == {
        "task_id": "t", "answer_length": 0
    }

    class FakeRedisFactory:
        def from_url(self, url, **kwargs):
            self.args = (url, kwargs)
            return QueueRedis()

    factory = FakeRedisFactory()
    monkeypatch.setattr("redis.Redis", factory)
    monkeypatch.delenv("WORKER_QUEUE_TRANSPORT", raising=False)
    queue = module.WorkerQueue(url="redis://test", prefix="p", max_attempts=4, visibility_timeout=9)
    assert queue.streams is None and queue.queue_key == "p:worker:requests"
    assert factory.args == ("redis://test", {"decode_responses": True})

    streams = SimpleNamespace(submit=lambda payload: None, close=lambda: None)
    monkeypatch.setenv("WORKER_QUEUE_TRANSPORT", "streams")
    monkeypatch.setattr(module, "RedisStreamsTransport", lambda *args, **kwargs: streams)
    stream_queue = module.WorkerQueue(prefix="streamed")
    assert stream_queue.streams is streams
    stream_queue.close()


def test_worker_submit_timeout_and_result_error(monkeypatch):
    from trpc_service.gateway.worker_queue import WorkerQueue

    client = QueueRedis()
    worker = queue_instance(WorkerQueue, client)
    context = TenantContext("tenant-a", "app", 1, "trace", "session", "web", "user")
    request = RunRequest(context, UserInput(text="hello", metadata={}), "request-key")
    monkeypatch.setattr("trpc_service.gateway.worker_queue.uuid4", lambda: "fixed")
    with pytest.raises(TimeoutError, match="did not complete"):
        worker.submit(request, default_demo_config(), timeout=0)

    client.values["test:worker:result:fixed"] = json.dumps({"error": "RuntimeError: failed"})
    with pytest.raises(RuntimeError, match="failed"):
        worker.submit(request, default_demo_config(), timeout=0.1)
    assert client.get("test:worker:result:fixed") is None


def test_stream_worker_handler_success_and_terminal_error():
    from trpc_service.gateway.worker_queue import WorkerQueue

    client = QueueRedis()
    worker = queue_instance(WorkerQueue, client)
    context = TenantContext("tenant-a", "app", 1, "trace", "session", "web", "user")
    request = RunRequest(context, UserInput(text="hello", metadata={}), "request-key")
    payload = {
        "request_id": "stream-success",
        "attempt": 0,
        "request": {"tenant_context": asdict(context), "user_input": asdict(request.user_input), "idempotency_key": "key"},
        "config": default_demo_config().to_dict(),
    }
    worker._stream_handler(lambda _request, _config: [])(payload)
    result = json.loads(client.values["test:worker:result:stream-success"])
    assert result == {"events": []}

    payload["request_id"] = "stream-failure"
    payload["attempt"] = 1
    with pytest.raises(RuntimeError):
        worker._stream_handler(lambda *_args: (_ for _ in ()).throw(RuntimeError("api_key=secret")))(payload)
    error = json.loads(client.values["test:worker:result:stream-failure"])["error"]
    assert "api_key=secret" not in error
    assert "[secret-redacted]" in error


def test_stream_consume_retries_transient_redis_and_reraises_fatal(monkeypatch):
    from trpc_service.gateway.worker_queue import WorkerQueue

    queue = queue_instance(WorkerQueue, QueueRedis())
    calls = iter([ConnectionError("down"), RuntimeError("fatal")])
    def consume_once(*_args, **_kwargs):
        error = next(calls)
        raise error
    queue.streams = SimpleNamespace(consume_once=consume_once, close=lambda: None)
    monkeypatch.setattr("trpc_service.gateway.worker_queue.time.sleep", lambda _seconds: None)
    with pytest.raises(RuntimeError, match="fatal"):
        queue.consume(lambda *_args: None, timeout=0)


def test_queue_stale_and_orphan_recovery_covers_retry_and_dead_paths(monkeypatch):
    from trpc_service.gateway.worker_queue import DurableWebhookQueue, WorkerQueue

    now = 100.0
    monkeypatch.setattr("trpc_service.gateway.worker_queue.time.time", lambda: now)
    for cls, identity, payload_key in (
        (WorkerQueue, "request_id", "request"),
        (DurableWebhookQueue, "task_id", "payload"),
    ):
        client = QueueRedis()
        queue = queue_instance(cls, client)
        queue.visibility_timeout = 1
        queue.orphan_grace_seconds = 1
        valid = {identity: "item-1", "attempt": 0, payload_key: {}}
        raw = json.dumps(valid)
        client.hashes[queue.processing_meta_key]["item-1"] = json.dumps({"raw": raw, "claimed_at": 0})
        client.lists[queue.processing_key].append(raw)
        assert queue.requeue_stale() == 1
        assert json.loads(client.lists[queue.queue_key][-1])["attempt"] == 1

        dead = {identity: "item-2", "attempt": queue.max_attempts - 1, payload_key: {}}
        dead_raw = json.dumps(dead)
        client.hashes[queue.processing_meta_key]["item-2"] = json.dumps({"raw": dead_raw, "claimed_at": 0})
        client.lists[queue.processing_key].append(dead_raw)
        assert queue.requeue_stale() == 1
        assert client.lists[queue.dead_letter_key]

        orphan = {identity: "orphan", "attempt": 0, payload_key: {}}
        orphan_raw = json.dumps(orphan)
        client.lists[queue.processing_key].append(orphan_raw)
        assert queue.requeue_stale() == 0
        queue._orphan_seen_at["orphan"] = 0
        assert queue.requeue_stale() == 1
        assert any(json.loads(value).get("attempt") == 1 for value in client.lists[queue.queue_key])


def test_queue_recovery_cleans_invalid_metadata_and_missing_processing_item():
    from trpc_service.gateway.worker_queue import DurableWebhookQueue, WorkerQueue

    for cls in (WorkerQueue, DurableWebhookQueue):
        client = QueueRedis()
        queue = queue_instance(cls, client)
        client.hashes[queue.processing_meta_key]["bad-json"] = "{"
        client.hashes[queue.processing_meta_key]["bad-time"] = json.dumps({"claimed_at": "bad"})
        client.hashes[queue.processing_meta_key]["missing"] = json.dumps({"claimed_at": 0, "raw": "gone"})
        assert queue.requeue_stale() == 0
        assert "bad-json" not in client.hashes[queue.processing_meta_key]
        assert "bad-time" not in client.hashes[queue.processing_meta_key]
        assert "missing" not in client.hashes[queue.processing_meta_key]


def test_queue_claim_eval_wait_fallback_and_invalid_fallback_payload(monkeypatch):
    from trpc_service.gateway.worker_queue import DurableWebhookQueue, WorkerQueue

    for cls, identity in ((WorkerQueue, "request_id"), (DurableWebhookQueue, "task_id")):
        client = QueueRedis()
        queue = queue_instance(cls, client)
        client.eval_result = None
        assert queue._claim(timeout=0) is None
        client.eval_result = TypeError("eval unavailable")
        client.lists[queue.queue_key].append("{bad")
        assert queue._claim(timeout=0) == "{bad"
        assert not client.hashes[queue.processing_meta_key]
        client.lists[queue.queue_key].append(json.dumps({identity: "valid"}))
        assert queue._claim(timeout=0)
        assert "valid" in client.hashes[queue.processing_meta_key]


def test_queue_heartbeat_updates_claim_and_stops_when_deleted(monkeypatch):
    from trpc_service.gateway import worker_queue as module

    class ControlledStop:
        def __init__(self):
            self.calls = 0

        def wait(self, _interval):
            self.calls += 1
            return self.calls > 1

        def set(self):
            return None

    class ControlledThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    for cls, identity in ((module.WorkerQueue, "request_id"), (module.DurableWebhookQueue, "task_id")):
        client = QueueRedis()
        queue = queue_instance(cls, client)
        queue._start_heartbeat = None
        stop = ControlledStop()
        monkeypatch.setattr(module, "Event", lambda: stop)
        monkeypatch.setattr(module, "Thread", ControlledThread)
        client.hashes[queue.processing_meta_key]["id"] = json.dumps({"raw": "x", "claimed_at": 0})
        returned = cls._start_heartbeat(queue, "id")
        assert returned is stop
        assert json.loads(client.hashes[queue.processing_meta_key]["id"])["claimed_at"] != 0


def test_webhook_status_missing_and_non_dict_values():
    from trpc_service.gateway.worker_queue import DurableWebhookQueue

    queue = queue_instance(DurableWebhookQueue, QueueRedis())
    assert queue.status("missing") is None
    queue.client.values["test:status:list"] = json.dumps([])
    assert queue.status("list") == {"task_id": "list", "status": "unknown"}


def test_runtime_helpers_and_sdk_worker_shortcuts(monkeypatch):
    monkeypatch.setenv("KEY_ENV", "env-key")
    assert _resolve_api_key("env://KEY_ENV", "MISSING") == "env-key"
    assert _resolve_api_key("", "KEY_ENV") == "env-key"
    assert _render_conversation([{"role": "user", "content": "hi"}, {"role": "assistant", "content": ""}]) == "user: hi"
    assert _max_sdk_llm_calls() == 16
    assert _max_sdk_tool_calls(0) == 8
    monkeypatch.setenv("AGENT_MAX_LLM_CALLS", "1")
    with pytest.raises(RuntimeError): _max_sdk_llm_calls()
    monkeypatch.setenv("AGENT_MAX_LLM_CALLS", "bad")
    with pytest.raises(RuntimeError): _max_sdk_llm_calls()
    monkeypatch.setenv("AGENT_MAX_LLM_CALLS", "16")
    monkeypatch.setenv("AGENT_MAX_TOOL_ROUNDS", "0")
    with pytest.raises(RuntimeError): _max_sdk_tool_calls(1)
    monkeypatch.setenv("AGENT_MAX_TOOL_ROUNDS", "bad")
    with pytest.raises(RuntimeError): _max_sdk_tool_calls(1)
    monkeypatch.setenv("AGENT_MAX_TOOL_ROUNDS", "8")

    def handler(value: str): return value
    proxy = _signature_proxy(handler, "tool")
    assert proxy.__name__ == "tool" and "tool_context" in str(proxy.__signature__)
    assert _run_coroutine_sync(asyncio.sleep(0, result=3)) == 3
    class CloseSync:
        def close(self): self.called = True
    sync = CloseSync()
    asyncio.run(_close_async(sync))
    assert sync.called
    class CloseAsync:
        async def close(self): self.called = True
    async_close = CloseAsync()
    asyncio.run(_close_async(async_close))
    assert async_close.called

    storage = create_storage(StorageProfile(), "data-test-runtime")
    worker = TrpcAgentWorker(storage)
    app = default_demo_config().app("app_support")
    request = __import__("trpc_service.tenant.models", fromlist=["RunRequest"]).RunRequest(
        TenantContext("tenant_demo", "app_support", 1, "trace", "session", "web", "user"),
        UserInput("hello"), "key",
    )
    assert worker._generate_answer("", app, []) == "Please enter a message."
    assert worker._generate_answer("/help", app, []) .startswith("This tenant")
    monkeypatch.setattr(worker, "_run_sdk_async", lambda *args: asyncio.sleep(0, result="answer"))
    assert worker._generate_answer("hello", app, [], tools=[{"function": {"name": "lookup"}}]) == "answer"
    assert worker._runtime_context() is None
    storage.close()


def test_sdk_runtime_validates_configuration_and_aggregates_runner_events(monkeypatch):
    storage = create_storage(StorageProfile(), "data-test-runtime-sdk")
    worker = TrpcAgentWorker(storage)
    app = default_demo_config().app("app_support")
    context = TenantContext("tenant_demo", "app_support", 1, "trace-sdk", "session-sdk", "web", "user")
    context_token = worker._active_context_var.set(context)
    try:
        monkeypatch.delenv("SDK_KEY", raising=False)
        app.model_config.api_key_env = "SDK_KEY"
        with pytest.raises(RuntimeError, match="API key"):
            asyncio.run(worker._run_sdk_async(app, [{"role": "user", "content": "hello"}]))

        monkeypatch.setenv("SDK_KEY", "key")
        app.model_config.model = ""
        with pytest.raises(RuntimeError, match="model name"):
            asyncio.run(worker._run_sdk_async(app, [{"role": "user", "content": "hello"}]))

        app.model_config.model = "sdk-model"
        app.model_config.base_url = ""
        monkeypatch.delenv("CPA_BASE_URL", raising=False)
        with pytest.raises(RuntimeError, match="base URL"):
            asyncio.run(worker._run_sdk_async(app, [{"role": "user", "content": "hello"}]))

        class Event:
            def __init__(self, value):
                self.value = value

            def get_text(self):
                return self.value

        class SessionService:
            def __init__(self):
                self.created = []

            async def get_session(self, **_kwargs):
                return None

            async def create_session(self, **kwargs):
                self.created.append(kwargs)

        class FakeRunner:
            last = None

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.closed = False
                type(self).last = self

            async def run_async(self, **_kwargs):
                yield Event("hello ")
                yield Event("from sdk")

            async def close(self):
                self.closed = True

        class FakeAgent:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeModel:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        class FakeRunConfig:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeAgentContext:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeContent:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakePart:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        import trpc_agent_sdk.context as sdk_context
        import trpc_agent_sdk.types as sdk_types
        from trpc_agent_sdk import agents, configs, models, runners, sessions
        with pytest.MonkeyPatch.context() as patcher:
            patcher.setattr(sessions, "InMemorySessionService", SessionService)
            patcher.setattr(agents, "LlmAgent", FakeAgent)
            patcher.setattr(configs, "RunConfig", FakeRunConfig)
            patcher.setattr(sdk_context, "AgentContext", FakeAgentContext)
            patcher.setattr(models, "OpenAIModel", FakeModel)
            patcher.setattr(runners, "Runner", FakeRunner)
            patcher.setattr(sdk_types, "Content", FakeContent)
            patcher.setattr(sdk_types, "Part", FakePart)
            app.model_config.base_url = "https://model.example.test"
            result = asyncio.run(worker._run_sdk_async(app, [{"role": "user", "content": "hello"}]))
            assert result.text == "hello from sdk"
            assert result.total_tokens == result.input_tokens + result.output_tokens
            assert FakeRunner.last.closed is True
    finally:
        worker._active_context_var.reset(context_token)
        storage.close()


def test_sdk_runtime_empty_answer_and_approval_paths(monkeypatch):
    storage = create_storage(StorageProfile(), "data-test-runtime-sdk-empty")
    worker = TrpcAgentWorker(storage)
    app = default_demo_config().app("app_support")
    app.model_config.api_key_env = "SDK_KEY"
    app.model_config.base_url = "https://model.example.test"
    monkeypatch.setenv("SDK_KEY", "key")
    context_token = worker._active_context_var.set(
        TenantContext("tenant_demo", "app_support", 1, "trace-sdk", "session-sdk", "web", "user")
    )

    class Event:
        def __init__(self, value):
            self.value = value

        def get_text(self):
            return self.value

    class SessionService:
        async def get_session(self, **_kwargs):
            return object()

        async def create_session(self, **_kwargs):
            raise AssertionError("session should already exist")

    class Runner:
        def __init__(self, **_kwargs):
            pass

        async def run_async(self, **_kwargs):
            yield Event("")

        async def close(self):
            pass

    from trpc_agent_sdk import runners, sessions
    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(sessions, "InMemorySessionService", SessionService)
        patcher.setattr(runners, "Runner", Runner)
        with pytest.raises(RuntimeError, match="no assistant text"):
            asyncio.run(worker._run_sdk_async(app, [{"role": "user", "content": "hello"}]))

    approval = __import__("trpc_service.tenant.models", fromlist=["AgentEvent"]).AgentEvent(
        "approval_required", "approval", metadata={}
    )
    token = worker._sdk_tool_events_var.set([approval])
    try:
        class ApprovalRunner(Runner):
            async def run_async(self, **_kwargs):
                yield Event("answer")

        with pytest.MonkeyPatch.context() as patcher:
            patcher.setattr(sessions, "InMemorySessionService", SessionService)
            patcher.setattr(runners, "Runner", ApprovalRunner)
            with pytest.raises(_ApprovalRequired):
                asyncio.run(worker._run_sdk_async(app, [{"role": "user", "content": "hello"}]))
    finally:
        worker._sdk_tool_events_var.reset(token)
        worker._active_context_var.reset(context_token)
        storage.close()


def test_sdk_tool_requires_active_request_context():
    storage = create_storage(StorageProfile(), "data-test-runtime-tool")
    worker = TrpcAgentWorker(storage)
    tool = _PlatformSdkTool(worker, "lookup", {"function": {"name": "lookup"}})
    assert tool._get_declaration().name == "lookup"
    with pytest.raises(RuntimeError, match="outside an active"):
        asyncio.run(tool._run_async_impl(tool_context=SimpleNamespace(function_call_id="call"), args={}))
    storage.close()


def test_migration_helpers_export_import_and_verification(tmp_path):
    source = create_storage(StorageProfile(), tmp_path / "source")
    now = datetime.now(UTC)
    source.session.append_event(SessionEvent("tenant-a", "session", "event", {"text": "hi"}, "trace", "key", created_at=now))
    source.session.compare_and_set_state("tenant-a", "session", 0, {"ok": True})
    source.summary.put(Summary("tenant-a", "session", "summary", 1, created_at=now))
    source.memory.put(MemoryItem("tenant-a", "memory", "scope", "content", {"x": 1}, created_at=now))
    source.audit.append(AuditRecord("audit", "tenant-a", "allow", "trace", created_at=now))
    source.idempotency.start("tenant-a", "key", "trace")
    source.idempotency.complete("tenant-a", "key", "response", {"ok": True})
    source.knowledge.upsert(KnowledgeChunk("tenant-a", "docs", "chunk", "hello", {"x": 1}))
    source.artifacts.put_with_id("tenant-a", "object", b"data", "text/plain")
    source.mailbox.enqueue("tenant-a", "session", "mailbox-message", "mailbox-key", {"text": "queued"})
    source.session_mailbox_v2.accept("tenant-a", "session", "v2-message", trace_id="trace")
    inbox, created = source.inbox_outbox.accept_inbox(
        "tenant-a", "inbox-key", "session", {"text": "inbox"}, "worker"
    )
    assert created
    source.inbox_outbox.complete_inbox("tenant-a", "inbox-key", "worker", {"ok": True})
    source.inbox_outbox.enqueue_outbox(
        "tenant-a", "session.ready", "session", {"generation": 1}, event_id="outbox-event"
    )
    approval = source.tool_governance.create_or_get(
        "tenant-a", "approval", "session", "request", "send", "args-hash"
    )
    source.tool_governance.approve("tenant-a", approval.approval_id, "operator")
    source.tool_governance.reserve_call("tenant-a", "request", "call", True, 2, 1)
    execution = source.tool_governance.begin_execution(
        "tenant-a", "execution", "request", "session", "send", "call", "args-hash", True
    )
    source.tool_governance.complete_execution("tenant-a", execution.call_key, {"ok": True})
    snapshot = export_tenant(source, "tenant-a")
    assert snapshot["version"] == 3 and snapshot["sessions"]
    assert _json(now).endswith("+00:00") and _dt(_json(now)) == now
    assert _counts(snapshot)["sessions"] == 1
    assert _checksums(snapshot) == snapshot["checksums"]
    assert _snapshot_fingerprint(snapshot)
    assert _migration_manifest("tenant-a", "memory", "sql")["tenant_id"] == "tenant-a"
    assert cutover_plan(snapshot)["steps"]
    target = create_storage(StorageProfile(), tmp_path / "target")
    import_tenant(target, snapshot)
    report = verify_tenant(target, snapshot)
    assert report["ok"]
    assert target.mailbox.get("tenant-a", "mailbox-key") is not None
    assert target.session_mailbox_v2.has_unresolved_message("tenant-a", "session", "v2-message")
    assert target.inbox_outbox.list_outbox_by_tenant("tenant-a")[0].event_id == "outbox-event"
    assert target.tool_governance.get_execution("tenant-a", "call").status == "succeeded"
    assert _profile("memory", "", "").session_backend == "memory"
    source.close()
    target.close()


def test_quota_enforcer_memory_and_redis_decisions(monkeypatch):
    quota = QuotaEnforcer()
    policy = __import__("trpc_service.tenant.models", fromlist=["QuotaPolicy"]).QuotaPolicy(qps_limit=1, daily_token_limit=2, daily_cost_limit=1)
    quota.reserve("tenant", policy, requested_tokens=1, requested_cost=0.25)
    with pytest.raises(QuotaExceeded, match="QPS"):
        quota.check("tenant", policy)
    quota.record("tenant", 2, 0.5, reserved_tokens=1, reserved_cost=0.25)
    quota.release("tenant", 1, 0.25)
    token_quota = QuotaEnforcer()
    with pytest.raises(QuotaExceeded, match="token"):
        token_quota.reserve("tenant", policy, persisted_usage=(2, 0), requested_tokens=1)
    cost_quota = QuotaEnforcer()
    with pytest.raises(QuotaExceeded, match="cost"):
        cost_quota.reserve("tenant", policy, persisted_usage=(0, 1), requested_cost=1)

    class Redis:
        def __init__(self): self.result = 0; self.calls = []
        def eval(self, *args): self.calls.append(args); return self.result
        def hincrby(self, *args): self.calls.append(args)
        def hincrbyfloat(self, *args): self.calls.append(args)
        def expire(self, *args): self.calls.append(args)
    redis = Redis()
    quota._redis = redis
    quota.reserve("tenant", policy)
    for code, text in ((1, "QPS"), (2, "token"), (3, "cost")):
        redis.result = code
        with pytest.raises(QuotaExceeded, match=text): quota.reserve("tenant", policy)
    quota.record("tenant", 1, 0.1)
    quota.release("tenant", 1, 0.1)
    monkeypatch.setenv("TOOL_APPROVAL_TTL_SECONDS", "bad")
    from trpc_service.storage.tool_governance import _approval_ttl
    with pytest.raises(ValueError): _approval_ttl()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_tool_governance_state_machine_error_and_retry_paths(backend):
    from threading import RLock

    from trpc_service.storage.tool_governance import (
        ApprovalStatus,
        InMemoryToolGovernanceStore,
        SQLiteToolGovernanceStore,
        ToolExecutionStatus,
    )

    connection = None
    if backend == "memory":
        store = InMemoryToolGovernanceStore()
    else:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        store = SQLiteToolGovernanceStore(connection, RLock())
    try:
        assert store.get("tenant", "missing") is None
        with pytest.raises(KeyError):
            store.approve("tenant", "missing")
        approval = store.create_or_get("tenant", "approval", "session", "request", "send", "hash")
        assert store.create_or_get("tenant", "approval", "session", "request", "send", "other").status == ApprovalStatus.AMBIGUOUS
        with pytest.raises(RuntimeError, match="ambiguous"):
            store.consume("tenant", "approval", "request", "hash")
        with pytest.raises(RuntimeError, match="pending"):
            store.approve("tenant", "approval")

        # Expiry is persisted in both implementations and is checked on read.
        expired = store.create_or_get("tenant", "expired", "session", "request", "send", "hash", expires_seconds=1)
        if backend == "memory":
            store._approvals[("tenant", "expired")].expires_at = datetime.now(UTC)
        else:
            connection.execute(
                "UPDATE tool_approval SET expires_at=? WHERE tenant_id=? AND approval_id=?",
                (datetime.now(UTC).isoformat(), "tenant", "expired"),
            )
            connection.commit()
        expired = store.get("tenant", "expired")
        assert expired.status == ApprovalStatus.EXPIRED
        with pytest.raises(RuntimeError, match="cannot be consumed"):
            store.consume("tenant", "expired", "request", "hash")

        execution = store.begin_execution(
            "tenant", "execution", "request", "session", "send", "call", "hash", True, 7
        )
        with pytest.raises(RuntimeError, match="stale"):
            store.complete_execution("tenant", "call", {"ok": True}, 8)
        failed = store.fail_execution("tenant", "call", "provider", "down", 7)
        assert failed.status == ToolExecutionStatus.FAILED
        retried = store.begin_execution(
            "tenant", "execution-2", "request", "session", "send", "call", "hash", True, 7
        )
        assert retried.status == ToolExecutionStatus.RUNNING and retried.attempt == 2
        completed = store.complete_execution("tenant", "call", {"ok": True}, 7)
        assert completed.status == ToolExecutionStatus.SUCCEEDED
        assert store.fail_execution("tenant", "call", "ignored", "ignored", 7).status == ToolExecutionStatus.SUCCEEDED
        with pytest.raises(RuntimeError, match="stale"):
            store.begin_execution(
                "tenant", "execution-3", "request", "session", "send", "call", "hash", True, 8
            )
        with pytest.raises(KeyError):
            store.complete_execution("tenant", "missing-call", {}, 1)

        assert store.reserve_call("tenant", "budget", "first", False, 1, 0).total_calls == 1
        with pytest.raises(RuntimeError, match="budget exceeded"):
            store.reserve_call("tenant", "budget", "second", False, 1, 0)
        assert store.reserve_call("tenant", "budget", "first", False, 1, 0).total_calls == 1
        assert store.restore_approval(approval)
    finally:
        if connection is not None:
            connection.close()


def test_runtime_bridge_modes_and_factory_signatures(monkeypatch):
    storage = create_storage(StorageProfile(), "data-test-bridge")
    monkeypatch.delenv("TRPC_AGENT_RUNTIME_MODE", raising=False)
    assert RuntimeBridgeSpec.from_env().mode == "trpc"
    monkeypatch.setenv("TRPC_AGENT_RUNTIME_SETTINGS_JSON", "[]")
    with pytest.raises(ValueError): RuntimeBridgeSpec.from_env()
    monkeypatch.delenv("TRPC_AGENT_RUNTIME_SETTINGS_JSON", raising=False)
    with pytest.raises(RuntimeError): build_runtime_workers(storage, spec=RuntimeBridgeSpec(mode="bad"))
    local = build_runtime_worker(storage, spec=RuntimeBridgeSpec(mode="local"))
    assert hasattr(local, "run")
    auto = build_runtime_worker(storage, spec=RuntimeBridgeSpec(mode="auto"))
    assert hasattr(auto, "run")
    with pytest.raises(RuntimeError): build_runtime_workers(storage, spec=RuntimeBridgeSpec(mode="external"))
    with pytest.raises(RuntimeError): build_runtime_workers(storage, spec=RuntimeBridgeSpec(mode="external", factory_path="bad"))
    monkeypatch.setattr("trpc_service.agent.bridge._load_external_runtime", lambda *args: [SimpleNamespace(run=lambda *_: [])])
    workers = build_runtime_workers(storage, spec=RuntimeBridgeSpec(mode="external", factory_path="x:y"))
    assert len(workers) == 1
    assert _invoke_factory(lambda storage: storage, storage, None, None, {}) is storage
    storage.close()


def test_tool_registry_schema_local_mcp_and_types(monkeypatch):
    registry = ToolRegistry()
    def lookup(query: str, limit: int = 3, *, request_id: str = "") -> ToolResult:
        """Lookup a record."""
        return ToolResult("lookup", query, {"limit": limit, "request_id": request_id})
    registry.register("lookup", lookup)
    registry.register("search_knowledge", lambda query: ToolResult("search_knowledge", query, {}))
    schema = registry.tool_schemas({"lookup", "search_knowledge"})
    assert len(schema) == 2 and schema[0]["function"]["name"] == "search_knowledge"
    assert registry.call("lookup", query="x", request_id="r").content == "x"
    assert _handler_schema(lookup)["properties"]["limit"]["type"] == "integer"
    assert _json_type(str) == "string" and _json_type(list[str]) == "array" and _json_type(dict) == "object"
    registry.register_mcp_server("mcp", "https://mcp.example/tools", ["remote"], headers={"X-Test": "1"})
    class Response:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self): return b'{"result":{"content":"ok"}}'
    monkeypatch.setattr("trpc_service.tool.runtime.urlopen", lambda *a, **k: Response())
    assert registry.call("remote", request_id="r", idempotency_key="k").content == "ok"
    class ErrorResponse(Response):
        def read(self): return b'{"error":"failed"}'
    monkeypatch.setattr("trpc_service.tool.runtime.urlopen", lambda *a, **k: ErrorResponse())
    with pytest.raises(RuntimeError): registry.call("remote")
    with pytest.raises(KeyError): registry.call("missing")
    with pytest.raises(ValueError): registry.register_mcp_server("bad", "ftp://bad", [])


def test_telegram_and_official_account_variants_and_fallback(monkeypatch, tmp_path):
    telegram = TelegramAdapter()
    binding = bind("telegram", token_ref="token", sdk_enabled=False)
    payload = {"message": {"message_id": 1, "from": {"id": "u"}, "chat": {"id": "c"}, "photo": [{"file_id": "small", "width": 1}, {"file_id": "large", "file_size": 2}], "document": {"file_id": "d", "file_name": "d.txt"}, "voice": {"file_id": "v"}}}
    inbound = telegram.parse_event(payload, binding)
    assert len(inbound.attachments) == 3
    assert telegram.is_noop({}, binding)
    monkeypatch.setattr("trpc_service.channels.telegram.SecretManager.resolve", lambda *_: "bot-token")
    sent = []
    monkeypatch.setattr("trpc_service.channels.telegram.post_multipart_json", lambda *a, **k: sent.append(k) or {"ok": True, "result": {"message_id": 4}})
    path = tmp_path / "x.png"; path.write_bytes(b"x")
    assert telegram.send(message("telegram", attachments=[Attachment("image", url=str(path))]), binding).ok
    assert sent
    monkeypatch.setattr("trpc_service.channels.telegram.post_multipart_json", lambda *a, **k: {"ok": False, "description": "bad"})
    assert not telegram.send(message("telegram", attachments=[Attachment("file", url=str(path))]), binding).ok
    with pytest.raises(ChannelVerificationError):
        telegram.verify_callback({"secret_token": "bad"}, bind("telegram", secret_ref="secret"))

    official = WeChatOfficialAccountAdapter()
    ob = bind("wechat_official_account", token_ref="token", api_base_url="https://api.example", sdk_enabled=False)
    official.verify_callback({}, ob)
    parsed = official.parse_event({"raw_body": "<xml><MsgType>file</MsgType><FromUserName>u</FromUserName><MsgId>m</MsgId><MediaId>f</MediaId></xml>"}, ob)
    assert parsed.attachments[0].kind == "file"
    monkeypatch.setattr("trpc_service.channels.wechat_official_account.SecretManager.resolve", lambda *_: "access")
    responses = iter([{"errcode": 0, "media_id": "mid"}, {"errcode": 0}])
    monkeypatch.setattr("trpc_service.channels.wechat_official_account.post_multipart_json", lambda *a, **k: next(responses))
    monkeypatch.setattr("trpc_service.channels.wechat_official_account._wechat_json", lambda *a, **k: {"errcode": 0})
    assert official.send(message("wechat_official_account"), ob).ok


def test_secret_manager_resolution_vault_and_redaction(monkeypatch):
    from trpc_service.security.secrets import (
        SecretManager,
        SecretResolutionError,
        redact_secret_data,
        redact_secret_text,
    )
    monkeypatch.setenv("SECRET_TENANT_A_TOKEN", "env-secret")
    manager = SecretManager({"secret://explicit": "value"})
    assert manager.resolve("secret://explicit") == "value"
    assert manager.resolve("secret://tenant-a/token") == "env-secret"
    digest_ref = "secret://digest"
    digest_name = "SECRET_SHA256_" + hashlib.sha256(digest_ref.encode()).hexdigest()[:24].upper()
    monkeypatch.setenv(digest_name, "digest-secret")
    assert manager.resolve(digest_ref) == "digest-secret"
    manager.validate(digest_ref)
    with pytest.raises(SecretResolutionError): manager.resolve("plain")
    with pytest.raises(SecretResolutionError): manager.resolve("secret://missing")
    monkeypatch.setenv("SECRETS_JSON", "{bad")
    with pytest.raises(SecretResolutionError): SecretManager()
    monkeypatch.setenv("SECRETS_JSON", "")
    class Response:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self): return b'{"data":{"data":{"value":"vault-secret"}}}'
    monkeypatch.setenv("VAULT_ADDR", "https://vault.example")
    monkeypatch.setenv("VAULT_TOKEN", "vault-token")
    monkeypatch.setattr("trpc_service.security.secrets.urlopen", lambda *a, **k: Response())
    assert manager.resolve("secret://vault/path") == "vault-secret"
    assert "[secret-redacted]" in redact_secret_text("Authorization: Bearer abc api_key=def", ("abc", "def"))
    safe = redact_secret_data({"api_key": "secret", "nested": ["secret", {"token": "x"}], "ok": 1})
    assert safe["api_key"] == "[secret-redacted]" and safe["ok"] == 1


def _outbound_queue(client, *, max_attempts=2, visibility_timeout=10):
    from trpc_service.channels.outbound_queue import OutboundDeliveryQueue

    queue = OutboundDeliveryQueue.__new__(OutboundDeliveryQueue)
    queue.client = client
    queue.prefix = "test"
    queue.queue_key = "test:outbound:requests"
    queue.processing_key = "test:outbound:processing"
    queue.processing_meta_key = "test:outbound:processing-meta"
    queue.data_key = "test:outbound:data"
    queue.dead_letter_key = "test:outbound:dead-letter"
    queue.max_attempts = max_attempts
    queue.visibility_timeout = visibility_timeout
    queue.orphan_grace_seconds = 0
    queue._orphan_seen_at = {}
    return queue


class _QueueRedis:
    def __init__(self):
        self.hashes = {}
        self.lists = {}
        self.streams = {}
        self.xacks = []
        self.xdels = []
        self.closed = False
        self.eval_results = []

    def hsetnx(self, key, field, value):
        bucket = self.hashes.setdefault(key, {})
        if str(field) in bucket:
            return False
        bucket[str(field)] = value
        return True

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(str(field))

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[str(field)] = value
        return 1

    def hdel(self, key, field):
        self.hashes.get(key, {}).pop(str(field), None)
        return 1

    def rpush(self, key, *values):
        self.lists.setdefault(key, []).extend(values)
        return len(self.lists[key])

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)
        return len(self.lists[key])

    def lrange(self, key, start, end):
        values = self.lists.get(key, [])
        return values[start:] if end == -1 else values[start : end + 1]

    def lrem(self, key, count, value):
        values = self.lists.setdefault(key, [])
        removed = 0
        result = []
        for current in values:
            if current == value and (count == 0 or removed < count):
                removed += 1
            else:
                result.append(current)
        self.lists[key] = result
        return removed

    def rpoplpush(self, source, destination):
        if not self.lists.get(source):
            return None
        value = self.lists[source].pop()
        self.lists.setdefault(destination, []).insert(0, value)
        return value

    def brpoplpush(self, source, destination, timeout=0):
        return self.rpoplpush(source, destination)

    def lmove(self, source, destination, *_args):
        return self.rpoplpush(source, destination)

    def eval(self, *_args):
        if self.eval_results:
            return self.eval_results.pop(0)
        queue_key, processing_key, meta_key = _args[2:5]
        if not self.lists.get(queue_key):
            return None
        task_id = self.lists[queue_key].pop()
        self.lists.setdefault(processing_key, []).insert(0, task_id)
        self.hset(meta_key, task_id, json.dumps({"claimed_at": time.time()}))
        return task_id

    def xgroup_create(self, *_args, **_kwargs):
        return True

    def xadd(self, key, fields):
        message_id = f"{len(self.streams.get(key, [])) + 1}-0"
        self.streams.setdefault(key, []).append((message_id, fields))
        return message_id

    def xreadgroup(self, *_args, **_kwargs):
        return []

    def xack(self, _stream, _group, message_id):
        self.xacks.append(message_id)

    def xdel(self, _stream, message_id):
        self.xdels.append(message_id)

    def xautoclaim(self, *_args, **_kwargs):
        return ["0-0", []]

    def close(self):
        self.closed = True


def test_outbound_queue_enqueue_consume_success_duplicate_and_missing_data():
    client = _QueueRedis()
    queue = _outbound_queue(client)
    task_id = queue.enqueue({"tenant_id": "tenant-a", "messages": [{"text": "hi"}]}, "task-1")
    assert task_id == "task-1"
    assert queue.enqueue({"tenant_id": "tenant-a", "messages": [{"text": "changed"}]}, "task-1") == "task-1"
    assert client.lists[queue.queue_key] == ["task-1"]
    seen = []
    assert queue.consume_once(lambda item: seen.append(item), timeout=0)
    assert seen[0]["messages"][0]["text"] == "hi"
    assert client.hget(queue.data_key, "task-1") is None

    client.lists[queue.queue_key].append("orphan")
    assert queue.consume_once(lambda _: None, timeout=0)
    assert client.lrem(queue.processing_key, 1, "orphan") == 0
    with pytest.raises(KeyError):
        queue.update({})
    queue.close()


def test_outbound_queue_retries_dead_letters_and_redacts_errors(monkeypatch):
    client = _QueueRedis()
    queue = _outbound_queue(client, max_attempts=2)
    queue.enqueue({"tenant_id": "tenant-a", "messages": [{"text": "hi"}]}, "task-1")
    callback = []
    fail = lambda _item: (_ for _ in ()).throw(RuntimeError("token=secret-value"))
    assert queue.consume_once(fail, on_dead_letter=lambda item, error: callback.append((item, error)), timeout=0)
    assert json.loads(client.hget(queue.data_key, "task-1"))["attempt"] == 1
    assert client.lists[queue.queue_key] == ["task-1"]
    assert queue.consume_once(fail, on_dead_letter=lambda item, error: callback.append((item, error)), timeout=0)
    assert callback and "secret-value" not in callback[0][1]
    assert json.loads(client.lists[queue.dead_letter_key][0])["item"]["attempt"] == 2

    class ConnectionFailure:
        __name__ = "ConnectionError"

    monkeypatch.setattr("trpc_service.channels.outbound_queue.time.sleep", lambda _: None)
    broken = _outbound_queue(client)
    broken.requeue_stale = lambda: (_ for _ in ()).throw(ConnectionError("redis down"))
    broken._move_to_processing = lambda _timeout: None
    assert broken.consume_once(lambda _: None, timeout=0) is False


def test_outbound_queue_stale_recovery_and_malformed_entries(monkeypatch):
    client = _QueueRedis()
    queue = _outbound_queue(client, max_attempts=2, visibility_timeout=60)
    client.lists[queue.processing_key] = ["fresh", "bad-meta", "missing", "bad-payload", "dead"]
    client.hashes[queue.processing_meta_key] = {
        "fresh": json.dumps({"claimed_at": time.time()}),
        "bad-meta": "{broken",
        "bad-payload": json.dumps({"claimed_at": 0}),
        "dead": json.dumps({"claimed_at": 0}),
    }
    client.hashes[queue.data_key] = {
        "bad-meta": json.dumps({"tenant_id": "tenant-a", "messages": [{"text": "x"}], "attempt": 0}),
        "bad-payload": "not-json",
        "dead": json.dumps({"tenant_id": "tenant-a", "messages": [{"text": "x"}], "attempt": 1}),
    }
    monkeypatch.setattr("trpc_service.channels.outbound_queue.time.time", lambda: 1000.0)
    assert queue.requeue_stale() == 3
    assert "fresh" in client.lists[queue.processing_key]
    assert "missing" not in client.lists[queue.processing_key]
    assert json.loads(client.hget(queue.data_key, "bad-meta"))["attempt"] == 1
    assert len(client.lists[queue.dead_letter_key]) == 2


def test_outbound_queue_move_to_processing_script_and_fallbacks(monkeypatch):
    client = _QueueRedis()
    queue = _outbound_queue(client)
    client.lists[queue.queue_key] = ["script-task"]
    assert queue._move_to_processing(0) == "script-task"
    assert json.loads(client.hget(queue.processing_meta_key, "script-task"))["claimed_at"]

    class NoEval(_QueueRedis):
        def eval(self, *_args):
            raise AttributeError("eval unavailable")

    fallback = _outbound_queue(NoEval())
    fallback.client.lists[fallback.queue_key] = ["brpop-task"]
    assert fallback._move_to_processing(1) == "brpop-task"

    class NoMove(NoEval):
        def lmove(self, *_args):
            raise AttributeError("lmove unavailable")

    no_move = _outbound_queue(NoMove())
    no_move.client.lists[no_move.queue_key] = ["rpop-task"]
    assert no_move._move_to_processing(0) == "rpop-task"

    class Busy(NoEval):
        def brpoplpush(self, *_args, **_kwargs):
            raise ConnectionError("temporary")

    monkeypatch.setattr("trpc_service.channels.outbound_queue.time.sleep", lambda _: None)
    assert _outbound_queue(Busy())._move_to_processing(1) is None


def test_redis_streams_init_submit_and_group_errors(monkeypatch):
    import sys
    import types

    from trpc_service.gateway.redis_streams import RedisStreamsTransport

    class Redis:
        def __init__(self, error=None):
            self.error = error
            self.groups = []
        @classmethod
        def from_url(cls, *_args, **_kwargs):
            return cls(Redis.error)
        def xgroup_create(self, *args, **kwargs):
            self.groups.append((args, kwargs))
            if self.error:
                raise self.error
        def xadd(self, key, fields):
            self.last_add = (key, fields)
            return "7-0"
        def close(self):
            self.closed = True

    module = types.SimpleNamespace(Redis=Redis)
    monkeypatch.setitem(sys.modules, "redis", module)
    Redis.error = RuntimeError("BUSYGROUP exists")
    transport = RedisStreamsTransport("redis://example", consumer="c", max_attempts=0)
    assert transport.consumer == "c" and transport.max_attempts == 1
    assert transport.submit({"x": 1}) == "7-0"
    transport.close()
    assert transport.client.closed
    Redis.error = RuntimeError("connection failed")
    with pytest.raises(RuntimeError, match="connection failed"):
        RedisStreamsTransport("redis://example")


def test_redis_streams_consume_reclaim_and_process_matrix(monkeypatch):
    from trpc_service.gateway.redis_streams import RedisStreamsTransport

    class Streams(_QueueRedis):
        def __init__(self):
            super().__init__()
            self.read_items = []
            self.claim_result = ["0-0", []]
        def xreadgroup(self, *_args, **_kwargs):
            return self.read_items
        def xautoclaim(self, *_args, **_kwargs):
            return self.claim_result

    client = Streams()
    transport = RedisStreamsTransport.__new__(RedisStreamsTransport)
    transport.client = client
    transport.stream_key = "test:stream"
    transport.dead_letter_key = "test:dead"
    transport.group = "workers"
    transport.consumer = "c"
    transport.max_attempts = 2
    transport.reclaim_interval_seconds = 0.1
    transport._last_reclaim_at = time.monotonic()

    assert transport.consume_once(lambda _: None, block_ms=-1) is False
    transport._last_reclaim_at = 0
    client.claim_result = ["0-0", [("claim-1", {"payload": json.dumps({"id": 1})})]]
    seen = []
    assert transport.consume_once(lambda item: seen.append(item), block_ms=0)
    assert seen == [{"id": 1}]

    client.read_items = [(transport.stream_key, [("read-1", {"payload": json.dumps({"id": 2})})])]
    assert transport.consume_once(lambda item: seen.append(item), block_ms=0)
    assert seen[-1] == {"id": 2}

    transport._process("empty", {}, lambda _: None)
    transport._process("bytes", {"payload": json.dumps({"ok": True}).encode()}, lambda item: seen.append(item))
    transport._process("bad-bytes", {"payload": b"\xff"}, lambda _: None)
    transport._process("list", {"payload": "[]"}, lambda _: None)
    assert len(client.streams[transport.dead_letter_key]) == 3

    transport._process("retry", {"payload": json.dumps({"attempt": "bad"})}, lambda _: (_ for _ in ()).throw(RuntimeError("token=hidden")))
    assert client.streams[transport.stream_key]
    transport._process("dead", {"payload": json.dumps({"attempt": 1})}, lambda _: (_ for _ in ()).throw(RuntimeError("permanent")))
    assert len(client.streams[transport.dead_letter_key]) == 4

    class NoClaim(_QueueRedis):
        def xautoclaim(self, *_args, **_kwargs):
            raise TypeError("unsupported")
    no_claim = RedisStreamsTransport.__new__(RedisStreamsTransport)
    no_claim.client = NoClaim()
    no_claim.stream_key = "s"
    no_claim.group = "g"
    no_claim.consumer = "c"
    assert no_claim.requeue_stale(lambda _: None) == 0
    no_claim.client.xautoclaim = lambda *a, **k: "malformed"
    assert no_claim.requeue_stale(lambda _: None) == 0


def test_wecom_callback_parse_and_outbound_protocol_matrix(monkeypatch, tmp_path):
    import sys
    import types

    from trpc_service.channels.wecom import WeComAdapter

    monkeypatch.setattr("trpc_service.channels.wecom.SecretManager.resolve", lambda self, ref: "wecom-token")
    adapter = WeComAdapter()
    binding = bind("wecom", token_ref="token", sdk_enabled=False, webhook_url="https://hook.example")
    timestamp, nonce = "10", "nonce"
    signature = __import__("hashlib").sha1("".join(sorted(("wecom-token", timestamp, nonce))).encode()).hexdigest()
    adapter.verify_callback({"timestamp": timestamp, "nonce": nonce, "signature": signature}, binding)
    with pytest.raises(ChannelVerificationError, match="incomplete"):
        adapter.verify_callback({"timestamp": timestamp}, binding)
    with pytest.raises(ChannelVerificationError, match="invalid"):
        adapter.verify_callback({"timestamp": timestamp, "nonce": nonce, "signature": "bad"}, binding)
    monkeypatch.setattr("trpc_service.channels.wecom.verify_handshake", lambda payload, token, key: "challenge")
    assert adapter.verify_handshake({}, binding) == "challenge"

    image = adapter.parse_event({"MsgType": "image", "PicUrl": "https://img", "FromUserName": "u", "MsgId": "m"}, binding)
    assert image.attachments[0].kind == "image"
    media = adapter.parse_event({"MsgType": "file", "MediaId": "media", "FromUserName": "u", "MsgId": "m"}, binding)
    assert media.attachments[0].metadata["media_id"] == "media"
    xml = adapter.parse_event({"raw_body": "<xml><MsgType>text</MsgType><FromUserName>u</FromUserName><MsgId>m</MsgId><Content>hello</Content></xml>"}, binding)
    assert xml.text == "hello"
    monkeypatch.setattr("trpc_service.channels.wecom._post_json", lambda url, payload: {"errcode": 0, "msgid": "id"})
    assert adapter.send(message("wecom"), binding).ok
    file_path = tmp_path / "x.txt"
    file_path.write_text("x", encoding="utf-8")
    assert not adapter.send(message("wecom", attachments=[Attachment("file", url=str(file_path))]), binding).ok
    monkeypatch.setattr("trpc_service.channels.wecom._post_json", lambda url, payload: {"errcode": 9, "errmsg": "bad"})
    assert not adapter.send(message("wecom"), binding).ok

    class Message:
        def send_text(self, *_args): return {"errcode": 0}
        def send_image(self, *_args): return {"errcode": 0}
        def send_file(self, *_args): return {"errcode": 0}
    class Media:
        def upload(self, *_args): return {"media_id": "mid"}
    class Client:
        def __init__(self, *_args):
            self.message, self.media = Message(), Media()
    monkeypatch.setitem(sys.modules, "wechatpy", types.ModuleType("wechatpy"))
    enterprise = types.ModuleType("wechatpy.enterprise")
    enterprise.WeChatClient = Client
    monkeypatch.setitem(sys.modules, "wechatpy.enterprise", enterprise)
    sdk_binding = bind("wecom", corp_id="corp", corp_secret_ref="secret", agent_id=1, sdk_enabled=True)
    assert adapter.send(message("wecom"), sdk_binding).ok
    assert adapter.send(message("wecom", attachments=[Attachment("image", url=str(file_path))]), sdk_binding).ok


def test_wechat_official_account_signature_parse_and_api_paths(monkeypatch, tmp_path):
    from trpc_service.channels.wechat_official_account import WeChatOfficialAccountAdapter

    monkeypatch.setattr("trpc_service.channels.wechat_official_account.SecretManager.resolve", lambda self, ref: "token")
    adapter = WeChatOfficialAccountAdapter()
    binding = bind("wechat_official_account", token_ref="token", api_base_url="https://api.example", sdk_enabled=False)
    timestamp, nonce = "10", "nonce"
    signature = __import__("hashlib").sha1("".join(sorted(("token", timestamp, nonce))).encode()).hexdigest()
    adapter.verify_callback({"timestamp": timestamp, "nonce": nonce, "signature": signature}, binding)
    with pytest.raises(ChannelVerificationError, match="incomplete"):
        adapter.verify_callback({"timestamp": timestamp}, binding)
    with pytest.raises(ChannelVerificationError, match="invalid"):
        adapter.verify_callback({"timestamp": timestamp, "nonce": nonce, "signature": "bad"}, binding)
    encrypted_signature = __import__("hashlib").sha1(
        "".join(sorted(("token", timestamp, nonce, "cipher"))).encode()
    ).hexdigest()
    with pytest.raises(ChannelVerificationError, match="AES"):
        adapter.verify_callback(
            {
                "timestamp": timestamp,
                "nonce": nonce,
                "signature": signature,
                "Encrypt": "cipher",
                "msg_signature": encrypted_signature,
            },
            binding,
        )
    monkeypatch.setattr("trpc_service.channels.wechat_official_account.verify_handshake", lambda *args: "ok")
    assert adapter.verify_handshake({}, binding) == "ok"

    image = adapter.parse_event({"MsgType": "image", "PicUrl": "https://img", "FromUserName": "u", "MsgId": "m"}, binding)
    assert image.attachments[0].kind == "image"
    monkeypatch.setattr("trpc_service.channels.wechat_official_account._wechat_json", lambda url, payload=None: {"errcode": 0, "msgid": "m1"})
    assert adapter.send(message("wechat_official_account"), binding).ok
    path = tmp_path / "x.png"
    path.write_bytes(b"x")
    monkeypatch.setattr("trpc_service.channels.wechat_official_account.post_multipart_json", lambda *args, **kwargs: {"media_id": "mid"})
    assert adapter.send(message("wechat_official_account", attachments=[Attachment("image", url=str(path))]), binding).ok
    assert not adapter.send(message("wechat_official_account", attachments=[Attachment("file", url=str(path))]), binding).ok
    monkeypatch.setattr("trpc_service.channels.wechat_official_account._wechat_json", lambda *args, **kwargs: {"errcode": 9, "errmsg": "bad"})
    assert not adapter.send(message("wechat_official_account"), binding).ok


def test_wechat_customer_service_signature_parse_and_api_paths(monkeypatch, tmp_path):
    from trpc_service.channels.wechat_customer_service import WeChatCustomerServiceAdapter

    monkeypatch.setattr("trpc_service.channels.wechat_customer_service.SecretManager.resolve", lambda self, ref: "token")
    adapter = WeChatCustomerServiceAdapter()
    binding = bind("wechat_customer_service", token_ref="token", api_base_url="https://api.example")
    timestamp, nonce = "10", "nonce"
    signature = __import__("hashlib").sha1("".join(sorted(("token", timestamp, nonce))).encode()).hexdigest()
    adapter.verify_callback({"timestamp": timestamp, "nonce": nonce, "signature": signature}, binding)
    with pytest.raises(ChannelVerificationError, match="incomplete"):
        adapter.verify_callback({"timestamp": timestamp}, binding)
    with pytest.raises(ChannelVerificationError, match="invalid"):
        adapter.verify_callback({"timestamp": timestamp, "nonce": nonce, "signature": "bad"}, binding)
    monkeypatch.setattr("trpc_service.channels.wechat_customer_service.verify_handshake", lambda *args: "ok")
    assert adapter.verify_handshake({}, binding) == "ok"

    image = adapter.parse_event({"MsgType": "image", "PicUrl": "https://img", "OpenId": "u", "MsgId": "m"}, binding)
    assert image.attachments[0].kind == "image"
    parsed = adapter.parse_event({"raw_body": "<xml><MsgType>file</MsgType><OpenId>u</OpenId><MsgId>m</MsgId><MediaId>f</MediaId></xml>"}, binding)
    assert parsed.attachments[0].kind == "file"
    path = tmp_path / "x.png"
    path.write_bytes(b"x")
    monkeypatch.setattr("trpc_service.channels.wechat_customer_service._wechat_json", lambda url, payload: {"errcode": 0})
    assert adapter.send(message("wechat_customer_service"), binding).ok
    monkeypatch.setattr("trpc_service.channels.wechat_customer_service.post_multipart_json", lambda *args, **kwargs: {"media_id": "mid"})
    assert adapter.send(message("wechat_customer_service", attachments=[Attachment("image", url=str(path))]), binding).ok
    assert not adapter.send(message("wechat_customer_service", attachments=[Attachment("file", url=str(path))]), binding).ok
    monkeypatch.setattr("trpc_service.channels.wechat_customer_service._wechat_json", lambda *args, **kwargs: {"errcode": 9, "errmsg": "bad"})
    assert not adapter.send(message("wechat_customer_service"), binding).ok
    no_token = bind("wechat_customer_service")
    assert adapter.send(message("wechat_customer_service"), no_token).ok


def test_outbound_mapper_edge_matrix_and_reliable_delivery(monkeypatch, tmp_path):
    from trpc_service.channels.base import SendResult
    from trpc_service.channels.outbound import build_outbound_messages, split_outbound_messages, visible_answer
    from trpc_service.channels.reliable import ChannelRateLimiter, persist_dead_letter, send_with_retry, split_text
    from trpc_service.tenant.models import AgentEvent

    base = {
        "channel": "web",
        "account_id": "account",
        "session_id": "session",
        "external_user_id": "user",
    }
    assert build_outbound_messages([AgentEvent("message_revoked", "gone")], **base) == []
    fallback = build_outbound_messages([AgentEvent("unknown", "fallback")], **base)
    assert fallback[0].metadata["message_type"] == "text"
    assert build_outbound_messages([], **base)[0].text == ""
    artifact = build_outbound_messages(
        [AgentEvent("artifact", "", {"attachment": {"url": "file:///tmp/a", "filename": "a.txt"}})], **base
    )
    assert artifact[0].attachments[0].name == "a.txt"
    with pytest.raises(ValueError):
        split_outbound_messages(fallback, 0)
    split = split_outbound_messages(fallback, 2, idempotency_key="fixed")
    assert split[0].metadata["idempotency_key"] == "fixed:part:0"
    assert visible_answer(split) == "fa\nll\nba\nck"

    assert split_text("abc", 0) == ["abc"]
    assert split_text("abc", 2) == ["ab", "c"]
    limiter = ChannelRateLimiter()
    limiter.acquire("key", 0)
    limiter.acquire("key", 1)
    with pytest.raises(RuntimeError, match="rate limit"):
        limiter.acquire("key", 1)
    monkeypatch.setattr("trpc_service.channels.reliable.monotonic", lambda: 10.0)
    limiter._windows["expired"].append(8.0)
    limiter.acquire("expired", 1)

    monkeypatch.setattr("trpc_service.channels.reliable.sleep", lambda _delay: None)
    outcomes = iter([SendResult(False, "", "retry", {"retry_after_seconds": "bad"}), SendResult(True, "ref")])
    assert send_with_retry(lambda: next(outcomes), attempts=2, jitter=0).ok
    dead = []
    failed = send_with_retry(lambda: SendResult(False, "", "token=secret"), attempts=1, dead_letter=dead.append)
    assert not failed.ok
    assert dead and dead[0].error == "token=secret"
    thrown = send_with_retry(lambda: (_ for _ in ()).throw(RuntimeError("token=secret")), attempts=1)
    assert "token=secret" not in thrown.error
    persist_dead_letter("web", "account", message(), SendResult(False, "", "failed"), root=str(tmp_path))
    assert list(tmp_path.rglob("*.json"))


def test_channel_http_helpers_and_wechat_crypto_handshake(monkeypatch):
    import hashlib

    from trpc_service.channels.wechat_crypto import verify_handshake
    from trpc_service.channels.wechat_official_account import _wechat_json
    from trpc_service.channels.wecom import _post_json

    class Response:
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def read(self): return b'{"ok": true}'

    monkeypatch.setattr("urllib.request.urlopen", lambda *args, **kwargs: Response())
    assert _post_json("https://example", {"x": 1})["ok"]
    assert _wechat_json("https://example", {"x": 1})["ok"]
    monkeypatch.setattr("trpc_service.channels.wecom.SecretManager.resolve", lambda self, ref: "token")
    timestamp, nonce = "1", "n"
    signature = hashlib.sha1("".join(sorted(("token", timestamp, nonce))).encode()).hexdigest()
    assert verify_handshake(
        {"timestamp": timestamp, "nonce": nonce, "echostr": "echo", "signature": signature}, "secret"
    ) == "echo"
    with pytest.raises(ValueError, match="incomplete"):
        verify_handshake({}, "secret")
    with pytest.raises(ValueError, match="invalid"):
        verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": "echo", "signature": "bad"}, "secret")


def test_media_materialization_and_multipart_protocol(monkeypatch, tmp_path):
    import base64
    import json as json_module

    from trpc_service.channels.media import attachment_kind, post_multipart_json, prepare_attachment_file

    assert attachment_kind(None) == "text"
    assert attachment_kind(Attachment("photo")) == "image"
    assert attachment_kind(Attachment("file", content_type="image/png")) == "image"
    assert attachment_kind(Attachment("file")) == "file"
    with prepare_attachment_file(None) as prepared:
        assert prepared is None
    with prepare_attachment_file(Attachment("file", metadata={"content_base64": base64.b64encode(b"data").decode()}, name="x.bin")) as prepared:
        assert prepared.path.read_bytes() == b"data"
        assert prepared.filename == "x.bin"
    with pytest.raises(ValueError, match="base64"):
        with prepare_attachment_file(Attachment("file", metadata={"content_base64": "%%%"})):
            pass

    path = tmp_path / "x.txt"
    path.write_bytes(b"local")
    with prepare_attachment_file(Attachment("file", url=f"file://{path}")) as prepared:
        assert prepared.path.read_bytes() == b"local"
    with pytest.raises(ValueError, match="path"):
        with prepare_attachment_file(Attachment("file", url=str(tmp_path / "missing"))):
            pass
    with pytest.raises(ValueError, match="exceeds"):
        with prepare_attachment_file(Attachment("file", url=str(path)), max_bytes=1):
            pass

    class Response:
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def read(self, *_args): return b"remote"
    monkeypatch.setattr("trpc_service.channels.media.urlopen", lambda *args, **kwargs: Response())
    with prepare_attachment_file(Attachment("file", url="https://example.test/x", content_type="text/plain")) as prepared:
        assert prepared.content_type == "text/plain"
    class CaptureResponse(Response):
        def read(self, *_args): return json_module.dumps({"ok": True}).encode()
    captured = {}
    def capture(request, timeout):
        captured["url"] = request.full_url
        captured["body"] = request.data
        captured["content_type"] = request.headers["Content-type"]
        captured["timeout"] = timeout
        return CaptureResponse()
    monkeypatch.setattr("trpc_service.channels.media.urlopen", capture)
    assert post_multipart_json("https://example.test/upload", fields={"type": "image", "n": 1}, file_field="media", file_path=path)["ok"]
    assert b"name=\"type\"" in captured["body"] and b"local" in captured["body"]
    assert captured["timeout"] == 20 and "multipart/form-data" in captured["content_type"]


def test_wechat_crypto_aes_round_trip_and_handshake_variants(monkeypatch):
    import base64
    import hashlib
    import struct

    from cryptography.hazmat.primitives import padding as crypto_padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    from trpc_service.channels import wechat_crypto

    key = b"0123456789abcdef0123456789abcdef"
    key_ref = "aes"
    encoded_key = base64.b64encode(key).decode().rstrip("=")
    monkeypatch.setattr("trpc_service.channels.wechat_crypto.SecretManager.resolve", lambda self, ref: encoded_key)
    xml = "<xml><Content>hello</Content></xml>"
    plaintext = b"0123456789abcdef" + struct.pack("!I", len(xml.encode())) + xml.encode() + b"sender"
    padder = crypto_padding.PKCS7(algorithms.AES.block_size).padder()
    padded = padder.update(plaintext) + padder.finalize()
    encrypted = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    ciphertext = encrypted.update(padded) + encrypted.finalize()
    encoded = base64.b64encode(ciphertext).decode()
    assert wechat_crypto.decrypt_message(encoded, key_ref)["Content"] == "hello"
    token, timestamp, nonce = "token", "1", "n"
    monkeypatch.setattr("trpc_service.channels.wechat_crypto.SecretManager.resolve", lambda self, ref: token)
    signature = hashlib.sha1("".join(sorted((token, timestamp, nonce))).encode()).hexdigest()
    assert wechat_crypto.verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": "echo", "signature": signature}, "token-ref") == "echo"
    encrypted_signature = hashlib.sha1("".join(sorted((token, timestamp, nonce, encoded))).encode()).hexdigest()
    real_decrypt = wechat_crypto._decrypt_aes_payload
    monkeypatch.setattr(wechat_crypto, "_decrypt_aes_payload", lambda *_args: plaintext)
    assert wechat_crypto.verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": encoded, "msg_signature": encrypted_signature}, "token-ref", key_ref) == xml
    assert wechat_crypto.verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": encoded, "msg_signature": encrypted_signature}, "token-ref") == encoded
    with pytest.raises(ValueError, match="invalid"):
        wechat_crypto.verify_handshake({"timestamp": timestamp, "nonce": nonce, "echostr": "echo", "signature": "bad"}, "token-ref")
    monkeypatch.setattr("trpc_service.channels.wechat_crypto.SecretManager.resolve", lambda self, ref: encoded_key)
    invalid_cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor().update(b"0" * 16)
    with pytest.raises(ValueError, match="padding"):
        real_decrypt(base64.b64encode(invalid_cipher).decode(), key_ref)
