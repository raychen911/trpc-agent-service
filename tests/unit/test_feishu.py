from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import time

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.base import WebhookRequest
from trpc_service.channels.envelopes import (
    DeliveryStatus,
    InboundEnvelope,
    OutboundEnvelope,
    PayloadKind,
)
from trpc_service.channels.feishu import (
    FeishuAdapter,
    FeishuResourceError,
    FeishuVerificationError,
    _event_type,
    _flatten_post,
    _integer,
    _json_object,
    _mapping,
    _normalize_content,
    _receive_id_type,
    _timestamp,
)
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.memory import InMemoryRuntimeRepository
from trpc_service.storage.models import BindingRoute
from trpc_service.tenant.models import (
    Channel,
    ChannelBinding,
    ConversationKind,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
)

APP_ID = "cli_test_app"
APP_SECRET = "test-app-secret"
VERIFICATION_TOKEN = "test-verification-token"
ENCRYPT_KEY = "test-encrypt-key"


def feishu_binding(*, encrypted: bool = True) -> ChannelBinding:
    refs = {
        "app_secret": SecretRef(uri=f"literal://{APP_SECRET}"),
        "verification_token": SecretRef(uri=f"literal://{VERIFICATION_TOKEN}"),
    }
    if encrypted:
        refs["encrypt_key"] = SecretRef(uri=f"literal://{ENCRYPT_KEY}")
    return ChannelBinding(
        binding_id="feishu-binding",
        tenant_id="tenant",
        app_id="support",
        channel=Channel.FEISHU,
        account_id=APP_ID,
        secret_refs=refs,
        capabilities=frozenset({"text", "image", "file", "reply"}),
    )


def event_payload(
    *,
    message_type: str = "text",
    content: dict[str, object] | None = None,
    chat_type: str = "group",
) -> dict[str, object]:
    return {
        "schema": "2.0",
        "header": {
            "event_id": "event-1",
            "event_type": "im.message.receive_v1",
            "create_time": "1787356800000",
            "token": VERIFICATION_TOKEN,
            "app_id": APP_ID,
            "tenant_key": "tenant-key",
        },
        "event": {
            "sender": {
                "sender_id": {
                    "open_id": "ou_sender",
                    "user_id": "user-id",
                    "union_id": "union-id",
                },
                "sender_type": "user",
                "tenant_key": "tenant-key",
            },
            "message": {
                "message_id": "om_message",
                "root_id": "om_root",
                "parent_id": "om_parent",
                "create_time": "1787356800000",
                "chat_id": "oc_chat",
                "thread_id": "omt_thread",
                "chat_type": chat_type,
                "message_type": message_type,
                "content": json.dumps(content or {"text": "@_user_1 hello"}),
                "mentions": [
                    {
                        "key": "@_user_1",
                        "id": {"open_id": "ou_bot"},
                        "name": "Agent Bot",
                    }
                ],
            },
        },
    }


def encrypted_request(
    payload: dict[str, object], *, timestamp: str | None = None
) -> WebhookRequest:
    plaintext = json.dumps(payload, separators=(",", ":")).encode()
    iv = bytes(range(16))
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    key = hashlib.sha256(ENCRYPT_KEY.encode()).digest()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    encrypted = base64.b64encode(iv + encryptor.update(padded) + encryptor.finalize()).decode()
    body = json.dumps({"encrypt": encrypted}, separators=(",", ":")).encode()
    timestamp = timestamp or str(int(time.time()))
    nonce = "nonce"
    signature = hashlib.sha256(timestamp.encode() + nonce.encode() + ENCRYPT_KEY.encode() + body)
    return WebhookRequest(
        method="POST",
        headers={
            "X-Lark-Request-Timestamp": timestamp,
            "X-Lark-Request-Nonce": nonce,
            "X-Lark-Signature": signature.hexdigest(),
        },
        body=body,
    )


def signed_encrypted_body(
    encrypt_value: object,
    *,
    key: str = ENCRYPT_KEY,
    timestamp: str | None = None,
) -> WebhookRequest:
    body = json.dumps({"encrypt": encrypt_value}, separators=(",", ":")).encode()
    timestamp = timestamp or str(int(time.time()))
    nonce = "nonce"
    signature = hashlib.sha256(timestamp.encode() + nonce.encode() + key.encode() + body)
    return WebhookRequest(
        method="POST",
        headers={
            "X-Lark-Request-Timestamp": timestamp,
            "X-Lark-Request-Nonce": nonce,
            "X-Lark-Signature": signature.hexdigest(),
        },
        body=body,
    )


def outbound(*, reply_to: str | None = "om_message") -> OutboundEnvelope:
    return OutboundEnvelope(
        outbound_id="0a57ad9b-7c77-4f00-b34e-9b2acdc47ec1",
        tenant_id="tenant",
        binding_id="feishu-binding",
        channel=Channel.FEISHU,
        target_id="oc_chat",
        session_id="session",
        payload_kind=PayloadKind.TEXT,
        text="agent reply",
        in_reply_to=reply_to,
    )


def test_plain_challenge_and_message_normalization() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    challenge = adapter.verify_and_parse(
        WebhookRequest(
            method="POST",
            body=json.dumps(
                {
                    "challenge": "challenge-value",
                    "token": VERIFICATION_TOKEN,
                    "type": "url_verification",
                }
            ).encode(),
        ),
        feishu_binding(encrypted=False),
    )
    assert challenge.challenge == "challenge-value"
    assert challenge.envelope is None
    assert challenge.acknowledgement == {"challenge": "challenge-value"}

    parsed = adapter.verify_and_parse(
        WebhookRequest(method="POST", body=json.dumps(event_payload()).encode()),
        feishu_binding(encrypted=False),
    )
    assert parsed.challenge is None and parsed.envelope is not None
    envelope = parsed.envelope
    assert envelope.channel == Channel.FEISHU
    assert envelope.account_id == APP_ID
    assert envelope.external_message_id == "om_message"
    assert envelope.external_user_id == "ou_sender"
    assert envelope.conversation_kind == ConversationKind.GROUP
    assert envelope.external_conversation_id == "oc_chat"
    assert envelope.payload_kind == PayloadKind.TEXT
    assert envelope.text == "hello"
    assert envelope.provider_metadata["target_id"] == "oc_chat"
    assert "content" not in envelope.provider_metadata


def test_unsigned_encrypted_challenge_is_challenge_scoped() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    request = encrypted_request(
        {
            "challenge": "real-console-challenge",
            "token": VERIFICATION_TOKEN,
            "type": "url_verification",
        }
    )
    unsigned = WebhookRequest(method="POST", body=request.body)

    callback = adapter.verify_and_parse(unsigned, feishu_binding())

    assert callback.acknowledgement == {"challenge": "real-console-challenge"}

    invalid_token = encrypted_request(
        {
            "challenge": "real-console-challenge",
            "token": "wrong-verification-token",
            "type": "url_verification",
        }
    )
    with pytest.raises(FeishuVerificationError, match="token"):
        adapter.verify_and_parse(
            WebhookRequest(method="POST", body=invalid_token.body), feishu_binding()
        )

    plaintext = json.dumps(
        {
            "challenge": "plain-console-challenge",
            "token": VERIFICATION_TOKEN,
            "type": "url_verification",
        }
    ).encode()
    with pytest.raises(FeishuVerificationError, match="authentication"):
        adapter.verify_and_parse(WebhookRequest(method="POST", body=plaintext), feishu_binding())


def test_encrypted_callback_signature_and_media_normalization() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    parsed = adapter.verify_and_parse(
        encrypted_request(
            event_payload(
                message_type="file",
                content={"file_key": "file-key", "file_name": "report.pdf"},
                chat_type="p2p",
            )
        ),
        feishu_binding(),
    )
    assert parsed.envelope is not None
    assert parsed.envelope.conversation_kind == ConversationKind.DIRECT
    assert parsed.envelope.external_conversation_id is None
    assert parsed.envelope.payload_kind == PayloadKind.FILE
    assert parsed.envelope.media[0].provider_media_id == "file-key"
    assert parsed.envelope.media[0].filename == "report.pdf"

    request = encrypted_request(event_payload())
    invalid = WebhookRequest(
        method=request.method,
        headers={**request.headers, "X-Lark-Signature": "invalid"},
        body=request.body,
    )
    with pytest.raises(FeishuVerificationError, match="signature"):
        adapter.verify_and_parse(invalid, feishu_binding())


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (lambda value: value["header"].update(token="wrong"), "token"),
        (lambda value: value["header"].update(app_id="other"), "binding"),
        (lambda value: value["event"].update(sender={}), "sender"),
        (lambda value: value["event"].update(message={}), "message"),
    ],
)
def test_callback_rejects_untrusted_identity(mutate, error) -> None:
    value = event_payload()
    mutate(value)
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    with pytest.raises(FeishuVerificationError, match=error):
        adapter.verify_and_parse(
            WebhookRequest(method="POST", body=json.dumps(value).encode()),
            feishu_binding(encrypted=False),
        )


@pytest.mark.asyncio
async def test_send_replies_and_caches_tenant_token() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "tenant-token", "expire": 7200},
            )
        assert request.headers["Authorization"] == "Bearer tenant-token"
        body = json.loads(request.content)
        assert body["msg_type"] == "text"
        assert json.loads(body["content"]) == {"text": "agent reply"}
        assert body["uuid"] == outbound().outbound_id
        return httpx.Response(200, json={"code": 0, "data": {"message_id": "om_reply"}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(
        LocalSecretProvider(allow_literal=True),
        http_client=client,
    )
    reply = await adapter.send(outbound(), feishu_binding())
    created = await adapter.send(outbound(reply_to=None), feishu_binding())
    assert reply.status == DeliveryStatus.DELIVERED
    assert reply.provider_message_id == "om_reply"
    assert created.status == DeliveryStatus.DELIVERED
    assert sum(call.url.path.endswith("/tenant_access_token/internal") for call in calls) == 1
    assert "/om_message/reply" in calls[1].url.path
    assert calls[2].url.params["receive_id_type"] == "chat_id"
    await adapter.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler", "status", "code", "retryable"),
    [
        (
            lambda request: (_ for _ in ()).throw(httpx.ReadTimeout("timeout", request=request)),
            DeliveryStatus.AMBIGUOUS,
            "transport_unknown",
            False,
        ),
        (
            lambda request: httpx.Response(429, json={"code": 99991400}, request=request),
            DeliveryStatus.FAILED,
            "99991400",
            True,
        ),
        (
            lambda request: httpx.Response(200, json={"code": 230027}, request=request),
            DeliveryStatus.FAILED,
            "230027",
            False,
        ),
    ],
)
async def test_delivery_error_mapping(handler, status, code, retryable) -> None:
    calls = 0

    def transport(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        return handler(request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    receipt = await adapter.send(outbound(), feishu_binding())
    assert (receipt.status, receipt.provider_code, receipt.retryable) == (
        status,
        code,
        retryable,
    )
    await adapter.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, content=b"not-json"),
        httpx.Response(200, json=["not", "an", "object"]),
        httpx.Response(503, content=b"not-json"),
    ],
)
async def test_delivery_receipt_rejects_invalid_provider_payload(response) -> None:
    calls = 0

    def transport(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        response.request = request
        return response

    client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    receipt = await adapter.send(outbound(), feishu_binding())
    assert receipt.provider_code == "invalid_response"
    assert receipt.retryable is (response.status_code >= 500)


@pytest.mark.asyncio
async def test_feishu_duplicate_and_cross_tenant_isolation() -> None:
    repository = InMemoryRuntimeRepository()
    for tenant, binding_id in (("tenant-a", "binding-a"), ("tenant-b", "binding-b")):
        config = TenantConfig(
            tenant_id=tenant,
            app_id="support",
            version=1,
            model=ModelPolicy(provider="fake", model="fake"),
            storage=StorageSelection(profile_id="default"),
        )
        binding = feishu_binding().model_copy(
            update={"tenant_id": tenant, "binding_id": binding_id}
        )
        repository.add_config(config)
        repository.add_route(BindingRoute(binding=binding, active_config_version=1))

    runtime = TenantRuntime(repository, routing_key=b"r" * 32)
    envelope = InboundEnvelope(
        channel=Channel.FEISHU,
        account_id=APP_ID,
        external_message_id="same-message",
        external_user_id="same-user",
        conversation_kind=ConversationKind.DIRECT,
        payload_kind=PayloadKind.TEXT,
        text="hello",
    )
    first = await runtime.accept("binding-a", envelope)
    duplicate = await runtime.accept("binding-a", envelope)
    other = await runtime.accept("binding-b", envelope)

    assert not first.duplicate and duplicate.duplicate
    assert other.context.tenant_id == "tenant-b"
    assert first.context.session_id != other.context.session_id


@pytest.mark.parametrize(
    ("webhook", "binding", "match"),
    [
        (WebhookRequest(method="GET", body=b"{}"), feishu_binding(encrypted=False), "method"),
        (
            WebhookRequest(method="POST", body=b"{}"),
            feishu_binding(encrypted=False).model_copy(update={"channel": Channel.WECOM_AI_BOT}),
            "binding",
        ),
        (WebhookRequest(method="POST", body=b""), feishu_binding(encrypted=False), "body"),
    ],
)
def test_callback_rejects_method_binding_and_empty_body(webhook, binding, match) -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    with pytest.raises(FeishuVerificationError, match=match):
        adapter.verify_and_parse(webhook, binding)

    oversized = FeishuAdapter(LocalSecretProvider(allow_literal=True), max_callback_bytes=1024)
    with pytest.raises(FeishuVerificationError, match="body"):
        oversized.verify_and_parse(
            WebhookRequest(method="POST", body=b"x" * 1025),
            feishu_binding(encrypted=False),
        )


def test_adapter_constructor_secret_resolution_and_json_errors() -> None:
    with pytest.raises(ValueError, match="refresh skew"):
        FeishuAdapter(LocalSecretProvider(allow_literal=True), refresh_skew_seconds=-1)
    with pytest.raises(ValueError, match="body limit"):
        FeishuAdapter(LocalSecretProvider(allow_literal=True), max_callback_bytes=1023)
    with pytest.raises(ValueError, match="age limit"):
        FeishuAdapter(LocalSecretProvider(allow_literal=True), max_callback_age_seconds=0)

    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    missing_token = feishu_binding(encrypted=False).model_copy(update={"secret_refs": {}})
    with pytest.raises(FeishuVerificationError, match="verification token"):
        adapter.verify_and_parse(
            WebhookRequest(method="POST", body=json.dumps(event_payload()).encode()),
            missing_token,
        )

    for body in (b"not-json", b"[]"):
        with pytest.raises(FeishuVerificationError, match="JSON"):
            adapter.verify_and_parse(
                WebhookRequest(method="POST", body=body), feishu_binding(encrypted=False)
            )

    invalid_challenge = event_payload()
    invalid_challenge["header"]["event_type"] = "url_verification"
    invalid_challenge["challenge"] = ""
    with pytest.raises(FeishuVerificationError, match="challenge"):
        adapter.verify_and_parse(
            WebhookRequest(method="POST", body=json.dumps(invalid_challenge).encode()),
            feishu_binding(encrypted=False),
        )

    with pytest.raises(FeishuVerificationError, match="signature"):
        adapter.verify_and_parse(
            WebhookRequest(method="POST", body=json.dumps(event_payload()).encode()),
            feishu_binding(),
        )


class _BrokenSecrets:
    def resolve(self, secret_ref: SecretRef) -> str:
        raise RuntimeError("must not expose secret")


class _EmptySecrets:
    def resolve(self, secret_ref: SecretRef) -> str:
        return ""


def test_secret_provider_errors_are_sanitized() -> None:
    request = WebhookRequest(method="POST", body=json.dumps(event_payload()).encode())
    with pytest.raises(FeishuVerificationError, match="verification token"):
        FeishuAdapter(_BrokenSecrets()).verify_and_parse(request, feishu_binding(encrypted=False))
    with pytest.raises(FeishuVerificationError, match="verification token"):
        FeishuAdapter(_EmptySecrets()).verify_and_parse(request, feishu_binding(encrypted=False))


def test_encrypted_payload_rejects_invalid_ciphertext_and_signature() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    with pytest.raises(FeishuVerificationError, match="configuration"):
        adapter.verify_and_parse(
            WebhookRequest(method="POST", body=b'{"encrypt": 1}'), feishu_binding()
        )

    for encrypt_value in ("not-base64", base64.b64encode(b"too-short").decode()):
        with pytest.raises(FeishuVerificationError, match="encrypted callback"):
            adapter.verify_and_parse(signed_encrypted_body(encrypt_value), feishu_binding())

    malformed = signed_encrypted_body("!!!")
    with pytest.raises(FeishuVerificationError, match="encrypted callback"):
        adapter.verify_and_parse(malformed, feishu_binding())

    signed_message = encrypted_request(event_payload())
    missing_headers = WebhookRequest(method="POST", body=signed_message.body)
    with pytest.raises(FeishuVerificationError, match="signature"):
        adapter.verify_and_parse(missing_headers, feishu_binding())

    strict_time = FeishuAdapter(
        LocalSecretProvider(allow_literal=True),
        wall_clock=lambda: 1_000,
        max_callback_age_seconds=300,
    )
    with pytest.raises(FeishuVerificationError, match="timestamp is stale"):
        strict_time.verify_and_parse(
            signed_encrypted_body("not-base64", timestamp="1"), feishu_binding()
        )
    with pytest.raises(FeishuVerificationError, match="timestamp is invalid"):
        strict_time.verify_and_parse(
            signed_encrypted_body("not-base64", timestamp="invalid"), feishu_binding()
        )


def test_non_message_events_and_sender_messages_are_safe() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    unsupported = event_payload()
    unsupported["header"]["event_type"] = "im.chat.member.bot.added"
    callback = adapter.verify_and_parse(
        WebhookRequest(method="POST", body=json.dumps(unsupported).encode()),
        feishu_binding(encrypted=False),
    )
    assert callback.envelope is None and callback.acknowledgement == {"msg": "success"}

    bot = event_payload()
    bot["event"]["sender"]["sender_type"] = "bot"
    callback = adapter.verify_and_parse(
        WebhookRequest(method="POST", body=json.dumps(bot).encode()),
        feishu_binding(encrypted=False),
    )
    assert callback.envelope is None


@pytest.mark.parametrize(
    ("message_type", "content", "expected"),
    [
        ("image", {"image_key": "img-key"}, PayloadKind.IMAGE),
        ("sticker", {"file_key": "sticker-key"}, PayloadKind.IMAGE),
        ("audio", {"file_key": "audio-key"}, PayloadKind.VOICE),
        ("media", {"file_key": "video-key"}, PayloadKind.VIDEO),
        ("unknown", {"anything": "ignored"}, PayloadKind.EVENT),
    ],
)
def test_media_and_unknown_message_normalization(message_type, content, expected) -> None:
    payload = event_payload(message_type=message_type, content=content)
    callback = FeishuAdapter(LocalSecretProvider(allow_literal=True)).verify_and_parse(
        WebhookRequest(method="POST", body=json.dumps(payload).encode()),
        feishu_binding(encrypted=False),
    )
    assert callback.envelope is not None
    assert callback.envelope.payload_kind == expected
    if expected == PayloadKind.EVENT:
        assert callback.envelope.event_type == "unknown"
        assert callback.envelope.media == ()
    else:
        assert callback.envelope.media[0].provider_media_id in content.values()


def test_post_mentions_and_content_shape_validation() -> None:
    post = event_payload(
        message_type="post",
        content={
            "title": "  Title ",
            "content": [[{"tag": "text", "text": " first "}], {"text": "second"}],
        },
    )
    callback = FeishuAdapter(LocalSecretProvider(allow_literal=True)).verify_and_parse(
        WebhookRequest(method="POST", body=json.dumps(post).encode()),
        feishu_binding(encrypted=False),
    )
    assert callback.envelope is not None
    assert callback.envelope.payload_kind == PayloadKind.MIXED
    assert callback.envelope.text == "Title\nfirst\nsecond"

    mention_variants = event_payload(content={"text": "@_user_1 hello"})
    mention_variants["event"]["message"]["mentions"] = [{"key": 123}, "not-a-map"]
    callback = FeishuAdapter(LocalSecretProvider(allow_literal=True)).verify_and_parse(
        WebhookRequest(method="POST", body=json.dumps(mention_variants).encode()),
        feishu_binding(encrypted=False),
    )
    assert callback.envelope is not None and callback.envelope.text == "@_user_1 hello"

    invalid_text = event_payload(content={"text": 123})
    with pytest.raises(FeishuVerificationError, match="content"):
        FeishuAdapter(LocalSecretProvider(allow_literal=True)).verify_and_parse(
            WebhookRequest(method="POST", body=json.dumps(invalid_text).encode()),
            feishu_binding(encrypted=False),
        )

    invalid_raw = event_payload()
    invalid_raw["event"]["message"]["content"] = {"text": "hello"}
    with pytest.raises(FeishuVerificationError, match="content"):
        FeishuAdapter(LocalSecretProvider(allow_literal=True)).verify_and_parse(
            WebhookRequest(method="POST", body=json.dumps(invalid_raw).encode()),
            feishu_binding(encrypted=False),
        )

    invalid_media = event_payload(message_type="image", content={})
    with pytest.raises(FeishuVerificationError, match="media"):
        FeishuAdapter(LocalSecretProvider(allow_literal=True)).verify_and_parse(
            WebhookRequest(method="POST", body=json.dumps(invalid_media).encode()),
            feishu_binding(encrypted=False),
        )


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda payload: payload["event"].update(sender={}), "sender"),
        (lambda payload: payload["event"]["sender"].update(sender_id={}), "sender"),
        (lambda payload: payload["event"]["sender"]["sender_id"].update(open_id=""), "sender"),
        (lambda payload: payload["event"]["message"].update(content="{}"), "message"),
    ],
)
def test_callback_rejects_malformed_message_parts(mutate, match) -> None:
    payload = event_payload()
    mutate(payload)
    with pytest.raises(FeishuVerificationError, match=match):
        FeishuAdapter(LocalSecretProvider(allow_literal=True)).verify_and_parse(
            WebhookRequest(method="POST", body=json.dumps(payload).encode()),
            feishu_binding(encrypted=False),
        )


def test_private_normalization_helpers_cover_safe_fallbacks() -> None:
    assert _event_type({"header": {"event_type": "event"}}) == "event"
    assert _event_type({"header": {"event_type": 1}}) is None
    assert _event_type({"type": "legacy"}) == "legacy"
    assert _event_type({"type": 1}) is None
    assert _receive_id_type("ou_user") == "open_id"
    assert _receive_id_type("oc_chat") == "chat_id"
    assert _flatten_post({"title": "", "content": [{"tag": "a"}]}) is None
    assert _flatten_post({"content": [{"text": "a"}, {"text": "  "}]}) == "a"
    assert _timestamp("not-a-time")
    assert _timestamp("999999999999999999999999")
    assert _integer(True, default=7) == 7
    assert _integer(object(), default=7) == 7
    assert _integer("not-an-int", default=7) == 7
    assert _integer("4", default=7) == 4
    assert _normalize_content("text", {"text": "x"}, [None])[1] == "x"
    with pytest.raises(FeishuVerificationError, match="JSON"):
        _json_object(b"[]")
    with pytest.raises(FeishuVerificationError, match="shape"):
        _mapping([], "shape")


@pytest.mark.asyncio
async def test_send_rejects_mismatch_and_unsupported_payload() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    mismatch = await adapter.send(
        outbound().model_copy(update={"channel": Channel.WECOM_AI_BOT}), feishu_binding()
    )
    unsupported = await adapter.send(
        outbound().model_copy(update={"payload_kind": PayloadKind.IMAGE}), feishu_binding()
    )
    assert mismatch.provider_code == "binding_mismatch"
    assert unsupported.provider_code == "unsupported_payload"


class _TransportErrorClient:
    def __init__(self, error: Exception) -> None:
        self.error = error

    async def post(self, *args, **kwargs):
        raise self.error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("binding", "provider_code", "retryable"),
    [
        (feishu_binding().model_copy(update={"secret_refs": {}}), "token_not_configured", False),
        (
            feishu_binding().model_copy(
                update={"secret_refs": {"verification_token": SecretRef(uri="literal://x")}}
            ),
            "token_not_configured",
            False,
        ),
    ],
)
async def test_send_token_not_configured(binding, provider_code, retryable) -> None:
    receipt = await FeishuAdapter(LocalSecretProvider(allow_literal=True)).send(outbound(), binding)
    assert (receipt.provider_code, receipt.retryable) == (provider_code, retryable)


@pytest.mark.asyncio
async def test_send_token_secret_and_response_errors() -> None:
    for secrets in (_BrokenSecrets(), _EmptySecrets()):
        receipt = await FeishuAdapter(secrets).send(outbound(), feishu_binding())
        assert receipt.provider_code == "token_secret_unavailable"
        assert receipt.retryable

    cases = [
        (httpx.ReadTimeout("timeout"), "token_transport", True),
        (httpx.HTTPError("protocol"), "token_invalid_response", True),
    ]
    for error, provider_code, retryable in cases:
        client = _TransportErrorClient(error)
        receipt = await FeishuAdapter(
            LocalSecretProvider(allow_literal=True), http_client=client
        ).send(outbound(), feishu_binding())
        assert (receipt.provider_code, receipt.retryable) == (provider_code, retryable)

    def invalid_json(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json", request=request)

    def list_json(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[1], request=request)

    for handler in (invalid_json, list_json):
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        receipt = await FeishuAdapter(
            LocalSecretProvider(allow_literal=True), http_client=client
        ).send(outbound(), feishu_binding())
        assert receipt.provider_code == "token_invalid_response"


@pytest.mark.asyncio
async def test_send_token_failures_and_expiry_are_mapped() -> None:
    responses = [
        httpx.Response(429, json={"code": 99991400}),
        httpx.Response(500, json={"code": 99}),
        httpx.Response(200, json={"code": 99}),
        httpx.Response(200, json={"code": 0, "tenant_access_token": "", "expire": 7200}),
        httpx.Response(200, json={"code": 0, "tenant_access_token": "token", "expire": 0}),
        httpx.Response(200, json={"code": 0, "tenant_access_token": "token", "expire": True}),
    ]
    for response in responses:

        def handler(request: httpx.Request, response=response) -> httpx.Response:
            return response

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        receipt = await FeishuAdapter(
            LocalSecretProvider(allow_literal=True), http_client=client
        ).send(outbound(), feishu_binding())
        if response.status_code == 429:
            assert receipt.provider_code == "token_99991400" and receipt.retryable
        elif response.status_code >= 500:
            assert receipt.provider_code == "token_99" and receipt.retryable
        elif response.json().get("code"):
            assert receipt.provider_code == "token_99" and not receipt.retryable
        else:
            assert receipt.provider_code == "token_invalid_response" and receipt.retryable


@pytest.mark.asyncio
async def test_send_token_rate_limit_code_preserves_json_retry_after() -> None:
    response = httpx.Response(
        200,
        json={"code": 99991400, "retry_after": 7},
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response))
    receipt = await FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client).send(
        outbound(), feishu_binding()
    )

    assert receipt.status == DeliveryStatus.FAILED
    assert receipt.provider_code == "token_99991400"
    assert receipt.retryable is True
    assert receipt.retry_after_seconds == 7.0
    await client.aclose()


@pytest.mark.asyncio
async def test_send_refreshes_invalid_token_and_handles_retry_outcomes() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.path.endswith("/tenant_access_token/internal"):
            token = "token-1" if calls == 1 else "token-2"
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": token, "expire": 7200},
                request=request,
            )
        if calls == 2:
            return httpx.Response(401, json={"code": 99991663}, request=request)
        return httpx.Response(200, json={"code": 0}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    receipt = await adapter.send(outbound(), feishu_binding())
    assert receipt.status == DeliveryStatus.DELIVERED and receipt.attempts == 2
    assert calls == 4
    adapter._tokens[
        ("feishu-binding", APP_ID, hashlib.sha256(f"literal://{APP_SECRET}".encode()).hexdigest())
    ] = ("stale", 9999999999)
    adapter._invalidate_token(feishu_binding())
    assert not adapter._tokens


@pytest.mark.asyncio
async def test_send_refresh_failure_and_ambiguous_retry() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        return httpx.Response(401, json={"code": 99991663}, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    receipt = await adapter.send(outbound(), feishu_binding())
    assert receipt.provider_code == "token_99991663"

    calls = 0

    def timeout_handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        raise httpx.ReadTimeout("timeout", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(timeout_handler))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    receipt = await adapter.send(outbound(), feishu_binding())
    assert receipt.status == DeliveryStatus.AMBIGUOUS and receipt.attempts == 1

    calls = 0

    def refresh_timeout_handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls in {1, 3}:
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "tenant_access_token": f"token-{calls}",
                    "expire": 7200,
                },
                request=request,
            )
        if calls == 2:
            return httpx.Response(401, json={"code": 99991663}, request=request)
        raise httpx.ReadTimeout("timeout", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(refresh_timeout_handler))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    receipt = await adapter.send(outbound(), feishu_binding())
    assert receipt.status == DeliveryStatus.AMBIGUOUS and receipt.attempts == 2


@pytest.mark.asyncio
async def test_token_cache_inner_lock_and_owned_client_close() -> None:
    now = 100.0
    client = _TransportErrorClient(httpx.ReadTimeout("not expected"))
    adapter = FeishuAdapter(
        LocalSecretProvider(allow_literal=True),
        http_client=client,
        clock=lambda: now,
        refresh_skew_seconds=0,
    )
    key = (
        "feishu-binding",
        APP_ID,
        hashlib.sha256(f"literal://{APP_SECRET}".encode()).hexdigest(),
    )
    lock = adapter._token_locks.setdefault(key, __import__("asyncio").Lock())
    await lock.acquire()
    task = asyncio.create_task(adapter._token_for(feishu_binding()))
    await asyncio.sleep(0)
    adapter._tokens[key] = ("cached", 200.0)
    lock.release()
    assert await task == "cached"

    owned = FeishuAdapter(LocalSecretProvider(allow_literal=True))
    await owned.close()


@pytest.mark.asyncio
async def test_downloads_image_and_file_with_metadata_and_cached_token() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        if request.url.params.get("type") == "image":
            return httpx.Response(
                200,
                content=b"\x89PNG",
                headers={
                    "Content-Type": "image/png; charset=binary",
                    "Content-Disposition": "inline; filename*=UTF-8''photo%20one.png",
                    "Content-Length": "4",
                },
                request=request,
            )
        return httpx.Response(
            200,
            content=b"PDF",
            headers={"Content-Type": "application/pdf", "Content-Length": "3"},
            request=request,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    image = await adapter.download_media(
        feishu_binding(), "om_message", "img-key", media_type="image"
    )
    file = await adapter.download_resource(
        feishu_binding(), "om_message", "file-key", resource_type="file", filename="report.pdf"
    )

    assert image.bytes == b"\x89PNG"
    assert image.content_type == "image/png"
    assert image.filename == "photo one.png"
    assert image.provider_media_id == "img-key"
    assert image.data == image.bytes and image.provider_id == image.provider_media_id
    assert file.bytes == b"PDF"
    assert file.content_type == "application/pdf"
    assert file.filename == "report.pdf"
    assert len([request for request in calls if request.url.path.endswith("internal")]) == 1
    resource_requests = [request for request in calls if "/resources/" in request.url.path]
    assert resource_requests[0].url.params["type"] == "image"
    assert resource_requests[1].url.params["type"] == "file"
    assert all(request.headers["Authorization"] == "Bearer token" for request in resource_requests)
    await adapter.close()
    assert not client.is_closed
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "provider_code", "retryable"),
    [
        (
            httpx.Response(200, content=b"012345", headers={"Content-Length": "6"}),
            "media_too_large",
            False,
        ),
        (httpx.Response(404, json={"code": 99991400}), "resource_not_found", False),
        (httpx.Response(429, json={"code": 99991400}), "rate_limited", True),
    ],
)
async def test_download_maps_size_and_provider_errors(
    response: httpx.Response, provider_code: str, retryable: bool
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        response.request = request
        return response

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(
        LocalSecretProvider(allow_literal=True), http_client=client, max_media_bytes=5
    )
    with pytest.raises(FeishuResourceError) as caught:
        await adapter.download_resource(feishu_binding(), "om_message", "file-key")
    assert caught.value.provider_code == provider_code
    assert caught.value.retryable is retryable
    await client.aclose()


@pytest.mark.asyncio
async def test_download_enforces_actual_size_without_content_length() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        return httpx.Response(
            200,
            content=b"012345",
            headers={"Content-Length": "1", "Content-Type": "application/octet-stream"},
            request=request,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(
        LocalSecretProvider(allow_literal=True), http_client=client, max_media_bytes=5
    )
    with pytest.raises(FeishuResourceError) as caught:
        await adapter.download_resource(feishu_binding(), "om_message", "file-key")
    assert caught.value.provider_code == "media_too_large"
    assert not caught.value.retryable and not caught.value.ambiguous
    await client.aclose()


@pytest.mark.asyncio
async def test_download_refreshes_invalid_token_once() -> None:
    calls = 0
    resource_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.path.endswith("/tenant_access_token/internal"):
            token = "token-1" if calls == 1 else "token-2"
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": token, "expire": 7200},
                request=request,
            )
        resource_requests.append(request)
        if calls == 2:
            return httpx.Response(401, json={"code": 99991663}, request=request)
        return httpx.Response(
            200,
            content=b"01234",
            headers={"Content-Length": "5", "Content-Type": "application/octet-stream"},
            request=request,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(
        LocalSecretProvider(allow_literal=True), http_client=client, max_media_bytes=5
    )
    result = await adapter.download_resource(feishu_binding(), "om_message", "file-key")
    assert result.bytes == b"01234"
    assert [request.headers["Authorization"] for request in resource_requests] == [
        "Bearer token-1",
        "Bearer token-2",
    ]
    assert calls == 4
    await client.aclose()


@pytest.mark.asyncio
async def test_download_refreshes_for_json_token_error_response() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": f"token-{calls}", "expire": 7200},
                request=request,
            )
        if calls == 2:
            return httpx.Response(
                200,
                json={"code": 99991663, "msg": "token expired"},
                request=request,
            )
        return httpx.Response(200, content=b"ok", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    result = await adapter.download_resource(feishu_binding(), "om_message", "file-key")
    assert result.bytes == b"ok"
    assert calls == 4
    await client.aclose()


@pytest.mark.asyncio
async def test_download_transport_timeout_is_ambiguous_and_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tenant_access_token/internal"):
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "token", "expire": 7200},
                request=request,
            )
        raise httpx.ReadTimeout("timeout", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True), http_client=client)
    with pytest.raises(FeishuResourceError) as caught:
        await adapter.download_resource(feishu_binding(), "om_message", "file-key")
    assert caught.value.provider_code == "transport_timeout"
    assert caught.value.retryable and caught.value.ambiguous
    await client.aclose()
