"""Binding, normalization, and control-routing tests for WeCom."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from tests.channels.helpers import IDENTITY_KEY, binding, callback_request, load_json
from trpc_service.channels.contracts import (
    AttachmentKind,
    CallbackKind,
    Channel,
    ChannelAdapter,
    ConversationKind,
    ReplyIntent,
    ReplyKind,
    SensitiveReplyRouteKind,
    SensitiveRouteChannelAdapter,
    TrustedBindingContext,
)
from trpc_service.channels.session import ChannelIdentityDeriver
from trpc_service.channels.wecom import (
    WeComAdapter,
    WeComCallbackError,
    WeComCrypto,
)


@pytest.fixture
def vector() -> dict[str, Any]:
    return load_json("wecom_vectors.json")


@pytest.fixture
def crypto(vector: Mapping[str, Any]) -> WeComCrypto:
    prefix = bytes.fromhex(str(vector["random_prefix_hex"]))
    return WeComCrypto(
        str(vector["token"]),
        str(vector["encoding_aes_key"]),
        random_source=lambda size: prefix,
    )


@pytest.fixture
def adapter(crypto: WeComCrypto) -> WeComAdapter:
    return WeComAdapter(crypto, ChannelIdentityDeriver(IDENTITY_KEY))


def _query(vector: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    return (
        ("msg_signature", str(vector["signature"])),
        ("timestamp", str(vector["timestamp"])),
        ("nonce", str(vector["nonce"])),
    )


def _encrypted_request(
    crypto: WeComCrypto,
    payload: Mapping[str, Any],
    *,
    timestamp: str = "1787932800",
    nonce: str = "nonce-dynamic",
):
    plaintext = json.dumps(
        dict(payload),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    ciphertext = crypto.encrypt_plaintext(plaintext)
    signature = crypto.signature(
        timestamp=timestamp,
        nonce=nonce,
        ciphertext=ciphertext,
    )
    body = json.dumps({"encrypt": ciphertext}, separators=(",", ":")).encode()
    return callback_request(
        body,
        query=(
            ("msg_signature", signature),
            ("timestamp", timestamp),
            ("nonce", nonce),
        ),
    )


def _base_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "msgid": "msg-001",
        "aibotid": "bot-001",
        "chattype": "single",
        "from": {"userid": "external-user-secret"},
        "response_url": "https://qyapi.weixin.qq.com/secret-response-url",
        "msgtype": "text",
        "text": {"content": "hello"},
    }
    payload.update(overrides)
    return payload


def test_static_callback_normalizes_without_leaking_secrets(
    adapter: WeComAdapter,
    vector: Mapping[str, Any],
) -> None:
    request = callback_request(
        str(vector["callback_body"]).encode(),
        query=_query(vector),
    )

    result = adapter.verify_and_normalize(request, binding(Channel.WECOM))

    assert isinstance(adapter, ChannelAdapter)
    assert result.kind is CallbackKind.USER_MESSAGE
    assert result.inbound is not None
    assert result.inbound.delivery_id == "msg-vector-001"
    assert result.inbound.text == vector["payload"]["text"]["content"]
    assert result.inbound.conversation_kind is ConversationKind.PRIVATE
    serialized = result.inbound.model_dump_json()
    assert "user-001" not in serialized
    assert "response_code" not in serialized
    assert str(vector["ciphertext"]) not in serialized


def test_sensitive_response_route_is_separate_masked_and_short_lived(
    adapter: WeComAdapter,
    vector: Mapping[str, Any],
) -> None:
    request = callback_request(
        str(vector["callback_body"]).encode(),
        query=_query(vector),
    )

    callback, route = adapter.verify_decrypt_and_normalize(
        request,
        binding(Channel.WECOM),
    )

    assert isinstance(adapter, SensitiveRouteChannelAdapter)
    assert callback.inbound is not None
    assert route is not None
    assert route.kind is SensitiveReplyRouteKind.WECOM_RESPONSE_URL
    assert route.route_key == callback.inbound.reply_route_key
    assert route.expires_at == request.received_at + timedelta(hours=1)
    assert route.max_uses == 1
    raw_route = str(vector["payload"]["response_url"])
    assert route.value.get_secret_value() == raw_route
    assert raw_route not in repr(route)
    assert raw_route not in route.model_dump_json()
    assert raw_route not in repr(route.model_dump())


def test_callback_timestamp_outside_replay_window_is_rejected(
    adapter: WeComAdapter,
    vector: Mapping[str, Any],
) -> None:
    request = callback_request(
        str(vector["callback_body"]).encode(),
        query=(
            ("msg_signature", str(vector["signature"])),
            ("timestamp", str(vector["timestamp"])),
            ("nonce", str(vector["nonce"])),
        ),
    ).model_copy(update={"received_at": datetime(2026, 9, 1, tzinfo=UTC)})

    with pytest.raises(WeComCallbackError, match="replay window"):
        adapter.verify_decrypt_and_normalize(request, binding(Channel.WECOM))


def test_get_url_validation(adapter: WeComAdapter, vector: Mapping[str, Any]) -> None:
    request = callback_request(
        b"",
        query=(*_query(vector), ("echostr", str(vector["ciphertext"]))),
    )

    assert adapter.verify_url(request, binding(Channel.WECOM)) == str(vector["plaintext"]).encode()


@pytest.mark.parametrize(
    "bad_binding",
    [
        binding(Channel.WECOM, enabled=False),
        binding(Channel.TELEGRAM),
        binding(Channel.WECOM, binding_id="other"),
    ],
)
def test_binding_state_channel_and_path_are_enforced(
    adapter: WeComAdapter,
    vector: Mapping[str, Any],
    bad_binding: TrustedBindingContext,
) -> None:
    request = callback_request(
        str(vector["callback_body"]).encode(),
        query=_query(vector),
    )

    with pytest.raises(WeComCallbackError, match="binding"):
        adapter.verify_and_normalize(request, bad_binding)


def test_decrypted_account_must_match_binding(
    adapter: WeComAdapter,
    vector: Mapping[str, Any],
) -> None:
    request = callback_request(
        str(vector["callback_body"]).encode(),
        query=_query(vector),
    )

    with pytest.raises(WeComCallbackError, match="account"):
        adapter.verify_and_normalize(
            request,
            binding(Channel.WECOM, account_id="different-bot"),
        )


def test_group_chat_and_voice_normalization(
    adapter: WeComAdapter,
    crypto: WeComCrypto,
) -> None:
    request = _encrypted_request(
        crypto,
        _base_payload(
            chattype="group",
            chatid="external-group-secret",
            msgtype="voice",
            voice={"content": "voice transcript"},
        ),
    )

    inbound = adapter.verify_and_normalize(
        request,
        binding(Channel.WECOM),
    ).inbound

    assert inbound is not None
    assert inbound.conversation_kind is ConversationKind.GROUP
    assert inbound.text == "voice transcript"
    assert "external-group-secret" not in inbound.model_dump_json()


def test_stream_refresh_is_control_and_never_agent_input(
    adapter: WeComAdapter,
    crypto: WeComCrypto,
) -> None:
    request = _encrypted_request(
        crypto,
        _base_payload(
            msgtype="stream",
            stream={"id": "stream-001"},
        ),
    )

    result = adapter.verify_and_normalize(request, binding(Channel.WECOM))

    assert result.kind is CallbackKind.CONTROL_REFRESH
    assert result.control_id == "stream-001"
    assert result.inbound is None
    callback, route = adapter.verify_decrypt_and_normalize(
        request,
        binding(Channel.WECOM),
    )
    assert callback == result
    assert route is None


def test_event_is_not_agent_input(
    adapter: WeComAdapter,
    crypto: WeComCrypto,
) -> None:
    request = _encrypted_request(
        crypto,
        _base_payload(msgtype="event", event={"eventtype": "enter_chat"}),
    )

    result = adapter.verify_and_normalize(request, binding(Channel.WECOM))

    assert result.kind is CallbackKind.CHANNEL_EVENT
    assert result.inbound is None
    callback, route = adapter.verify_decrypt_and_normalize(
        request,
        binding(Channel.WECOM),
    )
    assert callback == result
    assert route is None


@pytest.mark.parametrize(
    "raw_route",
    [
        "http://qyapi.weixin.qq.com/reply",
        "https://example.invalid/reply",
        "https://user@qyapi.weixin.qq.com/reply",
        "https://qyapi.weixin.qq.com:444/reply",
        "https://qyapi.weixin.qq.com/reply#fragment",
        "https://qyapi.weixin.qq.com:not-a-port/reply",
    ],
)
def test_response_route_is_constrained_to_trusted_https_endpoint(
    adapter: WeComAdapter,
    crypto: WeComCrypto,
    raw_route: str,
) -> None:
    request = _encrypted_request(crypto, _base_payload(response_url=raw_route))

    with pytest.raises(WeComCallbackError) as captured:
        adapter.verify_decrypt_and_normalize(request, binding(Channel.WECOM))

    assert raw_route not in str(captured.value)


def test_user_message_requires_response_route(
    adapter: WeComAdapter,
    crypto: WeComCrypto,
) -> None:
    payload = _base_payload()
    del payload["response_url"]

    with pytest.raises(WeComCallbackError, match="response_url"):
        adapter.verify_decrypt_and_normalize(
            _encrypted_request(crypto, payload),
            binding(Channel.WECOM),
        )


def test_mixed_content_uses_opaque_attachment_locator(
    adapter: WeComAdapter,
    crypto: WeComCrypto,
) -> None:
    temporary_url = "https://ww-aibot-img.example/temporary-secret"
    request = _encrypted_request(
        crypto,
        _base_payload(
            msgtype="mixed",
            mixed={
                "msg_item": [
                    {"msgtype": "text", "text": {"content": "inspect this"}},
                    {"msgtype": "image", "image": {"url": temporary_url}},
                ]
            },
        ),
    )

    inbound = adapter.verify_and_normalize(request, binding(Channel.WECOM)).inbound

    assert inbound is not None
    assert inbound.text == "inspect this"
    assert inbound.attachments[0].kind is AttachmentKind.IMAGE
    assert temporary_url not in inbound.model_dump_json()


@pytest.mark.parametrize(
    ("msg_type", "kind"),
    [
        ("image", AttachmentKind.IMAGE),
        ("file", AttachmentKind.FILE),
        ("video", AttachmentKind.VIDEO),
    ],
)
def test_single_media_types(
    adapter: WeComAdapter,
    crypto: WeComCrypto,
    msg_type: str,
    kind: AttachmentKind,
) -> None:
    request = _encrypted_request(
        crypto,
        _base_payload(
            msgtype=msg_type,
            **{msg_type: {"url": f"https://example.invalid/{msg_type}/secret"}},
        ),
    )

    inbound = adapter.verify_and_normalize(request, binding(Channel.WECOM)).inbound

    assert inbound is not None
    assert inbound.text is None
    assert inbound.attachments[0].kind is kind


@pytest.mark.parametrize(
    "payload",
    [
        _base_payload(chattype="unknown"),
        _base_payload(msgtype="location", location={}),
        _base_payload(msgtype="mixed", mixed={"msg_item": []}),
        _base_payload(msgtype="mixed", mixed={"msg_item": ["bad"]}),
    ],
)
def test_unsupported_or_malformed_messages_are_rejected(
    adapter: WeComAdapter,
    crypto: WeComCrypto,
    payload: Mapping[str, Any],
) -> None:
    request = _encrypted_request(crypto, payload)

    with pytest.raises(WeComCallbackError):
        adapter.verify_and_normalize(request, binding(Channel.WECOM))


def test_missing_or_duplicate_query_parameter_is_rejected(
    adapter: WeComAdapter,
    vector: Mapping[str, Any],
) -> None:
    body = str(vector["callback_body"]).encode()
    missing = callback_request(body, query=(("timestamp", "1"), ("nonce", "n")))
    duplicate = callback_request(
        body,
        query=(*_query(vector), ("nonce", "duplicate")),
    )

    with pytest.raises(WeComCallbackError, match="missing query"):
        adapter.verify_and_normalize(missing, binding(Channel.WECOM))
    with pytest.raises(WeComCallbackError, match="duplicate query"):
        adapter.verify_and_normalize(duplicate, binding(Channel.WECOM))


def test_passive_reply_and_utf8_rendering(
    adapter: WeComAdapter,
    vector: Mapping[str, Any],
) -> None:
    encrypted = adapter.encode_passive_reply(
        {"msgtype": "stream", "stream": {"id": "s", "finish": False, "content": "1"}},
        nonce=str(vector["nonce"]),
        timestamp=str(vector["timestamp"]),
    )
    intent = ReplyIntent(
        intent_id="intent",
        tenant_id="tenant-001",
        binding_id="binding-001",
        session_id="session",
        run_id="run",
        in_reply_to_delivery_id="msg",
        kind=ReplyKind.FINAL,
        text="你" * 10_000,
        idempotency_key="reply:msg",
    )

    assert json.loads(encrypted)["nonce"] == vector["nonce"]
    chunks = adapter.render_text(intent)
    assert "".join(chunks) == intent.text
    assert all(len(chunk.encode()) <= 20_480 for chunk in chunks)


def test_body_limit_is_enforced(
    crypto: WeComCrypto,
) -> None:
    with pytest.raises(ValueError, match="positive"):
        WeComAdapter(crypto, ChannelIdentityDeriver(IDENTITY_KEY), max_body_bytes=0)

    adapter = WeComAdapter(
        crypto,
        ChannelIdentityDeriver(IDENTITY_KEY),
        max_body_bytes=1,
    )
    request = callback_request(
        b"{}",
        query=(("msg_signature", "x"), ("timestamp", "1"), ("nonce", "n")),
    )
    with pytest.raises(WeComCallbackError, match="exceeds"):
        adapter.verify_and_normalize(request, binding(Channel.WECOM))
