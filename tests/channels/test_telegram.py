"""Security and normalization tests for Telegram Bot API webhooks."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import pytest

from tests.channels.helpers import (
    IDENTITY_KEY,
    TELEGRAM_AUTH_VALUE,
    binding,
    callback_request,
    load_json,
)
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
from trpc_service.channels.telegram import (
    TelegramAdapter,
    TelegramAuthenticationError,
    TelegramCallbackError,
    verify_telegram_secret,
)

_TELEGRAM_AUTH_HEADER = "X-Telegram-Bot-Api-Secret-Token"


@pytest.fixture
def updates() -> dict[str, Any]:
    return load_json("telegram_updates.json")


@pytest.fixture
def adapter() -> TelegramAdapter:
    return TelegramAdapter(TELEGRAM_AUTH_VALUE, ChannelIdentityDeriver(IDENTITY_KEY))


def _request(
    payload: Mapping[str, Any],
    *,
    secret: str = TELEGRAM_AUTH_VALUE,
    binding_id: str = "binding-001",
):
    return callback_request(
        json.dumps(dict(payload), separators=(",", ":")).encode(),
        binding_id=binding_id,
        headers=((_TELEGRAM_AUTH_HEADER, secret),),
    )


def _message_update(
    *,
    update_id: int = 1,
    user_id: int = 10,
    chat_id: int = 10,
    chat_type: str = "private",
    text: str | None = "hello",
    **message_fields: Any,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "message_id": 99,
        "date": 1787932800,
        "from": {"id": user_id, "is_bot": False, "first_name": "User"},
        "chat": {"id": chat_id, "type": chat_type},
    }
    if text is not None:
        message["text"] = text
    message.update(message_fields)
    return {"update_id": update_id, "message": message}


def test_private_text_static_fixture(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
) -> None:
    result = adapter.verify_and_normalize(
        _request(updates["private_text"]),
        binding(Channel.TELEGRAM, account_id="999"),
    )

    assert isinstance(adapter, ChannelAdapter)
    assert result.kind is CallbackKind.USER_MESSAGE
    assert result.delivery_id == "1001"
    assert result.inbound is not None
    assert result.inbound.text == "hello agent"
    assert result.inbound.conversation_kind is ConversationKind.PRIVATE
    assert "101" not in result.inbound.principal_id


def test_delivery_context_is_separate_masked_and_encrypted_store_ready(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
) -> None:
    request = _request(updates["private_text"])

    callback, route = adapter.verify_decrypt_and_normalize(
        request,
        binding(Channel.TELEGRAM),
    )

    assert isinstance(adapter, SensitiveRouteChannelAdapter)
    assert callback.inbound is not None
    assert route is not None
    assert route.kind is SensitiveReplyRouteKind.TELEGRAM_DELIVERY_CONTEXT
    assert route.route_key == callback.inbound.reply_route_key
    assert route.expires_at == request.received_at + timedelta(days=7)
    assert route.max_uses is None
    raw_route = route.value.get_secret_value()
    assert json.loads(raw_route) == {
        "chat_id": 101,
        "message_thread_id": None,
        "reply_to_message_id": 11,
    }
    assert raw_route not in repr(route)
    assert raw_route not in route.model_dump_json()
    assert "chat_id" not in callback.inbound.model_dump_json()


def test_group_topic_photo_and_reply_reference(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
) -> None:
    result = adapter.verify_and_normalize(
        _request(updates["group_topic_photo"]),
        binding(Channel.TELEGRAM),
    )

    inbound = result.inbound
    assert inbound is not None
    assert inbound.conversation_kind is ConversationKind.GROUP
    assert inbound.thread_id is not None
    assert inbound.text == "please inspect"
    assert inbound.reply_to is not None
    assert inbound.reply_to.delivery_id.startswith("msg_v1_")
    assert inbound.reply_to.delivery_id != "9"
    assert inbound.attachments[0].kind is AttachmentKind.IMAGE
    assert inbound.attachments[0].size_bytes == 90_000
    serialized = inbound.model_dump_json()
    assert "photo-large-secret-id" not in serialized
    assert "-1001234567890" not in serialized

    _, route = adapter.verify_decrypt_and_normalize(
        _request(updates["group_topic_photo"]),
        binding(Channel.TELEGRAM),
    )
    assert route is not None
    assert json.loads(route.value.get_secret_value()) == {
        "chat_id": -1001234567890,
        "message_thread_id": 77,
        "reply_to_message_id": 12,
    }


def test_callback_query_becomes_explicit_user_input(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
) -> None:
    result = adapter.verify_and_normalize(
        _request(updates["callback_query"]),
        binding(Channel.TELEGRAM),
    )

    assert result.inbound is not None
    assert result.inbound.text == "approve:ticket-7"
    assert result.inbound.conversation_kind is ConversationKind.GROUP

    _, route = adapter.verify_decrypt_and_normalize(
        _request(updates["callback_query"]),
        binding(Channel.TELEGRAM),
    )
    assert route is not None
    assert json.loads(route.value.get_secret_value())["callback_query_id"] == "callback-001"


def test_callback_query_without_data_does_not_replay_old_message_text(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
) -> None:
    update = json.loads(json.dumps(updates["callback_query"]))
    del update["callback_query"]["data"]

    callback, route = adapter.verify_decrypt_and_normalize(
        _request(update),
        binding(Channel.TELEGRAM),
    )

    assert callback.kind is CallbackKind.CHANNEL_EVENT
    assert callback.inbound is None
    assert route is None


def test_membership_and_unknown_updates_are_channel_events(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
) -> None:
    membership = adapter.verify_and_normalize(
        _request(updates["my_chat_member"]),
        binding(Channel.TELEGRAM),
    )
    unknown = adapter.verify_and_normalize(
        _request({"update_id": 9, "poll": {"id": "p"}}),
        binding(Channel.TELEGRAM),
    )

    assert membership.kind is CallbackKind.CHANNEL_EVENT
    assert membership.inbound is None
    assert unknown.kind is CallbackKind.CHANNEL_EVENT


def test_secret_header_is_mandatory_and_constant_time_api() -> None:
    with pytest.raises(TelegramAuthenticationError):
        verify_telegram_secret(None, TELEGRAM_AUTH_VALUE)
    with pytest.raises(TelegramAuthenticationError):
        verify_telegram_secret("wrong", TELEGRAM_AUTH_VALUE)
    with pytest.raises(ValueError, match="must not be empty"):
        verify_telegram_secret("x", "")
    with pytest.raises(ValueError, match="syntax"):
        verify_telegram_secret("bad value", "bad value")
    with pytest.raises(TelegramAuthenticationError):
        verify_telegram_secret("攻击者", TELEGRAM_AUTH_VALUE)
    verify_telegram_secret(TELEGRAM_AUTH_VALUE, TELEGRAM_AUTH_VALUE)


def test_adapter_rejects_missing_wrong_or_duplicate_secret(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
) -> None:
    body = json.dumps(updates["private_text"]).encode()
    missing = callback_request(body)
    wrong = callback_request(body, headers=((_TELEGRAM_AUTH_HEADER, "wrong"),))
    duplicate = callback_request(
        body,
        headers=(
            (_TELEGRAM_AUTH_HEADER, TELEGRAM_AUTH_VALUE),
            (_TELEGRAM_AUTH_HEADER.lower(), TELEGRAM_AUTH_VALUE),
        ),
    )

    for request in (missing, wrong, duplicate):
        with pytest.raises(TelegramAuthenticationError):
            adapter.verify_and_normalize(request, binding(Channel.TELEGRAM))


@pytest.mark.parametrize(
    "bad_binding",
    [
        binding(Channel.TELEGRAM, enabled=False),
        binding(Channel.WECOM),
        binding(Channel.TELEGRAM, binding_id="other"),
    ],
)
def test_binding_state_channel_and_path_are_enforced(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
    bad_binding: TrustedBindingContext,
) -> None:
    with pytest.raises(TelegramCallbackError, match="binding"):
        adapter.verify_and_normalize(_request(updates["private_text"]), bad_binding)


def test_constructor_and_body_limit_validation(updates: Mapping[str, Any]) -> None:
    with pytest.raises(ValueError, match="webhook_secret"):
        TelegramAdapter("", ChannelIdentityDeriver(IDENTITY_KEY))
    with pytest.raises(ValueError, match="webhook_secret"):
        TelegramAdapter("contains spaces", ChannelIdentityDeriver(IDENTITY_KEY))
    with pytest.raises(ValueError, match="route_lifetime"):
        TelegramAdapter(
            TELEGRAM_AUTH_VALUE,
            ChannelIdentityDeriver(IDENTITY_KEY),
            route_lifetime=timedelta(0),
        )
    with pytest.raises(ValueError, match="positive"):
        TelegramAdapter(
            TELEGRAM_AUTH_VALUE,
            ChannelIdentityDeriver(IDENTITY_KEY),
            max_body_bytes=0,
        )
    adapter = TelegramAdapter(
        TELEGRAM_AUTH_VALUE,
        ChannelIdentityDeriver(IDENTITY_KEY),
        max_body_bytes=1,
    )
    with pytest.raises(TelegramCallbackError, match="exceeds"):
        adapter.verify_and_normalize(
            _request(updates["private_text"]),
            binding(Channel.TELEGRAM),
        )


@pytest.mark.parametrize(
    "body",
    [
        b"[]",
        b"{",
        b'{"update_id":1,"update_id":2}',
        b'{"update_id":NaN}',
        b'{"update_id":true}',
        b'{"update_id":-1}',
        b'{"update_id":"1"}',
    ],
)
def test_strict_json_and_update_id_validation(
    adapter: TelegramAdapter,
    body: bytes,
) -> None:
    request = callback_request(
        body,
        headers=((_TELEGRAM_AUTH_HEADER, TELEGRAM_AUTH_VALUE),),
    )

    with pytest.raises(TelegramCallbackError):
        adapter.verify_and_normalize(request, binding(Channel.TELEGRAM))


def test_unseen_lower_update_id_is_not_discarded(adapter: TelegramAdapter) -> None:
    high = adapter.verify_and_normalize(
        _request(_message_update(update_id=900)),
        binding(Channel.TELEGRAM),
    )
    low = adapter.verify_and_normalize(
        _request(_message_update(update_id=2)),
        binding(Channel.TELEGRAM),
    )

    assert high.delivery_id == "900"
    assert low.delivery_id == "2"
    assert low.kind is CallbackKind.USER_MESSAGE


def test_group_session_shared_by_users_but_principals_isolated(
    adapter: TelegramAdapter,
) -> None:
    first = adapter.verify_and_normalize(
        _request(
            _message_update(
                update_id=1,
                user_id=100,
                chat_id=-500,
                chat_type="group",
            )
        ),
        binding(Channel.TELEGRAM),
    ).inbound
    second = adapter.verify_and_normalize(
        _request(
            _message_update(
                update_id=2,
                user_id=200,
                chat_id=-500,
                chat_type="group",
            )
        ),
        binding(Channel.TELEGRAM),
    ).inbound

    assert first is not None and second is not None
    assert first.principal_id != second.principal_id
    assert first.session_id == second.session_id


@pytest.mark.parametrize(
    ("field", "content", "kind"),
    [
        ("document", {"file_id": "doc", "file_name": "a.pdf", "file_size": 3}, AttachmentKind.FILE),
        ("audio", {"file_id": "audio"}, AttachmentKind.AUDIO),
        ("voice", {"file_id": "voice"}, AttachmentKind.VOICE),
        ("video", {"file_id": "video"}, AttachmentKind.VIDEO),
        ("animation", {"file_id": "animation"}, AttachmentKind.VIDEO),
    ],
)
def test_media_types_are_normalized_to_opaque_locators(
    adapter: TelegramAdapter,
    field: str,
    content: Mapping[str, Any],
    kind: AttachmentKind,
) -> None:
    update = _message_update(text=None, **{field: content})

    inbound = adapter.verify_and_normalize(
        _request(update),
        binding(Channel.TELEGRAM),
    ).inbound

    assert inbound is not None
    assert inbound.attachments[0].kind is kind
    assert str(content["file_id"]) not in inbound.attachments[0].locator_key


@pytest.mark.parametrize(
    "update",
    [
        _message_update(chat_type="channel"),
        _message_update(text=None),
        _message_update(text=None, photo=[]),
        _message_update(text=None, document={"file_id": "x", "file_size": -1}),
        _message_update(text=None, document={"file_id": "x", "mime_type": 1}),
        _message_update(text=None, document={"file_id": "x", "file_name": 1}),
        _message_update(text=1),  # type: ignore[arg-type]
        {
            "update_id": 1,
            "message": {
                "message_id": 1,
                "chat": {"id": 1, "type": "private"},
                "text": "anonymous",
            },
        },
    ],
)
def test_unsupported_or_malformed_messages_are_rejected(
    adapter: TelegramAdapter,
    update: Mapping[str, Any],
) -> None:
    with pytest.raises(TelegramCallbackError):
        adapter.verify_and_normalize(_request(update), binding(Channel.TELEGRAM))


def test_thread_id_type_and_reply_reference_type_are_validated(
    adapter: TelegramAdapter,
) -> None:
    bad_thread = _message_update(message_thread_id="bad")
    bad_reply = _message_update(reply_to_message={"message_id": True})

    for update in (bad_thread, bad_reply):
        with pytest.raises(TelegramCallbackError):
            adapter.verify_and_normalize(_request(update), binding(Channel.TELEGRAM))


def test_text_rendering_respects_telegram_limit(adapter: TelegramAdapter) -> None:
    intent = ReplyIntent(
        intent_id="intent",
        tenant_id="tenant",
        binding_id="binding-001",
        session_id="session",
        run_id="run",
        in_reply_to_delivery_id="1",
        kind=ReplyKind.FINAL,
        text="answer " * 2000,
        idempotency_key="reply:1",
    )

    chunks = adapter.render_text(intent)

    assert "".join(chunks) == intent.text
    assert all(len(chunk) <= 4096 for chunk in chunks)


def test_optional_aiogram_parser_is_compatible_with_static_fixture(
    adapter: TelegramAdapter,
    updates: Mapping[str, Any],
) -> None:
    body = json.dumps(updates["private_text"]).encode()

    parsed = adapter.parse_with_aiogram(body, object())

    assert parsed.update_id == 1001
