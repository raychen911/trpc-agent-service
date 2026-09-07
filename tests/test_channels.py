from __future__ import annotations

import base64
import json

import httpx
import pytest

from tenant_agent.channels import telegram as telegram_module
from tenant_agent.channels import wecom as wecom_module
from tenant_agent.channels.base import (
    PermanentDeliveryError,
    RateLimited,
    SignatureError,
    WebhookRequest,
    split_text,
)
from tenant_agent.channels.planning import plan_outbound
from tenant_agent.channels.telegram import TelegramAdapter
from tenant_agent.channels.wecom import WeComAdapter, WeComCrypto, validate_wecom_credentials
from tenant_agent.models import Attachment, ChannelType, ChatType, OutboundMessage
from tenant_agent.security import CompositeSecretResolver
from tests.helpers import make_tenant


@pytest.mark.asyncio
async def test_telegram_verifies_and_normalizes_group_media(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_TELEGRAM_SECRET", "webhook-secret")
    monkeypatch.setenv("TENANT_ALPHA_TELEGRAM_TOKEN", "123456:abcdefghijklmnopqrstuvwxyz")
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    update = {
        "update_id": 42,
        "message": {
            "message_id": 7,
            "date": 1_700_000_000,
            "message_thread_id": 99,
            "chat": {"id": -1001, "type": "supergroup"},
            "from": {"id": 123, "language_code": "en"},
            "caption": "inspect image",
            "photo": [
                {"file_id": "small", "file_size": 100},
                {"file_id": "large", "file_size": 200},
            ],
        },
    }
    request = WebhookRequest(
        method="POST",
        headers={"x-telegram-bot-api-secret-token": "webhook-secret"},
        query={},
        body=json.dumps(update).encode(),
    )
    resolver = CompositeSecretResolver(file_root=tmp_path)  # type: ignore[arg-type]
    parsed = await TelegramAdapter().parse(
        request,
        tenant=tenant,
        binding=tenant.channels[0],
        secrets=resolver,
    )
    message = parsed.messages[0]
    assert message.message_id == "42"
    assert message.chat_type is ChatType.GROUP
    assert message.thread_id == "99"
    assert message.attachments[0].external_id == "large"


@pytest.mark.asyncio
async def test_telegram_rejects_wrong_webhook_secret(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_TELEGRAM_SECRET", "right")
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    resolver = CompositeSecretResolver(file_root=tmp_path)  # type: ignore[arg-type]
    with pytest.raises(SignatureError):
        await TelegramAdapter().parse(
            WebhookRequest(
                method="POST",
                headers={"x-telegram-bot-api-secret-token": "wrong"},
                query={},
                body=b"{}",
            ),
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=resolver,
        )


@pytest.mark.asyncio
async def test_wecom_encrypted_callback_round_trip(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    token = "callback-token"
    aes_key = base64.b64encode(b"k" * 32).decode().rstrip("=")
    corp_id = "ww-corp-id"
    values = {
        "CALLBACK_TOKEN": token,
        "ENCODING_AES_KEY": aes_key,
        "CORP_ID": corp_id,
        "CORP_SECRET": "corp-secret",
        "AGENT_ID": "100001",
    }
    for key, value in values.items():
        monkeypatch.setenv(f"TENANT_ALPHA_WECOM_{key}", value)
    tenant = make_tenant(channel=ChannelType.WECOM)
    plaintext = """<xml>
      <ToUserName><![CDATA[ww-corp-id]]></ToUserName>
      <FromUserName><![CDATA[user-7]]></FromUserName>
      <CreateTime>1700000000</CreateTime>
      <MsgType><![CDATA[text]]></MsgType>
      <Content><![CDATA[hello WeCom]]></Content>
      <AgentID>100001</AgentID>
      <MsgId>9988</MsgId>
    </xml>"""
    crypto = WeComCrypto(token, aes_key, corp_id)
    encrypted, signature, timestamp = crypto.encrypt(plaintext, nonce="n-1")
    outer = f"<xml><Encrypt><![CDATA[{encrypted}]]></Encrypt></xml>".encode()
    request = WebhookRequest(
        method="POST",
        headers={},
        query={"msg_signature": signature, "timestamp": timestamp, "nonce": "n-1"},
        body=outer,
    )
    parsed = await WeComAdapter().parse(
        request,
        tenant=tenant,
        binding=tenant.channels[0],
        secrets=CompositeSecretResolver(file_root=tmp_path),  # type: ignore[arg-type]
    )
    message = parsed.messages[0]
    assert message.message_id == "9988"
    assert message.external_user_id == "user-7"
    assert message.chat_type is ChatType.DIRECT
    assert message.text == "hello WeCom"
    assert parsed.acknowledgement.body == b"success"

    wrong_agent_plaintext = plaintext.replace("<AgentID>100001</AgentID>", "<AgentID>999999</AgentID>")
    wrong_encrypted, wrong_signature, wrong_timestamp = crypto.encrypt(
        wrong_agent_plaintext,
        nonce="n-2",
    )
    with pytest.raises(SignatureError, match="AgentID"):
        await WeComAdapter().parse(
            WebhookRequest(
                method="POST",
                headers={},
                query={
                    "msg_signature": wrong_signature,
                    "timestamp": wrong_timestamp,
                    "nonce": "n-2",
                },
                body=f"<xml><Encrypt><![CDATA[{wrong_encrypted}]]></Encrypt></xml>".encode(),
            ),
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=CompositeSecretResolver(file_root=tmp_path),  # type: ignore[arg-type]
        )


def test_platform_text_splitting_preserves_content_and_utf8_limits() -> None:
    text = "alpha beta gamma delta " * 50 + "你好" * 50
    chunks = split_text(text, max_chars=80, max_utf8_bytes=120)
    assert "".join(chunks) == text
    assert all(len(chunk) <= 80 and len(chunk.encode()) <= 120 for chunk in chunks)


def test_wecom_preflight_rejects_invalid_credential_formats() -> None:
    valid = {
        "callback_token": "callbacktoken",
        "encoding_aes_key": base64.b64encode(b"k" * 32).decode().rstrip("="),
        "corp_id": "wwcorp1234",
        "corp_secret": "corpsecret123",
        "agent_id": "100001",
    }
    validate_wecom_credentials(**valid)
    for update in (
        {"callback_token": "x"},
        {"encoding_aes_key": "invalid"},
        {"corp_id": "not-a-corp"},
        {"corp_secret": "x"},
        {"agent_id": "01"},
        {"agent_id": "not-a-number"},
    ):
        with pytest.raises(ValueError):
            validate_wecom_credentials(**(valid | update))


def test_outbound_planning_separates_text_cards_and_media_in_order() -> None:
    logical = OutboundMessage(
        tenant_id="alpha",
        binding_id="wecom-binding-001",
        channel=ChannelType.WECOM,
        external_chat_id="chat",
        reply_to_message_id="reply",
        text="hello",
        cards=({"template_card": {"card_type": "text_notice"}},),
        attachments=(Attachment(kind="file", external_id="media"),),
    )
    planned = plan_outbound(logical)
    assert len(planned) == 3
    assert planned[0].text == "hello"
    assert planned[1].cards
    assert planned[2].attachments
    assert planned[0].reply_to_message_id == "reply"
    assert all(item.reply_to_message_id is None for item in planned[1:])
    assert [item.metadata["delivery_segment_index"] for item in planned] == [
        0,
        1,
        2,
    ]
    web = logical.model_copy(update={"channel": ChannelType.WEB})
    assert plan_outbound(web) == (web,)

    chinese = logical.model_copy(update={"text": "你" * 1_000, "cards": (), "attachments": ()})
    chinese_segments = plan_outbound(chinese)
    assert "".join(item.text for item in chinese_segments) == chinese.text
    assert all(len(item.text.encode()) <= 2_048 for item in chinese_segments)


@pytest.mark.asyncio
async def test_telegram_delivery_split_card_stream_and_rate_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_TELEGRAM_SECRET", "secret")
    monkeypatch.setenv("TENANT_ALPHA_TELEGRAM_TOKEN", "123456:abcdefghijklmnopqrstuvwxyz")
    tenant = make_tenant(channel=ChannelType.TELEGRAM)
    resolver = CompositeSecretResolver(file_root=tmp_path)  # type: ignore[arg-type]
    calls: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        calls.append({"path": request.url.path, "payload": payload})
        return httpx.Response(
            200,
            json={"ok": True, "result": {"message_id": len(calls)}},
        )

    real_client = httpx.AsyncClient

    def client_factory(**kwargs: object) -> httpx.AsyncClient:
        return real_client(transport=httpx.MockTransport(handler), timeout=kwargs.get("timeout"))

    monkeypatch.setattr(telegram_module.httpx, "AsyncClient", client_factory)
    adapter = TelegramAdapter()
    message = OutboundMessage(
        tenant_id=tenant.tenant_id,
        binding_id=tenant.channels[0].binding_id,
        channel=ChannelType.TELEGRAM,
        external_chat_id="123",
        reply_to_message_id="7",
        text="x" * 4_500,
        cards=({"buttons": [[{"text": "Open", "url": "https://example.com"}]]},),
    )
    result = await adapter.deliver(message, tenant=tenant, binding=tenant.channels[0], secrets=resolver)
    assert result.external_message_ids == ("1", "2")
    assert calls[0]["payload"]["reply_parameters"] == {"message_id": "7"}  # type: ignore[index]
    assert "reply_markup" in calls[1]["payload"]  # type: ignore[operator]

    partial = message.model_copy(
        update={"text": "working", "stream_key": "stream", "is_final": False, "cards": ()}
    )
    final = partial.model_copy(update={"text": "done", "is_final": True})
    await adapter.deliver(partial, tenant=tenant, binding=tenant.channels[0], secrets=resolver)
    await adapter.deliver(final, tenant=tenant, binding=tenant.channels[0], secrets=resolver)
    assert calls[-2]["path"].endswith("/sendMessage")  # type: ignore[union-attr]
    assert calls[-1]["path"].endswith("/editMessageText")  # type: ignore[union-attr]
    media = message.model_copy(
        update={
            "text": "",
            "cards": (),
            "reply_to_message_id": None,
            "attachments": (Attachment(kind="image", external_id="telegram-photo-file-id"),),
        }
    )
    await adapter.deliver(media, tenant=tenant, binding=tenant.channels[0], secrets=resolver)
    assert calls[-1]["path"].endswith("/sendPhoto")  # type: ignore[union-attr]

    def limited_handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 3}})

    monkeypatch.setattr(
        telegram_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(limited_handler)),
    )
    with pytest.raises(RateLimited) as limited:
        await TelegramAdapter().deliver(
            message.model_copy(update={"text": "one"}),
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=resolver,
        )
    assert limited.value.retry_after_seconds == 3


@pytest.mark.asyncio
async def test_wecom_url_verification_delivery_token_cache_and_rate_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    token = "callback-token"
    aes_key = base64.b64encode(b"z" * 32).decode().rstrip("=")
    values = {
        "CALLBACK_TOKEN": token,
        "ENCODING_AES_KEY": aes_key,
        "CORP_ID": "corp",
        "CORP_SECRET": "secret",
        "AGENT_ID": "100001",
    }
    for key, value in values.items():
        monkeypatch.setenv(f"TENANT_ALPHA_WECOM_{key}", value)
    tenant = make_tenant(channel=ChannelType.WECOM)
    resolver = CompositeSecretResolver(file_root=tmp_path)  # type: ignore[arg-type]
    crypto = WeComCrypto(token, aes_key, "corp")
    encrypted, signature, timestamp = crypto.encrypt("verified-echo", nonce="n")
    verified = await WeComAdapter().parse(
        WebhookRequest(
            method="GET",
            headers={},
            query={
                "msg_signature": signature,
                "timestamp": timestamp,
                "nonce": "n",
                "echostr": encrypted,
            },
            body=b"",
        ),
        tenant=tenant,
        binding=tenant.channels[0],
        secrets=resolver,
    )
    assert verified.acknowledgement.body == b"verified-echo"

    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.method == "GET":
            return httpx.Response(200, json={"errcode": 0, "access_token": "access", "expires_in": 7200})
        return httpx.Response(200, json={"errcode": 0, "msgid": f"m-{len(calls)}"})

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        wecom_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), timeout=10),
    )
    adapter = WeComAdapter()
    direct = OutboundMessage(
        tenant_id=tenant.tenant_id,
        binding_id=tenant.channels[0].binding_id,
        channel=ChannelType.WECOM,
        external_chat_id="user",
        text="hello",
    )
    await adapter.deliver(direct, tenant=tenant, binding=tenant.channels[0], secrets=resolver)
    group = direct.model_copy(
        update={
            "external_chat_id": "group",
            "metadata": {"chat_type": "group"},
            "cards": ({"template_card": {"card_type": "text_notice"}},),
        }
    )
    await adapter.deliver(group, tenant=tenant, binding=tenant.channels[0], secrets=resolver)
    assert calls.count("/cgi-bin/gettoken") == 1
    assert "/cgi-bin/message/send" in calls
    assert "/cgi-bin/appchat/send" in calls
    media = direct.model_copy(
        update={
            "text": "",
            "attachments": (Attachment(kind="file", external_id="wecom-media-id"),),
        }
    )
    await adapter.deliver(media, tenant=tenant, binding=tenant.channels[0], secrets=resolver)
    assert calls[-1] == "/cgi-bin/message/send"

    def limited(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"errcode": 0, "access_token": "a"})
        return httpx.Response(200, json={"errcode": 45009})

    monkeypatch.setattr(
        wecom_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(limited)),
    )
    with pytest.raises(RateLimited):
        await WeComAdapter().deliver(direct, tenant=tenant, binding=tenant.channels[0], secrets=resolver)

    def invalid_recipient(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"errcode": 0, "access_token": "a"})
        return httpx.Response(200, json={"errcode": 0, "invaliduser": "user"})

    monkeypatch.setattr(
        wecom_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(invalid_recipient)),
    )
    with pytest.raises(PermanentDeliveryError):
        await WeComAdapter().deliver(
            direct,
            tenant=tenant,
            binding=tenant.channels[0],
            secrets=resolver,
        )

    rotated_gets = 0

    def rotated(request: httpx.Request) -> httpx.Response:
        nonlocal rotated_gets
        if request.method == "GET":
            rotated_gets += 1
            return httpx.Response(
                200,
                json={"errcode": 0, "access_token": f"token-{rotated_gets}"},
            )
        return httpx.Response(200, json={"errcode": 0, "msgid": "delivered"})

    monkeypatch.setattr(
        wecom_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(rotated)),
    )
    rotating_adapter = WeComAdapter()
    await rotating_adapter.deliver(
        direct,
        tenant=tenant,
        binding=tenant.channels[0],
        secrets=resolver,
    )
    monkeypatch.setenv("TENANT_ALPHA_WECOM_CORP_SECRET", "rotated-secret")
    await rotating_adapter.deliver(
        direct,
        tenant=tenant,
        binding=tenant.channels[0],
        secrets=resolver,
    )
    assert rotated_gets == 2
