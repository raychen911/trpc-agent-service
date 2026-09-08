import asyncio
import base64
import json
from dataclasses import replace
from datetime import datetime, timezone

import httpx
import pytest

from trpc_service.channels import ResolvedChannelBinding, TelegramAdapter, WeComAdapter, WeComCrypto
from trpc_service.channels.delivery import ImDeliveryHandlers
from trpc_service.channels.telegram import ChannelAuthenticationError
from trpc_service.domain import ChannelType
from trpc_service.storage.contracts import OutboxRecord


def binding(channel: ChannelType, options: dict | None = None) -> ResolvedChannelBinding:
    return ResolvedChannelBinding(
        id="binding-1",
        tenant_id="tenant-1",
        agent_app_id="app-1",
        channel_type=channel,
        account_id="account-1",
        webhook_path=f"/webhooks/{channel.value}/account-1",
        token_secret_ref=None,
        secret_ref=None,
        options=options or {},
    )


def test_telegram_normalizes_private_messages() -> None:
    adapter = TelegramAdapter()
    private = adapter.normalize(
        {
            "update_id": 42,
            "message": {
                "message_id": 9,
                "text": "hello",
                "chat": {"id": 100, "type": "private"},
                "from": {"id": 200},
            },
        },
        binding(ChannelType.TELEGRAM),
        "secret",
        "secret",
        "trace",
    )
    assert private.external_message_id == "42"
    assert private.session_id.endswith("direct:200")
    assert TelegramAdapter.webhook_reply(private, "hi") == {
        "method": "sendMessage",
        "chat_id": "100",
        "text": "hi",
    }

    with pytest.raises(ChannelAuthenticationError):
        adapter.normalize({}, binding(ChannelType.TELEGRAM), "bad", "secret")


def test_telegram_normalizes_photo_and_document_metadata() -> None:
    normalized = TelegramAdapter().normalize(
        {
            "update_id": 43,
            "message": {
                "message_id": 10,
                "chat": {"id": -100, "type": "supergroup"},
                "from": {"id": 201},
                "photo": [
                    {"file_id": "small", "width": 90, "height": 90},
                    {"file_id": "large", "width": 800, "height": 600},
                ],
                "document": {
                    "file_id": "document-1",
                    "file_name": "report.pdf",
                    "mime_type": "application/pdf",
                },
            },
        },
        binding(ChannelType.TELEGRAM),
        "secret",
        "secret",
    )
    assert normalized.conversation_type == "group"
    assert normalized.session_id.endswith("group:-100")
    assert normalized.text == "[Telegram attachment: image, file]"
    assert [item["provider_file_id"] for item in normalized.metadata["attachments"]] == [
        "large",
        "document-1",
    ]


def test_wecom_crypto_verify_normalize_and_encrypted_reply() -> None:
    aes_key = base64.b64encode(bytes(range(32))).decode().rstrip("=")
    crypto = WeComCrypto("callback-token", aes_key, "corp-1")
    plaintext = (
        "<xml><ToUserName>corp-1</ToUserName><FromUserName>user-7</FromUserName>"
        "<CreateTime>1700000000</CreateTime><MsgType>text</MsgType>"
        "<Content>hello wecom</Content><MsgId>9001</MsgId><AgentID>3</AgentID></xml>"
    )
    encrypted, signature = crypto.encrypt(plaintext, "1700000001", "nonce-1")
    assert crypto.decrypt(encrypted, signature, "1700000001", "nonce-1") == plaintext
    envelope = f"<xml><Encrypt>{encrypted}</Encrypt></xml>".encode()
    adapter = WeComAdapter()
    normalized = adapter.normalize(
        envelope,
        binding(ChannelType.WECOM, {"receive_id": "corp-1"}),
        token="callback-token",
        encoding_aes_key=aes_key,
        signature=signature,
        timestamp="1700000001",
        nonce="nonce-1",
        trace_id="trace",
    )
    assert normalized.text == "hello wecom"
    assert normalized.external_message_id == "9001"

    reply_envelope = adapter.webhook_reply(
        normalized,
        "hello back",
        crypto=crypto,
        timestamp="1700000002",
        nonce="nonce-2",
    )
    assert "<Encrypt>" in reply_envelope


def test_wecom_plaintext_requires_explicit_development_option() -> None:
    body = (
        b"<xml><ToUserName>corp</ToUserName><FromUserName>user</FromUserName>"
        b"<CreateTime>1</CreateTime><MsgType>text</MsgType><Content>hello</Content>"
        b"<MsgId>2</MsgId></xml>"
    )
    with pytest.raises(ChannelAuthenticationError):
        WeComAdapter().normalize(
            body,
            binding(ChannelType.WECOM),
            token=None,
            encoding_aes_key=None,
            signature=None,
            timestamp=None,
            nonce=None,
        )


def test_wecom_normalizes_media_message() -> None:
    normalized = WeComAdapter().normalize(
        b"<xml><ToUserName>corp</ToUserName><FromUserName>user</FromUserName>"
        b"<CreateTime>2</CreateTime><MsgType>image</MsgType>"
        b"<PicUrl>https://example/image</PicUrl><MediaId>media-1</MediaId>"
        b"<MsgId>3</MsgId></xml>",
        binding(ChannelType.WECOM, {"allow_plaintext": True}),
        token=None,
        encoding_aes_key=None,
        signature=None,
        timestamp=None,
        nonce=None,
    )
    assert normalized.text == "[WeCom image attachment]"
    assert normalized.metadata["attachments"][0]["provider_media_id"] == "media-1"


def test_wecom_aibot_normalizes_encrypted_json_and_rejects_wrong_bot() -> None:
    aes_key = base64.b64encode(bytes(range(32))).decode().rstrip("=")
    crypto = WeComCrypto("callback-token", aes_key, "")
    payload = {
        "msgid": "aibot-message-1",
        "aibotid": "智能机器人-123",
        "chatid": "group-7",
        "chattype": "group",
        "from": {"userid": "user-7"},
        "response_url": "https://qyapi.weixin.qq.com/cgi-bin/aibot/response?response_code=code",
        "msgtype": "text",
        "text": {"content": "hello smart bot"},
    }
    encrypted, signature = crypto.encrypt(json.dumps(payload), "1700000010", "nonce-aibot")
    body = json.dumps({"encrypt": encrypted}).encode()
    adapter = WeComAdapter()
    configured = binding(ChannelType.WECOM, {"mode": "aibot", "aibot_id": "智能机器人-123"})
    normalized = adapter.normalize(
        body,
        configured,
        token="callback-token",
        encoding_aes_key=aes_key,
        signature=signature,
        timestamp="1700000010",
        nonce="nonce-aibot",
        trace_id="trace-aibot",
    )
    assert normalized.external_message_id == "aibot-message-1"
    assert normalized.conversation_type == "group"
    assert normalized.conversation_id == "group-7"
    assert normalized.text == "hello smart bot"
    assert normalized.metadata["wecom_mode"] == "aibot"
    assert normalized.metadata["response_url"].startswith("https://qyapi.weixin.qq.com/")

    wrong = binding(ChannelType.WECOM, {"mode": "aibot", "aibot_id": "another-bot"})
    with pytest.raises(ChannelAuthenticationError, match="id mismatch"):
        adapter.normalize(
            body,
            wrong,
            token="callback-token",
            encoding_aes_key=aes_key,
            signature=signature,
            timestamp="1700000010",
            nonce="nonce-aibot",
        )


@pytest.mark.parametrize(
    ("message_part", "expected_text", "expected_attachments"),
    [
        (
            {"msgtype": "voice", "voice": {"content": "voice transcript"}},
            "voice transcript",
            0,
        ),
        (
            {
                "msgtype": "mixed",
                "mixed": {
                    "msg_item": [
                        {"msgtype": "text", "text": {"content": "mixed text"}},
                        {"msgtype": "image", "image": {"url": "https://media/image"}},
                    ]
                },
            },
            "mixed text",
            1,
        ),
        (
            {"msgtype": "image", "image": {"url": "https://media/image"}},
            "[WeCom AIBot image attachment]",
            1,
        ),
        (
            {"msgtype": "file", "file": {"url": "https://media/file"}},
            "[WeCom AIBot file attachment]",
            1,
        ),
        (
            {"msgtype": "video", "video": {"url": "https://media/video"}},
            "[WeCom AIBot video attachment]",
            1,
        ),
    ],
)
def test_wecom_aibot_normalizes_voice_mixed_and_media(
    message_part: dict, expected_text: str, expected_attachments: int
) -> None:
    aes_key = base64.b64encode(bytes(range(32))).decode().rstrip("=")
    crypto = WeComCrypto("callback-token", aes_key, "")
    payload = {
        "msgid": f"message-{message_part['msgtype']}",
        "aibotid": "bot-123",
        "chattype": "single",
        "from": {"userid": "user-7"},
        "response_url": "https://qyapi.weixin.qq.com/cgi-bin/aibot/response?response_code=code",
        **message_part,
    }
    encrypted, signature = crypto.encrypt(json.dumps(payload), "1700000011", "nonce-media")
    normalized = WeComAdapter().normalize(
        json.dumps({"encrypt": encrypted}).encode(),
        binding(ChannelType.WECOM, {"mode": "aibot", "aibot_id": "bot-123"}),
        token="callback-token",
        encoding_aes_key=aes_key,
        signature=signature,
        timestamp="1700000011",
        nonce="nonce-media",
    )
    assert normalized.text == expected_text
    assert len(normalized.metadata["attachments"]) == expected_attachments


def test_wecom_aibot_uses_one_time_response_url_for_async_reply() -> None:
    requests: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((str(request.url), json.loads(request.content)))
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"})

    configured = binding(ChannelType.WECOM, {"mode": "aibot", "aibot_id": "bot-123"})

    class ConfiguredBindings:
        async def resolve(self, channel: ChannelType, account_id: str) -> ResolvedChannelBinding:
            assert channel == ChannelType.WECOM
            return configured

    class Secrets:
        async def resolve(self, reference: str) -> str:
            raise AssertionError("AIBot response_url delivery does not need another secret")

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        delivery = ImDeliveryHandlers(ConfiguredBindings(), Secrets(), client)
        record = OutboxRecord(
            id="outbox-aibot",
            tenant_id="tenant-1",
            topic="im.reply.wecom",
            payload={
                "account_id": "account-1",
                "conversation_id": "group-7",
                "sender_user_id": "user-7",
                "text": "smart bot reply",
                "metadata": {
                    "response_url": (
                        "https://qyapi.weixin.qq.com/cgi-bin/aibot/response"
                        "?response_code=one-time-code"
                    )
                },
                "delivery": {},
            },
            dedupe_key="reply-aibot",
            attempts=1,
            status="processing",
            available_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
        )
        await delivery.wecom_reply(record)
        malicious = replace(
            record,
            payload={
                **record.payload,
                "metadata": {"response_url": "https://example.com/steal"},
            },
        )
        with pytest.raises(RuntimeError, match="not allowed"):
            await delivery.wecom_reply(malicious)
        await client.aclose()

    asyncio.run(scenario())
    assert len(requests) == 1
    assert requests[0][1] == {
        "msgtype": "markdown",
        "markdown": {"content": "smart bot reply"},
    }


def test_telegram_stream_edits_once_without_duplicate_final_reply() -> None:
    requests: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 77}})

    class Secrets:
        async def resolve(self, reference: str) -> str:
            assert reference == "env://BOT_TOKEN"
            return "bot-token"

    configured = replace(binding(ChannelType.TELEGRAM), secret_ref="env://BOT_TOKEN")

    class ConfiguredBindings:
        async def resolve(self, channel: ChannelType, account_id: str) -> ResolvedChannelBinding:
            return configured

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        delivery = ImDeliveryHandlers(ConfiguredBindings(), Secrets(), client)
        await delivery.telegram_reply(
            OutboxRecord(
                id="outbox-1",
                tenant_id="tenant-1",
                topic="im.reply.telegram",
                payload={
                    "account_id": "account-1",
                    "conversation_id": "100",
                    "text": "hello world",
                    "metadata": {},
                    "delivery": {
                        "stream_updates": ["hello", "hello world"],
                        "card": {"inline_keyboard": []},
                    },
                },
                dedupe_key="reply-1",
                attempts=1,
                status="processing",
                available_at=datetime.now(timezone.utc),
                created_at=datetime.now(timezone.utc),
            )
        )
        await client.aclose()

    asyncio.run(scenario())
    assert len(requests) == 2
    assert requests[0]["text"] == "hello"
    assert requests[0]["reply_markup"] == {"inline_keyboard": []}
    assert requests[1]["text"] == "hello world"
