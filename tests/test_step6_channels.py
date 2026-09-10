"""Step 6 IM adapter, webhook, governance, and metrics tests."""

from __future__ import annotations

import base64
import hashlib
import struct
from collections.abc import AsyncGenerator

import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from fastapi.testclient import TestClient
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent.execution import AgentReply
from trpc_service.channels import (
    ChannelMessage,
    ChannelProcessResult,
    TelegramAdapter,
    WeComAdapter,
)
from trpc_service.channels.pull_runtime import TelegramPollingWorker, WeComAIBotWorker
from trpc_service.config.models import (
    AgentAppRecord,
    ChannelBindingRecord,
    ChannelMode,
    ChannelType,
    InboundMessageRecord,
)
from trpc_service.config.settings import ServiceSettings
from trpc_service.governance import (
    InputPolicy,
    InputRejectedError,
    RequestRateLimiter,
    redact_sensitive,
)
from trpc_service.storage.database import Database
from trpc_service.web.app import create_app


class ReplyRunner:
    async def run_async(self, **kwargs) -> AsyncGenerator[Event, None]:
        del kwargs
        yield Event(
            invocation_id="step6-test",
            author="assistant",
            content=Content(parts=[Part.from_text(text="channel reply")]),
        )


class ReplyRunnerProvider:
    async def get_runner(self, app: AgentAppRecord) -> ReplyRunner:
        del app
        return ReplyRunner()


class RecordingTelegram:
    def __init__(self) -> None:
        self.sent: list[tuple[str, ChannelMessage, str]] = []

    verify_secret = staticmethod(TelegramAdapter.verify_secret)
    parse = staticmethod(TelegramAdapter.parse)

    async def send(self, token: str, message: ChannelMessage, text: str) -> None:
        self.sent.append((token, message, text))

    async def close(self) -> None:
        pass


class PullTelegram(RecordingTelegram):
    def __init__(self, updates: list[dict[str, object]]) -> None:
        super().__init__()
        self.updates = updates

    async def get_updates(
        self, token: str, offset: int | None, timeout: int = 30
    ) -> list[dict[str, object]]:
        del token, offset, timeout
        return self.updates


class RecordingProcessor:
    def __init__(self) -> None:
        self.messages: list[ChannelMessage] = []
        self.delivery: list[tuple[str, str, bool]] = []

    async def process(
        self, binding: ChannelBindingRecord, message: ChannelMessage
    ) -> ChannelProcessResult:
        del binding
        self.messages.append(message)
        return ChannelProcessResult(
            reply=AgentReply(
                tenant_id="t1",
                app_id="assistant",
                user_id=message.sender_id,
                session_id="session",
                trace_id="trace",
                text="pull reply",
                tool_events=(),
            ),
            inbound=InboundMessageRecord(
                inbound_id="inbound",
                tenant_id="t1",
                binding_id="telegram-pull",
                external_message_id=message.external_message_id,
                session_id="session",
                trace_id="trace",
            ),
        )

    async def mark_delivery(self, inbound_id: str, channel: str, delivered: bool) -> None:
        self.delivery.append((inbound_id, channel, delivered))


def test_input_policy_and_secret_redaction() -> None:
    assert InputPolicy().validate("  hello  ") == "hello"
    with pytest.raises(InputRejectedError, match="empty"):
        InputPolicy().validate("  ")
    with pytest.raises(InputRejectedError, match="too long"):
        InputPolicy(max_chars=3).validate("four")
    assert redact_sensitive("Authorization: Bearer abc sk-abcdefghijkl") == (
        "Authorization: Bearer [REDACTED] [REDACTED]"
    )
    with pytest.raises(InputRejectedError, match="authorized"):
        InputPolicy().authorize("user-b", {"allowed_users": ["user-a"]})


@pytest.mark.asyncio
async def test_single_node_rate_limiter_enforces_tenant_user_budget() -> None:
    limiter = RequestRateLimiter()
    await limiter.check("tenant-a:user-a", 1)
    with pytest.raises(InputRejectedError, match="rate"):
        await limiter.check("tenant-a:user-a", 1)
    await limiter.check("tenant-a:user-b", 1)


def test_telegram_parser_normalizes_topic_message() -> None:
    message = TelegramAdapter.parse(
        "bot-main",
        {
            "update_id": 91,
            "message": {
                "message_id": 7,
                "date": 1_700_000_000,
                "message_thread_id": 3,
                "text": "hello",
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 42},
            },
        },
    )
    assert message is not None
    assert message.external_message_id == "91:7"
    assert message.conversation_id == "-1001:3"
    assert message.sender_id == "42"


@pytest.mark.asyncio
async def test_telegram_long_polling_processes_and_advances_offset() -> None:
    update = {
        "update_id": 120,
        "message": {
            "message_id": 8,
            "date": 1_700_000_000,
            "text": "hello from polling",
            "chat": {"id": 77, "type": "private"},
            "from": {"id": 77},
        },
    }
    adapter = PullTelegram([update])
    processor = RecordingProcessor()
    binding = ChannelBindingRecord(
        tenant_id="t1",
        app_id="assistant",
        binding_id="telegram-pull",
        channel_type=ChannelType.TELEGRAM,
        connection_mode=ChannelMode.PULL,
        account_id="telegram-main",
        token_ref="env://TELEGRAM_TOKEN",
    )
    worker = TelegramPollingWorker(binding, "test-token", processor, adapter)

    assert await worker.poll_once(None) == 121
    assert [message.text for message in processor.messages] == ["hello from polling"]
    assert adapter.sent[0][2] == "pull reply"
    assert processor.delivery == [("inbound", "telegram", True)]


def test_wecom_aibot_parser_normalizes_long_connection_message() -> None:
    message = WeComAIBotWorker.parse(
        "aibot-main",
        {
            "msgid": "wecom-1",
            "aibotid": "aibot-main",
            "chattype": "group",
            "chatid": "group-8",
            "from": {"userid": "user-2"},
            "msgtype": "text",
            "text": {"content": "hello from websocket"},
        },
    )
    assert message is not None
    assert message.external_message_id == "wecom-1"
    assert message.chat_id == "group-8"
    assert message.sender_id == "user-2"
    assert message.text == "hello from websocket"


def test_wecom_signature_decryption_and_normalization() -> None:
    receive_id = "corp-test"
    encoding_key = base64.b64encode(b"k" * 32).decode().rstrip("=")
    xml = (
        b"<xml><ToUserName>corp-test</ToUserName><FromUserName>user-1</FromUserName>"
        b"<CreateTime>1700000000</CreateTime><MsgType>text</MsgType>"
        b"<Content>hello</Content><MsgId>99</MsgId><AgentID>12</AgentID></xml>"
    )
    encrypted = _wecom_encrypt(xml, encoding_key, receive_id)
    token, timestamp, nonce = "callback-token", "1700000000", "nonce"
    signature = hashlib.sha1(
        "".join(sorted((token, timestamp, nonce, encrypted))).encode()
    ).hexdigest()

    assert WeComAdapter.verify_signature(signature, token, timestamp, nonce, encrypted)
    plaintext = WeComAdapter.decrypt(encrypted, encoding_key, receive_id)
    message = WeComAdapter.parse(receive_id, plaintext)
    assert message is not None
    assert message.text == "hello"
    assert message.sender_id == "user-1"
    assert message.metadata["agent_id"] == "12"


def test_telegram_webhook_is_verified_idempotent_and_observable(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STEP6_ADMIN", "admin-secret")
    monkeypatch.setenv("STEP6_SESSION", "session-secret-at-least-16")
    monkeypatch.setenv("STEP6_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("STEP6_WEBHOOK_SECRET", "webhook-secret")
    settings = ServiceSettings(
        _env_file=None,
        app_env="test",
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'step6.db').as_posix()}",
        admin_api_key_ref="env://STEP6_ADMIN",
        session_hmac_key_ref="env://STEP6_SESSION",
    )
    telegram = RecordingTelegram()
    application = create_app(
        settings=settings,
        database=Database(settings.database_url),
        runner_provider=ReplyRunnerProvider(),
        telegram_adapter=telegram,
    )
    admin_headers = {"X-Admin-API-Key": "admin-secret"}
    payload = {
        "update_id": 100,
        "message": {
            "message_id": 10,
            "date": 1_700_000_000,
            "text": "hello agent",
            "chat": {"id": 55, "type": "private"},
            "from": {"id": 55},
        },
    }

    with TestClient(application) as client:
        assert (
            client.post(
                "/admin/tenants", headers=admin_headers, json={"tenant_id": "t1", "name": "T1"}
            ).status_code
            == 201
        )
        assert (
            client.post(
                "/admin/tenants/t1/apps",
                headers=admin_headers,
                json={
                    "app_id": "assistant",
                    "name": "Assistant",
                    "system_prompt": "Be helpful.",
                },
            ).status_code
            == 201
        )
        binding = client.post(
            "/admin/tenants/t1/bindings",
            headers=admin_headers,
            json={
                "binding_id": "telegram-main",
                "app_id": "assistant",
                "channel_type": "telegram",
                "account_id": "bot-main",
                "token_ref": "env://STEP6_BOT_TOKEN",
                "secret_ref": "env://STEP6_WEBHOOK_SECRET",
            },
        )
        assert binding.status_code == 201, binding.text
        assert binding.json()["app_id"] == "assistant"

        assert client.post("/webhooks/telegram/bot-main", json=payload).status_code == 401
        headers = {"X-Telegram-Bot-Api-Secret-Token": "webhook-secret"}
        first = client.post("/webhooks/telegram/bot-main", headers=headers, json=payload)
        duplicate = client.post("/webhooks/telegram/bot-main", headers=headers, json=payload)
        metrics = client.get("/metrics")

    assert first.status_code == 200 and first.json()["status"] == "completed"
    assert duplicate.status_code == 200 and duplicate.json()["status"] == "duplicate"
    assert len(telegram.sent) == 1
    assert telegram.sent[0][0] == "bot-token"
    assert telegram.sent[0][2] == "channel reply"
    assert metrics.status_code == 200
    assert "trpc_service_requests_total" in metrics.text
    assert 'transport="telegram"' in metrics.text


def _wecom_encrypt(message: bytes, encoding_key: str, receive_id: str) -> str:
    key = base64.b64decode(f"{encoding_key}=")
    plain = b"r" * 16 + struct.pack(">I", len(message)) + message + receive_id.encode()
    padding = 32 - len(plain) % 32
    padded = plain + bytes([padding]) * padding
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode()
