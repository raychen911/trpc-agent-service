"""
Unit tests for IM Channel Adapters (WeCom & Telegram).
"""

import hashlib
import json
import pytest
from trpc_service.config.models import ChannelType, ChannelBindingConfig, OutboundMessage
from trpc_service.channels.wechat_work import WeChatWorkChannelAdapter
from trpc_service.channels.telegram import TelegramChannelAdapter


def test_wechat_work_signature_verification():
    adapter = WeChatWorkChannelAdapter()
    token = "test_wecom_token"
    timestamp = "1600000000"
    nonce = "12345"
    echostr = "hello_wecom"

    # Compute expected sha1
    items = sorted([token, timestamp, nonce, echostr])
    sig = hashlib.sha1("".join(items).encode("utf-8")).hexdigest()

    params = {"msg_signature": sig, "timestamp": timestamp, "nonce": nonce, "echostr": echostr}
    assert adapter.verify_signature(params, {}, b"", token=token) is True

    # Bad signature
    params["msg_signature"] = "invalid_sig"
    assert adapter.verify_signature(params, {}, b"", token=token) is False


def test_wechat_work_parse_and_split():
    adapter = WeChatWorkChannelAdapter()
    xml_body = b"""<xml>
        <ToUserName><![CDATA[wx_corp]]></ToUserName>
        <FromUserName><![CDATA[zhangsan]]></FromUserName>
        <CreateTime>1348831860</CreateTime>
        <MsgType><![CDATA[text]]></MsgType>
        <Content><![CDATA[Hello Assistant]]></Content>
        <MsgId>1234567890123456</MsgId>
    </xml>"""

    inbound = adapter.parse_inbound_message("tenant_test", {}, {}, xml_body)
    assert inbound.tenant_id == "tenant_test"
    assert inbound.raw_user_id == "zhangsan"
    assert inbound.content == "Hello Assistant"
    assert inbound.channel_type == ChannelType.WECOM

    # Message splitting
    long_text = "A" * 3000
    chunks = adapter.split_long_message(long_text, 2048)
    assert len(chunks) == 2
    assert len(chunks[0]) == 2048
    assert len(chunks[1]) == 952


def test_telegram_adapter_parse_and_format():
    adapter = TelegramChannelAdapter()
    secret = "my_tg_secret_123"

    # Header check
    headers = {"X-Telegram-Bot-Api-Secret-Token": secret}
    assert adapter.verify_signature({}, headers, b"", secret=secret) is True
    assert adapter.verify_signature({}, {"X-Telegram-Bot-Api-Secret-Token": "bad"}, b"", secret=secret) is False

    # Parse update JSON
    tg_payload = {
        "update_id": 99999,
        "message": {
            "message_id": 42,
            "from": {"id": 10086, "first_name": "Alice"},
            "chat": {"id": -100123456, "type": "supergroup", "title": "Dev Group"},
            "text": "What is the weather?",
        },
    }
    raw_bytes = json.dumps(tg_payload).encode("utf-8")
    inbound = adapter.parse_inbound_message("tenant_tg", {}, headers, raw_bytes)

    assert inbound.raw_user_id == "10086"
    assert inbound.raw_chat_id == "-100123456"
    assert inbound.is_group is True
    assert inbound.content == "What is the weather?"

    # Outbound payload
    outbound = OutboundMessage(
        trace_id="tr_1",
        tenant_id="tenant_tg",
        channel_type=ChannelType.TELEGRAM,
        target_user_id="10086",
        target_chat_id="-100123456",
        is_group=True,
        content="It is sunny.",
    )
    formatted = adapter.format_outbound_payload(outbound)
    assert formatted["chat_id"] == "-100123456"
    assert formatted["text"] == "It is sunny."
