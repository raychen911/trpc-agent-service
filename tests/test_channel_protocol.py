import base64
import json
import unittest

from trpc_service.channels.base import (
    Attachment,
    ChannelCapabilities,
    OutboundMessage,
    WebhookPayloadError,
    parse_attachments,
    parse_webhook_body,
    sanitize_event_payload,
)
from trpc_service.channels.outbound import split_outbound_messages
from trpc_service.channels.registry import default_channel_adapters
from trpc_service.channels.simple import SimpleJsonChannelAdapter
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.wecom_ai_bot import WeComAIBotAdapter
from trpc_service.tenant.models import ChannelBinding


class ChannelProtocolTests(unittest.TestCase):
    def test_event_sanitization_drops_credentials_and_bounds_shape(self):
        payload = {
            "message_id": "m-1",
            "text": "hello",
            "signature": "signed-body",
            "_raw_body": "raw-secret-body",
            "_headers": {"Authorization": "Bearer secret"},
            "nested": {"token": "token-value", "safe": "yes"},
        }
        sanitized = sanitize_event_payload(payload)
        encoded = json.dumps(sanitized, ensure_ascii=False)
        self.assertIn("hello", encoded)
        self.assertNotIn("signed-body", encoded)
        self.assertNotIn("raw-secret-body", encoded)
        self.assertNotIn("Bearer secret", encoded)
        self.assertEqual(sanitized["nested"], {"safe": "yes"})

    def test_simple_adapter_only_sets_revoke_target_for_revoke_events(self):
        binding = ChannelBinding("tenant", "simple:one", "simple", "one", "app")
        normal = SimpleJsonChannelAdapter().parse_event(
            {"message_id": "m-1", "user_id": "u-1", "text": "hello"}, binding
        )
        self.assertNotIn("target_message_id", normal.raw_event)
        revoked = SimpleJsonChannelAdapter().parse_event(
            {"message_id": "m-2", "user_id": "u-1", "event_type": "revoke", "msg_id": "m-1"}, binding
        )
        self.assertTrue(revoked.is_revoke)
        self.assertEqual(revoked.raw_event["target_message_id"], "m-1")
        explicit = SimpleJsonChannelAdapter().parse_event(
            {"message_id": "m-2", "user_id": "u-1", "event_type": "revoke", "target_message_id": "m-1"},
            binding,
        )
        self.assertEqual(explicit.raw_event["target_message_id"], "m-1")

    def test_webhook_parser_requires_object_and_enforces_body_limit(self):
        self.assertEqual(parse_webhook_body(b'{"ok": true}', "application/json"), {"ok": True})
        self.assertEqual(parse_webhook_body(b"a=1&b=two", "application/x-www-form-urlencoded"), {"a": "1", "b": "two"})
        self.assertEqual(parse_webhook_body(b"<xml><MsgId>m1</MsgId></xml>", "text/xml")["MsgId"], "m1")
        with self.assertRaises(WebhookPayloadError):
            parse_webhook_body(b"[]", "application/json")
        with self.assertRaises(WebhookPayloadError):
            parse_webhook_body(b"12345", "application/octet-stream", max_bytes=4)

    def test_attachment_parser_rejects_invalid_or_excessive_input(self):
        good = base64.b64encode(b"bytes").decode()
        parsed = parse_attachments([{"kind": "file", "metadata": {"content_base64": good}}])
        self.assertEqual(parsed[0].metadata["content_base64"], good)
        with self.assertRaises(ValueError):
            parse_attachments([{"kind": "file", "metadata": {"content_base64": "not-base64"}}])
        with self.assertRaises(ValueError):
            parse_attachments([{"kind": "file"}] * 33)

    def test_split_messages_have_stable_part_idempotency_keys(self):
        message = OutboundMessage("telegram", "bot", "session", "user", "abcdef")
        first = split_outbound_messages([message], 2, idempotency_key="delivery-1")
        second = split_outbound_messages([message], 2, idempotency_key="delivery-1")
        self.assertEqual(
            [item.metadata["idempotency_key"] for item in first],
            ["delivery-1:part:0", "delivery-1:part:1", "delivery-1:part:2"],
        )
        self.assertEqual(
            [item.metadata["idempotency_key"] for item in first],
            [item.metadata["idempotency_key"] for item in second],
        )

    def test_registry_exposes_capabilities_and_ai_bot_rejects_media(self):
        adapters = default_channel_adapters()
        self.assertTrue(all(isinstance(adapter.capabilities, ChannelCapabilities) for adapter in adapters.values()))
        binding = ChannelBinding("tenant", "wecom_ai_bot:bot", "wecom_ai_bot", "bot", "app")
        message = OutboundMessage(
            "wecom_ai_bot",
            "bot",
            "session",
            "user",
            "file",
            attachments=[Attachment("file", name="x.txt")],
        )
        result = WeComAIBotAdapter().send(message, binding)
        self.assertFalse(result.ok)
        self.assertTrue(result.metadata["unsupported"])

    def test_telegram_update_variants_and_unknown_updates(self):
        adapter = TelegramAdapter()
        binding = ChannelBinding("tenant", "telegram:bot", "telegram", "bot", "app")
        self.assertTrue(adapter.is_noop({"update_id": 1, "my_chat_member": {}}, binding))
        inbound = adapter.parse_event(
            {
                "update_id": 2,
                "edited_message": {
                    "message_id": 3,
                    "from": {"id": 9},
                    "chat": {"id": 9},
                    "caption": "edited caption",
                },
            },
            binding,
        )
        self.assertEqual(inbound.text, "edited caption")


if __name__ == "__main__":
    unittest.main()
