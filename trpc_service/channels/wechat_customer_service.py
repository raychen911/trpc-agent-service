"""WeChat Customer Service channel adapter.

The customer-service API has different callback identity fields and a
dedicated outbound endpoint, so it is intentionally independent from the
Official Account adapter.
"""

from __future__ import annotations

import hmac
from hashlib import sha1
from xml.etree import ElementTree

from trpc_service.channels.base import (
    ChannelCapabilities,
    ChannelVerificationError,
    InboundMessage,
    OutboundMessage,
    SendResult,
    normalize_attachment,
    parse_attachments,
    sanitize_event_payload,
    verify_optional_hmac,
)
from trpc_service.channels.media import attachment_kind, post_multipart_json, prepare_attachment_file
from trpc_service.channels.simple import SimpleJsonChannelAdapter
from trpc_service.channels.wechat_crypto import decrypt_message, verify_handshake
from trpc_service.security.secrets import SecretManager, redact_secret_data, redact_secret_text


def _wechat_json(url: str, payload: dict) -> dict:
    import json
    from urllib.request import Request, urlopen

    request = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


class WeChatCustomerServiceAdapter(SimpleJsonChannelAdapter):
    channel_name = "wechat_customer_service"
    capabilities = ChannelCapabilities(max_text_length=2048, supports_media=True, supports_cards=False)
    message_id_fields = ("MsgId", "MsgID", "message_id", "msg_id")
    user_id_fields = (
        "OpenId",
        "openid",
        "FromUserName",
        "external_user_id",
        "user_id",
    )
    group_id_fields = ("group_id", "conversation_id")
    text_fields = ("Content", "text", "message")

    def verify_callback(self, payload, binding):
        timestamp = str(payload.get("timestamp", ""))
        nonce = str(payload.get("nonce", ""))
        signature = str(payload.get("msg_signature", payload.get("signature", "")))
        encrypted = str(payload.get("Encrypt", ""))
        if any((timestamp, nonce, signature, encrypted)):
            if not (timestamp and nonce and signature and binding.token_ref):
                raise ChannelVerificationError("incomplete WeChat customer-service signature")
            try:
                token = SecretManager().resolve(binding.token_ref)
            except Exception as exc:
                raise ChannelVerificationError("WeChat customer-service token is unavailable") from exc
            values = (token, timestamp, nonce, encrypted) if encrypted else (token, timestamp, nonce)
            expected = sha1("".join(sorted(values)).encode()).hexdigest()
            if not hmac.compare_digest(expected, signature):
                raise ChannelVerificationError("invalid WeChat customer-service signature")
        verify_optional_hmac(payload, binding)

    def verify_handshake(self, payload, binding) -> str:
        return verify_handshake(
            payload,
            binding.token_ref or "",
            binding.config.get("aes_key_ref"),
        )

    def parse_event(self, payload, binding) -> InboundMessage:
        if payload.get("Encrypt") and binding.config.get("aes_key_ref"):
            payload = {**payload, **decrypt_message(payload["Encrypt"], binding.config["aes_key_ref"])}
        raw = payload.get("raw_body")
        if raw:
            root = ElementTree.fromstring(raw)
            payload = {**payload, **{child.tag: child.text or "" for child in root}}
        attachments = parse_attachments(payload.get("attachments"))
        if not attachments:
            msg_type = str(payload.get("MsgType", payload.get("msgtype", ""))).lower()
            media_id = payload.get("MediaId") or payload.get("media_id")
            if msg_type == "image" and (payload.get("PicUrl") or media_id):
                attachments.append(
                    normalize_attachment(
                        {
                            "kind": "image",
                            "url": payload.get("PicUrl"),
                            "name": media_id or payload.get("MsgId") or "wechat-kf-image",
                            "content_type": "image/jpeg",
                            "metadata": {
                                "media_id": media_id,
                                "pic_url": payload.get("PicUrl"),
                                "source": "wechat_customer_service.image",
                            },
                        },
                        default_kind="image",
                    )
                )
            elif media_id:
                attachments.append(
                    normalize_attachment(
                        {
                            "kind": "file",
                            "name": media_id or payload.get("MsgId") or "wechat-kf-media",
                            "content_type": payload.get("MimeType"),
                            "metadata": {
                                "media_id": media_id,
                                "msg_type": msg_type,
                                "source": f"wechat_customer_service.{msg_type or 'media'}",
                            },
                        },
                        default_kind="file",
                    )
                )
        external_user_id = self._first(payload, self.user_id_fields, "anonymous")
        return InboundMessage(
            channel=self.channel_name,
            account_id=str(payload.get("account_id", binding.account_id)),
            external_message_id=self._first(payload, self.message_id_fields, "local-message"),
            external_user_id=external_user_id,
            group_id=self._optional_first(payload, self.group_id_fields),
            text=self._optional_first(payload, self.text_fields),
            attachments=attachments,
            raw_event=sanitize_event_payload(payload),
            internal_user_id=binding.resolve_user_id(external_user_id),
        )

    def send(self, message: OutboundMessage, binding) -> SendResult:
        api_base = binding.config.get("api_base_url", "https://api.weixin.qq.com")
        token_ref = binding.config.get("access_token_ref") or binding.token_ref
        attachment = message.attachments[0] if message.attachments else None
        media_kind = attachment_kind(attachment) if attachment else "text"
        if attachment and media_kind == "file":
            return SendResult(
                False,
                "",
                "WeChat customer service does not support native file messages",
                {"message_type": "file", "unsupported": True},
            )
        if not token_ref:
            return super().send(message, binding)
        try:
            access_token = SecretManager().resolve(token_ref)
            if attachment and media_kind == "image":
                with prepare_attachment_file(attachment) as prepared:
                    if prepared is not None:
                        upload = post_multipart_json(
                            f"{api_base.rstrip('/')}/cgi-bin/media/upload?access_token={access_token}",
                            fields={"type": "image"},
                            file_field="media",
                            file_path=prepared.path,
                            content_type=prepared.content_type,
                        )
                        media_id = upload.get("media_id") or upload.get("mediaid")
                        if not media_id:
                            return SendResult(False, "", redact_secret_text(upload), redact_secret_data(upload))
                        body = _wechat_json(
                            f"{api_base.rstrip('/')}/cgi-bin/kf/send?access_token={access_token}",
                            {
                                "touser": message.external_user_id,
                                "msgtype": media_kind,
                                media_kind: {"media_id": media_id},
                            },
                        )
                        if body.get("errcode", 0) != 0:
                            return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                        return SendResult(
                            True,
                            f"wechat-kf:{message.session_id}",
                            metadata={"message_type": media_kind, "provider_response": redact_secret_data(body)},
                        )
            body = _wechat_json(
                f"{api_base.rstrip('/')}/cgi-bin/kf/send?access_token={access_token}",
                {
                    "touser": message.external_user_id,
                    "msgtype": "text",
                    "text": {"content": message.text},
                },
            )
            if body.get("errcode", 0) != 0:
                return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
            return SendResult(
                True,
                f"wechat-kf:{message.session_id}",
                metadata={
                    "message_type": "text",
                    "api": "customer_service",
                    "provider_response": redact_secret_data(body),
                },
            )
        except Exception as exc:
            return SendResult(False, "", redact_secret_text(f"{type(exc).__name__}: {exc}"))
