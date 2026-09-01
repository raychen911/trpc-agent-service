import base64
import hashlib
from hashlib import sha1
import hmac
from xml.etree import ElementTree

from trpc_service.channels.base import (
    ChannelVerificationError,
    OutboundMessage,
    SendResult,
    normalize_attachment,
    parse_attachments,
    verify_optional_hmac,
)
from trpc_service.channels.media import attachment_kind, post_multipart_json, prepare_attachment_file
from trpc_service.channels.simple import SimpleJsonChannelAdapter
from trpc_service.security.secrets import SecretManager
from trpc_service.security.secrets import redact_secret_data, redact_secret_text
from trpc_service.channels.wechat_crypto import decrypt_message, verify_handshake


def _post_json(url: str, payload: dict) -> dict:
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


class WeComAdapter(SimpleJsonChannelAdapter):
    channel_name = "wecom"
    message_id_fields = ("MsgId", "message_id", "msg_id")
    user_id_fields = ("FromUserName", "user_id", "external_user_id")
    group_id_fields = ("ChatId", "group_id", "room_id")
    text_fields = ("Content", "text", "message")

    def verify_callback(self, payload, binding):
        timestamp = str(payload.get("timestamp", ""))
        nonce = str(payload.get("nonce", ""))
        encrypted = str(payload.get("Encrypt", ""))
        signature = str(payload.get("msg_signature", payload.get("signature", "")))
        if any((timestamp, nonce, signature, encrypted)):
            if not (timestamp and nonce and signature and binding.token_ref):
                raise ChannelVerificationError("incomplete WeCom callback signature")
            try:
                token = SecretManager().resolve(binding.token_ref)
            except Exception as exc:
                raise ChannelVerificationError("WeCom token is unavailable") from exc
            values = (token, timestamp, nonce, encrypted) if encrypted else (token, timestamp, nonce)
            expected = sha1("".join(sorted(values)).encode()).hexdigest()
            if not hmac.compare_digest(expected, signature):
                raise ChannelVerificationError("invalid WeCom callback signature")
        verify_optional_hmac(payload, binding)

    def verify_handshake(self, payload, binding) -> str:
        return verify_handshake(
            payload,
            binding.token_ref or "",
            binding.config.get("aes_key_ref"),
        )

    def parse_event(self, payload, binding):
        if payload.get("Encrypt") and binding.config.get("aes_key_ref"):
            payload = {**payload, **decrypt_message(payload["Encrypt"], binding.config["aes_key_ref"])}
        raw = payload.get("raw_body")
        if raw:
            root = ElementTree.fromstring(raw)
            values = {child.tag: child.text or "" for child in root}
            payload = {**payload, **values}
        attachments = parse_attachments(payload.get("attachments"))
        if not attachments:
            msg_type = str(payload.get("MsgType", payload.get("msgtype", ""))).lower()
            media_id = payload.get("MediaId") or payload.get("media_id") or payload.get("FileId")
            if msg_type == "image" and (payload.get("PicUrl") or media_id):
                attachments.append(
                    normalize_attachment(
                        {
                            "kind": "image",
                            "url": payload.get("PicUrl"),
                            "name": payload.get("FileName") or media_id or payload.get("MsgId") or "wecom-image",
                            "content_type": "image/jpeg",
                            "metadata": {
                                "media_id": media_id,
                                "pic_url": payload.get("PicUrl"),
                                "source": "wecom.image",
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
                            "name": payload.get("FileName") or media_id or payload.get("MsgId") or "wecom-media",
                            "content_type": payload.get("MimeType"),
                            "metadata": {
                                "media_id": media_id,
                                "msg_type": msg_type,
                                "source": f"wecom.{msg_type or 'media'}",
                            },
                        },
                        default_kind="file",
                    )
                )
        payload = {
            **payload,
            "attachments": [
                {
                    "kind": item.kind,
                    "url": item.url,
                    "name": item.name,
                    "content_type": item.content_type,
                    "metadata": item.metadata,
                }
                for item in attachments
            ],
        }
        return super().parse_event(payload, binding)

    def send(self, message: OutboundMessage, binding) -> SendResult:
        attachment = message.attachments[0] if message.attachments else None
        media_kind = attachment_kind(attachment) if attachment else "text"
        if binding.config.get("sdk_enabled", True):
            corp_id = binding.config.get("corp_id") or binding.config.get("corpid")
            corp_secret_ref = binding.config.get("corp_secret_ref")
            agent_id = binding.config.get("agent_id")
            if corp_id and corp_secret_ref and agent_id:
                try:
                    from wechatpy.enterprise import WeChatClient

                    corp_secret = SecretManager().resolve(corp_secret_ref)
                    client = WeChatClient(corp_id, corp_secret)
                    if attachment and media_kind in {"image", "file"}:
                        with prepare_attachment_file(attachment) as prepared:
                            if prepared is None:
                                return super().send(message, binding)
                            with prepared.path.open("rb") as media_file:
                                uploaded = client.media.upload(media_kind, media_file)
                            media_id = uploaded.get("media_id") or uploaded.get("mediaid")
                            if not media_id:
                                return SendResult(False, "", "WeCom media upload returned no media_id")
                            if media_kind == "image":
                                body = client.message.send_image(
                                    str(agent_id),
                                    [message.external_user_id],
                                    media_id,
                                )
                            else:
                                body = client.message.send_file(
                                    str(agent_id),
                                    [message.external_user_id],
                                    media_id,
                                )
                            if body.get("errcode", 0) != 0:
                                return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                            return SendResult(
                                True,
                                f"wecom:{message.session_id}",
                                metadata={
                                    "sdk": "wechatpy.enterprise",
                                    "message_type": media_kind,
                                    "provider_response": redact_secret_data(body),
                                },
                            )
                    body = client.message.send_text(
                        str(agent_id),
                        [message.external_user_id],
                        message.text,
                    )
                    if body.get("errcode", 0) != 0:
                        return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                    return SendResult(
                        True,
                        f"wecom:{message.session_id}",
                        metadata={"sdk": "wechatpy.enterprise", "provider_response": redact_secret_data(body)},
                    )
                except ImportError:
                    pass
                except Exception as exc:
                    return SendResult(
                        False, "", redact_secret_text(f"WeCom SDK send failed: {type(exc).__name__}: {exc}")
                    )
        webhook_url = binding.config.get("webhook_url")
        webhook_url_ref = binding.config.get("webhook_url_ref")
        if webhook_url_ref:
            try:
                webhook_url = SecretManager().resolve(webhook_url_ref)
            except Exception as exc:
                return SendResult(False, "", redact_secret_text(f"WeCom webhook URL is unavailable: {exc}"))
        if webhook_url:
            if attachment and media_kind == "file":
                return SendResult(
                    False,
                    "",
                    "WeCom robot webhook does not support native file messages",
                    {"message_type": "file", "unsupported": True},
                )
            if attachment and media_kind == "image":
                with prepare_attachment_file(attachment) as prepared:
                    if prepared is not None:
                        raw = prepared.path.read_bytes()
                        body = _post_json(
                            webhook_url,
                            {
                                "msgtype": "image",
                                "image": {
                                    "base64": base64.b64encode(raw).decode("ascii"),
                                    "md5": hashlib.md5(raw).hexdigest(),
                                },
                            },
                        )
                        if body.get("errcode", 0) != 0:
                            return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                        return SendResult(
                            True,
                            f"wecom:{message.session_id}",
                            metadata={"message_type": "image", "provider_response": redact_secret_data(body)},
                        )
            body = _post_json(webhook_url, {"msgtype": "text", "text": {"content": message.text}})
            if body.get("errcode", 0) != 0:
                return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
            return SendResult(
                True,
                f"wecom:{message.session_id}",
                metadata={"message_type": "text", "provider_response": redact_secret_data(body)},
            )
        api_base = binding.config.get("api_base_url")
        if api_base and binding.token_ref:
            try:
                token = SecretManager().resolve(binding.token_ref)
                agent_id = binding.config.get("agent_id")
                if attachment and media_kind in {"image", "file"}:
                    with prepare_attachment_file(attachment) as prepared:
                        if prepared is None:
                            return super().send(message, binding)
                        upload = post_multipart_json(
                            f"{api_base.rstrip('/')}/cgi-bin/media/upload?access_token={token}",
                            fields={"type": media_kind},
                            file_field="media",
                            file_path=prepared.path,
                            content_type=prepared.content_type,
                        )
                        media_id = upload.get("media_id") or upload.get("mediaid")
                        if not media_id:
                            return SendResult(False, "", redact_secret_text(upload), redact_secret_data(upload))
                        payload = {
                            "touser": message.external_user_id,
                            "msgtype": media_kind,
                            "agentid": agent_id,
                            media_kind: {"media_id": media_id},
                        }
                        body = _post_json(
                            f"{api_base.rstrip('/')}/cgi-bin/message/send?access_token={token}",
                            payload,
                        )
                        if body.get("errcode", 0) != 0:
                            return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                        return SendResult(
                            True,
                            f"wecom:{body.get('msgid', message.session_id)}",
                            metadata={"message_type": media_kind, "provider_response": redact_secret_data(body)},
                        )
                body = _post_json(
                    f"{api_base.rstrip('/')}/cgi-bin/message/send?access_token={token}",
                    {
                        "touser": message.external_user_id,
                        "msgtype": "text",
                        "agentid": agent_id,
                        "text": {"content": message.text},
                    },
                )
                if body.get("errcode", 0) != 0:
                    return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                return SendResult(
                    True,
                    f"wecom:{body.get('msgid', message.session_id)}",
                    metadata={"message_type": "text", "provider_response": redact_secret_data(body)},
                )
            except Exception as exc:
                return SendResult(False, "", redact_secret_text(f"{type(exc).__name__}: {exc}"))
        return super().send(message, binding)
