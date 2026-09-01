from hashlib import sha1
import hmac
from xml.etree import ElementTree

from trpc_service.channels.base import (
    ChannelVerificationError,
    SendResult,
    normalize_attachment,
    parse_attachments,
)
from trpc_service.channels.media import attachment_kind, post_multipart_json, prepare_attachment_file
from trpc_service.channels.simple import SimpleJsonChannelAdapter
from trpc_service.security.secrets import SecretManager
from trpc_service.security.secrets import redact_secret_data, redact_secret_text
from trpc_service.channels.wechat_crypto import decrypt_message, verify_handshake


def _wechat_json(url: str, payload: dict | None = None) -> dict:
    import json
    from urllib.request import Request, urlopen

    request = Request(
        url,
        data=(json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None),
        headers={"Content-Type": "application/json"} if payload is not None else {},
        method="POST" if payload is not None else "GET",
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


class WeChatOfficialAccountAdapter(SimpleJsonChannelAdapter):
    channel_name = "wechat_official_account"
    message_id_fields = ("MsgId", "message_id", "msg_id")
    user_id_fields = ("FromUserName", "openid", "user_id")
    group_id_fields = ("group_id",)
    text_fields = ("Content", "text", "message")

    def verify_callback(self, payload, binding):
        timestamp = str(payload.get("timestamp", ""))
        nonce = str(payload.get("nonce", ""))
        signature = str(payload.get("signature", ""))
        if any((timestamp, nonce, signature)) and not (timestamp and nonce and signature and binding.token_ref):
            raise ChannelVerificationError("incomplete WeChat callback signature")
        if timestamp and nonce and signature and binding.token_ref:
            try:
                token = SecretManager().resolve(binding.token_ref)
            except Exception as exc:
                raise ChannelVerificationError("WeChat token is unavailable") from exc
            expected = sha1("".join(sorted((token, timestamp, nonce))).encode()).hexdigest()
            if not hmac.compare_digest(expected, signature):
                raise ChannelVerificationError("invalid WeChat callback signature")

        encrypted = str(payload.get("Encrypt", ""))
        msg_signature = str(payload.get("msg_signature", ""))
        if encrypted:
            if not (timestamp and nonce and msg_signature and binding.token_ref):
                raise ChannelVerificationError("incomplete WeChat encrypted callback signature")
            token = SecretManager().resolve(binding.token_ref)
            expected = sha1("".join(sorted((token, timestamp, nonce, encrypted))).encode()).hexdigest()
            if not hmac.compare_digest(expected, msg_signature):
                raise ChannelVerificationError("invalid WeChat encrypted callback signature")
            if not binding.config.get("aes_key_ref"):
                raise ChannelVerificationError("WeChat encrypted callback AES key is unavailable")

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
                            "name": media_id or payload.get("MsgId") or "wechat-image",
                            "content_type": "image/jpeg",
                            "metadata": {
                                "media_id": media_id,
                                "pic_url": payload.get("PicUrl"),
                                "source": "wechat_official_account.image",
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
                            "name": media_id or payload.get("MsgId") or "wechat-media",
                            "content_type": payload.get("MimeType"),
                            "metadata": {
                                "media_id": media_id,
                                "msg_type": msg_type,
                                "source": f"wechat_official_account.{msg_type or 'media'}",
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

    def send(self, message, binding):
        attachment = message.attachments[0] if message.attachments else None
        media_kind = attachment_kind(attachment) if attachment else "text"
        if attachment and media_kind == "file":
            return SendResult(
                False,
                "",
                "WeChat Official Account custom service does not support native file messages",
                {"message_type": "file", "unsupported": True},
            )
        if binding.config.get("sdk_enabled", True):
            app_id = binding.config.get("appid") or binding.config.get("app_id")
            app_secret_ref = binding.config.get("app_secret_ref")
            if app_id and app_secret_ref:
                try:
                    from wechatpy import WeChatClient

                    app_secret = SecretManager().resolve(app_secret_ref)
                    client = WeChatClient(app_id, app_secret)
                    if attachment and media_kind == "image":
                        with prepare_attachment_file(attachment) as prepared:
                            if prepared is None:
                                return super().send(message, binding)
                            with prepared.path.open("rb") as media_file:
                                uploaded = client.media.upload("image", media_file)
                            media_id = uploaded.get("media_id") or uploaded.get("mediaid")
                            if not media_id:
                                return SendResult(False, "", "WeChat media upload returned no media_id")
                            body = client.message.send_image(
                                message.external_user_id,
                                media_id,
                            )
                            if body.get("errcode", 0) != 0:
                                return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                            return SendResult(
                                True,
                                f"wechat:{message.session_id}",
                                metadata={
                                    "sdk": "wechatpy",
                                    "message_type": "image",
                                    "provider_response": redact_secret_data(body),
                                },
                            )
                    body = client.message.send_text(
                        message.external_user_id,
                        message.text,
                    )
                    if body.get("errcode", 0) != 0:
                        return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                    return SendResult(
                        True,
                        f"wechat:{message.session_id}",
                        metadata={"sdk": "wechatpy", "provider_response": redact_secret_data(body)},
                    )
                except ImportError:
                    pass
                except Exception as exc:
                    return SendResult(
                        False, "", redact_secret_text(f"WeChat SDK send failed: {type(exc).__name__}: {exc}")
                    )
        api_base = binding.config.get("api_base_url", "https://api.weixin.qq.com")
        token_ref = binding.config.get("access_token_ref") or binding.token_ref
        if not token_ref:
            return super().send(message, binding)
        try:
            token = SecretManager().resolve(token_ref)
            if attachment and media_kind == "image":
                with prepare_attachment_file(attachment) as prepared:
                    if prepared is not None:
                        upload = post_multipart_json(
                            f"{api_base.rstrip('/')}/cgi-bin/media/upload?access_token={token}",
                            fields={"type": "image"},
                            file_field="media",
                            file_path=prepared.path,
                            content_type=prepared.content_type,
                        )
                        media_id = upload.get("media_id") or upload.get("mediaid")
                        if not media_id:
                            return SendResult(False, "", redact_secret_text(upload), redact_secret_data(upload))
                        body = _wechat_json(
                            f"{api_base.rstrip('/')}/cgi-bin/message/custom/send?access_token={token}",
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
                            f"wechat:{message.session_id}",
                            metadata={"message_type": media_kind, "provider_response": redact_secret_data(body)},
                        )
            body = _wechat_json(
                f"{api_base.rstrip('/')}/cgi-bin/message/custom/send?access_token={token}",
                {"touser": message.external_user_id, "msgtype": "text", "text": {"content": message.text}},
            )
            if body.get("errcode", 0) != 0:
                return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
            return SendResult(
                True,
                f"wechat:{message.session_id}",
                metadata={"message_type": "text", "provider_response": redact_secret_data(body)},
            )
        except Exception as exc:
            return SendResult(False, "", redact_secret_text(f"{type(exc).__name__}: {exc}"))
