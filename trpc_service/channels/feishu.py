"""Feishu/Lark webhook and OpenAPI adapter with SDK-first fallback.

The adapter prefers lark-oapi SDK when available and configured, falling back
to HTTP API when SDK is missing or credentials are incomplete. Provider
credentials are always resolved through SecretManager.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from collections.abc import Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.base import (
    Attachment,
    ChannelCapabilities,
    ChannelVerificationError,
    InboundMessage,
    OutboundMessage,
    SendResult,
)
from trpc_service.channels.simple import SimpleJsonChannelAdapter
from trpc_service.security.secrets import SecretManager, redact_secret_data, redact_secret_text
from trpc_service.tenant.models import ChannelBinding

# Optional SDK import
try:
    import lark_oapi as lark
    from lark_oapi.api.im.v1 import (
        CreateMessageRequest,
        CreateMessageRequestBody,
        GetMessageResourceRequest,
    )
    LARK_SDK_AVAILABLE = True
except ImportError:
    LARK_SDK_AVAILABLE = False


class FeishuVerificationError(ChannelVerificationError):
    """Safe callback rejection without provider or credential contents."""


class FeishuAdapter(SimpleJsonChannelAdapter):
    channel_name = "feishu"
    capabilities = ChannelCapabilities(max_text_length=30_000, supports_media=False, supports_cards=False)
    message_id_fields = ("message_id",)
    user_id_fields = ("open_id", "user_id", "external_user_id")
    group_id_fields = ("chat_id", "group_id")
    text_fields = ("text", "message")

    _API_ROOT = "https://open.feishu.cn"
    _TOKEN_PATH = "/open-apis/auth/v3/tenant_access_token/internal"
    _MESSAGE_PATH = "/open-apis/im/v1/messages"
    _RESOURCE_PATH = "/open-apis/im/v1/messages/{message_id}/resources/{resource_key}"
    _TOKEN_INVALID_CODES = {99991663, 99991664, 99991668}

    def __init__(self, secrets: SecretManager | None = None) -> None:
        self.secrets = secrets or SecretManager()
        self._token_cache: dict[tuple[str, str], tuple[str, float]] = {}
        self._token_lock = threading.RLock()
        self._sdk_client_cache: dict[tuple[str, str], object] = {}

    def verify_callback(self, payload: dict, binding: ChannelBinding) -> None:
        """Verify callback signature and token, preferring SDK when available."""
        # Try SDK-based verification first
        if LARK_SDK_AVAILABLE and self._has_sdk_config(binding):
            try:
                self._verify_callback_sdk(payload, binding)
                return
            except Exception:
                pass  # Fall back to HTTP API verification

        # HTTP API verification
        decoded, signature_verified, encrypted = self._decode_payload(payload, binding)
        payload["_feishu_payload"] = decoded
        payload["_feishu_signature_verified"] = signature_verified
        payload["_feishu_encrypted"] = encrypted

        verification_token = self._resolve_ref(
            binding.token_ref or binding.config.get("verification_token_ref"),
            "verification token",
        )
        supplied_token = self._header_value(decoded, "token")
        if not supplied_token or not hmac.compare_digest(verification_token, supplied_token):
            raise FeishuVerificationError("Feishu callback token is invalid")

        event_type = self._event_type(decoded)
        encrypt_key = self._encrypt_key(binding)
        if event_type == "url_verification":
            if encrypt_key and not (encrypted or signature_verified):
                raise FeishuVerificationError("Feishu challenge authentication is invalid")
            if not isinstance(decoded.get("challenge"), str) or not decoded["challenge"]:
                raise FeishuVerificationError("Feishu challenge is invalid")
            return
        if encrypt_key and not signature_verified:
            raise FeishuVerificationError("Feishu callback signature is missing")

    def _verify_callback_sdk(self, payload: dict, binding: ChannelBinding) -> None:
        """Verify callback using lark-oapi SDK event handler."""
        if not LARK_SDK_AVAILABLE:
            raise ValueError("SDK not available")

        encrypt_key = self._encrypt_key(binding)
        verification_token = self._resolve_ref(
            binding.token_ref or binding.config.get("verification_token_ref"),
            "verification token",
        )

        raw_body = payload.get("_raw_body")
        if isinstance(raw_body, bytes):
            body_str = raw_body.decode("utf-8")
        elif isinstance(raw_body, str):
            body_str = raw_body
        else:
            body_str = json.dumps(payload, ensure_ascii=False)

        headers = payload.get("_headers", {})

        # SDK event handler for signature verification
        event_handler = lark.EventDispatcherHandler.builder(
            verification_token or "",
            encrypt_key or ""
        ).build()

        # This will raise if verification fails
        event_handler.do(headers, body_str)

    def parse_event(self, payload: dict, binding: ChannelBinding) -> InboundMessage:
        decoded = self._decoded_payload(payload, binding)
        header = decoded.get("header")
        if not isinstance(header, Mapping):
            raise FeishuVerificationError("Feishu callback header is invalid")
        app_id = str(header.get("app_id") or "")
        expected_app_id = str(binding.config.get("app_id") or binding.account_id)
        if not app_id or not hmac.compare_digest(app_id, expected_app_id):
            raise FeishuVerificationError("Feishu callback binding is invalid")

        event = decoded.get("event")
        if not isinstance(event, Mapping):
            raise FeishuVerificationError("Feishu callback event is invalid")
        sender = event.get("sender")
        message = event.get("message")
        if not isinstance(sender, Mapping) or not isinstance(message, Mapping):
            raise FeishuVerificationError("Feishu callback message is invalid")
        sender_id = sender.get("sender_id")
        if not isinstance(sender_id, Mapping):
            raise FeishuVerificationError("Feishu callback sender is invalid")
        open_id = str(sender_id.get("open_id") or "")
        message_id = str(message.get("message_id") or "")
        chat_id = str(message.get("chat_id") or "")
        message_type = str(message.get("message_type") or "")
        if not open_id or not message_id or not chat_id or not message_type:
            raise FeishuVerificationError("Feishu callback message is invalid")

        content = message.get("content")
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                raise FeishuVerificationError("Feishu callback message content is invalid") from None
        if not isinstance(content, Mapping):
            raise FeishuVerificationError("Feishu callback message content is invalid")

        text, attachments = self._normalize_content(
            message_type,
            content,
            message.get("mentions"),
            message_id,
        )
        chat_type = str(message.get("chat_type") or "")
        raw_event = {
            "normalized_event_type": "message",
            "event_type": self._event_type(decoded) or "",
            "event_id": header.get("event_id"),
            "tenant_key": header.get("tenant_key"),
            "chat_type": chat_type,
            "message_type": message_type,
            "thread_id": message.get("thread_id"),
            "root_id": message.get("root_id"),
            "parent_id": message.get("parent_id"),
        }
        raw_event = {key: value for key, value in raw_event.items() if value not in (None, "")}
        group_id = chat_id if chat_type != "p2p" else None
        return InboundMessage(
            channel=self.channel_name,
            account_id=binding.account_id,
            external_message_id=message_id,
            external_user_id=open_id,
            text=text,
            group_id=group_id,
            attachments=attachments,
            raw_event=raw_event,
            internal_user_id=binding.resolve_user_id(open_id),
        )

    def is_noop(self, payload: dict, binding: ChannelBinding) -> bool:
        decoded = self._decoded_payload(payload, binding)
        event_type = self._event_type(decoded)
        if event_type == "url_verification":
            return True
        event = decoded.get("event")
        if not isinstance(event, Mapping):
            return True
        sender = event.get("sender")
        if isinstance(sender, Mapping) and sender.get("sender_type") in {"app", "bot"}:
            return True
        return event_type not in {"im.message.receive_v1", "im.message.receive_v2"}

    def webhook_ack(self, payload: dict, binding: ChannelBinding) -> dict[str, str]:
        decoded = self._decoded_payload(payload, binding)
        if self._event_type(decoded) == "url_verification":
            challenge = decoded.get("challenge")
            if isinstance(challenge, str) and challenge:
                return {"challenge": challenge}
        return {"msg": "success"}

    def send(self, message: OutboundMessage, binding: ChannelBinding) -> SendResult:
        """Send message via SDK when available, fallback to HTTP API."""
        if message.attachments:
            return SendResult(False, "", "Feishu outbound media is not supported by this adapter")

        app_secret_ref = binding.config.get("app_secret_ref")
        if not app_secret_ref:
            return super().send(message, binding)

        # Try SDK-based send first
        if LARK_SDK_AVAILABLE and self._has_sdk_config(binding):
            try:
                return self._send_sdk(message, binding)
            except Exception:
                # Fall back to HTTP API
                pass

        # HTTP API send
        try:
            token = self._tenant_access_token(binding, force_refresh=False)
            response = self._send_text(message, token, binding)
            if self._response_code(response) in self._TOKEN_INVALID_CODES:
                token = self._tenant_access_token(binding, force_refresh=True)
                response = self._send_text(message, token, binding)
            code = self._response_code(response)
            if code != 0:
                return SendResult(
                    False,
                    "",
                    redact_secret_text(response),
                    redact_secret_data(response),
                )
            provider_message_id = (response.get("data") or {}).get("message_id", message.session_id)
            return SendResult(
                True,
                f"feishu:{provider_message_id}",
                metadata={"message_type": "text", "provider_response": redact_secret_data(response)},
            )
        except Exception as exc:
            return SendResult(
                False,
                "",
                redact_secret_text(f"Feishu send failed: {type(exc).__name__}: {exc}"),
            )

    def _send_sdk(self, message: OutboundMessage, binding: ChannelBinding) -> SendResult:
        """Send message using lark-oapi SDK."""
        client = self._get_sdk_client(binding)

        target = message.group_id or message.external_user_id
        receive_id_type = "chat_id" if message.group_id else "open_id"

        body = CreateMessageRequestBody.builder() \
            .receive_id(target) \
            .msg_type("text") \
            .content(json.dumps({"text": message.text}, ensure_ascii=False)) \
            .uuid(str(message.metadata.get("idempotency_key") or message.session_id)) \
            .build()
        request = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(body) \
            .build()

        response = client.im.v1.message.create(request)

        if not response.success():
            return SendResult(
                False,
                "",
                f"Feishu SDK send failed: code={response.code}, msg={response.msg}",
                {"code": response.code, "msg": response.msg},
            )

        msg_id = response.data.message_id if response.data else message.session_id
        return SendResult(
            True,
            f"feishu:{msg_id}",
            metadata={"message_type": "text", "sdk_used": True},
        )

    def download_media(
        self,
        binding: ChannelBinding,
        message_id: str,
        resource_key: str,
        *,
        resource_type: str = "file",
        max_bytes: int = 10 * 1024 * 1024,
    ) -> tuple[bytes, str | None, str | None]:
        """Download media via SDK when available, fallback to HTTP API."""
        # Try SDK first
        if LARK_SDK_AVAILABLE and self._has_sdk_config(binding):
            try:
                return self._download_media_sdk(binding, message_id, resource_key, resource_type, max_bytes)
            except Exception:
                pass  # Fall back to HTTP API

        # HTTP API download
        token = self._tenant_access_token(binding, force_refresh=False)
        path = self._RESOURCE_PATH.format(
            message_id=quote(str(message_id), safe=""),
            resource_key=quote(str(resource_key), safe=""),
        )
        url = f"{self._api_root(binding)}{path}?type={quote(resource_type, safe='')}"
        try:
            response = self._request("GET", url, token=token, max_bytes=max_bytes)
            if self._response_code(response) in self._TOKEN_INVALID_CODES:
                token = self._tenant_access_token(binding, force_refresh=True)
                response = self._request("GET", url, token=token, max_bytes=max_bytes)
            if "error" in response:
                raise ValueError("Feishu media download failed")
            body = response.get("_bytes")
            if not isinstance(body, bytes):
                raise ValueError("Feishu media response is invalid")
            return body, response.get("_content_type"), response.get("_filename")
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError(f"Feishu media download failed: {type(exc).__name__}") from exc

    def _download_media_sdk(
        self,
        binding: ChannelBinding,
        message_id: str,
        resource_key: str,
        resource_type: str,
        max_bytes: int,
    ) -> tuple[bytes, str | None, str | None]:
        """Download media using lark-oapi SDK."""
        client = self._get_sdk_client(binding)

        request = GetMessageResourceRequest.builder() \
            .message_id(message_id) \
            .file_key(resource_key) \
            .type(resource_type) \
            .build()

        response = client.im.v1.message_resource.get(request)

        if not response.success():
            raise ValueError(f"Feishu SDK media download failed: code={response.code}")

        if not response.file:
            raise ValueError("Feishu SDK media response is empty")

        return response.file, None, None

    def _has_sdk_config(self, binding: ChannelBinding) -> bool:
        """Check if binding has complete SDK configuration."""
        app_id = binding.config.get("app_id") or binding.account_id
        app_secret_ref = binding.config.get("app_secret_ref")
        return bool(app_id and app_secret_ref)

    def _get_sdk_client(self, binding: ChannelBinding):
        """Get or create SDK client for binding."""
        if not LARK_SDK_AVAILABLE:
            raise ValueError("lark-oapi SDK not available")

        app_id = str(binding.config.get("app_id") or binding.account_id)
        app_secret_ref = str(binding.config.get("app_secret_ref") or "")

        if not app_id or not app_secret_ref:
            raise ValueError("Feishu SDK requires app_id and app_secret_ref")

        key = (app_id, app_secret_ref)

        with self._token_lock:
            if key in self._sdk_client_cache:
                return self._sdk_client_cache[key]

            app_secret = self._resolve_ref(app_secret_ref, "app secret")

            # Build SDK client
            client = lark.Client.builder() \
                .app_id(app_id) \
                .app_secret(app_secret) \
                .log_level(lark.LogLevel.ERROR) \
                .build()

            self._sdk_client_cache[key] = client
            return client

    def _decode_payload(
        self,
        payload: dict,
        binding: ChannelBinding,
    ) -> tuple[dict, bool, bool]:
        raw = payload.get("_raw_body")
        if isinstance(raw, bytes):
            raw_bytes = raw
        elif isinstance(raw, str):
            raw_bytes = raw.encode("utf-8")
        else:
            raw_bytes = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        try:
            outer = json.loads(raw_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise FeishuVerificationError("Feishu callback JSON is invalid") from None
        if not isinstance(outer, dict):
            raise FeishuVerificationError("Feishu callback JSON is invalid")

        encrypt_key = self._encrypt_key(binding)
        signature_verified = False
        headers = payload.get("_headers")
        if not isinstance(headers, Mapping):
            headers = {}
        if any(self._header(headers, name) for name in (
            "X-Lark-Request-Timestamp",
            "X-Lark-Request-Nonce",
            "X-Lark-Signature",
        )):
            if not encrypt_key:
                raise FeishuVerificationError("Feishu callback encryption key is unavailable")
            self._verify_signature(raw_bytes, headers, encrypt_key)
            signature_verified = True

        encrypted = outer.get("encrypt")
        if encrypted is None:
            return outer, signature_verified, False
        if not isinstance(encrypted, str) or not encrypted or not encrypt_key:
            raise FeishuVerificationError("Feishu encrypted callback configuration is invalid")
        try:
            ciphertext = base64.b64decode(encrypted, validate=True)
            if len(ciphertext) < 32 or len(ciphertext) % 16:
                raise ValueError
            iv, encrypted_body = ciphertext[:16], ciphertext[16:]
            decryptor = Cipher(
                algorithms.AES(hashlib.sha256(encrypt_key.encode("utf-8")).digest()),
                modes.CBC(iv),
            ).decryptor()
            padded = decryptor.update(encrypted_body) + decryptor.finalize()
            unpadder = padding.PKCS7(128).unpadder()
            plain = unpadder.update(padded) + unpadder.finalize()
            decoded = json.loads(plain)
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
            raise FeishuVerificationError("Feishu encrypted callback is invalid") from None
        if not isinstance(decoded, dict):
            raise FeishuVerificationError("Feishu encrypted callback is invalid")
        return decoded, signature_verified, True

    def _verify_signature(self, body: bytes, headers: Mapping, encrypt_key: str) -> None:
        timestamp = self._header(headers, "X-Lark-Request-Timestamp")
        nonce = self._header(headers, "X-Lark-Request-Nonce")
        supplied = self._header(headers, "X-Lark-Signature")
        if not timestamp or not nonce or not supplied:
            raise FeishuVerificationError("Feishu callback signature is missing")
        try:
            if abs(time.time() - int(timestamp)) > 300:
                raise FeishuVerificationError("Feishu callback timestamp is stale")
        except ValueError:
            raise FeishuVerificationError("Feishu callback timestamp is invalid") from None
        expected = hashlib.sha256(
            timestamp.encode() + nonce.encode() + encrypt_key.encode() + body
        ).hexdigest()
        if not hmac.compare_digest(expected, supplied):
            raise FeishuVerificationError("Feishu callback signature is invalid")

    def _tenant_access_token(self, binding: ChannelBinding, *, force_refresh: bool) -> str:
        app_secret_ref = str(binding.config.get("app_secret_ref") or "")
        app_id = str(binding.config.get("app_id") or binding.account_id)
        if not app_secret_ref:
            raise ValueError("Feishu app secret is not configured")
        key = (app_id, app_secret_ref)
        with self._token_lock:
            cached = self._token_cache.get(key)
            if not force_refresh and cached and cached[1] > time.monotonic() + 60:
                return cached[0]
            app_secret = self._resolve_ref(app_secret_ref, "app secret")
            response = self._request(
                "POST",
                f"{self._api_root(binding)}{self._TOKEN_PATH}",
                body={"app_id": app_id, "app_secret": app_secret},
            )
            if self._response_code(response) != 0:
                raise ValueError("Feishu access token request failed")
            token = response.get("tenant_access_token")
            expire = response.get("expire", 0)
            if not isinstance(token, str) or not token:
                raise ValueError("Feishu access token response is invalid")
            try:
                ttl = max(60, int(expire))
            except (TypeError, ValueError):
                ttl = 3600
            self._token_cache[key] = (token, time.monotonic() + ttl)
            return token

    def _send_text(self, message: OutboundMessage, token: str, binding: ChannelBinding) -> dict:
        target = message.group_id or message.external_user_id
        receive_id_type = "chat_id" if message.group_id else "open_id"
        body = {
            "receive_id": target,
            "msg_type": "text",
            "content": json.dumps({"text": message.text}, ensure_ascii=False, separators=(",", ":")),
            "uuid": str(message.metadata.get("idempotency_key") or message.session_id),
        }
        return self._request(
            "POST",
            f"{self._api_root(binding)}{self._MESSAGE_PATH}?receive_id_type={receive_id_type}",
            body=body,
            token=token,
        )

    def _request(
        self,
        method: str,
        url: str,
        *,
        body: dict | None = None,
        token: str | None = None,
        max_bytes: int = 64 * 1024,
    ) -> dict:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = Request(
            url,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=15) as response:
                raw = response.read(max_bytes + 1)
                content_type = response.headers.get("Content-Type")
                filename = response.headers.get("Content-Disposition")
                if len(raw) > max_bytes:
                    raise ValueError("Feishu response exceeds limit")
                try:
                    parsed = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    return {
                        "_bytes": raw,
                        "_content_type": content_type,
                        "_filename": filename,
                        "code": 0,
                    }
                if isinstance(parsed, dict):
                    parsed["_content_type"] = content_type
                    parsed["_filename"] = filename
                    return parsed
                return {"code": -1}
        except HTTPError as exc:
            try:
                raw = exc.read(max_bytes)
                parsed = json.loads(raw.decode("utf-8"))
                return parsed if isinstance(parsed, dict) else {"code": exc.code}
            except Exception:
                return {"code": exc.code}
        except (URLError, TimeoutError):
            raise RuntimeError("Feishu provider transport failed") from None

    def _decoded_payload(self, payload: dict, binding: ChannelBinding) -> dict:
        decoded = payload.get("_feishu_payload")
        if isinstance(decoded, dict):
            return decoded
        return self._decode_payload(payload, binding)[0]

    def _encrypt_key(self, binding: ChannelBinding) -> str | None:
        ref = binding.config.get("encrypt_key_ref")
        if not ref and binding.secret_ref and binding.config.get("use_secret_ref_as_encrypt_key"):
            ref = binding.secret_ref
        return self._resolve_ref(ref, "encryption key") if ref else None

    def _resolve_ref(self, reference: str | None, label: str) -> str:
        if not reference:
            raise FeishuVerificationError(f"Feishu {label} is not configured")
        try:
            return self.secrets.resolve(str(reference))
        except Exception:
            raise FeishuVerificationError(f"Feishu {label} is unavailable") from None

    @staticmethod
    def _event_type(payload: Mapping) -> str:
        header = payload.get("header")
        if isinstance(header, Mapping) and isinstance(header.get("event_type"), str):
            return header["event_type"]
        value = payload.get("type")
        return str(value) if value else ""

    @staticmethod
    def _header_value(payload: Mapping, name: str) -> str | None:
        header = payload.get("header")
        if isinstance(header, Mapping):
            value = header.get(name)
            if value is not None:
                return str(value)
        value = payload.get(name)
        return str(value) if value is not None else None

    @staticmethod
    def _header(headers: Mapping, name: str) -> str | None:
        wanted = name.casefold()
        for key, value in headers.items():
            if str(key).casefold() == wanted:
                return str(value)
        return None

    @staticmethod
    def _response_code(response: Mapping) -> int:
        value = response.get("code", response.get("errcode", -1))
        try:
            return int(value)
        except (TypeError, ValueError):
            return -1

    @staticmethod
    def _normalize_content(
        message_type: str,
        content: Mapping,
        mentions: object,
        message_id: str,
    ) -> tuple[str | None, list[Attachment]]:
        text: str | None = None
        attachments: list[Attachment] = []
        if message_type == "text":
            value = content.get("text")
            text = str(value) if value is not None else ""
            for mention in mentions if isinstance(mentions, list) else []:
                if isinstance(mention, Mapping) and mention.get("key"):
                    text = text.replace(str(mention["key"]), "")
            return text.strip(), attachments
        if message_type == "post":
            return FeishuAdapter._flatten_post(content), attachments
        key = {
            "image": "image_key",
            "file": "file_key",
            "audio": "file_key",
            "media": "file_key",
            "sticker": "image_key",
        }.get(message_type)
        if key:
            resource_key = content.get(key)
            if resource_key:
                kind = "image" if message_type in {"image", "sticker"} else "file"
                attachments.append(
                    Attachment(
                        kind=kind,
                        name=str(content.get("file_name") or f"feishu-{resource_key}"),
                        content_type="image/*" if kind == "image" else None,
                        metadata={
                            "resource_key": str(resource_key),
                            "resource_type": "image" if kind == "image" else "file",
                            "message_id": message_id,
                            "source": f"feishu.{message_type}",
                        },
                    )
                )
        return text, attachments

    @staticmethod
    def _flatten_post(content: Mapping) -> str | None:
        values: list[str] = []
        title = content.get("title")
        if title:
            values.append(str(title))
        stack: list[object] = [content.get("content")]
        while stack and len(values) < 1000:
            item = stack.pop()
            if isinstance(item, list):
                stack.extend(reversed(item))
            elif isinstance(item, Mapping):
                if item.get("tag") in {"text", "a"} and item.get("text"):
                    values.append(str(item["text"]))
                stack.extend(reversed([value for key, value in item.items() if key not in {"tag", "text"}]))
        rendered = "\n".join(value.strip() for value in values if value.strip())
        return rendered or None

    def _api_root(self, binding: ChannelBinding) -> str:
        return str(binding.config.get("api_base_url") or self._API_ROOT).rstrip("/")


__all__ = ["LARK_SDK_AVAILABLE", "FeishuAdapter", "FeishuVerificationError"]
