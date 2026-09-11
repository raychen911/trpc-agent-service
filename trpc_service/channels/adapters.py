"""WeCom, Telegram, and in-process mock channel implementations.

The adapters do no database lookup themselves.  The gateway resolves the protected
binding locator first and passes a fully-qualified :class:`ChannelBinding` here.  This
prevents a callback body from choosing its own tenant or agent.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import http.client
import json
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as element_tree
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

from trpc_service.config import SecretProvider, parse_secret_json
from trpc_service.metrics import current_trace_context, inject_trace_context


class DeliveryCapability(StrEnum):
    IDEMPOTENT = "idempotent"
    QUERYABLE = "queryable"
    NON_RETRIABLE = "non_retriable"


class ChannelError(RuntimeError):
    """A provider failure with an explicit retry classification."""

    def __init__(
        self, code: str, message: str, *, retryable: bool = False, status_code: int = 400
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class ChannelBinding:
    tenant_id: str
    binding_id: str
    agent_id: str
    provider: str
    external_account_id: str
    secret_ref: str
    capabilities: Mapping[str, Any] = field(default_factory=dict)
    status: str = "active"

    def require_active(self, expected_provider: str) -> None:
        if self.provider != expected_provider:
            raise ChannelError(
                "binding_provider_mismatch",
                "callback adapter does not match binding",
                status_code=404,
            )
        if self.status != "active":
            raise ChannelError("binding_disabled", "channel binding is not active", status_code=403)


@dataclass(frozen=True, slots=True)
class CallbackRequest:
    body: bytes | str | Mapping[str, Any]
    headers: Mapping[str, str] = field(default_factory=dict)
    query: Mapping[str, str] = field(default_factory=dict)
    received_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class Attachment:
    kind: str
    provider_media_id: str | None = None
    url: str | None = None
    filename: str | None = None
    content_type: str | None = None


@dataclass(frozen=True, slots=True)
class Principal:
    external_user_id: str
    display_name: str | None = None
    attributes: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class InboundEnvelope:
    """Provider-neutral payload ready for durable Inbox acceptance."""

    event_id: str
    idempotency_key: str
    tenant_id: str
    binding_id: str
    agent_id: str
    channel: str
    channel_account_id: str
    external_message_id: str
    conversation_id: str
    conversation_type: str
    principal: Principal
    text: str | None
    attachments: tuple[Attachment, ...]
    occurred_at: datetime
    session_id: str
    traceparent: str
    raw_type: str = "text"
    # Correlation data created only by a verified provider transport.  Durable
    # reply delivery needs it for protocols, such as WeCom Smart Bot, whose
    # response must reuse the original connection request ID.
    transport_context: Mapping[str, Any] = field(default_factory=dict)

    @property
    def payload_hash(self) -> str:
        safe = {
            "channel": self.channel,
            "external_message_id": self.external_message_id,
            "conversation_id": self.conversation_id,
            "principal": self.principal.external_user_id,
            "text": self.text,
            "attachments": [attachment.kind for attachment in self.attachments],
        }
        return hashlib.sha256(
            json.dumps(safe, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class ReplyBlock:
    kind: str
    text: str | None = None
    url: str | None = None
    media_id: str | None = None
    title: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def text_block(cls, text: str) -> ReplyBlock:
        return cls(kind="text", text=text)


@dataclass(frozen=True, slots=True)
class ReplyEnvelope:
    tenant_id: str
    binding_id: str
    session_id: str
    recipient_id: str
    delivery_id: str
    blocks: tuple[ReplyBlock, ...]
    traceparent: str
    reply_to_external_message_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def plain_text(self, limit: int | None = None) -> str:
        text = "\n".join(block.text for block in self.blocks if block.kind == "text" and block.text)
        return text[:limit] if limit else text


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    status: str  # accepted | failed | unknown | reconciling | manual_review
    capability: DeliveryCapability
    provider_message_id: str | None = None
    error_code: str | None = None
    retry_after_seconds: int | None = None


class ChannelAdapter(Protocol):
    provider: str

    async def validate_and_normalize(
        self, binding: ChannelBinding, request: CallbackRequest
    ) -> InboundEnvelope:
        """Validate a provider callback and return a trusted normalized envelope."""

    async def deliver(self, binding: ChannelBinding, reply: ReplyEnvelope) -> DeliveryResult:
        """Persisted delivery callers invoke this only after creating a delivery attempt."""


class QueryableDeliveryAdapter(ChannelAdapter, Protocol):
    """A provider that can prove the result of a previous delivery attempt."""

    async def reconcile_delivery(
        self,
        binding: ChannelBinding,
        reply: ReplyEnvelope,
        *,
        provider_message_id: str | None,
        provider_idempotency_key: str | None,
    ) -> DeliveryResult | None:
        """Return a concrete result or ``None`` when the provider is still inconclusive."""


def deterministic_session_id(
    binding: ChannelBinding,
    *,
    conversation_id: str,
    conversation_type: str,
    principal_id: str,
    thread_id: str | None = None,
) -> str:
    """Stable per-tenant session addressing without leaking provider identity values."""

    scope = "dm" if conversation_type == "direct" else "group"
    pieces = [
        binding.tenant_id,
        binding.agent_id,
        binding.provider,
        binding.external_account_id,
        scope,
        conversation_id,
    ]
    if scope == "dm":
        pieces.append(principal_id)
    if thread_id:
        pieces.append(thread_id)
    return "ses_" + hashlib.sha256("\x1f".join(pieces).encode()).hexdigest()[:32]


def _stable_id(prefix: str, *pieces: object) -> str:
    raw = "\x1f".join(str(piece) for piece in pieces)
    return f"{prefix}_{hashlib.sha256(raw.encode()).hexdigest()[:32]}"


def _decode_body(body: bytes | str | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(body, Mapping):
        return body
    raw = body.decode("utf-8") if isinstance(body, bytes) else body
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ChannelError("invalid_json", "provider callback was not valid JSON") from exc
    if not isinstance(result, dict):
        raise ChannelError("invalid_json", "provider callback JSON must be an object")
    return result


def _xml_body(body: bytes | str | Mapping[str, Any]) -> element_tree.Element:
    if isinstance(body, Mapping):
        raise ChannelError("invalid_xml", "WeCom callback must be XML")
    raw = body.decode("utf-8") if isinstance(body, bytes) else body
    try:
        return element_tree.fromstring(raw)
    except element_tree.ParseError as exc:
        raise ChannelError("invalid_xml", "WeCom callback was not valid XML") from exc


def _xml_value(root: element_tree.Element, name: str, default: str = "") -> str:
    child = root.find(name)
    return child.text.strip() if child is not None and child.text else default


def _secret_field(secret: str, field_name: str, *, fallback_to_raw: bool = False) -> str | None:
    try:
        parsed = parse_secret_json(secret)
    except ValueError:
        # A malformed JSON-shaped secret must never be treated as a raw token:
        # it could then be interpolated into a provider URL and leak in a
        # low-level client exception.  Raw credentials remain supported.
        candidate = secret.strip()
        if fallback_to_raw and candidate and candidate[0] not in {"{", "[", "'", '"'}:
            return candidate
        return None
    value = parsed.get(field_name)
    return str(value) if value is not None else None


async def _http_json(
    method: str,
    url: str,
    *,
    body: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
) -> tuple[int, Mapping[str, Any], Mapping[str, str]]:
    """Small stdlib HTTP client used only when optional httpx is not wired in."""

    def request() -> tuple[int, Mapping[str, Any], Mapping[str, str]]:
        payload = json.dumps(body).encode() if body is not None else None
        request_headers = {"Accept": "application/json", **(headers or {})}
        if payload is not None:
            request_headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=payload, headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as response:  # noqa: S310 - provider URL is fixed/configured
                raw = response.read().decode("utf-8")
                parsed = json.loads(raw) if raw else {}
                return (
                    response.status,
                    parsed if isinstance(parsed, dict) else {},
                    dict(response.headers.items()),
                )
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = {}
            return exc.code, parsed if isinstance(parsed, dict) else {}, dict(exc.headers.items())
        except (http.client.HTTPException, OSError, ValueError) as exc:
            # Never propagate a stdlib exception: its rendered URL may embed a
            # provider token in path-based APIs such as Telegram's.
            raise ChannelError(
                "provider_transport_error",
                "provider HTTP transport failed",
                retryable=True,
                status_code=503,
            ) from exc

    return await asyncio.to_thread(request)


class WeComAdapter:
    """Enterprise WeChat adapter with signature verification and optional AES decrypt."""

    provider = "wecom"

    def __init__(self, secrets: SecretProvider, *, http_json=_http_json) -> None:
        self._secrets = secrets
        self._http_json = http_json

    async def validate_and_normalize(
        self, binding: ChannelBinding, request: CallbackRequest
    ) -> InboundEnvelope:
        binding.require_active(self.provider)
        root = _xml_body(request.body)
        encrypted = _xml_value(root, "Encrypt")
        secret = await self._secrets.get(binding.secret_ref)
        token = _secret_field(secret, "token", fallback_to_raw=True)
        if not token:
            raise ChannelError("binding_secret_invalid", "WeCom token is missing", status_code=500)
        timestamp = request.query.get("timestamp", "")
        nonce = request.query.get("nonce", "")
        signature_name = "msg_signature" if encrypted else "signature"
        signature = request.query.get(signature_name, "")
        signed_values = [token, timestamp, nonce] + ([encrypted] if encrypted else [])
        expected = hashlib.sha1("".join(sorted(signed_values)).encode()).hexdigest()
        if not signature or not hmac.compare_digest(expected, signature):
            raise ChannelError(
                "invalid_signature", "WeCom callback signature did not verify", status_code=401
            )
        if encrypted:
            root = self._decrypt_xml(encrypted, secret, binding)

        message_type = _xml_value(root, "MsgType", "text").lower()
        from_user = _xml_value(root, "FromUserName")
        to_user = _xml_value(root, "ToUserName", binding.external_account_id)
        if not from_user:
            raise ChannelError("invalid_message", "WeCom callback did not contain a sender")
        expected_receiver = str(binding.capabilities.get("receiver_id", ""))
        if expected_receiver and to_user != expected_receiver:
            raise ChannelError(
                "wrong_receiver", "WeCom callback receiver does not match binding", status_code=403
            )
        occurred_seconds = _xml_value(root, "CreateTime")
        occurred_at = _unix_time(occurred_seconds, request.received_at)
        chat_id = _xml_value(root, "ChatId") or _xml_value(root, "GroupId")
        conversation_id = chat_id or from_user
        conversation_type = "group" if chat_id else "direct"
        external_message_id = _xml_value(root, "MsgId") or _stable_id(
            "wecom_event",
            to_user,
            from_user,
            message_type,
            occurred_seconds,
            _xml_value(root, "Event"),
        )
        text = _xml_value(root, "Content") if message_type == "text" else None
        attachments = _wecom_attachments(root, message_type)
        trace = current_trace_context()
        session_id = deterministic_session_id(
            binding,
            conversation_id=conversation_id,
            conversation_type=conversation_type,
            principal_id=from_user,
        )
        return InboundEnvelope(
            event_id=_stable_id(
                "evt", self.provider, binding.external_account_id, external_message_id
            ),
            idempotency_key=f"wecom:{binding.external_account_id}:{external_message_id}",
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            agent_id=binding.agent_id,
            channel=self.provider,
            channel_account_id=to_user,
            external_message_id=external_message_id,
            conversation_id=conversation_id,
            conversation_type=conversation_type,
            principal=Principal(external_user_id=from_user),
            text=text,
            attachments=attachments,
            occurred_at=occurred_at,
            session_id=session_id,
            traceparent=trace.traceparent,
            raw_type=message_type,
        )

    def _decrypt_xml(
        self, encrypted: str, secret: str, binding: ChannelBinding
    ) -> element_tree.Element:
        aes_key = _secret_field(secret, "encoding_aes_key")
        if not aes_key:
            raise ChannelError(
                "encrypted_payload_unsupported",
                "binding lacks a WeCom encoding AES key",
                status_code=400,
            )
        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        except ImportError as exc:  # pragma: no cover - dependency is installed in production image
            raise ChannelError(
                "crypto_unavailable",
                "encrypted WeCom payload support requires cryptography",
                status_code=500,
            ) from exc
        try:
            key = base64.b64decode(aes_key + "=")
            raw = base64.b64decode(encrypted)
            decrypted = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor().update(raw)
            padding = decrypted[-1]
            if not 1 <= padding <= 32 or decrypted[-padding:] != bytes([padding]) * padding:
                raise ValueError("invalid PKCS#7 padding")
            content = decrypted[16:-padding]
            message_length = int.from_bytes(content[:4], "big")
            xml_bytes = content[4 : 4 + message_length]
            receiver = content[4 + message_length :].decode("utf-8")
            expected_receiver = str(binding.capabilities.get("receiver_id", ""))
            if expected_receiver and receiver != expected_receiver:
                raise ValueError("receiver mismatch")
            return element_tree.fromstring(xml_bytes)
        except (ValueError, UnicodeDecodeError, element_tree.ParseError) as exc:
            raise ChannelError(
                "invalid_encrypted_payload", "WeCom encrypted callback could not be decrypted"
            ) from exc

    async def deliver(self, binding: ChannelBinding, reply: ReplyEnvelope) -> DeliveryResult:
        binding.require_active(self.provider)
        secret = await self._secrets.get(binding.secret_ref)
        corp_id = _secret_field(secret, "corp_id") or str(binding.capabilities.get("corp_id", ""))
        corp_secret = _secret_field(secret, "corp_secret")
        agent_id = str(binding.capabilities.get("wecom_agent_id", ""))
        if not corp_id or not corp_secret or not agent_id:
            raise ChannelError(
                "credentials_unavailable",
                "WeCom delivery requires corp_id, corp_secret, and agent id",
                retryable=False,
                status_code=503,
            )
        token_url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?" + urllib.parse.urlencode(
            {"corpid": corp_id, "corpsecret": corp_secret}
        )
        token_status, token_data, _ = await self._http_json("GET", token_url)
        access_token = token_data.get("access_token")
        if token_status != 200 or not access_token:
            return _wecom_delivery_error(token_status, token_data)
        url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?" + urllib.parse.urlencode(
            {"access_token": str(access_token)}
        )
        payload = {
            "touser": reply.recipient_id,
            "msgtype": "text",
            "agentid": int(agent_id),
            "text": {"content": reply.plain_text(2048)},
            "safe": 0,
        }
        status, data, _ = await self._http_json("POST", url, body=payload)
        if status == 200 and int(data.get("errcode", -1)) == 0:
            message_id = str(data.get("msgid") or _stable_id("wecom_delivery", reply.delivery_id))
            return DeliveryResult(
                "accepted", DeliveryCapability.NON_RETRIABLE, provider_message_id=message_id
            )
        return _wecom_delivery_error(status, data)


def _wecom_attachments(root: element_tree.Element, message_type: str) -> tuple[Attachment, ...]:
    if message_type == "text":
        return ()
    media_id = _xml_value(root, "MediaId") or None
    return (Attachment(kind=message_type, provider_media_id=media_id),)


def _wecom_delivery_error(status: int, data: Mapping[str, Any]) -> DeliveryResult:
    code = str(data.get("errcode", f"http_{status}"))
    if status == 429 or code in {"45009", "45011"}:
        return DeliveryResult(
            "failed", DeliveryCapability.NON_RETRIABLE, error_code=code, retry_after_seconds=1
        )
    if status >= 500:
        return DeliveryResult("failed", DeliveryCapability.NON_RETRIABLE, error_code=code)
    return DeliveryResult("failed", DeliveryCapability.NON_RETRIABLE, error_code=code)


class TelegramAdapter:
    """Telegram webhook adapter with secret-token verification and update ID dedupe keys."""

    provider = "telegram"

    def __init__(self, secrets: SecretProvider, *, http_json=_http_json) -> None:
        self._secrets = secrets
        self._http_json = http_json

    async def validate_and_normalize(
        self, binding: ChannelBinding, request: CallbackRequest
    ) -> InboundEnvelope:
        binding.require_active(self.provider)
        secret = await self._secrets.get(binding.secret_ref)
        expected = _secret_field(secret, "webhook_secret") or str(
            binding.capabilities.get("webhook_secret", "")
        )
        provided = _header(request.headers, "x-telegram-bot-api-secret-token") or ""
        if not expected or not hmac.compare_digest(expected, provided):
            raise ChannelError(
                "invalid_webhook_secret", "Telegram webhook secret did not verify", status_code=401
            )
        update = _decode_body(request.body)
        update_id = update.get("update_id")
        if not isinstance(update_id, int):
            raise ChannelError("invalid_update", "Telegram update_id is required")
        message = _telegram_message(update)
        if message is None:
            raise ChannelError(
                "unsupported_update", "Telegram update has no deliverable message", status_code=202
            )
        chat = message.get("chat") if isinstance(message.get("chat"), Mapping) else {}
        sender = message.get("from") if isinstance(message.get("from"), Mapping) else {}
        chat_id = chat.get("id")
        sender_id = sender.get("id")
        if chat_id is None or sender_id is None:
            raise ChannelError("invalid_update", "Telegram update lacks chat or sender")
        chat_type = str(chat.get("type", "private"))
        conversation_type = "direct" if chat_type == "private" else "group"
        message_id = str(message.get("message_id", update_id))
        occurred_at = _unix_time(str(message.get("date", "")), request.received_at)
        attachments = _telegram_attachments(message)
        text = message.get("text") or message.get("caption")
        if text is not None and not isinstance(text, str):
            text = str(text)
        principal = str(sender_id)
        conversation = str(chat_id)
        trace = current_trace_context()
        return InboundEnvelope(
            event_id=_stable_id("evt", self.provider, binding.external_account_id, update_id),
            idempotency_key=f"telegram:{binding.external_account_id}:update:{update_id}",
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            agent_id=binding.agent_id,
            channel=self.provider,
            channel_account_id=binding.external_account_id,
            external_message_id=message_id,
            conversation_id=conversation,
            conversation_type=conversation_type,
            principal=Principal(external_user_id=principal, display_name=_telegram_name(sender)),
            text=text,
            attachments=attachments,
            occurred_at=occurred_at,
            session_id=deterministic_session_id(
                binding,
                conversation_id=conversation,
                conversation_type=conversation_type,
                principal_id=principal,
            ),
            traceparent=trace.traceparent,
            raw_type="text" if text else "media",
        )

    async def deliver(self, binding: ChannelBinding, reply: ReplyEnvelope) -> DeliveryResult:
        binding.require_active(self.provider)
        secret = await self._secrets.get(binding.secret_ref)
        token = _secret_field(secret, "bot_token", fallback_to_raw=True)
        if not token:
            raise ChannelError(
                "credentials_unavailable", "Telegram bot token is unavailable", status_code=503
            )
        headers: dict[str, str] = {}
        inject_trace_context(headers)
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        status, data, response_headers = await self._http_json(
            "POST",
            url,
            body={"chat_id": reply.recipient_id, "text": reply.plain_text(4096)},
            headers=headers,
        )
        result = data.get("result") if isinstance(data.get("result"), Mapping) else {}
        if status == 200 and data.get("ok") is True:
            return DeliveryResult(
                "accepted",
                DeliveryCapability.NON_RETRIABLE,
                provider_message_id=str(result.get("message_id", "")) or None,
            )
        if status == 429:
            retry = (
                data.get("parameters", {}).get("retry_after")
                if isinstance(data.get("parameters"), Mapping)
                else None
            )
            return DeliveryResult(
                "failed",
                DeliveryCapability.NON_RETRIABLE,
                error_code="rate_limited",
                retry_after_seconds=int(retry or 1),
            )
        if status >= 500:
            return DeliveryResult(
                "unknown", DeliveryCapability.NON_RETRIABLE, error_code=f"http_{status}"
            )
        return DeliveryResult(
            "failed",
            DeliveryCapability.NON_RETRIABLE,
            error_code=str(data.get("description", f"http_{status}")),
        )


def _telegram_message(update: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for key in ("message", "edited_message", "channel_post"):
        value = update.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def _telegram_name(sender: Mapping[str, Any]) -> str | None:
    name = " ".join(str(sender.get(key, "")) for key in ("first_name", "last_name")).strip()
    return name or None


def _telegram_attachments(message: Mapping[str, Any]) -> tuple[Attachment, ...]:
    if isinstance(message.get("photo"), Sequence):
        photos = message["photo"]
        if photos and isinstance(photos[-1], Mapping):
            return (
                Attachment(
                    kind="image", provider_media_id=str(photos[-1].get("file_id", "")) or None
                ),
            )
    for field_name, kind in (
        ("document", "file"),
        ("video", "video"),
        ("audio", "audio"),
        ("voice", "audio"),
    ):
        item = message.get(field_name)
        if isinstance(item, Mapping):
            return (
                Attachment(
                    kind=kind,
                    provider_media_id=str(item.get("file_id", "")) or None,
                    filename=str(item.get("file_name", "")) or None,
                    content_type=str(item.get("mime_type", "")) or None,
                ),
            )
    return ()


def _header(headers: Mapping[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name), None)


def _unix_time(value: str, fallback: datetime) -> datetime:
    try:
        return datetime.fromtimestamp(int(value), UTC)
    except (TypeError, ValueError, OSError):
        return fallback


class MockChannelAdapter:
    """Functional credential-free channel used by local demo and focused tests.

    Re-submit an identical event ID to simulate provider callback retries.  Delivery
    outcomes can be queued per binding to exercise accepted, failed, and unknown paths.
    ``unknown_accepted`` simulates a provider that applied a request but lost its
    synchronous acknowledgement; its query endpoint can later prove delivery.
    """

    provider = "mock"

    def __init__(self) -> None:
        self.callbacks: list[InboundEnvelope] = []
        self.deliveries: list[ReplyEnvelope] = []
        self._delivery_outcomes: defaultdict[str, deque[str]] = defaultdict(deque)
        self._accepted_delivery_ids: dict[str, str] = {}

    def queue_delivery_outcome(self, binding_id: str, outcome: str) -> None:
        if outcome not in {"accepted", "failed", "unknown", "unknown_accepted"}:
            raise ValueError("outcome must be accepted, failed, unknown, or unknown_accepted")
        self._delivery_outcomes[binding_id].append(outcome)

    @staticmethod
    def delivery_capability(binding: ChannelBinding) -> DeliveryCapability:
        """Select the recovery contract used by a focused integration binding."""

        raw = str(binding.capabilities.get("delivery_capability", DeliveryCapability.IDEMPOTENT))
        try:
            return DeliveryCapability(raw)
        except ValueError as exc:
            raise ChannelError("invalid_delivery_capability", "mock delivery capability is invalid") from exc

    async def validate_and_normalize(
        self, binding: ChannelBinding, request: CallbackRequest
    ) -> InboundEnvelope:
        binding.require_active(self.provider)
        payload = _decode_body(request.body)
        expected = str(binding.capabilities.get("callback_secret", ""))
        if expected:
            provided = _header(request.headers, "x-mock-secret") or ""
            if not hmac.compare_digest(expected, provided):
                raise ChannelError(
                    "invalid_mock_secret", "mock callback secret did not verify", status_code=401
                )
        external_message_id = str(payload.get("message_id") or payload.get("event_id") or "")
        principal = str(payload.get("user_id") or "")
        conversation = str(payload.get("conversation_id") or principal)
        if not external_message_id or not principal:
            raise ChannelError(
                "invalid_mock_callback", "mock callback requires message_id and user_id"
            )
        conversation_type = str(payload.get("conversation_type", "direct"))
        if conversation_type not in {"direct", "group"}:
            raise ChannelError(
                "invalid_mock_callback", "mock conversation_type must be direct or group"
            )
        raw_attachments = payload.get("attachments", [])
        attachments = tuple(
            Attachment(
                kind=str(item.get("kind", "file")),
                url=item.get("url"),
                filename=item.get("filename"),
            )
            for item in raw_attachments
            if isinstance(item, Mapping)
        )
        trace = current_trace_context()
        envelope = InboundEnvelope(
            event_id=_stable_id(
                "evt", self.provider, binding.external_account_id, external_message_id
            ),
            idempotency_key=f"mock:{binding.external_account_id}:{external_message_id}",
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            agent_id=binding.agent_id,
            channel=self.provider,
            channel_account_id=binding.external_account_id,
            external_message_id=external_message_id,
            conversation_id=conversation,
            conversation_type=conversation_type,
            principal=Principal(
                external_user_id=principal,
                display_name=str(payload.get("display_name", "")) or None,
            ),
            text=str(payload.get("text")) if payload.get("text") is not None else None,
            attachments=attachments,
            occurred_at=request.received_at,
            session_id=deterministic_session_id(
                binding,
                conversation_id=conversation,
                conversation_type=conversation_type,
                principal_id=principal,
            ),
            traceparent=trace.traceparent,
            raw_type="text",
        )
        self.callbacks.append(envelope)
        return envelope

    async def deliver(self, binding: ChannelBinding, reply: ReplyEnvelope) -> DeliveryResult:
        binding.require_active(self.provider)
        self.deliveries.append(reply)
        capability = self.delivery_capability(binding)
        if reply.delivery_id in self._accepted_delivery_ids:
            return DeliveryResult(
                "accepted",
                capability,
                provider_message_id=self._accepted_delivery_ids[reply.delivery_id],
            )
        outcome = (
            self._delivery_outcomes[binding.binding_id].popleft()
            if self._delivery_outcomes[binding.binding_id]
            else "accepted"
        )
        if outcome == "accepted":
            provider_id = _stable_id("mock_msg", binding.binding_id, reply.delivery_id)
            self._accepted_delivery_ids[reply.delivery_id] = provider_id
            return DeliveryResult("accepted", capability, provider_message_id=provider_id)
        if outcome == "unknown_accepted":
            provider_id = _stable_id("mock_msg", binding.binding_id, reply.delivery_id)
            self._accepted_delivery_ids[reply.delivery_id] = provider_id
            return DeliveryResult(
                "unknown", DeliveryCapability.QUERYABLE, error_code="simulated_lost_ack"
            )
        if outcome == "unknown":
            return DeliveryResult(
                "unknown", capability, error_code="simulated_crash_gap"
            )
        return DeliveryResult(
            "failed", capability, error_code="simulated_provider_failure"
        )

    async def reconcile_delivery(
        self,
        binding: ChannelBinding,
        reply: ReplyEnvelope,
        *,
        provider_message_id: str | None,
        provider_idempotency_key: str | None,
    ) -> DeliveryResult | None:
        """Concrete provider-query seam used to prove an ambiguous mock send.

        Real adapters must implement the same operation only when their provider
        exposes a queryable delivery identifier.  Returning ``reconciling`` is
        deliberately distinct from resending the original message.
        """

        del provider_message_id, provider_idempotency_key
        binding.require_active(self.provider)
        if self.delivery_capability(binding) != DeliveryCapability.QUERYABLE:
            raise ChannelError(
                "mock_delivery_not_queryable", "mock binding was not configured as queryable"
            )
        provider_id = self._accepted_delivery_ids.get(reply.delivery_id)
        if provider_id:
            return DeliveryResult(
                "accepted", DeliveryCapability.QUERYABLE, provider_message_id=provider_id
            )
        return DeliveryResult(
            "reconciling", DeliveryCapability.QUERYABLE, error_code="mock_query_inconclusive"
        )
