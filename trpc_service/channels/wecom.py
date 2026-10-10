"""Enterprise WeChat AI Bot long-connection adapter.

The concrete client uses the public ``wecom-aibot-sdk-python`` API pulled in by
tRPC-Agent's ``openclaw`` extra. Client and lock interfaces remain injectable so
protocol tests never open a network connection.
"""

from __future__ import annotations

import asyncio
import builtins
import hashlib
import inspect
import logging
import mimetypes
import random
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Protocol, cast
from urllib.parse import urlsplit

from trpc_service.channels.base import ChannelCapabilities, InboundSink
from trpc_service.channels.envelopes import (
    DeliveryReceipt,
    DeliveryStatus,
    InboundEnvelope,
    MediaReference,
    OutboundEnvelope,
    PayloadKind,
    RecallEnvelope,
)
from trpc_service.channels.media_locator import (
    WeComMediaLocatorCipher,
    WeComMediaLocatorError,
)
from trpc_service.channels.wecom_download import (
    BoundedWeComDownloadClient,
    WeComDownloadError,
)
from trpc_service.config.secrets import SecretProvider, SecretRef
from trpc_service.storage.models import WeComBindingLeaseGrant
from trpc_service.tenant.models import Channel, ChannelBinding, ConversationKind

logger = logging.getLogger(__name__)
_MEDIA_DOWNLOAD_ATTEMPTS = 3
_WECOM_RATE_LIMIT_CODES = frozenset({429, 45009, 45011})
_WECOM_RETRYABLE_CODES = frozenset({429, 500, 502, 503, 504, 45009, 45011})
_MISSING = object()
_GROUP_MENTION_RE = re.compile(r"^\s*(?:@[^\s@]+|<@[^>]+>)\s*", flags=re.UNICODE)
_JITTER_RANDOM = random.SystemRandom()


class BindingLease(Protocol):
    async def acquire_binding(
        self, binding: ChannelBinding, owner_id: str
    ) -> WeComBindingLeaseGrant | None: ...

    async def mark_authenticated(self, grant: WeComBindingLeaseGrant) -> bool: ...

    async def record_provider_event(
        self, grant: WeComBindingLeaseGrant, provider_event_id: str
    ) -> bool: ...

    async def mark_disconnected(self, grant: WeComBindingLeaseGrant) -> bool: ...

    async def release_binding(self, grant: WeComBindingLeaseGrant) -> None: ...


class WeComBindingLeaseUnavailable(RuntimeError):
    """Signal that a healthy peer currently owns the binding connection."""


class WeComDownloadClient(Protocol):
    async def download_file(self, url: str, aes_key: str) -> tuple[bytes, str | None]: ...

    def disconnect(self) -> Any: ...


class WeComClient(WeComDownloadClient, Protocol):
    is_connected: bool
    is_authenticated: bool

    def on(self, event: str, handler: Callable[..., Awaitable[None] | None]) -> Any: ...

    async def connect_async(self) -> None: ...

    def disconnect(self) -> Any: ...

    async def send_message(self, chat_id: str, body: Mapping[str, Any]) -> Any: ...


WeComClientFactory = Callable[[str, str], WeComClient]
WeComDownloadClientFactory = Callable[[str, str], WeComDownloadClient]


def sdk_client_factory(bot_id: str, secret: str) -> WeComClient:
    from wecom_aibot_sdk import WSClient

    return cast(
        WeComClient,
        WSClient({"bot_id": bot_id, "secret": secret, "max_reconnect_attempts": -1}),
    )


def _body(frame: object) -> Mapping[str, Any]:
    if isinstance(frame, Mapping):
        body = frame.get("body", {})
    else:
        body = getattr(frame, "body", {})
    if not isinstance(body, Mapping):
        raise ValueError("invalid WeCom frame body")
    _validate_frame_shape(body)
    return body


def _validate_frame_shape(
    value: object, *, depth: int = 0, fields: list[int] | None = None
) -> None:
    """Bound untrusted WebSocket frames before hashing or model construction."""

    if fields is None:
        fields = [0]
    if depth > 10:
        raise ValueError("WeCom frame nesting is too deep")
    if isinstance(value, Mapping):
        fields[0] += len(value)
        if fields[0] > 256:
            raise ValueError("WeCom frame has too many fields")
        for key, item in value.items():
            if not isinstance(key, str) or len(key) > 128:
                raise ValueError("WeCom frame field name is invalid")
            _validate_frame_shape(item, depth=depth + 1, fields=fields)
    elif isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise ValueError("WeCom frame list is too large")
        for item in value:
            _validate_frame_shape(item, depth=depth + 1, fields=fields)
    elif isinstance(value, str) and len(value.encode("utf-8")) > 128 * 1024:
        raise ValueError("WeCom frame field is too large")


def parse_wecom_frame(frame: object, *, account_id: str) -> InboundEnvelope:
    body = _body(frame)
    sender = body.get("from", {})
    if not isinstance(sender, Mapping) or not sender.get("userid"):
        raise ValueError("WeCom frame has no sender")
    user_id = str(sender["userid"]).strip()
    if not user_id or len(user_id) > 512:
        raise ValueError("WeCom frame sender is invalid")
    raw_message_id = body.get("msgid")
    if raw_message_id is not None and not isinstance(raw_message_id, (str, int)):
        raise ValueError("WeCom frame message id is invalid")
    message_id = str(raw_message_id or "")[:512]
    if not message_id:
        digest = hashlib.sha256(repr(sorted(body.items())).encode()).hexdigest()
        message_id = "frame_" + digest
    kind = (
        ConversationKind.GROUP
        if str(body.get("chattype", "single")) == "group"
        else ConversationKind.DIRECT
    )
    chat_id = str(body.get("chatid") or user_id)
    message_type = str(body.get("msgtype") or "event").lower()
    event = body.get("event", {})
    event_type = None
    if isinstance(event, Mapping):
        event_type = str(event.get("eventtype") or "") or None
    bot_id = body.get("aibotid") or body.get("botid") or account_id
    raw_mentions = body.get("atuserlist", body.get("mentioned_list"))
    bot_mentioned = (
        kind == ConversationKind.GROUP
        and isinstance(bot_id, str)
        and _mentions_bot(raw_mentions, bot_id)
    )

    text: str | None = None
    media: list[MediaReference] = []
    if message_type == "text":
        text_data = body.get("text", {})
        text = (
            _normalize_group_text(
                str(text_data.get("content") or ""),
                mentioned=bot_mentioned,
            )
            if isinstance(text_data, Mapping)
            else ""
        )
    elif message_type == "voice":
        voice = body.get("voice", {})
        text = str(voice.get("content") or "") if isinstance(voice, Mapping) else ""
    elif message_type == "mixed":
        mixed = body.get("mixed", {})
        items = mixed.get("msg_item", []) if isinstance(mixed, Mapping) else []
        text_parts: list[str] = []
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, Mapping):
                continue
            if item.get("msgtype") == "text" and isinstance(item.get("text"), Mapping):
                text_parts.append(
                    _normalize_group_text(
                        str(item["text"].get("content") or ""),
                        mentioned=bot_mentioned,
                    )
                )
            item_type = str(item.get("msgtype") or "").lower()
            if item_type in {"image", "file", "video"} and isinstance(item.get(item_type), Mapping):
                media.append(_media_reference(item[item_type], media_type=item_type))
        text = "\n".join(part for part in text_parts if part) or None
    elif message_type in {"image", "file", "video"}:
        media_data = body.get(message_type, {})
        if isinstance(media_data, Mapping):
            media.append(_media_reference(media_data, media_type=message_type))

    created = body.get("create_time")
    if created is not None:
        if isinstance(created, bool):
            raise ValueError("WeCom frame timestamp is invalid")
        try:
            timestamp = int(created)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("WeCom frame timestamp is invalid") from exc
        now = int(time.time())
        if timestamp < 0 or timestamp > now + 86_400:
            raise ValueError("WeCom frame timestamp is invalid")
        occurred = datetime.fromtimestamp(timestamp, UTC)
    else:
        occurred = datetime.now(UTC)
    try:
        payload_kind = PayloadKind(message_type)
    except ValueError:
        payload_kind = PayloadKind.EVENT
    return InboundEnvelope(
        channel=Channel.WECOM_AI_BOT,
        account_id=account_id,
        external_message_id=message_id,
        external_user_id=user_id,
        conversation_kind=kind,
        external_conversation_id=chat_id if kind == ConversationKind.GROUP else None,
        payload_kind=payload_kind,
        text=text,
        media=tuple(media),
        event_type=event_type,
        occurred_at=occurred,
        provider_metadata={"target_id": chat_id},
    )


def _normalize_group_text(value: str, *, mentioned: bool) -> str:
    """Remove only the bot's leading group mention marker.

    WeCom delivers the actual user text separately from ``atuserlist``.  The
    marker is useful to the platform but should not become part of the Agent
    prompt.  We only strip it when the verified frame explicitly names this
    bot; ordinary ``@`` text and ``@all`` are preserved.
    """

    if not mentioned:
        return value
    return _GROUP_MENTION_RE.sub("", value, count=1)


def _mentions_bot(value: object, bot_id: str) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    for item in value:
        candidate = item.get("userid") if isinstance(item, Mapping) else item
        if str(candidate) == bot_id:
            return True
    return False


def _media_reference(data: Mapping[str, Any], *, media_type: str | None = None) -> MediaReference:
    content_type = {
        "image": "image/*",
        "video": "video/*",
    }.get(media_type or "")
    return MediaReference(
        provider_url=str(data.get("url") or "") or None,
        encryption_key_ref=str(data.get("aeskey") or "") or None,
        filename=_safe_filename(data.get("filename") or data.get("name")),
        content_type=content_type,
    )


class WeComMediaError(RuntimeError):
    """Safe WeCom media failure without provider response or secret content."""

    def __init__(
        self,
        provider_code: str,
        *,
        retryable: bool = False,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__("WeCom media download failed")
        self.provider_code = provider_code
        self.retryable = retryable
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True, slots=True)
class WeComResource:
    """Downloaded and decrypted WeCom media with safe provider metadata."""

    bytes: builtins.bytes
    content_type: str
    filename: str | None
    provider_media_id: str

    @property
    def data(self) -> builtins.bytes:
        return self.bytes

    @property
    def provider_id(self) -> str:
        return self.provider_media_id


class WeComMediaDownloader:
    """Download WeCom media through per-binding, bounded HTTP clients.

    The default client streams a signed HTTPS response with a ciphertext
    ceiling before applying the provider-compatible AES-CBC decryption. A
    custom SDK-compatible factory remains injectable for offline contracts.
    The URL and AES key exist only in a transient ``MediaReference`` and are
    never included in exceptions or logs.
    """

    def __init__(
        self,
        secrets: SecretProvider,
        *,
        client_factory: WeComDownloadClientFactory | None = None,
        locator_cipher: WeComMediaLocatorCipher | None = None,
        max_media_bytes: int = 20 * 1024 * 1024,
        media_timeout_seconds: float = 30.0,
    ) -> None:
        if max_media_bytes <= 0:
            raise ValueError("media byte limit must be positive")
        if media_timeout_seconds <= 0:
            raise ValueError("media timeout must be positive")
        self._secrets = secrets
        self._client_factory = client_factory
        self._locator_cipher = locator_cipher
        self._max_media_bytes = max_media_bytes
        self._media_timeout_seconds = media_timeout_seconds
        self._clients: dict[str, WeComDownloadClient] = {}
        self._client_refs: dict[str, tuple[str, str]] = {}
        self._client_lock = asyncio.Lock()
        self._closed = False

    async def download_media(
        self,
        binding: ChannelBinding,
        message_id: str,
        media_key: str,
        *,
        media_type: str = "file",
        filename: str | None = None,
        media_reference: MediaReference | None = None,
    ) -> WeComResource:
        if binding.channel != Channel.WECOM_AI_BOT:
            raise WeComMediaError("binding_mismatch")
        reference = self._open_media_reference(binding, message_id, media_reference)
        _media_inputs(reference)
        client = await self._client_for(binding)
        return await _download_wecom_media_with_retry(
            client,
            media_key,
            reference,
            media_type=media_type,
            filename=filename or reference.filename,
            max_media_bytes=self._max_media_bytes,
            media_timeout_seconds=self._media_timeout_seconds,
            require_connection=False,
        )

    def _open_media_reference(
        self,
        binding: ChannelBinding,
        message_id: str,
        reference: MediaReference | None,
    ) -> MediaReference:
        reference = _require_media_reference(reference)
        if not _is_sealed_locator(reference):
            return reference
        if self._locator_cipher is None:
            raise WeComMediaError("media_locator_unavailable")
        try:
            return self._locator_cipher.open(reference, binding.binding_id, message_id)
        except WeComMediaLocatorError:
            raise WeComMediaError("media_locator_invalid") from None

    async def close(self) -> None:
        async with self._client_lock:
            if self._closed:
                return
            self._closed = True
            clients = tuple(self._clients.values())
            self._clients.clear()
            self._client_refs.clear()
        for client in clients:
            try:
                result = client.disconnect()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.warning("WeCom media client close failed")

    async def _client_for(self, binding: ChannelBinding) -> WeComDownloadClient:
        async with self._client_lock:
            if self._closed:
                raise WeComMediaError("downloader_closed")
            secret_ref = binding.secret_refs.get("bot_secret")
            cache_ref = (
                binding.account_id,
                secret_ref.uri if self._client_factory is not None and secret_ref else "bounded:v1",
            )
            client = self._clients.get(binding.binding_id)
            if client is not None and self._client_refs.get(binding.binding_id) == cache_ref:
                return client
            if client is not None:
                result = client.disconnect()
                if inspect.isawaitable(result):
                    # A stale client's HTTP session must not outlive its binding.
                    await result
                self._clients.pop(binding.binding_id, None)
                self._client_refs.pop(binding.binding_id, None)
            if self._client_factory is None:
                client = BoundedWeComDownloadClient(
                    self._max_media_bytes,
                    self._media_timeout_seconds,
                )
            else:
                if secret_ref is None:
                    raise WeComMediaError("credentials_missing")
                try:
                    secret = _resolve_tenant_secret(self._secrets, secret_ref)
                    client = self._client_factory(binding.account_id, secret)
                except WeComMediaError:
                    raise
                except Exception:
                    raise WeComMediaError("credentials_unavailable", retryable=True) from None
            self._clients[binding.binding_id] = client
            self._client_refs[binding.binding_id] = cache_ref
            return client


def _require_media_reference(reference: MediaReference | None) -> MediaReference:
    if reference is None:
        raise WeComMediaError("media_metadata_missing")
    return reference


def _is_sealed_locator(reference: MediaReference) -> bool:
    provider_id = reference.provider_media_id
    return (
        isinstance(provider_id, str)
        and provider_id.startswith("v1.")
        and reference.provider_url is None
        and reference.encryption_key_ref is None
    )


def _media_inputs(reference: MediaReference) -> tuple[str, str]:
    url = reference.provider_url
    aes_key = reference.encryption_key_ref
    if not isinstance(url, str) or not _is_https_url(url):
        raise WeComMediaError("media_url_invalid")
    if not isinstance(aes_key, str) or not aes_key.strip():
        raise WeComMediaError("media_key_missing")
    return url, aes_key


def _is_https_url(value: str) -> bool:
    try:
        parsed = urlsplit(value.strip())
    except ValueError:
        return False
    return parsed.scheme.lower() == "https" and bool(parsed.hostname) and not parsed.username


def _media_provider_id(media_key: str, url: str) -> str:
    if isinstance(media_key, str) and media_key.strip() and not _looks_like_url(media_key):
        return media_key.strip()[:512]
    return "url_" + hashlib.sha256(url.encode("utf-8")).hexdigest()


def _looks_like_url(value: str) -> bool:
    return value.lstrip().lower().startswith(("http://", "https://"))


async def _download_wecom_media(
    client: WeComDownloadClient,
    media_key: str,
    reference: MediaReference,
    *,
    media_type: str,
    filename: str | None,
    max_media_bytes: int,
    media_timeout_seconds: float,
    require_connection: bool,
) -> WeComResource:
    if require_connection and not _client_ready(client):
        raise WeComMediaError("connector_unavailable", retryable=True)
    media_type = _media_kind(media_type, reference)
    if media_type not in {"image", "file", "mixed", "video"}:
        raise WeComMediaError("media_type_invalid")
    url, aes_key = _media_inputs(reference)
    downloader = getattr(client, "download_file", None)
    if not callable(downloader):
        raise WeComMediaError("media_api_unavailable", retryable=True)
    try:
        result = await asyncio.wait_for(
            downloader(url, aes_key),
            timeout=media_timeout_seconds,
        )
    except asyncio.CancelledError:
        raise
    except WeComDownloadError as exc:
        raise WeComMediaError(
            exc.provider_code,
            retryable=exc.retryable,
            status_code=exc.status_code,
            retry_after_seconds=exc.retry_after_seconds,
        ) from None
    except TimeoutError:
        raise WeComMediaError("transport_timeout", retryable=True) from None
    except ValueError:
        raise WeComMediaError("decrypt_failed") from None
    except Exception as exc:
        status = getattr(exc, "status", None)
        if isinstance(status, bool):
            status = None
        if isinstance(status, int) and status == 404:
            raise WeComMediaError("media_not_found", status_code=status) from None
        if isinstance(status, int) and status == 429:
            raise WeComMediaError(
                "rate_limited",
                retryable=True,
                status_code=status,
                retry_after_seconds=_response_retry_after(exc),
            ) from None
        if isinstance(status, int) and status >= 500:
            raise WeComMediaError(
                "provider_unavailable", retryable=True, status_code=status
            ) from None
        if isinstance(exc, (ConnectionError, OSError)):
            raise WeComMediaError("transport_unknown", retryable=True) from None
        raise WeComMediaError("provider_error", retryable=True) from None

    if not isinstance(result, tuple) or len(result) != 2:
        raise WeComMediaError("media_response_invalid")
    body, response_filename = result
    if not isinstance(body, (bytes, bytearray, memoryview)):
        raise WeComMediaError("media_response_invalid")
    body_bytes = bytes(body)
    if len(body_bytes) > max_media_bytes:
        raise WeComMediaError("media_too_large")
    safe_name = _safe_filename(filename) or _safe_filename(response_filename)
    return WeComResource(
        bytes=body_bytes,
        content_type=_content_type(safe_name, media_type),
        filename=safe_name,
        provider_media_id=_media_provider_id(media_key, url),
    )


async def _download_wecom_media_with_retry(
    client: WeComDownloadClient,
    media_key: str,
    reference: MediaReference,
    *,
    media_type: str,
    filename: str | None,
    max_media_bytes: int,
    media_timeout_seconds: float,
    require_connection: bool,
) -> WeComResource:
    for attempt in range(_MEDIA_DOWNLOAD_ATTEMPTS):
        try:
            return await _download_wecom_media(
                client,
                media_key,
                reference,
                media_type=media_type,
                filename=filename,
                max_media_bytes=max_media_bytes,
                media_timeout_seconds=media_timeout_seconds,
                require_connection=require_connection,
            )
        except WeComMediaError as error:
            if not error.retryable or attempt + 1 == _MEDIA_DOWNLOAD_ATTEMPTS:
                raise
            base_delay = 0.1 * (2**attempt)
            retry_after = error.retry_after_seconds
            if retry_after is not None:
                base_delay = max(base_delay, retry_after)
            # Keep the provider's minimum Retry-After intact while adding a
            # small positive jitter so concurrent media retries do not stampede
            # the platform after an outage.
            jittered_delay = min(base_delay * (1.0 + 0.2 * _JITTER_RANDOM.random()), 3600.0)
            await asyncio.sleep(jittered_delay)
    raise AssertionError("unreachable")


def _client_ready(client: object) -> bool:
    if not bool(getattr(client, "is_connected", False)):
        return False
    return getattr(client, "is_authenticated", False) is True


def _safe_filename(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.replace("\x00", "").strip().strip('"')
    value = value.replace("\\", "/").rsplit("/", 1)[-1]
    if not value or any(ord(char) < 32 for char in value):
        return None
    return value[:512]


def _content_type(filename: str | None, media_type: str) -> str:
    if filename:
        guessed, _ = mimetypes.guess_type(filename)
        if guessed:
            return guessed.split(";", 1)[0].lower()
    return {
        "image": "image/*",
        "video": "video/*",
    }.get(media_type, "application/octet-stream")


def _media_kind(media_type: str, reference: MediaReference) -> str:
    content_type = reference.content_type or ""
    if media_type == "file" and content_type.startswith("image/"):
        return "image"
    if media_type == "file" and content_type.startswith("video/"):
        return "video"
    return media_type


class WeComConnector:
    capabilities = ChannelCapabilities(
        outbound_payloads=frozenset({PayloadKind.TEXT}),
        stream=False,
        card=False,
        media=False,
        recall=False,
        proactive=True,
        text_split=False,
        max_text_bytes=None,
    )

    def __init__(
        self,
        secrets: SecretProvider,
        binding_lease: BindingLease,
        *,
        owner_id: str,
        client_factory: WeComClientFactory = sdk_client_factory,
        locator_cipher: WeComMediaLocatorCipher | None = None,
        max_media_bytes: int = 20 * 1024 * 1024,
        media_timeout_seconds: float = 30.0,
        reconnect_delay_seconds: float = 0.5,
        max_reconnect_delay_seconds: float = 30.0,
        authentication_timeout_seconds: float = 10.0,
    ) -> None:
        if max_media_bytes <= 0:
            raise ValueError("media byte limit must be positive")
        if media_timeout_seconds <= 0:
            raise ValueError("media timeout must be positive")
        if authentication_timeout_seconds <= 0:
            raise ValueError("authentication timeout must be positive")
        self._secrets = secrets
        self._binding_lease = binding_lease
        self._owner_id = owner_id
        self._client_factory = client_factory
        self._locator_cipher = locator_cipher
        self._max_media_bytes = max_media_bytes
        self._media_timeout_seconds = media_timeout_seconds
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnect_delay_seconds = max_reconnect_delay_seconds
        self._authentication_timeout_seconds = authentication_timeout_seconds
        self._clients: dict[str, WeComClient] = {}
        self._fenced_bindings: set[str] = set()

    async def run(
        self,
        binding: ChannelBinding,
        sink: InboundSink,
        stop_event: asyncio.Event | None = None,
        emergency_sink: InboundSink | None = None,
    ) -> None:
        grant = await self._binding_lease.acquire_binding(binding, self._owner_id)
        if grant is None:
            raise WeComBindingLeaseUnavailable("another connector owns this channel binding")
        client: WeComClient | None = None
        authenticated = False
        authentication_started = False
        authentication_complete = asyncio.Event()
        authentication_error: Exception | None = None
        disconnected = asyncio.Event()
        state_lock = asyncio.Lock()
        closing = False
        try:
            secret_ref = binding.secret_refs.get("bot_secret")
            if secret_ref is None:
                raise RuntimeError("WeCom bot secret reference is not configured")
            client = self._client_factory(
                binding.account_id,
                _resolve_tenant_secret(self._secrets, secret_ref),
            )
            self._clients[binding.binding_id] = client

            async def activate_if_authenticated() -> bool:
                nonlocal authenticated, authentication_error, authentication_started
                wait_for_existing = False
                async with state_lock:
                    if authenticated:
                        return True
                    if closing or disconnected.is_set() or authentication_complete.is_set():
                        return False
                    if authentication_started:
                        wait_for_existing = True
                    elif not _client_ready(client):
                        return False
                    else:
                        authentication_started = True
                if wait_for_existing:
                    await authentication_complete.wait()
                    return authenticated and binding.binding_id in self._fenced_bindings
                try:
                    fenced = await self._binding_lease.mark_authenticated(grant)
                except Exception as error:
                    async with state_lock:
                        if not closing:
                            authentication_error = error
                        authentication_complete.set()
                    return False
                async with state_lock:
                    if fenced and not closing and not disconnected.is_set():
                        authenticated = True
                        self._fenced_bindings.add(binding.binding_id)
                    authentication_complete.set()
                    return authenticated

            async def on_authenticated(*_: object) -> None:
                await activate_if_authenticated()

            async def on_disconnected(*_: object) -> None:
                nonlocal closing
                async with state_lock:
                    closing = True
                    self._fenced_bindings.discard(binding.binding_id)
                    disconnected.set()
                    authentication_complete.set()
                try:
                    await self._binding_lease.mark_disconnected(grant)
                except Exception:
                    logger.warning(
                        "WeCom disconnect evidence failed",
                        extra={"binding_id": binding.binding_id},
                    )

            async def accept_frame(frame: object) -> None:
                if not await activate_if_authenticated():
                    if not authentication_complete.is_set():
                        await authentication_complete.wait()
                    if binding.binding_id not in self._fenced_bindings:
                        return
                envelope = parse_wecom_frame(frame, account_id=binding.account_id)
                if self._locator_cipher is not None and envelope.media:
                    envelope = envelope.model_copy(
                        update={
                            "media": tuple(
                                self._locator_cipher.seal(
                                    reference,
                                    binding.binding_id,
                                    envelope.external_message_id,
                                )
                                for reference in envelope.media
                            )
                        }
                    )
                try:
                    await sink(binding.binding_id, envelope)
                except Exception:
                    if emergency_sink is None:
                        raise
                    # The connector does not silently discard a sink failure:
                    # the explicit emergency sink owns durable encrypted
                    # buffering and may itself raise to trigger backpressure.
                    await emergency_sink(binding.binding_id, envelope)
                await self._binding_lease.record_provider_event(grant, envelope.external_message_id)

            async def accept_video_frame(frame: object) -> None:
                """Handle video through the SDK's generic message event."""

                try:
                    message_type = str(_body(frame).get("msgtype") or "").lower()
                except ValueError:
                    return
                if message_type == "video":
                    await accept_frame(frame)

            client.on("authenticated", on_authenticated)
            client.on("disconnected", on_disconnected)
            client.on("event.disconnected_event", on_disconnected)
            for event_name in (
                "message.text",
                "message.image",
                "message.mixed",
                "message.voice",
                "message.file",
            ):
                client.on(event_name, accept_frame)
            # Lifecycle events such as ``enter_chat`` are not user prompts.
            # They need a dedicated idempotent handler before they can produce
            # outbound messages; routing them through the Agent duplicates the
            # reply when the user's first text frame follows the event.
            client.on("message", accept_video_frame)
            await client.connect_async()
            await activate_if_authenticated()
            activation = await _wait_for_authentication_or_end(
                authentication_complete,
                disconnected,
                stop_event,
                timeout_seconds=self._authentication_timeout_seconds,
            )
            if activation == "timeout":
                raise TimeoutError("WeCom authentication timed out")
            if activation == "authenticated":
                if authentication_error is not None:
                    raise authentication_error
                if not authenticated:
                    raise RuntimeError("WeCom authentication fencing rejected")
                if not _client_ready(client):
                    return
                if stop_event is None:
                    await disconnected.wait()
                else:
                    await _wait_for_disconnect_or_stop(disconnected, stop_event)
        finally:
            async with state_lock:
                closing = True
                self._fenced_bindings.discard(binding.binding_id)
                authentication_complete.set()
            try:
                if client is not None:
                    result = client.disconnect()
                    if inspect.isawaitable(result):
                        await result
            finally:
                self._clients.pop(binding.binding_id, None)
                try:
                    if authenticated:
                        await self._binding_lease.mark_disconnected(grant)
                finally:
                    await self._binding_lease.release_binding(grant)

    async def download_media(
        self,
        binding: ChannelBinding,
        message_id: str,
        media_key: str,
        *,
        media_type: str = "file",
        filename: str | None = None,
        media_reference: MediaReference | None = None,
    ) -> WeComResource:
        """Download media through this binding's authenticated WSClient."""
        if binding.channel != Channel.WECOM_AI_BOT:
            raise WeComMediaError("binding_mismatch")
        reference = _require_media_reference(media_reference)
        if _is_sealed_locator(reference):
            if self._locator_cipher is None:
                raise WeComMediaError("media_locator_unavailable")
            try:
                reference = self._locator_cipher.open(
                    reference,
                    binding.binding_id,
                    message_id,
                )
            except WeComMediaLocatorError:
                raise WeComMediaError("media_locator_invalid") from None
        _media_inputs(reference)
        connected_client = self._clients.get(binding.binding_id)
        if (
            connected_client is None
            or binding.binding_id not in self._fenced_bindings
            or not _client_ready(connected_client)
        ):
            raise WeComMediaError("connector_unavailable", retryable=True)
        client = BoundedWeComDownloadClient(
            self._max_media_bytes,
            self._media_timeout_seconds,
        )
        try:
            return await _download_wecom_media_with_retry(
                client,
                media_key,
                reference,
                media_type=media_type,
                filename=filename or reference.filename,
                max_media_bytes=self._max_media_bytes,
                media_timeout_seconds=self._media_timeout_seconds,
                require_connection=False,
            )
        finally:
            await client.close()

    def ready_for_delivery(self, binding: ChannelBinding) -> bool:
        """Return whether this replica owns an authenticated client for a binding."""

        client = self._clients.get(binding.binding_id)
        return (
            client is not None
            and binding.binding_id in self._fenced_bindings
            and _client_ready(client)
        )

    async def send(self, envelope: OutboundEnvelope, binding: ChannelBinding) -> DeliveryReceipt:
        client = self._clients.get(binding.binding_id)
        if (
            client is None
            or binding.binding_id not in self._fenced_bindings
            or not _client_ready(client)
        ):
            return DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.FAILED,
                provider_code="connector_unavailable",
                retryable=True,
            )
        if envelope.payload_kind != PayloadKind.TEXT or envelope.text is None:
            return DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.FAILED,
                provider_code="unsupported_payload",
            )
        try:
            response = await client.send_message(
                envelope.target_id,
                {
                    "msgtype": "markdown",
                    "markdown": {"content": envelope.text},
                    # The AI Bot API accepts this opaque client id in the
                    # message envelope.  It lets a gateway/provider dedupe a
                    # retry when supported; an unknown transport remains
                    # AMBIGUOUS below and is never blindly replayed.
                    "client_msg_id": envelope.outbound_id,
                },
            )
        except (TimeoutError, ConnectionError, OSError):
            return DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.AMBIGUOUS,
                provider_code="transport_unknown",
            )
        except RuntimeError as error:
            return _runtime_error_receipt(envelope, error)
        except Exception as error:
            mapped = _runtime_error_receipt(envelope, error)
            if mapped.provider_code != "runtime_unknown":
                return mapped
            # An SDK-specific exception may mean that the server accepted the
            # request before the client failed.  Keep it ambiguous so the
            # dispatcher cannot turn an unknown result into a duplicate send.
            return DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.AMBIGUOUS,
                provider_code="runtime_unknown",
            )
        return _wecom_response_receipt(envelope, response)

    async def recall(self, envelope: RecallEnvelope, binding: ChannelBinding) -> DeliveryReceipt:
        """Fail closed because the AI Bot adapter has no recall protocol."""

        del binding
        return DeliveryReceipt(
            outbound_id=envelope.outbound_id,
            status=DeliveryStatus.FAILED,
            provider_code="unsupported_capability",
        )


async def _wait_for_disconnect_or_stop(
    disconnected: asyncio.Event,
    stop_event: asyncio.Event,
) -> None:
    stop_task = asyncio.create_task(stop_event.wait())
    disconnect_task = asyncio.create_task(disconnected.wait())
    try:
        await asyncio.wait(
            (stop_task, disconnect_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        for task in (stop_task, disconnect_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(stop_task, disconnect_task, return_exceptions=True)


async def _wait_for_authentication_or_end(
    authentication_complete: asyncio.Event,
    disconnected: asyncio.Event,
    stop_event: asyncio.Event | None,
    *,
    timeout_seconds: float,
) -> str:
    authentication_task = asyncio.create_task(authentication_complete.wait())
    disconnect_task = asyncio.create_task(disconnected.wait())
    tasks = [authentication_task, disconnect_task]
    stop_task = asyncio.create_task(stop_event.wait()) if stop_event is not None else None
    if stop_task is not None:
        tasks.append(stop_task)
    try:
        await asyncio.wait(
            tasks,
            timeout=timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if disconnected.is_set():
            return "disconnected"
        if stop_event is not None and stop_event.is_set():
            return "stopped"
        if authentication_complete.is_set():
            return "authenticated"
        return "timeout"
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _runtime_error_receipt(envelope: OutboundEnvelope, error: BaseException) -> DeliveryReceipt:
    raw_code = _response_code(error)
    if raw_code is _MISSING:
        match = re.search(
            r"\b(?:err_?code|code)\s*[=:]\s*(-?\d+)",
            str(error),
            flags=re.IGNORECASE,
        )
        raw_code = int(match.group(1)) if match is not None else _MISSING
    if raw_code is _MISSING:
        status = _response_int(error, "status_code")
        if status is None:
            status = _response_int(error, "status")
        if status is not None and status >= 400:
            return _wecom_status_failure(
                envelope,
                status,
                retry_after=_response_retry_after(error),
            )
        return DeliveryReceipt(
            outbound_id=envelope.outbound_id,
            status=DeliveryStatus.AMBIGUOUS,
            provider_code="runtime_unknown",
        )
    try:
        code = int(cast(Any, raw_code))
    except (TypeError, ValueError, OverflowError):
        return DeliveryReceipt(
            outbound_id=envelope.outbound_id,
            status=DeliveryStatus.AMBIGUOUS,
            provider_code="runtime_unknown",
        )
    return _wecom_ack_failure(
        envelope,
        code,
        retry_after=_response_retry_after(error),
    )


def _resolve_tenant_secret(secrets: SecretProvider, ref: SecretRef) -> str:
    resolver = getattr(secrets, "resolve_tenant", None)
    if not ref.uri.startswith("literal://"):
        if resolver is None:
            raise ValueError("tenant-scoped secret resolver is required")
        return cast(Callable[[SecretRef], str], resolver)(ref)
    return secrets.resolve(ref)


def _wecom_response_receipt(envelope: OutboundEnvelope, response: object) -> DeliveryReceipt:
    status = _response_int(response, "status_code")
    if status is None:
        status = _response_int(response, "status")
    retry_after = _response_retry_after(response)
    if status is not None and status >= 400:
        return _wecom_status_failure(envelope, status, retry_after=retry_after)

    raw_code = _response_code(response)
    if raw_code is not _MISSING:
        try:
            code = int(cast(Any, raw_code))
        except (TypeError, ValueError, OverflowError):
            return DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.AMBIGUOUS,
                provider_code="response_unknown",
            )
        if code != 0:
            return _wecom_ack_failure(envelope, code, retry_after=retry_after)

    provider_id = _provider_message_id(response)
    if raw_code is not _MISSING or provider_id is not None:
        return DeliveryReceipt(
            outbound_id=envelope.outbound_id,
            status=DeliveryStatus.DELIVERED,
            provider_message_id=provider_id,
        )
    return DeliveryReceipt(
        outbound_id=envelope.outbound_id,
        status=DeliveryStatus.AMBIGUOUS,
        provider_code="response_unknown",
    )


def _wecom_ack_failure(
    envelope: OutboundEnvelope,
    code: int,
    *,
    retry_after: float | None = None,
) -> DeliveryReceipt:
    rate_limited = code in _WECOM_RATE_LIMIT_CODES
    return DeliveryReceipt(
        outbound_id=envelope.outbound_id,
        status=DeliveryStatus.FAILED,
        provider_code="rate_limited" if rate_limited else f"ack_{code}",
        retryable=code in _WECOM_RETRYABLE_CODES,
        retry_after_seconds=retry_after if rate_limited else None,
    )


def _wecom_status_failure(
    envelope: OutboundEnvelope,
    status: int,
    *,
    retry_after: float | None = None,
) -> DeliveryReceipt:
    rate_limited = status == 429
    return DeliveryReceipt(
        outbound_id=envelope.outbound_id,
        status=DeliveryStatus.FAILED,
        provider_code="rate_limited" if rate_limited else f"http_{status}",
        retryable=rate_limited or status >= 500,
        retry_after_seconds=retry_after if rate_limited else None,
    )


def _provider_message_id(response: object) -> str | None:
    value = _response_field(response, "req_id")
    if value is _MISSING:
        headers = _response_field(response, "headers")
        value = headers.get("req_id") if isinstance(headers, Mapping) else _MISSING
    if value is _MISSING or value is None or isinstance(value, bool):
        return None
    return str(value) if str(value) else None


def _response_field(response: object, name: str) -> object:
    if isinstance(response, Mapping):
        return response.get(name, _MISSING)
    return getattr(response, name, _MISSING)


def _response_int(response: object, name: str) -> int | None:
    value = _response_field(response, name)
    if value is _MISSING or value is None or isinstance(value, bool):
        return None
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError, OverflowError):
        return None


def _response_code(response: object) -> object:
    """Read the native WeCom code across SDK/object/HTTP response shapes."""

    for name in ("errcode", "err_code", "code"):
        value = _response_field(response, name)
        if value is not _MISSING:
            return value
    body = _response_field(response, "body")
    if isinstance(body, Mapping):
        for name in ("errcode", "err_code", "code"):
            value = body.get(name, _MISSING)
            if value is not _MISSING:
                return value
    return _MISSING


def _response_retry_after(response: object) -> float | None:
    candidates: list[object] = []
    for name in ("retry_after", "retry_after_seconds", "retryAfter", "retry-after"):
        candidates.append(_response_field(response, name))
    headers = _response_field(response, "headers")
    if isinstance(headers, Mapping):
        candidates.append(headers.get("Retry-After", headers.get("retry-after", _MISSING)))
    body = _response_field(response, "body")
    if isinstance(body, Mapping):
        stack: list[Mapping[str, Any]] = [body]
        seen: set[int] = set()
        while stack and len(seen) < 16:
            current = stack.pop()
            if id(current) in seen:
                continue
            seen.add(id(current))
            for name in ("retry_after", "retry_after_seconds", "retryAfter", "retry-after"):
                if name in current:
                    candidates.append(current[name])
            for name in ("error", "data", "result"):
                nested = current.get(name)
                if isinstance(nested, Mapping):
                    stack.append(nested)
    for candidate in candidates:
        if candidate is _MISSING or candidate is None or isinstance(candidate, bool):
            continue
        try:
            seconds = float(cast(Any, candidate))
        except (TypeError, ValueError, OverflowError):
            if not isinstance(candidate, str):
                continue
            try:
                retry_at = parsedate_to_datetime(candidate)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=UTC)
                seconds = retry_at.timestamp() - time.time()
            except (TypeError, ValueError, OverflowError, OSError):
                continue
        if 0 <= seconds <= 3600:
            return seconds
    return None


__all__ = [
    "BindingLease",
    "WeComBindingLeaseUnavailable",
    "WeComClient",
    "WeComConnector",
    "WeComMediaDownloader",
    "WeComMediaError",
    "WeComResource",
    "parse_wecom_frame",
    "sdk_client_factory",
]
