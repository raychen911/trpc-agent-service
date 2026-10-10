from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from tests.conftest import binding
from trpc_service.channels.envelopes import PayloadKind
from trpc_service.channels.media_locator import WeComMediaLocatorCipher
from trpc_service.channels.wecom import WeComConnector
from trpc_service.channels.wecom_download import (
    BoundedWeComDownloadClient,
    WeComDownloadError,
)
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.storage.models import WeComBindingLeaseGrant
from trpc_service.tenant.models import Channel

MEDIA_URL = "https://private.example.invalid/media/opaque-url"
AES_KEY_BYTES = bytes(range(32))
AES_KEY = base64.urlsafe_b64encode(AES_KEY_BYTES).decode()


def encrypt_bytes(plaintext: bytes, key: bytes = AES_KEY_BYTES) -> bytes:
    padding = 16 - len(plaintext) % 16
    return encrypt_raw(plaintext + bytes([padding]) * padding, key)


def encrypt_raw(padded: bytes, key: bytes = AES_KEY_BYTES) -> bytes:
    assert len(padded) % 16 == 0
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return encryptor.update(padded) + encryptor.finalize()


def encrypt_with_padding(padding: int, key: bytes = AES_KEY_BYTES) -> bytes:
    if 1 <= padding <= 16:
        body = b"P" * (16 - padding) + bytes([padding]) * padding
    else:
        body = b"P" * (32 - padding) + bytes([padding]) * padding
    return encrypt_raw(body, key)


def transport_for(
    content: bytes,
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=content, headers=headers, request=request)

    return httpx.MockTransport(handler)


async def download(
    ciphertext: bytes,
    *,
    key: str = AES_KEY,
    max_plaintext_bytes: int = 64,
    chunk_bytes: int = 64 * 1024,
    status: int = 200,
    headers: dict[str, str] | None = None,
) -> tuple[bytes, str | None]:
    client = BoundedWeComDownloadClient(
        max_plaintext_bytes,
        5,
        transport=transport_for(ciphertext, status=status, headers=headers),
        chunk_bytes=chunk_bytes,
    )
    try:
        return await client.download_file(MEDIA_URL, key)
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [0, 1, 31, 32, 33])
async def test_plaintext_boundaries_are_bounded(size: int) -> None:
    plaintext = bytes([size % 251]) * size

    if size <= 32:
        result, _ = await download(encrypt_bytes(plaintext), max_plaintext_bytes=32)
        assert result == plaintext
    else:
        with pytest.raises(WeComDownloadError) as caught:
            await download(encrypt_bytes(plaintext), max_plaintext_bytes=32)
        assert caught.value.code == "media_too_large"


@pytest.mark.asyncio
@pytest.mark.parametrize("padded", [True, False])
async def test_padded_and_unpadded_base64_aes_keys_are_supported(padded: bool) -> None:
    key = AES_KEY if padded else AES_KEY.rstrip("=")

    result, _ = await download(encrypt_bytes(b"key-form"), key=key)

    assert result == b"key-form"


@pytest.mark.asyncio
@pytest.mark.parametrize("padding", range(1, 33))
async def test_wecom_padding_lengths_one_through_thirty_two_are_supported(
    padding: int,
) -> None:
    result, _ = await download(encrypt_with_padding(padding))

    assert result == b"P" * (16 - padding if padding <= 16 else 32 - padding)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "padded",
    [
        b"P" * 31 + b"\x00",
        b"P" * 31 + b"\x21",
        b"P" * 31 + b"\x02",
    ],
)
async def test_invalid_padding_values_or_bytes_are_rejected(padded: bytes) -> None:
    with pytest.raises(WeComDownloadError) as caught:
        await download(encrypt_raw(padded))

    assert caught.value.code == "decrypt_failed"


@pytest.mark.asyncio
async def test_content_length_over_limit_is_rejected_before_download() -> None:
    with pytest.raises(WeComDownloadError) as caught:
        await download(
            encrypt_bytes(b"small"),
            max_plaintext_bytes=32,
            headers={"Content-Length": "65"},
        )

    assert caught.value.code == "media_too_large"


@pytest.mark.asyncio
async def test_underreported_content_length_cannot_hide_actual_oversize() -> None:
    with pytest.raises(WeComDownloadError) as caught:
        await download(
            encrypt_bytes(b"x" * 33),
            max_plaintext_bytes=32,
            headers={"Content-Length": "1"},
        )

    assert caught.value.code == "media_too_large"


@pytest.mark.asyncio
@pytest.mark.parametrize("chunk_bytes", [1, 15, 16, 17])
async def test_decryption_is_correct_across_chunk_boundaries(chunk_bytes: int) -> None:
    plaintext = b"chunked media" * 3

    result, _ = await download(
        encrypt_bytes(plaintext),
        max_plaintext_bytes=len(plaintext),
        chunk_bytes=chunk_bytes,
    )

    assert result == plaintext


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (302, "redirect_rejected", False),
        (404, "media_not_found", False),
        (429, "rate_limited", True),
        (500, "upstream_unavailable", True),
        (503, "upstream_unavailable", True),
    ],
)
async def test_http_statuses_are_classified_without_provider_body(
    status: int, code: str, retryable: bool
) -> None:
    secret_body = f"{MEDIA_URL} {AES_KEY}".encode()

    with pytest.raises(WeComDownloadError) as caught:
        await download(secret_body, status=status)

    error = caught.value
    assert error.code == code
    assert error.retryable is retryable
    assert MEDIA_URL not in str(error)
    assert AES_KEY not in str(error)
    assert MEDIA_URL not in repr(error)
    assert AES_KEY not in repr(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_key", ["", "not-base64", "A", "A" * 42, "A" * 44, "a b"])
async def test_invalid_aes_keys_are_rejected_without_leaking_input(invalid_key: str) -> None:
    with pytest.raises(WeComDownloadError) as caught:
        await download(b"not-used", key=invalid_key)

    assert caught.value.code == "invalid_key"
    if invalid_key:
        assert invalid_key not in str(caught.value)
        assert invalid_key not in repr(caught.value)
    assert MEDIA_URL not in str(caught.value)


@pytest.mark.asyncio
async def test_valid_32_byte_key_with_wrong_ciphertext_is_decrypt_failure() -> None:
    valid_but_different_key = base64.urlsafe_b64encode(b"A" * 32).decode()

    with pytest.raises(WeComDownloadError) as caught:
        await download(encrypt_bytes(b"wrong-key"), key=valid_but_different_key)

    assert caught.value.code == "decrypt_failed"
    assert valid_but_different_key not in repr(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ciphertext",
    [b"", b"not-a-block", b"\x00" * 16, encrypt_with_padding(0)],
)
async def test_invalid_ciphertext_and_padding_are_rejected(ciphertext: bytes) -> None:
    with pytest.raises(WeComDownloadError) as caught:
        await download(ciphertext)

    assert caught.value.code == "decrypt_failed"
    assert MEDIA_URL not in repr(caught.value)
    assert AES_KEY not in repr(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("content_disposition", "expected"),
    [
        ("attachment; filename*=UTF-8''%2Fprivate%2Freport%20one.pdf", "report one.pdf"),
        ('attachment; filename="C:\\private\\report.txt"', "report.txt"),
    ],
)
async def test_content_disposition_filename_is_path_cleaned(
    content_disposition: str, expected: str
) -> None:
    result, filename = await download(
        encrypt_bytes(b"file"),
        headers={"Content-Disposition": content_disposition},
    )

    assert result == b"file"
    assert filename == expected


@dataclass
class _LocatorClient:
    is_connected: bool = True
    is_authenticated: bool = True
    handlers: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        self.handlers = {}

    def on(self, event: str, handler: Any) -> None:
        assert self.handlers is not None
        self.handlers[event] = handler

    async def connect_async(self) -> None:
        assert self.handlers is not None
        await self.handlers["message.file"](
            {
                "body": {
                    "msgid": "locator-message",
                    "from": {"userid": "user"},
                    "msgtype": "file",
                    "file": {
                        "url": MEDIA_URL,
                        "aeskey": AES_KEY,
                        "name": "report.pdf",
                    },
                }
            }
        )
        self.is_connected = False
        await self.handlers["disconnected"]()

    def disconnect(self) -> None:
        return None


class _Lease:
    async def acquire_binding(self, binding: Any, _owner_id: str) -> WeComBindingLeaseGrant:
        return WeComBindingLeaseGrant(
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            owner_hash="a" * 64,
            epoch=1,
            acquired_at=datetime.now(UTC),
        )

    async def mark_authenticated(self, _grant: WeComBindingLeaseGrant) -> bool:
        return True

    async def record_provider_event(
        self, _grant: WeComBindingLeaseGrant, _provider_event_id: str
    ) -> bool:
        return True

    async def mark_disconnected(self, _grant: WeComBindingLeaseGrant) -> bool:
        return True

    async def release_binding(self, _grant: WeComBindingLeaseGrant) -> None:
        return None


@pytest.mark.asyncio
async def test_connector_locator_cipher_seals_envelope_before_sink() -> None:
    client = _LocatorClient()
    route_binding = binding(
        channel=Channel.WECOM_AI_BOT,
        binding_id="locator-binding",
        account_id="locator-account",
    ).model_copy(update={"secret_refs": {"bot_secret": SecretRef(uri="literal://secret")}})
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        _Lease(),
        owner_id="locator-owner",
        client_factory=lambda _account_id, _secret: client,
        locator_cipher=WeComMediaLocatorCipher(b"l" * 32),
    )
    received: list[Any] = []

    async def sink(_binding_id: str, envelope: Any) -> None:
        received.append(envelope)

    await connector.run(route_binding, sink)

    assert len(received) == 1
    envelope = received[0]
    dumped = envelope.model_dump()
    assert envelope.payload_kind == PayloadKind.FILE
    assert envelope.media[0].provider_url is None
    assert envelope.media[0].encryption_key_ref is None
    assert str(dumped).find(MEDIA_URL) == -1
    assert str(dumped).find(AES_KEY) == -1
    assert str(dumped).find("v1.") >= 0
