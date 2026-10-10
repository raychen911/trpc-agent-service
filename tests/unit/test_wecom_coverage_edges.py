from __future__ import annotations

import base64
import json
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

import trpc_service.channels.media_locator as locator_module
import trpc_service.channels.wecom_download as download_module
from trpc_service.channels.envelopes import MediaReference
from trpc_service.channels.media_locator import (
    WeComMediaLocatorCipher,
    WeComMediaLocatorError,
)
from trpc_service.channels.wecom_download import (
    BoundedWeComDownloadClient,
    WeComDownloadError,
)

URL = "https://private.example.invalid/media"
AES_KEY_BYTES = b"k" * 32
AES_KEY = base64.urlsafe_b64encode(AES_KEY_BYTES).decode()


def reference() -> MediaReference:
    return MediaReference(provider_url=URL, encryption_key_ref="secret-aes-key")


def locator_token(cipher: WeComMediaLocatorCipher, payload: object) -> str:
    nonce = b"n" * 12
    body = json.dumps(payload, separators=(",", ":")).encode()
    encrypted = AESGCM(cipher._key).encrypt(nonce, body, cipher._aad("binding", "message"))
    encoded = base64.urlsafe_b64encode(nonce + encrypted).rstrip(b"=").decode()
    return "v1." + encoded


@pytest.mark.parametrize(
    "key",
    [b"", b"short", "not-bytes", None],
)
def test_locator_cipher_rejects_invalid_key_lengths_and_types(key: object) -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        WeComMediaLocatorCipher(key)  # type: ignore[arg-type]


def test_locator_seal_rejects_non_media_and_missing_provider_values() -> None:
    cipher = WeComMediaLocatorCipher(AES_KEY_BYTES)
    with pytest.raises(WeComMediaLocatorError):
        cipher.seal(object(), "binding", "message")  # type: ignore[arg-type]

    for value in (
        MediaReference(encryption_key_ref="key"),
        MediaReference(provider_url=" ", encryption_key_ref="key"),
        MediaReference(provider_url=URL),
        MediaReference(provider_url=URL, encryption_key_ref=" "),
    ):
        with pytest.raises(WeComMediaLocatorError):
            cipher.seal(value, "binding", "message")


@pytest.mark.parametrize(
    ("binding_id", "message_id"),
    [
        (None, "message"),
        ("", "message"),
        ("bad\x00binding", "message"),
        ("binding", None),
        ("binding", ""),
        ("binding", "bad\x00message"),
    ],
)
def test_locator_context_must_be_nonempty_and_nul_free(
    binding_id: object, message_id: object
) -> None:
    cipher = WeComMediaLocatorCipher(AES_KEY_BYTES)

    with pytest.raises(WeComMediaLocatorError):
        cipher.seal(reference(), binding_id, message_id)  # type: ignore[arg-type]


def test_locator_open_rejects_non_reference_and_invalid_token_shapes() -> None:
    cipher = WeComMediaLocatorCipher(AES_KEY_BYTES)
    with pytest.raises(WeComMediaLocatorError):
        cipher.open(object(), "binding", "message")  # type: ignore[arg-type]

    valid = cipher.seal(reference(), "binding", "message")
    invalid_references = (
        MediaReference(),
        MediaReference(provider_media_id=""),
        MediaReference(provider_media_id="v2.token"),
        MediaReference(provider_media_id="v1.bad!"),
        valid.model_copy(update={"provider_url": URL}),
        MediaReference(
            provider_media_id="v1." + base64.urlsafe_b64encode(b"short").rstrip(b"=").decode()
        ),
    )
    for invalid in invalid_references:
        with pytest.raises(WeComMediaLocatorError):
            cipher.open(invalid, "binding", "message")

    with pytest.raises(ValueError):
        locator_module._b64url_decode("")
    with pytest.raises(ValueError):
        locator_module._b64url_decode("not-valid!")


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"provider_url": URL},
        {"provider_url": URL, "encryption_key_ref": ""},
        {"provider_url": 1, "encryption_key_ref": "key"},
        {"provider_url": URL, "encryption_key_ref": 1},
    ],
)
def test_locator_open_rejects_authenticated_but_invalid_payload(payload: object) -> None:
    cipher = WeComMediaLocatorCipher(AES_KEY_BYTES)
    sealed = MediaReference(provider_media_id=locator_token(cipher, payload))

    with pytest.raises(WeComMediaLocatorError):
        cipher.open(sealed, "binding", "message")


def encrypted_bytes(plaintext: bytes = b"ok") -> bytes:
    padding = 16 - len(plaintext) % 16
    padded = plaintext + bytes([padding]) * padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    encryptor = Cipher(algorithms.AES(AES_KEY_BYTES), modes.CBC(AES_KEY_BYTES[:16])).encryptor()
    return encryptor.update(padded) + encryptor.finalize()


def success_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=encrypted_bytes(), request=request)

    return httpx.MockTransport(handler)


def transport_with_exception(exception_factory: Any) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exception_factory(request)

    return httpx.MockTransport(handler)


@pytest.mark.parametrize(
    "url",
    [
        None,
        "http://private.example.invalid/media",
        "https://user:pass@example.invalid/media",
        "https://",
        "https://[::1",
    ],
)
async def test_download_rejects_non_https_or_userinfo_urls(url: object) -> None:
    client = BoundedWeComDownloadClient(32, 5, transport=success_transport())
    try:
        with pytest.raises(WeComDownloadError) as caught:
            await client.download_file(url, AES_KEY)  # type: ignore[arg-type]
        assert caught.value.code == "invalid_url"
    finally:
        await client.close()


@pytest.mark.parametrize("value", [None, "", "invalid", "-1", "0", "1"])
def test_content_length_parser_handles_missing_malformed_and_negative_values(value: object) -> None:
    expected = None if value in {None, "", "invalid", "-1"} else int(value)
    assert download_module._content_length(value) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("exception_factory", "code"),
    [
        (lambda request: httpx.ReadTimeout("timeout", request=request), "transport_timeout"),
        (lambda request: httpx.NetworkError("network", request=request), "transport_error"),
        (lambda request: httpx.ProtocolError("protocol", request=request), "transport_error"),
        (
            lambda request: RuntimeError(
                "unexpected",
            ),
            "transport_error",
        ),
    ],
)
async def test_download_transport_failures_are_classified_without_input_leaks(
    exception_factory: Any, code: str
) -> None:
    client = BoundedWeComDownloadClient(
        32,
        5,
        transport=transport_with_exception(exception_factory),
    )
    try:
        with pytest.raises(WeComDownloadError) as caught:
            await client.download_file(URL, AES_KEY)
        assert caught.value.code == code
        assert URL not in repr(caught.value)
        assert AES_KEY not in repr(caught.value)
    finally:
        await client.close()


@pytest.mark.parametrize(("status", "code"), [(204, "decrypt_failed"), (400, "upstream_rejected")])
async def test_download_handles_success_without_body_and_other_4xx(status: int, code: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=b"", request=request)

    client = BoundedWeComDownloadClient(32, 5, transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(WeComDownloadError) as caught:
            await client.download_file(URL, AES_KEY)
        assert caught.value.code == code
        assert caught.value.status == (status if status != 204 else None)
    finally:
        await client.close()


@pytest.mark.parametrize(
    "header",
    [None, "", "attachment", "attachment; filename*=UTF-8''%00", "attachment; foo=bar"],
)
def test_filename_parsing_handles_missing_invalid_and_unmatched_headers(header: str | None) -> None:
    assert download_module._content_disposition_filename(header) is None


@pytest.mark.parametrize("value", [None, "", "\x00", "\x01control", "C:\\private\\name.txt"])
def test_filename_sanitizer_handles_non_string_control_and_paths(value: object) -> None:
    expected = None if value in {None, "", "\x00", "\x01control"} else "name.txt"
    assert download_module._safe_filename(value) == expected  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_download_client_close_and_disconnect_are_safe() -> None:
    client = BoundedWeComDownloadClient(32, 5, transport=success_transport())
    await client.disconnect()
    await client.close()
