from __future__ import annotations

import pytest

from trpc_service.channels.envelopes import MediaReference
from trpc_service.channels.media_locator import (
    WeComMediaLocatorCipher,
    WeComMediaLocatorError,
)

MEDIA_URL = "https://private.example.invalid/media/opaque-url"
AES_KEY = "opaque-aes-key"
BINDING_ID = "wecom-binding"
MESSAGE_ID = "wecom-message"


def media_reference() -> MediaReference:
    return MediaReference(
        provider_url=MEDIA_URL,
        encryption_key_ref=AES_KEY,
        filename="report.pdf",
        content_type="application/pdf",
    )


def test_seal_removes_raw_provider_credentials_and_open_recovers_them() -> None:
    cipher = WeComMediaLocatorCipher(b"k" * 32)

    sealed = cipher.seal(media_reference(), BINDING_ID, MESSAGE_ID)
    dumped = sealed.model_dump()

    assert sealed.provider_url is None
    assert sealed.encryption_key_ref is None
    assert isinstance(sealed.provider_media_id, str)
    assert sealed.provider_media_id.startswith("v1.")
    assert MEDIA_URL not in repr(sealed)
    assert AES_KEY not in repr(sealed)
    assert MEDIA_URL not in repr(dumped)
    assert AES_KEY not in repr(dumped)

    opened = cipher.open(sealed, BINDING_ID, MESSAGE_ID)
    assert opened.provider_url == MEDIA_URL
    assert opened.encryption_key_ref == AES_KEY
    assert opened.filename == "report.pdf"
    assert opened.content_type == "application/pdf"
    assert opened.provider_media_id == sealed.provider_media_id


@pytest.mark.parametrize(
    "wrong_context", [("other-binding", MESSAGE_ID), (BINDING_ID, "other-message")]
)
def test_open_rejects_wrong_binding_or_message_without_sensitive_error(
    wrong_context: tuple[str, str],
) -> None:
    cipher = WeComMediaLocatorCipher(b"k" * 32)
    sealed = cipher.seal(media_reference(), BINDING_ID, MESSAGE_ID)

    with pytest.raises(WeComMediaLocatorError) as caught:
        cipher.open(sealed, *wrong_context)

    assert caught.value.provider_code == "invalid_locator"
    assert MEDIA_URL not in str(caught.value)
    assert AES_KEY not in str(caught.value)
    assert MEDIA_URL not in repr(caught.value)
    assert AES_KEY not in repr(caught.value)


def test_open_rejects_tampered_token_without_leaking_locator_inputs() -> None:
    cipher = WeComMediaLocatorCipher(b"k" * 32)
    sealed = cipher.seal(media_reference(), BINDING_ID, MESSAGE_ID)
    token = sealed.provider_media_id
    assert isinstance(token, str) and token.startswith("v1.")
    position = len("v1.") + 8
    replacement = "A" if token[position] != "A" else "B"
    tampered = sealed.model_copy(
        update={"provider_media_id": token[:position] + replacement + token[position + 1 :]}
    )

    with pytest.raises(WeComMediaLocatorError) as caught:
        cipher.open(tampered, BINDING_ID, MESSAGE_ID)

    assert caught.value.provider_code == "invalid_locator"
    assert MEDIA_URL not in str(caught.value)
    assert AES_KEY not in str(caught.value)
