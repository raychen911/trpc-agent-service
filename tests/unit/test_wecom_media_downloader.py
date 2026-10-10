from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest

from trpc_service.channels.envelopes import MediaReference, PayloadKind
from trpc_service.channels.wecom import (
    WeComConnector,
    WeComMediaDownloader,
    WeComMediaError,
    parse_wecom_frame,
)
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.storage.models import WeComBindingLeaseGrant
from trpc_service.tenant.models import Channel, ChannelBinding

MEDIA_URL = "https://private.example.invalid/media/opaque-url"
AES_KEY = "opaque-aes-key"


class Lease:
    async def acquire_binding(
        self, binding: ChannelBinding, _owner_id: str
    ) -> WeComBindingLeaseGrant:
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


@dataclass
class FakeClient:
    """Offline stand-in for the authenticated WeCom SDK client."""

    connected: bool = True
    authenticated: bool = True
    result: tuple[bytes, str | None] = (b"media-bytes", "server-name.bin")
    failure: BaseException | None = None
    download_calls: list[tuple[str | None, str | None]] = field(default_factory=list)

    @property
    def is_connected(self) -> bool:
        return self.connected

    @property
    def is_authenticated(self) -> bool:
        return self.authenticated

    async def download_file(self, url: str | None, aes_key: str | None) -> tuple[bytes, str | None]:
        self.download_calls.append((url, aes_key))
        if self.failure is not None:
            raise self.failure
        return self.result


def binding(binding_id: str = "wecom-a") -> ChannelBinding:
    return ChannelBinding(
        binding_id=binding_id,
        tenant_id=f"tenant-{binding_id}",
        app_id="support",
        channel=Channel.WECOM_AI_BOT,
        account_id=f"bot-{binding_id}",
        secret_refs={"bot_secret": SecretRef(uri="literal://bot-secret")},
        capabilities=frozenset({"text", "image", "file", "mixed"}),
    )


def reference(
    *,
    url: str | None = MEDIA_URL,
    aes_key: str | None = AES_KEY,
    filename: str | None = None,
    content_type: str | None = None,
) -> MediaReference:
    return MediaReference(
        provider_url=url,
        encryption_key_ref=aes_key,
        filename=filename,
        content_type=content_type,
    )


def connector_for(binding_value: ChannelBinding, client: FakeClient) -> WeComConnector:
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        Lease(),
        owner_id="test-owner",
        client_factory=lambda *_args: client,
    )
    # ``run`` owns connection setup; injecting a client here keeps this test
    # entirely offline while exercising the same binding-scoped client map.
    connector._clients[binding_value.binding_id] = client
    return connector


def downloader_for(client: FakeClient) -> WeComMediaDownloader:
    return WeComMediaDownloader(
        LocalSecretProvider(allow_literal=True),
        client_factory=lambda *_args: client,
    )


async def download(
    downloader: Any,
    binding_value: ChannelBinding,
    media_reference: MediaReference,
    *,
    media_type: str = "file",
    filename: str | None = None,
) -> Any:
    """Call the backward-compatible MediaDownloader contract."""
    if filename is None:
        filename = media_reference.filename
    return await downloader.download_media(
        binding_value,
        "message-id",
        "stable-media-key",
        media_type=media_type,
        filename=filename,
        media_reference=media_reference,
    )


@pytest.mark.asyncio
async def test_authenticated_client_downloads_image_with_url_and_aeskey_metadata() -> None:
    binding_value = binding()
    client = FakeClient(result=(b"PNG", "from-sdk.png"))
    downloader = downloader_for(client)

    result = await download(
        downloader,
        binding_value,
        reference(),
        media_type="image",
    )

    assert client.download_calls == [(MEDIA_URL, AES_KEY)]
    assert result.data == b"PNG"
    assert result.bytes == b"PNG"
    assert result.filename == "from-sdk.png"
    assert result.content_type == "image/png"


@pytest.mark.asyncio
async def test_file_metadata_honors_callback_filename_and_content_type() -> None:
    binding_value = binding()
    client = FakeClient(result=(b"PDF", None))
    downloader = downloader_for(client)

    result = await download(
        downloader,
        binding_value,
        reference(filename="report.pdf", content_type="application/pdf"),
        media_type="file",
    )

    assert client.download_calls == [(MEDIA_URL, AES_KEY)]
    assert result.data == b"PDF"
    assert result.filename == "report.pdf"
    assert result.content_type == "application/pdf"


@pytest.mark.asyncio
async def test_mixed_media_uses_image_item_metadata_without_network() -> None:
    binding_value = binding()
    client = FakeClient(result=(b"JPEG", "mixed-image.jpg"))
    downloader = downloader_for(client)
    envelope = parse_wecom_frame(
        {
            "body": {
                "msgid": "mixed-message",
                "from": {"userid": "user"},
                "msgtype": "mixed",
                "mixed": {
                    "msg_item": [
                        {"msgtype": "text", "text": {"content": "caption"}},
                        {
                            "msgtype": "image",
                            "image": {
                                "url": MEDIA_URL,
                                "aeskey": AES_KEY,
                                "filename": "mixed-image.jpg",
                            },
                        },
                    ]
                },
            }
        },
        account_id="bot",
    )
    assert envelope.payload_kind == PayloadKind.MIXED
    media_reference = envelope.media[0]

    result = await download(
        downloader,
        binding_value,
        MediaReference(
            provider_url=media_reference.provider_url,
            encryption_key_ref=media_reference.encryption_key_ref,
            filename=media_reference.filename,
            content_type="image/jpeg",
        ),
        media_type="image",
    )

    assert client.download_calls == [(MEDIA_URL, AES_KEY)]
    assert result.filename == "mixed-image.jpg"
    assert result.content_type == "image/jpeg"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("media_reference", "missing"),
    [
        (reference(url=None), "url"),
        (reference(aes_key=None), "aeskey"),
    ],
)
async def test_missing_url_or_aeskey_is_rejected_without_calling_sdk(
    media_reference: MediaReference, missing: str
) -> None:
    binding_value = binding()
    client = FakeClient()
    downloader = downloader_for(client)

    with pytest.raises(WeComMediaError) as caught:
        await download(downloader, binding_value, media_reference)

    expected_code = "media_url_invalid" if missing == "url" else "media_key_missing"
    assert caught.value.provider_code == expected_code
    assert MEDIA_URL not in repr(caught.value)
    assert AES_KEY not in repr(caught.value)
    assert client.download_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "provider_code", "retryable"),
    [
        (
            TimeoutError("download timeout: " + MEDIA_URL + " " + AES_KEY),
            "transport_timeout",
            True,
        ),
        (
            ConnectionError("connection failed for " + MEDIA_URL + " " + AES_KEY),
            "transport_unknown",
            True,
        ),
        (RuntimeError("SDK rejected " + MEDIA_URL + " " + AES_KEY), "provider_error", True),
    ],
)
async def test_download_errors_are_classified_without_leaking_url_or_aeskey(
    failure: BaseException, provider_code: str, retryable: bool
) -> None:
    binding_value = binding()
    client = FakeClient(failure=failure)
    downloader = downloader_for(client)

    with pytest.raises(WeComMediaError) as caught:
        await download(downloader, binding_value, reference())

    error = caught.value
    assert getattr(error, "provider_code", None) == provider_code
    assert getattr(error, "retryable", None) is retryable
    assert MEDIA_URL not in str(error)
    assert AES_KEY not in str(error)
    assert MEDIA_URL not in repr(error)
    assert AES_KEY not in repr(error)


@pytest.mark.asyncio
async def test_binding_scopes_client_selection_and_does_not_fall_back() -> None:
    left_binding = binding("wecom-left")
    right_binding = binding("wecom-right")
    left_client = FakeClient(result=(b"left", "left.txt"))
    right_client = FakeClient(result=(b"right", "right.txt"))
    clients = {
        left_binding.account_id: left_client,
        right_binding.account_id: right_client,
    }
    downloader = WeComMediaDownloader(
        LocalSecretProvider(allow_literal=True),
        client_factory=lambda account_id, _secret: clients[account_id],
    )

    left = await download(downloader, left_binding, reference())
    right = await download(downloader, right_binding, reference())

    assert left.data == b"left"
    assert right.data == b"right"
    assert left_client.download_calls == [(MEDIA_URL, AES_KEY)]
    assert right_client.download_calls == [(MEDIA_URL, AES_KEY)]

    unknown_binding = binding("wecom-unknown")
    with pytest.raises(WeComMediaError) as caught:
        await download(downloader, unknown_binding, reference())
    assert caught.value.provider_code == "credentials_unavailable"
    assert left_client.download_calls == [(MEDIA_URL, AES_KEY)]
    assert right_client.download_calls == [(MEDIA_URL, AES_KEY)]


@pytest.mark.asyncio
async def test_download_requires_connected_client() -> None:
    binding_value = binding()
    client = FakeClient(connected=False)
    connector = connector_for(binding_value, client)

    with pytest.raises(WeComMediaError) as caught:
        await download(connector, binding_value, reference())

    assert caught.value.provider_code == "connector_unavailable"
    assert client.download_calls == []
