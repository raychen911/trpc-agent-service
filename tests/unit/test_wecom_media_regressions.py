from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from tests.conftest import binding, envelope, repository, tenant_config
from trpc_service.agent.worker import AgentWorker
from trpc_service.channels.envelopes import MediaReference, PayloadKind
from trpc_service.channels.wecom import WeComConnector, parse_wecom_frame
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.artifacts import InMemoryArtifactStore
from trpc_service.storage.models import BindingRoute, WeComBindingLeaseGrant
from trpc_service.tenant.models import Channel

MEDIA_URL = "https://private.example.invalid/wecom/video"
AES_KEY = "private-aes-key"


def wecom_binding() -> Any:
    return binding(
        channel=Channel.WECOM_AI_BOT,
        binding_id="wecom-regression",
        account_id="wecom-account",
    ).model_copy(update={"secret_refs": {"bot_secret": SecretRef(uri="literal://secret")}})


def test_wecom_file_name_is_parsed_as_filename() -> None:
    envelope_value = parse_wecom_frame(
        {
            "body": {
                "msgid": "file-message",
                "from": {"userid": "user"},
                "msgtype": "file",
                "file": {"url": MEDIA_URL, "aeskey": AES_KEY, "name": "report.pdf"},
            }
        },
        account_id="wecom-account",
    )

    assert envelope_value.payload_kind == PayloadKind.FILE
    assert envelope_value.media[0].filename == "report.pdf"
    assert envelope_value.media[0].provider_url == MEDIA_URL
    assert envelope_value.media[0].encryption_key_ref == AES_KEY


@dataclass(frozen=True)
class _DownloadedVideo:
    data: bytes = b"video-bytes"
    content_type: str = "video/mp4"
    filename: str = "clip.mp4"


class _VideoDownloader:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def download_media(self, *args: Any, **kwargs: Any) -> _DownloadedVideo:
        self.calls.append((args, kwargs))
        return _DownloadedVideo()


class _Audit:
    async def append(self, _tenant_id: str, **_entry: Any) -> str:
        return "audit-id"


@pytest.mark.asyncio
async def test_worker_passes_video_resource_type_to_downloader() -> None:
    repo = repository()
    config = tenant_config()
    repo.add_config(config)
    route_binding = wecom_binding()
    repo.add_route(BindingRoute(binding=route_binding, active_config_version=1))
    inbound = envelope(
        "video-message",
        account_id=route_binding.account_id,
        text="",
    ).model_copy(
        update={
            "channel": Channel.WECOM_AI_BOT,
            "payload_kind": PayloadKind.VIDEO,
            "media": (
                MediaReference(
                    provider_media_id="video-key",
                    provider_url=MEDIA_URL,
                    encryption_key_ref=AES_KEY,
                    filename="clip.mp4",
                    content_type="video/mp4",
                ),
            ),
            "text": None,
        }
    )
    accepted = await TenantRuntime(repo, routing_key=b"v" * 32).accept(
        route_binding.binding_id, inbound
    )
    downloader = _VideoDownloader()
    worker = AgentWorker(
        repo,
        worker_id="video-regression-worker",
        agent_loader=lambda _config: None,
        media_downloaders={Channel.WECOM_AI_BOT: downloader},
    )

    prepared = await worker._prepare_media(
        accepted,
        config,
        SimpleNamespace(artifact=InMemoryArtifactStore(), audit=_Audit()),
    )

    assert prepared[0].content_type == "video/mp4"
    assert len(downloader.calls) == 1
    assert downloader.calls[0][1]["media_type"] == "video"
    assert downloader.calls[0][1]["media_reference"].provider_url == MEDIA_URL


class _GenericMessageClient:
    def __init__(self) -> None:
        self.is_connected = True
        self.is_authenticated = True
        self.handlers: dict[str, Any] = {}

    def on(self, event: str, handler: Any) -> None:
        self.handlers[event] = handler

    async def connect_async(self) -> None:
        video = {
            "body": {
                "msgid": "video-message",
                "from": {"userid": "user"},
                "msgtype": "video",
                "video": {"url": MEDIA_URL, "aeskey": AES_KEY, "name": "clip.mp4"},
            }
        }
        text = {
            "body": {
                "msgid": "text-message",
                "from": {"userid": "user"},
                "msgtype": "text",
                "text": {"content": "hello"},
            }
        }
        # Simulate an SDK that emits both generic and typed events. Video must
        # be accepted once; generic text must not duplicate message.text.
        if "message.video" in self.handlers:
            await self.handlers["message.video"](video)
        await self.handlers["message"](video)
        await self.handlers["message.text"](text)
        await self.handlers["message"](text)
        self.is_connected = False
        await self.handlers["disconnected"]()

    def disconnect(self) -> None:
        return None

    async def send_message(self, _chat_id: str, _body: Any) -> dict[str, str]:
        return {"req_id": "req"}


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
async def test_connector_generic_message_routes_video_once_and_not_text_twice() -> None:
    client = _GenericMessageClient()
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        _Lease(),
        owner_id="regression-owner",
        client_factory=lambda _account_id, _secret: client,
    )
    accepted: list[Any] = []

    async def sink(_binding_id: str, inbound: Any) -> None:
        accepted.append(inbound)

    await connector.run(wecom_binding(), sink)

    assert [item.external_message_id for item in accepted] == [
        "video-message",
        "text-message",
    ]
    assert [item.payload_kind for item in accepted] == [PayloadKind.VIDEO, PayloadKind.TEXT]
    assert "message.video" not in client.handlers
