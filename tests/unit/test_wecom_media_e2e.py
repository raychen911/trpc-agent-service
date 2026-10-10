from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from tests.conftest import tenant_config
from trpc_service.agent.worker import AgentWorker
from trpc_service.channels.envelopes import MediaReference
from trpc_service.channels.media_locator import WeComMediaLocatorCipher
from trpc_service.channels.wecom import WeComConnector, WeComMediaDownloader
from trpc_service.channels.wecom_download import BoundedWeComDownloadClient
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.artifacts import InMemoryArtifactStore
from trpc_service.storage.memory import InMemoryRuntimeRepository
from trpc_service.storage.models import BindingRoute, WeComBindingLeaseGrant
from trpc_service.tenant.models import Channel, ChannelBinding

MEDIA_URL = "https://media.example.invalid/download/private-file"
AES_KEY_BYTES = bytes(range(32))
AES_KEY = base64.urlsafe_b64encode(AES_KEY_BYTES).decode()
TENANT_ID = "tenant-wecom-e2e"
BINDING_ID = "wecom-e2e-binding"
ACCOUNT_ID = "wecom-e2e-bot"
MESSAGE_ID = "wecom-e2e-message"


def _encrypt(plaintext: bytes, key: bytes = AES_KEY_BYTES) -> bytes:
    padding = 16 - len(plaintext) % 16
    padded = plaintext + bytes([padding]) * padding
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return encryptor.update(padded) + encryptor.finalize()


def _wecom_binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id=BINDING_ID,
        tenant_id=TENANT_ID,
        app_id="support",
        channel=Channel.WECOM_AI_BOT,
        account_id=ACCOUNT_ID,
        secret_refs={"bot_secret": SecretRef(uri="literal://bot-secret")},
    )


@dataclass
class _Audit:
    entries: list[dict[str, Any]] = field(default_factory=list)

    async def append(self, _tenant_id: str, **entry: Any) -> str:
        self.entries.append(entry)
        return "audit-id"


class _FrameClient:
    def __init__(self, frame: object) -> None:
        self.frame = frame
        self.handlers: dict[str, Any] = {}
        self.is_connected = True
        self.is_authenticated = True
        self.disconnect_calls = 0

    def on(self, event: str, handler: Any) -> None:
        self.handlers[event] = handler

    async def connect_async(self) -> None:
        await self.handlers["message.file"](self.frame)
        self.is_connected = False

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.is_connected = False

    async def send_message(self, _chat_id: str, _body: dict[str, Any]) -> object:
        return object()


@pytest.mark.asyncio
async def test_wecom_frame_to_worker_download_and_artifact_stays_offline() -> None:
    raw_frame = {
        "body": {
            "msgid": MESSAGE_ID,
            "from": {"userid": "customer-1"},
            "msgtype": "file",
            "file": {"url": MEDIA_URL, "aeskey": AES_KEY, "name": "notes.txt"},
        }
    }
    binding = _wecom_binding()
    cipher = WeComMediaLocatorCipher(b"l" * 32)
    frame_client = _FrameClient(raw_frame)
    received = []

    async def sink(binding_id: str, envelope: object) -> None:
        received.append((binding_id, envelope))

    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        _TrackingLease(),
        owner_id="connector-e2e",
        client_factory=lambda _account, _secret: frame_client,
        locator_cipher=cipher,
    )
    await connector.run(binding, sink)

    assert len(received) == 1
    assert received[0][0] == BINDING_ID
    sealed = received[0][1]
    assert sealed.media[0].provider_url is None
    assert sealed.media[0].encryption_key_ref is None
    assert sealed.media[0].filename == "notes.txt"
    assert sealed.media[0].provider_media_id.startswith("v1.")

    repo = InMemoryRuntimeRepository()
    config = tenant_config(tenant_id=TENANT_ID)
    repo.add_config(config)
    repo.add_route(BindingRoute(binding=binding, active_config_version=config.version))
    accepted = await TenantRuntime(repo, routing_key=b"r" * 32).accept(BINDING_ID, sealed)
    persisted = await repo.get_acceptance(TENANT_ID, accepted.inbound_id)
    assert persisted is not None
    serialized = json.dumps(persisted.model_dump(mode="json"), sort_keys=True)
    assert MEDIA_URL not in serialized
    assert AES_KEY not in serialized

    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(
            200,
            content=_encrypt(b"hello"),
            headers={"Content-Disposition": 'attachment; filename="notes.txt"'},
            request=request,
        )

    transport = httpx.MockTransport(handler)

    def download_factory(account_id: str, secret: str) -> BoundedWeComDownloadClient:
        assert account_id == ACCOUNT_ID
        assert secret == "bot-secret"
        return BoundedWeComDownloadClient(
            20 * 1024 * 1024,
            5,
            transport=transport,
            chunk_bytes=1,
        )

    downloader = WeComMediaDownloader(
        LocalSecretProvider(allow_literal=True),
        client_factory=download_factory,
        locator_cipher=cipher,
    )
    artifacts = InMemoryArtifactStore()
    audit = _Audit()
    worker = AgentWorker(
        repo,
        worker_id="media-e2e-worker",
        agent_loader=lambda _config: None,
        media_downloaders={Channel.WECOM_AI_BOT: downloader},
    )
    services = type("MediaServices", (), {"artifact": artifacts, "audit": audit})()
    try:
        prepared = await worker._prepare_media(accepted, config, services)
    finally:
        await downloader.close()

    assert requests == [MEDIA_URL]
    assert len(prepared) == 1
    assert prepared[0].text == "hello"
    assert prepared[0].filename == "notes.txt"
    assert prepared[0].content_type == "text/plain"
    assert len(artifacts.objects) == 1
    artifact_key = next(iter(artifacts.objects))
    assert "/staging/" not in artifact_key
    assert await artifacts.read(TENANT_ID, artifact_key) == b"hello"
    assert audit.entries[-1]["decision"] == "media_ingested"
    assert audit.entries[-1]["metadata"] == {
        "item_index": 0,
        "size_bytes": 5,
        "kind": "text",
        "status": "extracted",
        "truncated": False,
    }


@dataclass
class _TrackingLease:
    acquired: list[tuple[str, str]] = field(default_factory=list)
    released: list[tuple[str, str]] = field(default_factory=list)
    owners: dict[int, str] = field(default_factory=dict)

    async def acquire_binding(
        self, binding: ChannelBinding, owner_id: str
    ) -> WeComBindingLeaseGrant:
        self.acquired.append((binding.binding_id, owner_id))
        self.owners[1] = owner_id
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

    async def release_binding(self, grant: WeComBindingLeaseGrant) -> None:
        self.released.append((grant.binding_id, self.owners[grant.epoch]))


class _FailingSecrets:
    def resolve(self, _secret_ref: SecretRef) -> str:
        raise RuntimeError("secret resolver failed")


class _FailingLifecycleClient:
    def __init__(
        self,
        *,
        handler_failure: bool = False,
        connect_failure: bool = False,
        disconnect_failure: bool = False,
    ) -> None:
        self.handler_failure = handler_failure
        self.connect_failure = connect_failure
        self.disconnect_failure = disconnect_failure
        self.is_connected = True
        self.is_authenticated = True
        self.disconnect_calls = 0

    def on(self, _event: str, _handler: Any) -> None:
        if self.handler_failure:
            raise RuntimeError("handler registration failed")

    async def connect_async(self) -> None:
        if self.connect_failure:
            raise RuntimeError("connect failed")
        self.is_connected = False

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        if self.disconnect_failure:
            raise RuntimeError("disconnect failed")

    async def send_message(self, _chat_id: str, _body: dict[str, Any]) -> object:
        return object()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["factory", "secret", "handler", "connect", "disconnect"])
async def test_wecom_connector_releases_lease_for_every_startup_failure(failure: str) -> None:
    lease = _TrackingLease()
    client = _FailingLifecycleClient(
        handler_failure=failure == "handler",
        connect_failure=failure == "connect",
        disconnect_failure=failure == "disconnect",
    )

    def factory(_account: str, _secret: str) -> _FailingLifecycleClient:
        if failure == "factory":
            raise RuntimeError("client factory failed")
        return client

    secrets: Any = (
        _FailingSecrets() if failure == "secret" else LocalSecretProvider(allow_literal=True)
    )
    connector = WeComConnector(
        secrets,
        lease,
        owner_id="lifecycle-owner",
        client_factory=factory,
    )

    async def sink(_binding_id: str, _envelope: object) -> None:
        return None

    with pytest.raises(RuntimeError):
        await connector.run(_wecom_binding(), sink)

    assert lease.acquired == [(BINDING_ID, "lifecycle-owner")]
    assert lease.released == [(BINDING_ID, "lifecycle-owner")]
    assert client.disconnect_calls == (0 if failure in {"factory", "secret"} else 1)


@pytest.mark.asyncio
async def test_wecom_retryable_download_succeeds_on_third_attempt(monkeypatch) -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request.extensions.get("timeout", 0))
        if len(attempts) < 3:
            return httpx.Response(503, request=request)
        return httpx.Response(200, content=_encrypt(b"retry-ok"), request=request)

    transport = httpx.MockTransport(handler)

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("trpc_service.channels.wecom.asyncio.sleep", no_sleep)
    binding = _wecom_binding()

    def factory(_account: str, _secret: str) -> BoundedWeComDownloadClient:
        return BoundedWeComDownloadClient(
            1024,
            5,
            transport=transport,
            chunk_bytes=17,
        )

    downloader = WeComMediaDownloader(
        LocalSecretProvider(allow_literal=True),
        client_factory=factory,
    )
    reference = MediaReference(
        provider_url=MEDIA_URL,
        encryption_key_ref=AES_KEY,
        filename="retry.txt",
    )
    try:
        result = await downloader.download_media(
            binding,
            MESSAGE_ID,
            "retry-media",
            media_reference=reference,
        )
    finally:
        await downloader.close()

    assert len(attempts) == 3
    assert result.data == b"retry-ok"
    assert result.filename == "retry.txt"
