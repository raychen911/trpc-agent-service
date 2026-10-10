from __future__ import annotations

import asyncio
import math
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

import trpc_service.agent.wecom_manager as manager_module
import trpc_service.channels.dispatcher as dispatcher_module
import trpc_service.channels.feishu as feishu_module
import trpc_service.channels.wecom as wecom_module
from trpc_service.agent.wecom_manager import WeComConnectionManager
from trpc_service.channels.dispatcher import ChannelDispatcher
from trpc_service.channels.envelopes import (
    DeliveryReceipt,
    DeliveryStatus,
    InboundEnvelope,
    MediaReference,
    OutboundEnvelope,
    PayloadKind,
)
from trpc_service.channels.feishu import (
    FeishuAdapter,
    FeishuResourceError,
    FeishuVerificationError,
)
from trpc_service.channels.wecom import (
    WeComConnector,
    WeComMediaDownloader,
    WeComMediaError,
    parse_wecom_frame,
)
from trpc_service.config.secrets import LocalSecretProvider, SecretRef
from trpc_service.storage.models import (
    BindingRoute,
    DeliveryAttempt,
    OutboxRecord,
    WeComBindingLeaseGrant,
)
from trpc_service.storage.protocols import DeliveryInProgress, FencingConflict
from trpc_service.tenant.models import Channel, ChannelBinding, ConversationKind


def binding(
    *,
    binding_id: str = "binding-a",
    tenant_id: str = "tenant-a",
    channel: Channel = Channel.WECOM_AI_BOT,
    enabled: bool = True,
    secret_name: str = "bot_secret",
) -> ChannelBinding:
    return ChannelBinding(
        binding_id=binding_id,
        tenant_id=tenant_id,
        app_id="support",
        channel=channel,
        account_id=f"account-{binding_id}",
        enabled=enabled,
        secret_refs={secret_name: SecretRef(uri="literal://secret")},
    )


def route(value: ChannelBinding, *, active: bool = True) -> BindingRoute:
    return BindingRoute(binding=value, tenant_active=active, active_config_version=1)


def outbound(
    *,
    channel: Channel = Channel.FEISHU,
    binding_id: str = "binding-a",
    tenant_id: str = "tenant-a",
    target_id: str = "ou_user",
    reply_to: str | None = "message",
    kind: PayloadKind = PayloadKind.TEXT,
    text: str | None = "reply",
) -> OutboundEnvelope:
    return OutboundEnvelope(
        outbound_id="outbound-1",
        tenant_id=tenant_id,
        binding_id=binding_id,
        channel=channel,
        target_id=target_id,
        session_id="session-1",
        payload_kind=kind,
        text=text,
        in_reply_to=reply_to,
    )


def outbox(payload: object | None = None, *, attempts: int = 0) -> OutboxRecord:
    return OutboxRecord(
        outbox_id="outbox-1",
        tenant_id="tenant-a",
        event_type="outbound.feishu.ready",
        aggregate_id="session-1",
        payload=(payload if payload is not None else outbound().model_dump(mode="json")),
        attempts=attempts,
    )


class ManagerRepository:
    def __init__(self, bindings: tuple[ChannelBinding, ...] = ()) -> None:
        self.bindings = bindings
        self.routes: dict[str, BindingRoute | None] = {}

    async def list_bindings(self, _channel: Channel) -> tuple[ChannelBinding, ...]:
        return self.bindings

    async def resolve_binding(self, binding_id: str) -> BindingRoute | None:
        return self.routes.get(binding_id)


class SignatureConnector:
    def __init__(self, mode: int, *, fail: bool = False) -> None:
        self.mode = mode
        self.fail = fail
        self.calls: list[tuple[Any, ...]] = []

    async def run(self, *args: Any) -> None:
        self.calls.append(args)
        if self.fail:
            raise RuntimeError("connector failed")


class DispatcherRepository:
    def __init__(self, records: tuple[OutboxRecord, ...] = ()) -> None:
        self.records = records
        self.route: BindingRoute | None = None
        self.released: list[tuple[Any, ...]] = []
        self.marked: list[tuple[Any, ...]] = []
        self.receipts: list[tuple[Any, ...]] = []
        self.dead: list[tuple[Any, ...]] = []
        self.release_error = False
        self.persist_error = False

    async def claim_outbox(self, **_kwargs: Any) -> tuple[OutboxRecord, ...]:
        return self.records

    async def resolve_binding(self, _binding_id: str) -> BindingRoute | None:
        return self.route

    async def release_outbox(self, *args: Any, **kwargs: Any) -> None:
        if self.release_error:
            raise RuntimeError("release failed")
        self.released.append((*args, kwargs))

    async def mark_outbox_published(self, *args: Any, **kwargs: Any) -> None:
        self.marked.append((*args, kwargs))

    async def record_delivery(self, *args: Any, **kwargs: Any) -> None:
        if self.persist_error:
            raise RuntimeError("persist failed")
        self.receipts.append((*args, kwargs))

    async def dead_letter_outbox(self, *args: Any, **kwargs: Any) -> None:
        if self.persist_error:
            raise RuntimeError("dead-letter failed")
        self.dead.append((*args, kwargs))


class NonAtomicRepository(DispatcherRepository):
    begin_delivery = None
    finish_delivery = None


class AtomicRepository(DispatcherRepository):
    def __init__(self, records: tuple[OutboxRecord, ...] = ()) -> None:
        super().__init__(records)
        self.begin_value: object = DeliveryAttempt(
            tenant_id="tenant-a", outbound_id="outbound-1", attempt_number=1, owner_id="owner"
        )
        self.finish_error = False
        self.finished: list[tuple[Any, ...]] = []

    async def begin_delivery(self, _record: OutboxRecord, *, owner_id: str) -> object:
        if isinstance(self.begin_value, BaseException):
            raise self.begin_value
        return self.begin_value

    async def finish_delivery(self, *args: Any, **kwargs: Any) -> None:
        if self.finish_error:
            raise RuntimeError("finish failed")
        self.finished.append((*args, kwargs))


class Adapter:
    def __init__(self, result: object = None, error: BaseException | None = None) -> None:
        self.result = result
        self.error = error

    async def send(self, _envelope: OutboundEnvelope, _binding: ChannelBinding) -> object:
        if self.error is not None:
            raise self.error
        return self.result


@pytest.mark.asyncio
async def test_wecom_manager_reconcile_filters_routes_and_cleans_tasks() -> None:
    first = binding(binding_id="first")
    disabled = binding(binding_id="disabled", enabled=False)
    repository = ManagerRepository((first, disabled))
    repository.routes = {
        "first": route(first),
        "disabled": route(disabled),
        "stale": None,
    }
    connector = SignatureConnector(4)
    manager = WeComConnectionManager(
        repository, connector, lambda *_args: None, lambda *_args: None
    )
    manager._routes["stale"] = route(binding(binding_id="stale"))

    async def failed() -> None:
        raise RuntimeError("old task")

    async def waiting() -> None:
        await asyncio.Event().wait()

    failed_task = asyncio.create_task(failed())
    await asyncio.gather(failed_task, return_exceptions=True)
    waiting_task = asyncio.create_task(waiting())
    manager._tasks = {"stale": failed_task, "disabled": waiting_task}
    await manager.reconcile_once()
    assert "stale" not in manager._routes
    assert "disabled" not in manager._tasks
    manager._stop_event.set()
    await asyncio.gather(*manager._tasks.values(), return_exceptions=True)
    assert "first" in manager._tasks


@pytest.mark.asyncio
async def test_wecom_manager_binding_runner_signatures_and_failures(monkeypatch) -> None:
    binding_value = binding()
    manager = WeComConnectionManager(
        ManagerRepository(), SignatureConnector(4), lambda *_args: None
    )
    manager._stop_event.set()
    await manager._run_binding(binding_value)

    manager._stop_event.clear()
    manager._connector = SignatureConnector(2)
    manager._stop_event.set()
    await manager._run_binding(binding_value)

    manager._stop_event.clear()
    manager._connector = SignatureConnector(3, fail=True)

    async def fail_wait(awaitable: Any, **_kwargs: Any) -> None:
        awaitable.close()
        manager._stop_event.set()
        raise TimeoutError

    monkeypatch.setattr(manager_module.asyncio, "wait_for", fail_wait)
    await manager._run_binding(binding_value)

    class CancelConnector:
        async def run(self, _binding: Any, _sink: Any) -> None:
            raise asyncio.CancelledError

    manager._connector = CancelConnector()
    manager._stop_event.clear()
    with pytest.raises(asyncio.CancelledError):
        await manager._run_binding(binding_value)


@pytest.mark.asyncio
async def test_wecom_manager_run_error_backoff_and_wait_helpers(monkeypatch) -> None:
    manager = WeComConnectionManager(
        ManagerRepository(), SignatureConnector(2), lambda *_args: None
    )
    with pytest.raises(ValueError, match="non-negative"):
        await manager.run(refresh_seconds=-1)

    calls = 0
    stop_event = asyncio.Event()

    async def reconcile() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        stop_event.set()

    async def wait_and_stop(_event: asyncio.Event | None, _seconds: float) -> None:
        stop_event.set()

    monkeypatch.setattr(manager, "reconcile_once", reconcile)
    monkeypatch.setattr(manager_module, "_wait_or_stop", wait_and_stop)
    await manager.run(refresh_seconds=0, stop_event=stop_event)
    assert calls == 1

    async def wait_noop(awaitable: Any, _timeout: float) -> None:
        awaitable.close()
        stop_event.set()
        raise TimeoutError

    monkeypatch.setattr(manager_module.asyncio, "wait_for", wait_noop)
    await manager.run(refresh_seconds=0, stop_event=stop_event)


@pytest.mark.asyncio
async def test_wecom_manager_emergency_and_signature_helpers() -> None:
    manager = WeComConnectionManager(
        ManagerRepository(), SignatureConnector(2), lambda *_args: None
    )
    envelope = InboundEnvelope(
        channel=Channel.WECOM_AI_BOT,
        account_id="account",
        external_message_id="message",
        external_user_id="user",
        conversation_kind=ConversationKind.DIRECT,
        payload_kind=PayloadKind.TEXT,
        text="hello",
    )
    with pytest.raises(RuntimeError, match="not configured"):
        await manager._emergency_for_binding("missing", envelope)
    manager = WeComConnectionManager(
        ManagerRepository(), SignatureConnector(2), lambda *_args: None, lambda *_args: None
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        await manager._emergency_for_binding("missing", envelope)
    received: list[tuple[Any, ...]] = []
    manager._routes["binding"] = route(binding(binding_id="binding"))

    async def emergency(*args: Any) -> None:
        received.append(args)

    manager._emergency_sink = emergency  # type: ignore[method-assign]
    await manager._emergency_for_binding("binding", envelope)
    assert received

    assert not manager_module._accepts_stop_event(object())
    assert not manager_module._accepts_emergency_sink(object())
    assert manager_module._accepts_stop_event(lambda _a, _b, _c: None)
    assert manager_module._accepts_emergency_sink(lambda _a, _b, _c, _d: None)

    async def no_sleep(_seconds: float) -> None:
        return None

    original_sleep = manager_module.asyncio.sleep
    manager_module.asyncio.sleep = no_sleep
    try:
        await manager_module._wait_or_stop(None, 0)
    finally:
        manager_module.asyncio.sleep = original_sleep


@pytest.mark.asyncio
async def test_dispatcher_constructor_stop_and_poison_paths() -> None:
    with pytest.raises(ValueError, match="batch"):
        ChannelDispatcher(
            DispatcherRepository(), {}, owner_id="owner", event_type="event", batch_limit=0
        )
    with pytest.raises(ValueError, match="lease"):
        ChannelDispatcher(
            DispatcherRepository(), {}, owner_id="owner", event_type="event", lease_seconds=0
        )

    first = outbox()
    second = outbox().model_copy(update={"outbox_id": "outbox-2"})
    repo = DispatcherRepository((first, second))
    repo.release_error = True
    stop = asyncio.Event()
    stop.set()
    dispatcher = ChannelDispatcher(repo, {}, owner_id="owner", event_type=first.event_type)
    assert await dispatcher.dispatch_once(stop) == 0

    repo = DispatcherRepository((outbox({"broken": True}),))
    dispatcher = ChannelDispatcher(repo, {}, owner_id="owner", event_type=first.event_type)
    assert await dispatcher.dispatch_once() == 1
    assert repo.dead


@pytest.mark.asyncio
async def test_dispatcher_non_atomic_all_receipt_states() -> None:
    base = outbox()
    repo = NonAtomicRepository((base,))
    repo.route = route(binding(channel=Channel.FEISHU))

    failed = DeliveryReceipt(
        outbound_id="outbound-1",
        status=DeliveryStatus.FAILED,
        retryable=True,
        provider_code="rate_limit",
    )
    dispatcher = ChannelDispatcher(
        repo,
        {Channel.FEISHU: Adapter(failed)},
        owner_id="owner",
        event_type=base.event_type,
        max_attempts=2,
    )
    assert await dispatcher.dispatch_once() == 0
    assert repo.released

    repo.records = (base.model_copy(update={"attempts": 2}),)
    assert await dispatcher.dispatch_once() == 1
    assert repo.dead

    repo.records = (base,)
    mismatch = DeliveryReceipt(outbound_id="wrong", status=DeliveryStatus.DELIVERED)
    dispatcher = ChannelDispatcher(
        repo,
        {Channel.FEISHU: Adapter(mismatch)},
        owner_id="owner",
        event_type=base.event_type,
    )
    assert await dispatcher.dispatch_once() == 1
    assert repo.dead[-1][1]["reason"] == "receipt_mismatch"

    repo.records = (base,)
    repo.route = route(binding(channel=Channel.FEISHU))
    repo.persist_error = True
    dispatcher = ChannelDispatcher(
        repo,
        {
            Channel.FEISHU: Adapter(
                DeliveryReceipt(outbound_id="outbound-1", status=DeliveryStatus.DELIVERED)
            )
        },
        owner_id="owner",
        event_type=base.event_type,
    )
    assert await dispatcher.dispatch_once() == 0


@pytest.mark.asyncio
async def test_dispatcher_atomic_fencing_and_finish_paths() -> None:
    base = outbox()
    repo = AtomicRepository((base,))
    repo.route = route(binding(channel=Channel.FEISHU))
    delivered = DeliveryReceipt(outbound_id="outbound-1", status=DeliveryStatus.DELIVERED)
    dispatcher = ChannelDispatcher(
        repo,
        {Channel.FEISHU: Adapter(delivered)},
        owner_id="owner",
        event_type=base.event_type,
    )
    assert await dispatcher.dispatch_once() == 1
    assert repo.finished

    repo.begin_value = FencingConflict("stale")
    assert await dispatcher.dispatch_once() == 0
    repo.begin_value = object()
    assert await dispatcher.dispatch_once() == 0
    repo.begin_value = DeliveryAttempt(
        tenant_id="tenant-a", outbound_id="outbound-1", attempt_number=2, owner_id="owner"
    )
    repo.finish_error = True
    assert await dispatcher.dispatch_once() == 0

    progress = DeliveryInProgress("delivery in progress", attempt_number=3)
    repo.begin_value = progress
    repo.finish_error = False
    assert await dispatcher.dispatch_once() == 1
    assert repo.finished[-1][1]["attempt_number"] == 3

    repo.begin_value = DeliveryInProgress("old ledger")
    repo.records = (base,)
    assert await dispatcher._resolve_in_progress(base, outbound(), repo.begin_value) == (
        DeliveryReceipt(
            outbound_id="outbound-1",
            status=DeliveryStatus.AMBIGUOUS,
            provider_code="delivery_in_progress",
        ),
        True,
    )

    class NoPersist(AtomicRepository):
        async def record_delivery(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("record failed")

    no_persist = NoPersist((base,))
    no_persist.route = repo.route
    no_persist.begin_value = DeliveryInProgress("old ledger")
    dispatcher2 = ChannelDispatcher(
        no_persist,
        {Channel.FEISHU: Adapter(delivered)},
        owner_id="owner",
        event_type=base.event_type,
    )
    assert await dispatcher2.dispatch_once() == 0


@pytest.mark.asyncio
async def test_dispatcher_helpers_and_run_failures(monkeypatch) -> None:
    dispatcher = ChannelDispatcher(DispatcherRepository(), {}, owner_id="owner", event_type="event")
    record = outbox()
    await dispatcher._retry_or_dead_letter(record, reason="x", delay=dispatcher_module.timedelta(0))
    assert (
        dispatcher._normalize_receipt(
            DeliveryReceipt(outbound_id="wrong", status=DeliveryStatus.DELIVERED), outbound()
        ).provider_code
        == "receipt_mismatch"
    )
    assert (
        dispatcher._retry_delay(
            DeliveryReceipt.model_construct(
                outbound_id="outbound-1", status=DeliveryStatus.FAILED, retry_after_seconds=math.nan
            ),
            record,
        ).total_seconds()
        == 1
    )
    assert (
        dispatcher._retry_delay(
            DeliveryReceipt(
                outbound_id="outbound-1",
                status=DeliveryStatus.FAILED,
                retryable=True,
            ),
            record.model_copy(update={"attempts": 3}),
            jitter=lambda base: base * 0.75,
        ).total_seconds()
        == 6
    )
    assert (
        dispatcher._retry_delay(
            DeliveryReceipt(
                outbound_id="outbound-1",
                status=DeliveryStatus.FAILED,
                retryable=True,
                retry_after_seconds=7,
            ),
            record,
            jitter=lambda _base: 99,
        ).total_seconds()
        == 7
    )
    with pytest.raises(ValueError, match="retry jitter"):
        dispatcher._retry_delay(
            DeliveryReceipt(
                outbound_id="outbound-1",
                status=DeliveryStatus.FAILED,
                retryable=True,
            ),
            record,
            jitter=lambda _base: math.inf,
        )

    with pytest.raises(ValueError, match="finite"):
        await dispatcher.run(poll_seconds=math.inf)

    stop = asyncio.Event()
    calls = 0

    async def broken_dispatch(_stop: asyncio.Event | None = None) -> int:
        nonlocal calls
        calls += 1
        raise RuntimeError("cycle")

    async def stop_wait(_event: asyncio.Event | None, _seconds: float) -> None:
        stop.set()

    monkeypatch.setattr(dispatcher, "dispatch_once", broken_dispatch)
    monkeypatch.setattr(dispatcher, "_wait_or_stop", stop_wait)
    await dispatcher.run(stop_event=stop)
    assert calls == 1


@pytest.mark.parametrize(
    "value",
    [None, "", "not-a-url", "https://user:pass@example.test/x", "https://[::1"],
)
def test_wecom_frame_and_media_shape_guards(value: object) -> None:
    with pytest.raises(ValueError):
        wecom_module._body(SimpleNamespace(body=value))
    for invalid in (
        {"x": {"y": {"z": {"w": {"q": {"n": {"m": {"o": {"p": {"r": {"s": 1}}}}}}}}}}},
        {"x": "x" * (128 * 1024 + 1)},
        {str(i): i for i in range(257)},
        {"x": [0] * 257},
        {1: "bad"},
    ):
        with pytest.raises(ValueError):
            wecom_module._validate_frame_shape(invalid)
    assert not wecom_module._is_https_url(value) if isinstance(value, str) else True


@pytest.mark.parametrize(
    "frame",
    [
        {"body": {"from": {"userid": "u"}, "msgid": []}},
        {"body": {"from": {"userid": "u"}, "create_time": True}},
        {"body": {"from": {"userid": "u"}, "create_time": "bad"}},
        {"body": {"from": {"userid": "u"}, "create_time": -1}},
        {"body": {"from": {"userid": "u"}, "create_time": 9_999_999_999}},
    ],
)
def test_wecom_parse_rejects_invalid_ids_and_timestamps(frame: object) -> None:
    with pytest.raises(ValueError):
        parse_wecom_frame(frame, account_id="bot")


def test_wecom_parse_mixed_unknown_and_media_metadata() -> None:
    parsed = parse_wecom_frame(
        {
            "body": {
                "from": {"userid": "u"},
                "chattype": "group",
                "chatid": "chat",
                "msgtype": "mixed",
                "mixed": {
                    "msg_item": [
                        "noise",
                        {"msgtype": "text", "text": {"content": "one"}},
                        {"msgtype": "image", "image": {"url": "https://x", "aeskey": "a"}},
                    ]
                },
            }
        },
        account_id="bot",
    )
    assert parsed.text == "one" and parsed.media[0].content_type == "image/*"
    event = parse_wecom_frame(
        {"body": {"from": {"userid": "u"}, "msgtype": "unknown", "event": "bad"}},
        account_id="bot",
    )
    assert event.payload_kind == PayloadKind.EVENT


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("C:\\folder\\file.txt", "file.txt"),
        ('"photo.JPG"', "photo.JPG"),
        ("\x00", None),
        ("\x01bad", None),
    ],
)
def test_wecom_filename_content_and_provider_helpers(value: object, expected: str | None) -> None:
    assert wecom_module._safe_filename(value) == expected
    assert wecom_module._content_type(value if isinstance(value, str) else None, "file")
    assert wecom_module._media_provider_id("https://provider", "https://source").startswith("url_")
    assert wecom_module._media_provider_id(" media-key ", "https://source") == "media-key"
    assert wecom_module._media_kind("file", MediaReference(content_type="image/png")) == "image"
    assert wecom_module._media_kind("file", MediaReference(content_type="video/mp4")) == "video"


@pytest.mark.asyncio
async def test_wecom_media_download_all_failure_categories(monkeypatch) -> None:
    reference = MediaReference(provider_url="https://media.example/x", encryption_key_ref="key")

    class Client:
        is_connected = True
        is_authenticated = True

        def __init__(
            self, result: object = (b"ok", "file.txt"), error: BaseException | None = None
        ):
            self.result = result
            self.error = error

        async def download_file(self, _url: str, _key: str) -> object:
            if self.error:
                raise self.error
            return self.result

    with pytest.raises(WeComMediaError, match="failed"):
        await wecom_module._download_wecom_media(
            SimpleNamespace(is_connected=False),
            "key",
            reference,
            media_type="file",
            filename=None,
            max_media_bytes=10,
            media_timeout_seconds=1,
            require_connection=True,
        )
    with pytest.raises(WeComMediaError):
        await wecom_module._download_wecom_media(
            Client(),
            "key",
            reference,
            media_type="bad",
            filename=None,
            max_media_bytes=10,
            media_timeout_seconds=1,
            require_connection=False,
        )
    with pytest.raises(WeComMediaError):
        await wecom_module._download_wecom_media(
            SimpleNamespace(),
            "key",
            reference,
            media_type="file",
            filename=None,
            max_media_bytes=10,
            media_timeout_seconds=1,
            require_connection=False,
        )

    class ErrorClient(Client):
        pass

    for error in (
        wecom_module.WeComDownloadError("download", retryable=True, status=503),
        TimeoutError(),
        ValueError(),
        SimpleNamespace(status=404),
        SimpleNamespace(status=429),
        SimpleNamespace(status=503),
        ConnectionError(),
        RuntimeError(),
    ):
        with pytest.raises(WeComMediaError):
            await wecom_module._download_wecom_media(
                ErrorClient(error=error),
                "key",
                reference,
                media_type="file",
                filename=None,
                max_media_bytes=10,
                media_timeout_seconds=1,
                require_connection=False,
            )

    for result in (((b"x",),), ("not-bytes", "name"), (b"too-long", "name")):
        value = result[0] if isinstance(result, tuple) and len(result) == 1 else result
        with pytest.raises(WeComMediaError):
            await wecom_module._download_wecom_media(
                Client(result=value),
                "key",
                reference,
                media_type="file",
                filename=None,
                max_media_bytes=3,
                media_timeout_seconds=1,
                require_connection=False,
            )

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(wecom_module.asyncio, "sleep", no_sleep)
    failures = Client(error=TimeoutError())
    with pytest.raises(WeComMediaError):
        await wecom_module._download_wecom_media_with_retry(
            failures,
            "key",
            reference,
            media_type="file",
            filename=None,
            max_media_bytes=10,
            media_timeout_seconds=1,
            require_connection=False,
        )


@pytest.mark.asyncio
async def test_wecom_downloader_cache_locator_and_close_paths() -> None:
    value = binding()
    ref = MediaReference(provider_url="https://media.example/x", encryption_key_ref="key")
    with pytest.raises(ValueError):
        WeComMediaDownloader(LocalSecretProvider(allow_literal=True), max_media_bytes=0)
    with pytest.raises(ValueError):
        WeComMediaDownloader(LocalSecretProvider(allow_literal=True), media_timeout_seconds=0)
    downloader = WeComMediaDownloader(LocalSecretProvider(allow_literal=True))
    with pytest.raises(WeComMediaError):
        await downloader.download_media(value, "message", "key", media_reference=None)
    closed = WeComMediaDownloader(LocalSecretProvider(allow_literal=True))
    await closed.close()
    with pytest.raises(WeComMediaError):
        await closed.download_media(value, "message", "key", media_reference=ref)
    other = value.model_copy(update={"channel": Channel.FEISHU})
    with pytest.raises(WeComMediaError):
        await downloader.download_media(other, "message", "key", media_reference=ref)


class ConnectorLease:
    def __init__(self, allowed: bool = True) -> None:
        self.allowed = allowed
        self.released: list[tuple[str, str]] = []

    async def acquire_binding(self, binding: Any, _owner_id: str) -> WeComBindingLeaseGrant | None:
        if not self.allowed:
            return None
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
        self.released.append((grant.binding_id, grant.owner_hash))


class ConnectorClient:
    is_connected = True
    is_authenticated = True

    def __init__(self, response: object = {"errcode": 0, "req_id": "id"}) -> None:
        self.response = response
        self.handlers: dict[str, Any] = {}
        self.error: BaseException | None = None

    def on(self, event: str, handler: Any) -> None:
        self.handlers[event] = handler

    async def connect_async(self) -> None:
        await self.handlers["message"]({"body": {"msgtype": "bad"}})
        await self.handlers["message"](
            {
                "body": {
                    "from": {"userid": "u"},
                    "msgtype": "video",
                    "video": {"url": "https://x", "aeskey": "a"},
                }
            }
        )
        await self.handlers["disconnected"]()

    def disconnect(self) -> Any:
        return None

    async def send_message(self, _target: str, _body: Any) -> object:
        if self.error:
            raise self.error
        return self.response

    async def download_file(self, _url: str, _key: str) -> tuple[bytes, str | None]:
        return b"ok", "video.mp4"


@pytest.mark.asyncio
async def test_wecom_connector_accepts_video_emergency_and_send_mappings() -> None:
    lease = ConnectorLease()
    client = ConnectorClient()
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        lease,
        owner_id="owner",
        client_factory=lambda *_: client,
    )
    accepted: list[Any] = []
    emergency: list[Any] = []

    async def sink(_binding: str, envelope: Any) -> None:
        accepted.append(envelope)
        raise RuntimeError("database unavailable")

    async def emergency_sink(*args: Any) -> None:
        emergency.append(args)

    await connector.run(binding(), sink, asyncio.Event(), emergency_sink)
    assert accepted and emergency and lease.released

    connector._clients["binding-a"] = client
    connector._fenced_bindings.add("binding-a")
    for response, code in (
        ({"status_code": 429, "retry_after": 2}, "rate_limited"),
        ({"status_code": 500}, "http_500"),
        ({"errcode": "bad"}, "response_unknown"),
        ({}, "response_unknown"),
    ):
        client.response = response
        receipt = await connector.send(outbound(channel=Channel.WECOM_AI_BOT), binding())
        assert receipt.provider_code == code
    for error, code in (
        (TimeoutError(), "transport_unknown"),
        (RuntimeError("errcode=45009"), "rate_limited"),
        (RuntimeError("unknown"), "runtime_unknown"),
        (ValueError(), "runtime_unknown"),
    ):
        client.error = error
        receipt = await connector.send(outbound(channel=Channel.WECOM_AI_BOT), binding())
        assert receipt.provider_code == code
        client.error = None


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"status_code": 404}, "http_404"),
        ({"status": 429, "headers": {"Retry-After": "3"}}, "rate_limited"),
        ({"errcode": 0, "req_id": "req"}, None),
        ({"errcode": 500}, "ack_500"),
        ({"errcode": True}, "ack_1"),
        ({"headers": {"req_id": "header-id"}}, None),
    ],
)
def test_wecom_response_helper_variants(response: object, expected: str | None) -> None:
    value = wecom_module._wecom_response_receipt(outbound(channel=Channel.WECOM_AI_BOT), response)
    assert value.provider_code == expected
    assert (
        wecom_module._runtime_error_receipt(
            outbound(channel=Channel.WECOM_AI_BOT), RuntimeError("without code")
        ).provider_code
        == "runtime_unknown"
    )
    assert (
        wecom_module._resolve_tenant_secret(
            LocalSecretProvider(allow_literal=True), SecretRef(uri="literal://value")
        )
        == "value"
    )


def test_wecom_response_retry_after_and_client_ready_helpers() -> None:
    assert (
        wecom_module._response_retry_after({"retry_after": True, "headers": {}, "body": {}}) is None
    )
    assert (
        wecom_module._response_retry_after({"retry_after": "bad", "body": {"retry_after": 4}}) == 4
    )
    assert wecom_module._response_retry_after({"retry_after": -1}) is None
    assert wecom_module._response_retry_after({"retry_after": 4001}) is None
    assert wecom_module._response_int({"status": "bad"}, "status") is None
    assert wecom_module._response_int({"status": True}, "status") is None
    assert not wecom_module._client_ready(SimpleNamespace(is_connected=False))
    assert not wecom_module._client_ready(
        SimpleNamespace(is_connected=True, is_authenticated=False)
    )
    assert not wecom_module._client_ready(SimpleNamespace(is_connected=True))


def feishu_binding(*, encrypted: bool = True) -> ChannelBinding:
    refs = {
        "app_secret": SecretRef(uri="literal://app-secret"),
        "verification_token": SecretRef(uri="literal://verify"),
    }
    if encrypted:
        refs["encrypt_key"] = SecretRef(uri="literal://encrypt")
    return binding(
        binding_id="feishu-binding",
        tenant_id="tenant-a",
        channel=Channel.FEISHU,
        secret_name="app_secret",
    ).model_copy(update={"secret_refs": refs, "account_id": "app-id"})


def feishu_outbound() -> OutboundEnvelope:
    return outbound(channel=Channel.FEISHU, binding_id="feishu-binding", target_id="ou_user")


@pytest.mark.parametrize(
    "kwargs", [{"max_media_bytes": 0}, {"media_timeout_seconds": 0}, {"media_chunk_bytes": 0}]
)
def test_feishu_constructor_and_private_helpers(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        FeishuAdapter(LocalSecretProvider(allow_literal=True), **kwargs)
    assert feishu_module._header({"x-lark-request-token": "x"}, "X-LARK-REQUEST-TOKEN") == "x"
    assert feishu_module._header({}, "missing") is None
    for value in (b"bad", b"[]"):
        with pytest.raises(FeishuVerificationError):
            feishu_module._json_object(value)
    with pytest.raises(FeishuVerificationError):
        feishu_module._mapping([], "field")
    assert feishu_module._event_type({"type": "event"}) == "event"
    assert feishu_module._event_type({"header": {"event_type": 1}}) is None
    assert feishu_module._integer(True, default=3) == 3
    assert feishu_module._integer("bad", default=3) == 3
    assert feishu_module._timestamp("bad")
    assert feishu_module._timestamp(10**40)
    assert feishu_module._receive_id_type("ou_user") == "open_id"
    assert feishu_module._receive_id_type("chat") == "chat_id"


@pytest.mark.parametrize(
    ("message_type", "content", "expected"),
    [
        ("text", {"text": " hi "}, PayloadKind.TEXT),
        ("post", {"title": "title", "content": [[{"text": "body"}]]}, PayloadKind.MIXED),
        ("image", {"image_key": "image"}, PayloadKind.IMAGE),
        ("sticker", {"image_key": "image"}, PayloadKind.IMAGE),
        ("file", {"file_key": "file", "file_name": 1}, PayloadKind.FILE),
        ("audio", {"file_key": "audio"}, PayloadKind.VOICE),
        ("media", {"file_key": "video"}, PayloadKind.VIDEO),
        ("event", {}, PayloadKind.EVENT),
    ],
)
def test_feishu_content_normalization_variants(
    message_type: str, content: dict[str, Any], expected: PayloadKind
) -> None:
    value = feishu_module._normalize_content(message_type, content, [{"key": "@bot"}])
    assert value[0] == expected
    if expected in {PayloadKind.IMAGE, PayloadKind.FILE, PayloadKind.VOICE, PayloadKind.VIDEO}:
        assert value[2]
    with pytest.raises(FeishuVerificationError):
        feishu_module._normalize_content("image", {}, [])


@pytest.mark.asyncio
async def test_feishu_resource_error_and_receipt_branches() -> None:
    adapter = FeishuAdapter(
        LocalSecretProvider(allow_literal=True), http_client=httpx.AsyncClient()
    )
    envelope = feishu_outbound()
    for status, code in (
        (401, None),
        (404, None),
        (429, None),
        (500, None),
        (400, 99991400),
        (400, 999),
    ):
        error = await adapter._resource_response_error(
            httpx.Response(status, json={"code": code or status}), code=code
        )
        assert error.provider_code
    for response in (
        httpx.Response(200, content=b"not-json"),
        httpx.Response(200, json=[]),
        httpx.Response(200, json={"code": "bad"}),
        httpx.Response(400, json=[]),
        httpx.Response(200, json={"code": 0, "data": {}}),
        httpx.Response(400, json={"code": 500}),
    ):
        receipt, _ = adapter._receipt(envelope, response)
        assert receipt.provider_code is not None
    await adapter.close()


@pytest.mark.asyncio
async def test_feishu_resource_download_size_and_transport_branches() -> None:
    class StreamContext:
        def __init__(self, response: Any = None, error: BaseException | None = None) -> None:
            self.response = response
            self.error = error

        async def __aenter__(self) -> Any:
            if self.error:
                raise self.error
            return self.response

        async def __aexit__(self, *_args: Any) -> None:
            return None

    class Http:
        def __init__(self, response: Any = None, error: BaseException | None = None) -> None:
            self.response = response
            self.error = error

        def stream(self, *_args: Any, **_kwargs: Any) -> StreamContext:
            return StreamContext(self.response, self.error)

        async def aclose(self) -> None:
            return None

    binding_value = feishu_binding(encrypted=False)
    for response in (
        httpx.Response(200, headers={"Content-Length": "99"}, content=b"ok"),
        httpx.Response(200, content=b'{"code": 500}', headers={"Content-Type": "application/json"}),
    ):
        adapter = FeishuAdapter(
            LocalSecretProvider(allow_literal=True), http_client=Http(response), max_media_bytes=3
        )
        with pytest.raises(FeishuResourceError):
            await adapter._download_resource_once(
                binding_value, "message", "key", "file", None, "token"
            )

    for error in (
        httpx.ReadTimeout("timeout"),
        httpx.NetworkError("network"),
        httpx.ProtocolError("protocol"),
        httpx.HTTPError("http"),
    ):
        adapter = FeishuAdapter(
            LocalSecretProvider(allow_literal=True), http_client=Http(error=error)
        )
        with pytest.raises(FeishuResourceError):
            await adapter._download_resource_once(
                binding_value, "message", "key", "file", None, "token"
            )


@pytest.mark.asyncio
async def test_feishu_secret_resolution_retry_after_and_token_invalidation() -> None:
    class Empty:
        def resolve(self, _ref: SecretRef) -> str:
            return ""

    adapter = FeishuAdapter(Empty(), http_client=httpx.AsyncClient())
    with pytest.raises(FeishuVerificationError):
        adapter._resolve(SecretRef(uri="env://MISSING"), "token")
    with pytest.raises(feishu_module._TokenError):
        adapter._resolve_token_secret(SecretRef(uri="env://MISSING"))
    assert (
        feishu_module._retry_after_seconds(httpx.Response(200, headers={"Retry-After": "-1"}))
        is None
    )
    assert (
        feishu_module._retry_after_seconds(httpx.Response(200, headers={"Retry-After": "bad"}))
        is None
    )
    assert (
        feishu_module._retry_after_seconds(httpx.Response(200, headers={"Retry-After": "99999"}))
        == 3600
    )
    adapter._tokens[("feishu-binding", "app-id", "hash")] = ("token", 100)
    adapter._invalidate_token(feishu_binding())
    assert not adapter._tokens
    await adapter.close()
