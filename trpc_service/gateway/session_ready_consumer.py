"""Synchronous SessionReady v2 consumer.

Redis carries only a reconstructible wake-up.  Ordering and ownership remain
in the durable session mailbox; the consumer claims that mailbox before it
loads and executes the corresponding durable inbox record.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from threading import Event, Thread, current_thread
from typing import Any, Protocol

from trpc_service.gateway.redis_streams import RedisStreamsTransport
from trpc_service.storage.base import now_utc
from trpc_service.storage.retry import retry_delay_seconds


class SessionReadyMailbox(Protocol):
    def claim_session(
        self,
        tenant_id: str,
        session_id: str,
        owner: str,
        lease_seconds: int,
        *,
        expected_generation: int | None = None,
    ) -> Any: ...

    def commit(self, lease: Any) -> Any: ...

    def retry(self, lease: Any, retry_at: Any = None) -> Any: ...

    def dead_letter(self, lease: Any, error: str) -> Any: ...


class SessionReadyInbox(Protocol):
    def accept_inbox(
        self,
        tenant_id: str,
        dedupe_key: str,
        session_id: str,
        payload: dict[str, Any],
        owner: str,
        lease_seconds: int = 180,
        message_id: str | None = None,
    ) -> tuple[Any, bool]: ...

    def list_inbox_by_tenant(self, tenant_id: str) -> list[Any]: ...

    def get_inbox_by_message_id(self, tenant_id: str, message_id: str) -> Any | None: ...

    def complete_inbox(self, tenant_id: str, dedupe_key: str, owner: str, result: dict[str, Any]) -> None: ...

    def fail_inbox(self, tenant_id: str, dedupe_key: str, owner: str, error: str) -> None: ...

    def dead_inbox(self, tenant_id: str, dedupe_key: str, owner: str, error: str) -> None: ...


@dataclass(slots=True)
class SessionReadyClaim:
    tenant_id: str
    session_id: str
    lease: Any
    inbox: Any


class _MailboxLeaseHeartbeat:
    def __init__(self, mailbox: SessionReadyMailbox, lease: Any, lease_seconds: int) -> None:
        self.mailbox = mailbox
        self.lease = lease
        self.lease_seconds = lease_seconds
        self.interval = max(0.1, lease_seconds / 3.0)
        self.stop_event = Event()
        self.error: BaseException | None = None
        self.thread: Thread | None = None

    def start(self) -> _MailboxLeaseHeartbeat:
        def beat() -> None:
            try:
                while not self.stop_event.wait(self.interval):
                    renew = getattr(self.mailbox, "renew", None)
                    if renew is None:
                        return
                    self.lease = renew(self.lease, self.lease_seconds)
            except BaseException as exc:
                self.error = exc

        self.thread = Thread(target=beat, name="mailbox-lease-heartbeat", daemon=True)
        self.thread.start()
        return self

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread is not current_thread():
            self.thread.join(timeout=max(1.0, self.interval))

    def raise_if_failed(self) -> None:
        if self.error is not None:
            raise self.error


class SessionReadyConsumer:
    """Consume one ready notice at a time with durable claim/fencing."""

    def __init__(
        self,
        mailbox: SessionReadyMailbox,
        inbox: SessionReadyInbox,
        transport: RedisStreamsTransport,
        executor: Callable[[SessionReadyClaim], dict[str, Any] | None],
        *,
        owner: str,
        lease_seconds: int | None = None,
    ) -> None:
        if not owner:
            raise ValueError("owner must not be empty")
        self.mailbox = mailbox
        self.inbox = inbox
        self.transport = transport
        self.executor = executor
        self.owner = owner
        self.lease_seconds = lease_seconds or int(os.getenv("MAILBOX_LEASE_SECONDS", "180"))
        if self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")

    @staticmethod
    def _max_attempts() -> int:
        raw = os.getenv("SESSION_MAILBOX_MAX_ATTEMPTS", "5")
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError("SESSION_MAILBOX_MAX_ATTEMPTS must be an integer") from exc
        if value < 1:
            raise ValueError("SESSION_MAILBOX_MAX_ATTEMPTS must be positive")
        return value

    def consume_once(self, block_ms: int = 1000) -> bool:
        return self.transport.consume_once(self._handle, block_ms=block_ms)

    def consume_forever(self, block_ms: int = 1000) -> None:
        while True:
            self.consume_once(block_ms)

    def handle(self, payload: dict[str, Any]) -> None:
        """Handle a decoded notice (also used by a multi-tenant router)."""
        self._handle(payload)

    def _handle(self, payload: dict[str, Any]) -> None:
        # The generic durable-outbox stream wraps the event body under
        # ``payload``; accept both that form and a direct ready notice.
        if isinstance(payload.get("payload"), dict):
            nested = payload["payload"]
            if payload.get("topic") == "session.ready.v2" or nested.get("topic") == "session.ready.v2":
                payload = {**payload, **nested}
        tenant_id = str(payload.get("tenant_id", ""))
        session_id = str(payload.get("aggregate_id") or payload.get("session_id", ""))
        if not tenant_id or not session_id:
            raise ValueError("session.ready.v2 payload requires tenant_id and aggregate_id")
        try:
            claim = self.mailbox.claim_session(
                tenant_id,
                session_id,
                self.owner,
                self.lease_seconds,
                expected_generation=payload.get("generation"),
            )
        except TypeError:
            # Legacy mailbox implementations may not expose generation-aware
            # claims; they still retain the durable lease/fencing contract.
            claim = self.mailbox.claim_session(tenant_id, session_id, self.owner, self.lease_seconds)
        lease = getattr(claim, "lease", None)
        if not getattr(claim, "claimed", False) or lease is None:
            # A duplicate/stale wake-up is harmless: the authoritative mailbox
            # has either already been claimed or has no unresolved work.
            return
        message_id = str(getattr(lease, "message_id", ""))
        lookup = getattr(self.inbox, "get_inbox_by_message_id", None)
        record = lookup(tenant_id, message_id) if lookup is not None else next(
            (item for item in self.inbox.list_inbox_by_tenant(tenant_id)
             if str(getattr(item, "message_id", "")) == message_id),
            None,
        )
        if record is None:
            self.mailbox.dead_letter(lease, "session mailbox inbox record is missing")
            raise LookupError("session mailbox inbox record is missing")

        # A retry can follow a transient inbox failure, in which case the
        # previous gateway owner was cleared. Reclaim the durable inbox before
        # executing so the final completion is fenced to this worker.
        accept = getattr(self.inbox, "accept_inbox", None)
        if accept is not None and getattr(record, "status", None) != "completed":
            refreshed, _ = accept(
                tenant_id,
                str(record.dedupe_key),
                str(record.session_id),
                dict(record.payload),
                self.owner,
                self.lease_seconds,
                message_id,
            )
            record = refreshed
            if getattr(record, "status", None) in {"completed", "dead"}:
                self.mailbox.commit(lease)
                return
        claimed = SessionReadyClaim(tenant_id, session_id, lease, record)
        heartbeat = _MailboxLeaseHeartbeat(self.mailbox, lease, self.lease_seconds).start()
        try:
            result = self.executor(claimed) or {}
            heartbeat.raise_if_failed()
        except Exception as exc:
            heartbeat.stop()
            owner = str(getattr(record, "owner", "") or self.owner)
            retry_count = int(getattr(heartbeat.lease, "retry_count", 0)) + 1
            error = type(exc).__name__
            if retry_count >= self._max_attempts():
                dead_letter = getattr(self.mailbox, "dead_letter", None)
                if dead_letter is None:
                    raise RuntimeError("session mailbox does not support dead-lettering") from exc
                dead_letter(heartbeat.lease, error)
                dead_inbox = getattr(self.inbox, "dead_inbox", None)
                if dead_inbox is not None:
                    dead_inbox(tenant_id, record.dedupe_key, owner, error)
                else:
                    self.inbox.fail_inbox(tenant_id, record.dedupe_key, owner, error)
            else:
                delay = retry_delay_seconds(
                    retry_count,
                    identity=f"{tenant_id}:{record.session_id}:{record.message_id}",
                    base_env="SESSION_MAILBOX_RETRY_BASE_SECONDS",
                    cap_env="SESSION_MAILBOX_RETRY_MAX_SECONDS",
                    default_base=2,
                    default_cap=120,
                )
                self.mailbox.retry(
                    heartbeat.lease,
                    retry_at=now_utc() + timedelta(seconds=delay),
                )
                self.inbox.fail_inbox(tenant_id, record.dedupe_key, owner, error)
            raise
        heartbeat.stop()
        heartbeat.raise_if_failed()
        self.mailbox.commit(heartbeat.lease)
        owner = str(getattr(record, "owner", "") or self.owner)
        self.inbox.complete_inbox(tenant_id, record.dedupe_key, owner, result)


class SessionReadyMultiplexer:
    """Route one Redis consumer-group stream to tenant-local consumers."""

    def __init__(self, transport: RedisStreamsTransport, consumers: dict[str, SessionReadyConsumer]) -> None:
        self.transport = transport
        self.consumers = consumers

    def consume_once(self, block_ms: int = 1000) -> bool:
        def route(payload: dict[str, Any]) -> None:
            tenant_id = str(payload.get("tenant_id", ""))
            consumer = self.consumers.get(tenant_id)
            if consumer is None:
                raise LookupError(f"no session-ready consumer configured for tenant {tenant_id!r}")
            consumer.handle(payload)

        return self.transport.consume_once(route, block_ms=block_ms)

    def consume_forever(self, block_ms: int = 1000) -> None:
        while True:
            self.consume_once(block_ms)


def publish_session_ready(record: Any, publisher: Any, stream: str | None = None) -> str | None:
    """Publish one durable outbox record to Redis Streams.

    The outbox record remains pending until the caller marks it complete, so a
    Redis outage can be recovered by rerunning the normal outbox dispatcher.
    """

    if getattr(record, "topic", "") != "session.ready.v2":
        return None
    payload = {
        "event_id": record.event_id,
        "tenant_id": record.tenant_id,
        "aggregate_id": record.aggregate_id,
        **dict(record.payload or {}),
    }
    encoded = json.dumps(payload, ensure_ascii=False, default=str)
    key = stream or os.getenv("SESSION_READY_STREAM", "trpc-agent:session-ready:stream")
    return str(publisher.xadd(key, {"payload": encoded}))


__all__ = ["SessionReadyClaim", "SessionReadyConsumer", "SessionReadyMultiplexer", "publish_session_ready"]
