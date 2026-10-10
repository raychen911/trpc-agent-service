"""Additive Redis Streams transport for PostgreSQL ``SessionReady`` notices.

This transport is deliberately separate from the v1 inbound queue.  A v2
notice is only a wake-up signal: PostgreSQL remains authoritative and the
consumer must claim the session in PostgreSQL before acknowledging the Redis
delivery.  Redis PEL ownership therefore covers only the short
``receive -> PostgreSQL claim -> ack`` window.  This module does not implement
business leases, BUSY retries, or long-running heartbeats.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field
from redis.exceptions import ResponseError

from trpc_service.metrics.prometheus import (
    SESSION_READY_ACK_LATENCY,
    SESSION_READY_ACKS,
    SESSION_READY_RECEIVE_LATENCY,
    SESSION_READY_RECEIVES,
    SESSION_READY_RECLAIM_LATENCY,
    SESSION_READY_RECLAIMS,
)

SESSION_READY_STREAM_V2 = "trpc:session-ready:v2"
SESSION_READY_GROUP_V2 = "trpc-session-ready-v2"

_SESSION_READY_FIELDS = (
    "event_id",
    "tenant_id",
    "session_id",
    "generation",
    "priority",
    "trace_id",
    "created_at",
)

_DEFAULT_XDEL_ATTEMPTS = 3
_DEFAULT_XDEL_RETRY_DELAY_SECONDS = 0.05
logger = logging.getLogger(__name__)


class SessionReady(BaseModel):
    """The complete v2 wire payload; no provider or lease fields are allowed."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str = Field(min_length=1, max_length=512)
    tenant_id: str = Field(min_length=1, max_length=512)
    session_id: str = Field(min_length=1, max_length=512)
    generation: int = Field(ge=1)
    priority: int = Field(ge=0)
    trace_id: str = Field(min_length=1, max_length=512)
    created_at: datetime


@dataclass(frozen=True, slots=True)
class SessionReadyDelivery:
    """A received notice together with its Redis stream id for explicit ack."""

    stream_id: str
    message: SessionReady


class SessionReadyCodec:
    """Encode/decode the seven-field v2 payload without a JSON envelope."""

    @staticmethod
    def encode(message: SessionReady) -> dict[str, str]:
        created_at = message.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        else:
            created_at = created_at.astimezone(UTC)
        return {
            "event_id": message.event_id,
            "tenant_id": message.tenant_id,
            "session_id": message.session_id,
            "generation": str(message.generation),
            "priority": str(message.priority),
            "trace_id": message.trace_id,
            "created_at": created_at.isoformat().replace("+00:00", "Z"),
        }

    @classmethod
    def decode(cls, fields: Mapping[str | bytes, str | bytes]) -> SessionReady:
        normalized = {_text(key): _text(value) for key, value in fields.items()}
        expected = set(_SESSION_READY_FIELDS)
        if set(normalized) != expected:
            missing = sorted(expected - set(normalized))
            extra = sorted(set(normalized) - expected)
            details: list[str] = []
            if missing:
                details.append(f"missing={','.join(missing)}")
            if extra:
                details.append(f"extra={','.join(extra)}")
            raise ValueError(f"invalid session-ready:v2 fields ({'; '.join(details)})")
        try:
            return SessionReady(
                event_id=normalized["event_id"],
                tenant_id=normalized["tenant_id"],
                session_id=normalized["session_id"],
                generation=int(normalized["generation"]),
                priority=int(normalized["priority"]),
                trace_id=normalized["trace_id"],
                created_at=datetime.fromisoformat(normalized["created_at"].replace("Z", "+00:00")),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid session-ready:v2 field value") from exc


class SessionReadyRedisClient(Protocol):
    async def xgroup_create(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xadd(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xack(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xdel(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xautoclaim(self, *args: Any, **kwargs: Any) -> Any: ...


class SessionReadyQueue:
    """Explicit receive/ack transport for ``trpc:session-ready:v2``."""

    def __init__(
        self,
        redis: SessionReadyRedisClient,
        *,
        stream: str = SESSION_READY_STREAM_V2,
        group: str = SESSION_READY_GROUP_V2,
        xdel_attempts: int = _DEFAULT_XDEL_ATTEMPTS,
        xdel_retry_delay_seconds: float = _DEFAULT_XDEL_RETRY_DELAY_SECONDS,
    ) -> None:
        if xdel_attempts < 1:
            raise ValueError("xdel_attempts must be positive")
        if xdel_retry_delay_seconds < 0 or not math.isfinite(xdel_retry_delay_seconds):
            raise ValueError("xdel_retry_delay_seconds must be non-negative and finite")
        self._redis = redis
        self.stream = stream
        self.group = group
        self._xdel_attempts = xdel_attempts
        self._xdel_retry_delay_seconds = xdel_retry_delay_seconds

    async def ensure_group(self) -> None:
        await _ensure_group(self._redis, self.stream, self.group)

    async def publish(self, message: SessionReady) -> str:
        """Publish one wake-up notice; duplicates are intentionally permitted."""

        stream_id = await self._redis.xadd(
            self.stream,
            SessionReadyCodec.encode(message),
            id="*",
        )
        return _text(stream_id)

    async def receive_new(
        self,
        *,
        consumer: str,
        count: int = 10,
        block_ms: int | None = 5_000,
    ) -> tuple[SessionReadyDelivery, ...]:
        """Receive new notices only; PEL recovery belongs to the reclaimer."""

        if not consumer:
            raise ValueError("consumer must not be empty")
        if count < 1:
            raise ValueError("count must be positive")
        read_kwargs: dict[str, Any] = {"count": count}
        # BLOCK 0 means an infinite wait in Redis.  Non-positive values are
        # intentionally non-blocking for deterministic shutdown/tests.
        if block_ms is not None and block_ms > 0:
            read_kwargs["block"] = block_ms
        started = time.perf_counter()
        try:
            try:
                rows = await self._redis.xreadgroup(
                    self.group,
                    consumer,
                    streams={self.stream: ">"},
                    **read_kwargs,
                )
            except ResponseError as error:
                if "NOGROUP" not in str(error):
                    raise
                # Redis is a reconstructable wake-up transport. A total Redis
                # loss removes both the stream and its consumer group, so a
                # running worker must self-heal instead of requiring restart.
                await self.ensure_group()
                rows = await self._redis.xreadgroup(
                    self.group,
                    consumer,
                    streams={self.stream: ">"},
                    **read_kwargs,
                )
            deliveries = await self._decode_rows_safely(rows)
        except asyncio.CancelledError:
            raise
        except Exception:
            SESSION_READY_RECEIVES.labels(outcome="error").inc()
            raise
        else:
            SESSION_READY_RECEIVES.labels(outcome="received" if deliveries else "empty").inc()
            return deliveries
        finally:
            SESSION_READY_RECEIVE_LATENCY.observe(time.perf_counter() - started)

    async def receive(
        self,
        *,
        consumer: str,
        count: int = 10,
        block_ms: int | None = 5_000,
    ) -> tuple[SessionReadyDelivery, ...]:
        """Compatibility spelling for :meth:`receive_new`."""

        return await self.receive_new(consumer=consumer, count=count, block_ms=block_ms)

    async def ack(self, delivery: SessionReadyDelivery) -> bool:
        """Acknowledge exactly the received stream entry after PG claim."""

        started = time.perf_counter()
        try:
            acknowledged = await self._redis.xack(self.stream, self.group, delivery.stream_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            SESSION_READY_ACKS.labels(outcome="error").inc()
            raise
        else:
            if acknowledged:
                # XACK removes the entry from this consumer group's PEL.  Only
                # after that successful state transition is exact XDEL safe:
                # deleting before XACK could leave an unconfirmed PEL entry
                # dangling and remove its recoverable wake-up payload.
                await self._delete_after_ack(delivery.stream_id)
            outcome = "acked" if acknowledged else "missing"
            SESSION_READY_ACKS.labels(outcome=outcome).inc()
            return bool(acknowledged)
        finally:
            SESSION_READY_ACK_LATENCY.observe(time.perf_counter() - started)

    async def _delete_after_ack(self, stream_id: str) -> None:
        """Best-effort bounded cleanup of one already-ACKed stream entry.

        XACK is the correctness boundary; XDEL only bounds the retained
        wake-up history.  A transient deletion failure must therefore not
        make a successfully acknowledged delivery look unacknowledged.  A
        small finite retry closes common short Redis blips without introducing
        an unbounded task or an unsafe stream-wide trim.
        """

        for attempt in range(1, self._xdel_attempts + 1):
            try:
                await self._redis.xdel(self.stream, stream_id)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if attempt == self._xdel_attempts:
                    logger.warning(
                        "session-ready exact delete failed after ACK",
                        extra={
                            "error_type": type(error).__name__,
                            "attempts": self._xdel_attempts,
                        },
                    )
                    return
                if self._xdel_retry_delay_seconds:
                    await asyncio.sleep(self._xdel_retry_delay_seconds)
            else:
                return

    async def _decode_rows_safely(self, rows: Any) -> tuple[SessionReadyDelivery, ...]:
        deliveries: list[SessionReadyDelivery] = []
        for group_row in rows or ():
            try:
                _, entries = group_row
            except (TypeError, ValueError):
                continue
            for row in entries or ():
                stream_id = _safe_stream_id(row)
                try:
                    raw_stream_id, fields = row
                    deliveries.append(
                        SessionReadyDelivery(
                            stream_id=_text(raw_stream_id),
                            message=SessionReadyCodec.decode(fields),
                        )
                    )
                except (TypeError, ValueError, UnicodeError) as error:
                    await self._quarantine(stream_id, type(error).__name__)
        return tuple(deliveries)

    async def _quarantine(self, stream_id: str | bytes, reason: str) -> None:
        stream = _text(stream_id)
        logger.warning(
            "session-ready poison record quarantined",
            extra={"error_type": reason, "safe_code": "session_ready_poison"},
        )
        try:
            await self._redis.xack(self.stream, self.group, stream)
            await self._delete_after_ack(stream)
        except Exception as error:
            logger.warning(
                "session-ready poison cleanup failed",
                extra={"error_type": type(error).__name__, "safe_code": "session_ready_cleanup"},
            )


DeliveryHandler = Callable[[SessionReadyDelivery], Awaitable[None]]
PermitCallback = Callable[[], Awaitable[bool]]


class SessionReadyReclaimer:
    """Independent, cursor-preserving XAUTOCLAIM loop for stale v2 PEL entries."""

    def __init__(
        self,
        redis: SessionReadyRedisClient,
        *,
        consumer: str,
        on_delivery: DeliveryHandler | None = None,
        stream: str = SESSION_READY_STREAM_V2,
        group: str = SESSION_READY_GROUP_V2,
        min_idle_ms: int = 60_000,
        count: int = 100,
        poll_seconds: float = 1.0,
        permit: PermitCallback | None = None,
    ) -> None:
        if not consumer:
            raise ValueError("consumer must not be empty")
        if min_idle_ms < 0:
            raise ValueError("min_idle_ms must be non-negative")
        if count < 1:
            raise ValueError("count must be positive")
        if poll_seconds < 0:
            raise ValueError("poll_seconds must be non-negative")
        self._redis = redis
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self._on_delivery = on_delivery
        self._min_idle_ms = min_idle_ms
        self._count = count
        self._poll_seconds = poll_seconds
        self._permit = permit
        self._cursor = "0-0"

    @property
    def cursor(self) -> str:
        """The next XAUTOCLAIM cursor, useful for diagnostics and tests."""

        return self._cursor

    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]:
        """Claim one cursor page and invoke the handler without implicit ACK."""

        started = time.perf_counter()
        deliveries: tuple[SessionReadyDelivery, ...]
        try:
            if self._permit is not None and not await self._permit():
                return ()
            try:
                result = await self._redis.xautoclaim(
                    self.stream,
                    self.group,
                    self.consumer,
                    min_idle_time=self._min_idle_ms,
                    start_id=self._cursor,
                    count=self._count,
                )
            except ResponseError as error:
                if "NOGROUP" not in str(error):
                    raise
                await _ensure_group(self._redis, self.stream, self.group)
                self._cursor = "0-0"
                deliveries = ()
            else:
                next_cursor, entries = _unpack_xautoclaim(result)
                self._cursor = next_cursor
                deliveries = await self._decode_entries_safely(entries)
                if self._on_delivery is not None:
                    for delivery in deliveries:
                        await self._on_delivery(delivery)
        except asyncio.CancelledError:
            raise
        except Exception:
            SESSION_READY_RECLAIMS.labels(outcome="error").inc()
            raise
        else:
            SESSION_READY_RECLAIMS.labels(outcome="claimed" if deliveries else "empty").inc()
            return deliveries
        finally:
            SESSION_READY_RECLAIM_LATENCY.observe(time.perf_counter() - started)

    async def _decode_entries_safely(self, entries: Any) -> tuple[SessionReadyDelivery, ...]:
        deliveries: list[SessionReadyDelivery] = []
        for row in entries or ():
            stream_id = _safe_stream_id(row)
            try:
                raw_stream_id, fields = row
                deliveries.append(
                    SessionReadyDelivery(
                        stream_id=_text(raw_stream_id),
                        message=SessionReadyCodec.decode(fields),
                    )
                )
            except (TypeError, ValueError, UnicodeError) as error:
                # A reclaimed poison entry already belongs to this consumer;
                # ACK/XDEL it here so one malformed notice cannot starve the
                # rest of the PEL.  The caller still owns valid delivery ACK.
                logger.warning(
                    "session-ready reclaimed poison quarantined",
                    extra={"error_type": type(error).__name__, "safe_code": "session_ready_poison"},
                )
                try:
                    await self._redis.xack(self.stream, self.group, _text(stream_id))
                    await self._delete_reclaimed(_text(stream_id))
                except Exception as cleanup_error:
                    logger.warning(
                        "session-ready reclaimed poison cleanup failed",
                        extra={
                            "error_type": type(cleanup_error).__name__,
                            "safe_code": "session_ready_cleanup",
                        },
                    )
        return tuple(deliveries)

    async def _delete_reclaimed(self, stream_id: str) -> None:
        delete = getattr(self._redis, "xdel", None)
        if not callable(delete):
            return
        try:
            await delete(self.stream, stream_id)
        except Exception as error:
            logger.warning(
                "session-ready reclaimed poison delete failed",
                extra={"error_type": type(error).__name__, "safe_code": "session_ready_cleanup"},
            )

    async def reclaim_once(self) -> tuple[SessionReadyDelivery, ...]:
        """Compatibility spelling for :meth:`reclaim`."""

        return await self.reclaim()

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Run until stopped; PostgreSQL claim and explicit ack stay in handler."""

        if self._on_delivery is None:
            raise RuntimeError("on_delivery is required when running the reclaimer")
        while stop_event is None or not stop_event.is_set():
            deliveries = await self.reclaim()
            if deliveries or self._poll_seconds == 0:
                continue
            if stop_event is None:
                await asyncio.sleep(self._poll_seconds)
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self._poll_seconds)
                except TimeoutError:
                    pass


def _decode_rows(rows: Any) -> tuple[SessionReadyDelivery, ...]:
    if not rows:
        return ()
    deliveries: list[SessionReadyDelivery] = []
    for _, entries in rows:
        deliveries.extend(_decode_entries(entries))
    return tuple(deliveries)


def _decode_entries(entries: Any) -> tuple[SessionReadyDelivery, ...]:
    return tuple(
        SessionReadyDelivery(stream_id=_text(stream_id), message=SessionReadyCodec.decode(fields))
        for stream_id, fields in entries
    )


def _unpack_xautoclaim(result: Any) -> tuple[str, Any]:
    if not result:
        return "0-0", ()
    next_cursor = _text(result[0]) if len(result) > 0 else "0-0"
    entries = result[1] if len(result) > 1 else ()
    return (next_cursor or "0-0"), entries


async def _ensure_group(
    redis: SessionReadyRedisClient,
    stream: str,
    group: str,
) -> None:
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except ResponseError as error:
        if "BUSYGROUP" not in str(error):
            raise


def _text(value: str | bytes) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _safe_stream_id(row: Any) -> str:
    try:
        value = row[0] if row else ""
        return _text(value)
    except (IndexError, TypeError, UnicodeError):
        return ""


__all__ = [
    "SESSION_READY_GROUP_V2",
    "SESSION_READY_STREAM_V2",
    "SessionReady",
    "SessionReadyCodec",
    "SessionReadyDelivery",
    "SessionReadyQueue",
    "SessionReadyReclaimer",
]
