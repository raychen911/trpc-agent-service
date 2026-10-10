"""Encrypted Redis emergency buffer used only while PostgreSQL is unavailable."""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import asyncpg
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from redis.exceptions import RedisError, ResponseError

from trpc_service.config.settings import SchedulerVersion
from trpc_service.storage.models import PreparedInbound
from trpc_service.storage.protocols import RuntimeRepository

logger = logging.getLogger(__name__)


class EmergencyRedisClient(Protocol):
    async def xadd(self, stream: str, fields: dict[str, Any]) -> str | bytes: ...

    async def xgroup_create(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xautoclaim(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xack(self, *args: Any) -> Any: ...

    async def xdel(self, *args: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class EmergencyMessage:
    message_id: str
    prepared: PreparedInbound


class EmergencyQueue:
    def __init__(
        self,
        redis: EmergencyRedisClient,
        key: bytes,
        *,
        scheduler_version: SchedulerVersion = SchedulerVersion.V1,
        stream: str | None = None,
        group: str | None = None,
        reclaim_after_ms: int = 30_000,
        key_version: str = "v1",
        previous_keys: Mapping[str, bytes] | None = None,
    ) -> None:
        if len(key) != 32:
            raise ValueError("emergency queue AES-GCM key must be 32 bytes")
        self._redis = redis
        self._cipher = AESGCM(key)
        self._stream = stream or f"trpc:emergency:{scheduler_version.value}"
        self._group = group or f"trpc-emergency-drainers-{scheduler_version.value}"
        self._reclaim_after_ms = reclaim_after_ms
        if not key_version or len(key_version) > 32:
            raise ValueError("emergency key version is invalid")
        self._key_version = key_version
        self._keys = {key_version: key}
        for version, previous in (previous_keys or {}).items():
            if not version or len(previous) != 32:
                raise ValueError("emergency previous key is invalid")
            self._keys[version] = previous
        self._reclaim_cursor = "0-0"

    async def enqueue(self, prepared: PreparedInbound) -> str:
        binding_id = prepared.context.channel_binding_id
        nonce = os.urandom(12)
        plaintext = prepared.model_dump_json().encode()
        encrypted = self._cipher.encrypt(nonce, plaintext, binding_id.encode())
        value = await self._redis.xadd(
            self._stream,
            {
                "binding_id": binding_id,
                "key_version": self._key_version,
                "nonce": base64.b64encode(nonce),
                "payload": base64.b64encode(encrypted),
            },
        )
        return value.decode() if isinstance(value, bytes) else value

    def decrypt(
        self,
        binding_id: str,
        nonce: str | bytes,
        payload: str | bytes,
    ) -> PreparedInbound:
        raw_nonce = base64.b64decode(nonce, validate=True)
        raw_payload = base64.b64decode(payload, validate=True)
        plain = None
        for key in self._keys.values():
            try:
                plain = AESGCM(key).decrypt(raw_nonce, raw_payload, binding_id.encode())
            except InvalidTag:
                continue
            break
        if plain is None:
            raise InvalidTag
        prepared = PreparedInbound.model_validate_json(plain)
        if prepared.context.channel_binding_id != binding_id:
            raise ValueError("emergency record binding does not match authenticated payload")
        return prepared

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._stream,
                self._group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise

    async def consume(
        self,
        *,
        consumer: str,
        count: int = 10,
        block_ms: int = 1_000,
    ) -> tuple[EmergencyMessage, ...]:
        await self.ensure_group()
        claimed = await self._redis.xautoclaim(
            self._stream,
            self._group,
            consumer,
            self._reclaim_after_ms,
            self._reclaim_cursor,
            count=count,
        )
        if claimed and len(claimed) > 0:
            cursor = claimed[0]
            self._reclaim_cursor = _text(cursor) if cursor else "0-0"
        claimed_rows = claimed[1] if claimed and len(claimed) > 1 else []
        if claimed_rows:
            return await self._decode_rows(claimed_rows)
        rows = await self._redis.xreadgroup(
            self._group,
            consumer,
            {self._stream: ">"},
            count=count,
            block=block_ms,
        )
        if not rows:
            return ()
        return await self._decode_rows(row for _, entries in rows for row in entries)

    async def ack(self, message: EmergencyMessage) -> None:
        await self._redis.xack(self._stream, self._group, message.message_id)
        await self._exact_delete(message.message_id)

    async def quarantine(self, message: EmergencyMessage, *, reason: str) -> None:
        """Drop one poison/invalid record after durable audit-safe logging."""

        logger.warning(
            "emergency record quarantined",
            extra={"error_type": reason, "safe_code": "emergency_poison"},
        )
        await self._redis.xack(self._stream, self._group, message.message_id)
        await self._exact_delete(message.message_id)

    async def _exact_delete(self, message_id: str) -> None:
        delete = getattr(self._redis, "xdel", None)
        if not callable(delete):
            return
        try:
            await delete(self._stream, message_id)
        except Exception as error:
            logger.warning(
                "emergency exact delete failed",
                extra={"error_type": type(error).__name__, "safe_code": "emergency_xdel"},
            )

    async def _decode_rows(self, rows: Any) -> tuple[EmergencyMessage, ...]:
        messages: list[EmergencyMessage] = []
        for row in rows:
            try:
                message_id = _text(row[0]) if row else ""
                messages.append(self._decode(row))
            except (
                InvalidTag,
                ValueError,
                TypeError,
                KeyError,
                IndexError,
                UnicodeError,
                binascii.Error,
            ) as error:
                message_id = _safe_message_id(row)
                if message_id:
                    await self._quarantine_raw(message_id, type(error).__name__)
        return tuple(messages)

    async def _quarantine_raw(self, message_id: str, reason: str) -> None:
        logger.warning(
            "emergency poison record quarantined",
            extra={"error_type": reason, "safe_code": "emergency_poison"},
        )
        try:
            await self._redis.xack(self._stream, self._group, message_id)
            await self._exact_delete(message_id)
        except Exception as error:
            # Keep decoding the remainder of this page.  The poison entry
            # remains recoverable in the PEL and will be retried by the next
            # bounded reclaim cycle if Redis is transiently unavailable.
            logger.warning(
                "emergency poison cleanup deferred",
                extra={
                    "error_type": type(error).__name__,
                    "safe_code": "emergency_poison_cleanup_deferred",
                },
            )

    def _decode(self, row: Any) -> EmergencyMessage:
        message_id, fields = row
        binding_id = _text(_field(fields, "binding_id"))
        return EmergencyMessage(
            message_id=_text(message_id),
            prepared=self.decrypt(
                binding_id,
                _field(fields, "nonce"),
                _field(fields, "payload"),
            ),
        )


class EmergencyQueueDrainer:
    """Replay revision-pinned callbacks after PostgreSQL becomes available."""

    def __init__(
        self,
        repository: RuntimeRepository,
        queue: EmergencyQueue,
        *,
        consumer_id: str,
        scheduler_version: SchedulerVersion = SchedulerVersion.V1,
        retry_seconds: float = 1.0,
    ) -> None:
        self._repository = repository
        self._queue = queue
        self._consumer_id = consumer_id
        self._scheduler_version = scheduler_version
        self._retry_seconds = retry_seconds

    async def drain_once(self, stop_event: asyncio.Event | None = None) -> int:
        messages = await self._queue.consume(consumer=self._consumer_id)
        processed = 0
        for message in messages:
            if stop_event is not None and stop_event.is_set():
                break
            context = message.prepared.context
            try:
                await self._repository.get_config(
                    context.tenant_id,
                    context.app_id,
                    context.config_version,
                )
                if self._scheduler_version == SchedulerVersion.V2:
                    await self._repository.accept_inbound_v2(
                        context=context,
                        envelope=message.prepared.envelope,
                        trace_headers=message.prepared.trace_headers,
                    )
                else:
                    await self._repository.accept_inbound(
                        context=context,
                        envelope=message.prepared.envelope,
                        trace_headers=message.prepared.trace_headers,
                    )
            except (asyncpg.PostgresError, RedisError):
                raise
            except (LookupError, ValueError, TypeError) as error:
                quarantine = getattr(self._queue, "quarantine", None)
                if callable(quarantine):
                    await quarantine(message, reason=type(error).__name__)
                else:
                    await self._queue.ack(message)
            else:
                await self._queue.ack(message)
                processed += 1
        return processed

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        while stop_event is None or not stop_event.is_set():
            try:
                await self.drain_once(stop_event)
            except (asyncpg.PostgresError, InvalidTag, LookupError, RedisError, ValueError):
                logger.warning(
                    "emergency queue replay degraded",
                    extra={"safe_code": "emergency_replay_failed"},
                )
                if stop_event is None:
                    await asyncio.sleep(self._retry_seconds)
                else:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=self._retry_seconds)
                    except TimeoutError:
                        pass


def _field(fields: dict[Any, Any], name: str) -> Any:
    if name in fields:
        return fields[name]
    encoded = name.encode()
    if encoded in fields:
        return fields[encoded]
    raise ValueError(f"emergency record is missing {name}")


def _text(value: str | bytes) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _safe_message_id(row: Any) -> str:
    try:
        return _text(row[0]) if row else ""
    except (IndexError, TypeError, UnicodeError):
        return ""


__all__ = ["EmergencyMessage", "EmergencyQueue", "EmergencyQueueDrainer"]
