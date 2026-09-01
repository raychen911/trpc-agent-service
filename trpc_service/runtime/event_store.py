"""Durable encrypted object stores used by the Worker event codec.

Only authenticated ciphertext crosses this boundary.  The local implementation is
for a single-host development process; Redis is the shared implementation used by
the composed runtime.  Neither implementation accepts arbitrary filesystem paths or
silently replaces an existing content-addressed object.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import os
import re
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from sqlalchemy import select

from trpc_service.storage import Database
from trpc_service.storage.models import EventObject

_OBJECT_KEY = re.compile(r"\Asdk-events/v1/[0-9a-f]{64}/[0-9a-f]{64}\Z")
_DIGEST = re.compile(r"\A[0-9a-f]{64}\Z")


class EventStoreError(RuntimeError):
    """An encrypted event object is missing, corrupt, or conflicting."""


class AsyncRedisClient(Protocol):
    """Small Redis surface required by :class:`RedisEventObjectStore`."""

    async def set(self, name: str, value: bytes, *, nx: bool = False) -> object:
        """Set a value, optionally only when absent."""

    async def get(self, name: str) -> object:
        """Return bytes or ``None``."""

    async def aclose(self) -> None:
        """Close network resources."""


class LocalEventObjectStore:
    """Crash-resistant, create-only encrypted event storage for development.

    The write is prepared and fsynced in the destination directory, then published
    with a hard link.  The link operation cannot overwrite an existing object.
    """

    def __init__(self, root: Path, *, max_object_bytes: int) -> None:
        if max_object_bytes < 1:
            raise ValueError("max_object_bytes must be positive")
        self._root = root.resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        self._max_object_bytes = max_object_bytes

    async def put_if_absent(
        self,
        object_key: str,
        ciphertext: str,
        *,
        tenant_id: str,
        ciphertext_sha256: str,
    ) -> None:
        """Publish one immutable object or verify an identical existing object."""

        _validate_tenant_id(tenant_id)
        payload = _validated_payload(
            object_key,
            ciphertext,
            ciphertext_sha256,
            max_object_bytes=self._max_object_bytes,
        )
        await asyncio.to_thread(
            self._put_sync,
            object_key,
            payload,
        )

    async def get(self, object_key: str, *, tenant_id: str) -> str:
        """Read and bound one encrypted object without following user paths."""

        _validate_tenant_id(tenant_id)
        target = self._path_for(object_key)
        payload = await asyncio.to_thread(self._read_bounded, target)
        try:
            return payload.decode("ascii")
        except UnicodeDecodeError:
            raise EventStoreError("encrypted event object encoding is invalid") from None

    def _put_sync(self, object_key: str, payload: bytes) -> None:
        target = self._path_for(object_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Keep the temporary basename short enough for legacy Windows path limits.
        temporary = target.parent / f".{uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as stream:
                os.chmod(temporary, 0o600)
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                existing = self._read_bounded(target)
                if not hmac.compare_digest(existing, payload):
                    raise EventStoreError(
                        "encrypted event object conflicts with existing value"
                    ) from None
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    def _read_bounded(self, target: Path) -> bytes:
        try:
            size = target.stat().st_size
            if size < 1 or size > self._max_object_bytes:
                raise EventStoreError("encrypted event object size is invalid")
            payload = target.read_bytes()
        except FileNotFoundError:
            raise EventStoreError("encrypted event object is unavailable") from None
        if len(payload) != size:
            raise EventStoreError("encrypted event object changed while being read")
        return payload

    def _path_for(self, object_key: str) -> Path:
        _validate_object_key(object_key)
        _, version, context_hash, ciphertext_hash = object_key.split("/")
        target = (self._root / version / context_hash / ciphertext_hash).resolve()
        if self._root not in target.parents:
            raise EventStoreError("encrypted event object key escapes its store")
        return target


class RedisEventObjectStore:
    """Shared create-only Redis store for encrypted event envelopes.

    Production Redis must be configured as non-evicting durable storage (AOF and
    backups).  The runtime never sets a TTL because committed session replay depends
    on these objects for the lifetime of the canonical event log.
    """

    def __init__(
        self,
        client: AsyncRedisClient,
        *,
        max_object_bytes: int,
        prefix: str = "trpc-agent-service:event-object:v1:",
    ) -> None:
        if max_object_bytes < 1:
            raise ValueError("max_object_bytes must be positive")
        if not prefix or any(character.isspace() for character in prefix):
            raise ValueError("Redis event prefix must be non-empty and whitespace-free")
        self._client = client
        self._max_object_bytes = max_object_bytes
        self._prefix = prefix

    async def put_if_absent(
        self,
        object_key: str,
        ciphertext: str,
        *,
        tenant_id: str,
        ciphertext_sha256: str,
    ) -> None:
        """Use Redis SET NX and verify a concurrent identical writer."""

        _validate_tenant_id(tenant_id)
        payload = _validated_payload(
            object_key,
            ciphertext,
            ciphertext_sha256,
            max_object_bytes=self._max_object_bytes,
        )
        redis_key = self._redis_key(object_key)
        created = await self._client.set(redis_key, payload, nx=True)
        if created:
            return
        existing = _redis_bytes(await self._client.get(redis_key), self._max_object_bytes)
        if not hmac.compare_digest(existing, payload):
            raise EventStoreError("encrypted event object conflicts with existing value")

    async def get(self, object_key: str, *, tenant_id: str) -> str:
        """Return one bounded ASCII ciphertext envelope."""

        _validate_tenant_id(tenant_id)
        payload = _redis_bytes(
            await self._client.get(self._redis_key(object_key)),
            self._max_object_bytes,
        )
        try:
            return payload.decode("ascii")
        except UnicodeDecodeError:
            raise EventStoreError("encrypted event object encoding is invalid") from None

    async def aclose(self) -> None:
        """Close the owned Redis client."""

        await self._client.aclose()

    def _redis_key(self, object_key: str) -> str:
        _validate_object_key(object_key)
        return self._prefix + object_key


class SqlEventObjectStore:
    """Tenant-scoped, create-only encrypted event storage in authoritative SQL.

    The table is protected by PostgreSQL RLS.  Objects are immutable and addressed
    by an authenticated context/content hash pair, so a retry may only confirm an
    identical value; it can never replace committed replay material.
    """

    def __init__(self, database: Database, *, max_object_bytes: int) -> None:
        if max_object_bytes < 1:
            raise ValueError("max_object_bytes must be positive")
        self._database = database
        self._max_object_bytes = max_object_bytes

    async def put_if_absent(
        self,
        object_key: str,
        ciphertext: str,
        *,
        tenant_id: str,
        ciphertext_sha256: str,
    ) -> None:
        """Insert one immutable row or verify the existing bytes exactly match."""

        _validate_tenant_id(tenant_id)
        payload = _validated_payload(
            object_key,
            ciphertext,
            ciphertext_sha256,
            max_object_bytes=self._max_object_bytes,
        )
        async with self._database.tenant_transaction(tenant_id) as session:
            existing = await session.scalar(
                select(EventObject).where(
                    EventObject.tenant_id == tenant_id,
                    EventObject.object_key == object_key,
                )
            )
            if existing is None:
                session.add(
                    EventObject(
                        tenant_id=tenant_id,
                        object_key=object_key,
                        ciphertext=ciphertext,
                        ciphertext_sha256=ciphertext_sha256,
                        size_bytes=len(payload),
                    )
                )
                await session.flush()
                return
            existing_payload = existing.ciphertext.encode("ascii")
            if (
                existing.ciphertext_sha256 != ciphertext_sha256
                or existing.size_bytes != len(payload)
                or not hmac.compare_digest(existing_payload, payload)
            ):
                raise EventStoreError("encrypted event object conflicts with existing value")

    async def get(self, object_key: str, *, tenant_id: str) -> str:
        """Read one tenant-owned encrypted envelope through the RLS transaction."""

        _validate_tenant_id(tenant_id)
        _validate_object_key(object_key)
        async with self._database.tenant_transaction(tenant_id) as session:
            existing = await session.scalar(
                select(EventObject).where(
                    EventObject.tenant_id == tenant_id,
                    EventObject.object_key == object_key,
                )
            )
            if existing is None:
                raise EventStoreError("encrypted event object is unavailable")
            payload = existing.ciphertext.encode("ascii")
            if len(payload) != existing.size_bytes or len(payload) > self._max_object_bytes:
                raise EventStoreError("encrypted event object size is invalid")
            if hashlib.sha256(payload).hexdigest() != existing.ciphertext_sha256:
                raise EventStoreError("encrypted event object digest does not match")
            return existing.ciphertext


def _validated_payload(
    object_key: str,
    ciphertext: str,
    ciphertext_sha256: str,
    *,
    max_object_bytes: int,
) -> bytes:
    _validate_object_key(object_key)
    if _DIGEST.fullmatch(ciphertext_sha256) is None:
        raise EventStoreError("encrypted event object digest is invalid")
    if object_key.rsplit("/", maxsplit=1)[-1] != ciphertext_sha256:
        raise EventStoreError("encrypted event object key and digest differ")
    try:
        payload = ciphertext.encode("ascii")
    except UnicodeEncodeError:
        raise EventStoreError("encrypted event object encoding is invalid") from None
    if not payload or len(payload) > max_object_bytes:
        raise EventStoreError("encrypted event object size is invalid")
    actual_digest = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(actual_digest, ciphertext_sha256):
        raise EventStoreError("encrypted event object digest does not match")
    if not ciphertext.startswith("v1."):
        raise EventStoreError("encrypted event object envelope is unsupported")
    return payload


def _validate_object_key(object_key: str) -> None:
    if _OBJECT_KEY.fullmatch(object_key) is None:
        raise EventStoreError("encrypted event object key is invalid")


def _validate_tenant_id(tenant_id: str) -> None:
    if not tenant_id or len(tenant_id) > 64:
        raise EventStoreError("tenant_id must contain 1..64 characters")


def _redis_bytes(value: object, max_object_bytes: int) -> bytes:
    if value is None:
        raise EventStoreError("encrypted event object is unavailable")
    if isinstance(value, str):
        try:
            payload = value.encode("ascii")
        except UnicodeEncodeError:
            raise EventStoreError("encrypted event object encoding is invalid") from None
    elif isinstance(value, bytes):
        payload = value
    else:
        raise EventStoreError("encrypted event object type is invalid")
    if not payload or len(payload) > max_object_bytes:
        raise EventStoreError("encrypted event object size is invalid")
    return payload
