"""Reference-counted cache for immutable tenant configuration revisions."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")
RevisionKey = tuple[str, str, int]


@dataclass(slots=True)
class _Entry(Generic[T]):
    value: T
    references: int = 0
    retired: bool = False


class RevisionRegistry(Generic[T]):
    """Keep old revisions alive only while in-flight turns reference them."""

    def __init__(self) -> None:
        self._entries: dict[RevisionKey, _Entry[T]] = {}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def use(self, key: RevisionKey, loader: Callable[[], Awaitable[T]]) -> AsyncIterator[T]:
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _Entry(await loader())
                self._entries[key] = entry
            if entry.retired:
                raise LookupError("configuration revision has been retired")
            entry.references += 1
        try:
            yield entry.value
        finally:
            async with self._lock:
                entry.references -= 1
                if entry.retired and entry.references == 0:
                    self._entries.pop(key, None)

    async def retire(self, key: RevisionKey) -> None:
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            entry.retired = True
            if entry.references == 0:
                self._entries.pop(key, None)

    async def size(self) -> int:
        async with self._lock:
            return len(self._entries)


__all__ = ["RevisionKey", "RevisionRegistry"]
