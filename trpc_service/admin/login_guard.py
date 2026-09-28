"""Bound unauthenticated login work before database access and password hashing."""

import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from time import monotonic
from typing import TypeVar

from fastapi import HTTPException

T = TypeVar("T")


async def password_work(operation: Callable[..., T], *args: str) -> T:
    """Keep the caller's capacity slot until the underlying KDF actually stops."""

    task = asyncio.create_task(asyncio.to_thread(operation, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Repeated cancellation must not release the caller's admission slot
        # while the uncancellable executor thread is still using KDF resources.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        raise


class LoginGuard:
    """Per-process admission plus bounded source counters; account locks remain SQL-owned."""

    def __init__(self, concurrency: int, per_minute: int) -> None:
        self._slots = asyncio.Semaphore(concurrency)
        self._per_minute = per_minute
        self._sources: OrderedDict[str, tuple[float, int]] = OrderedDict()

    @asynccontextmanager
    async def admit(self, source: str) -> AsyncIterator[None]:
        now = monotonic()
        since, count = self._sources.pop(source, (now, 0))
        if now - since >= 60:
            since, count = now, 0
        self._sources[source] = (since, count + 1)
        while len(self._sources) > 4096:
            self._sources.popitem(last=False)
        if count >= self._per_minute or self._slots.locked():
            raise HTTPException(status_code=429,
                                detail="login temporarily rate limited",
                                headers={"Retry-After": "60"})
        async with self._slots:
            yield
