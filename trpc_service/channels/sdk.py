"""Small helpers for optional third-party IM SDK integrations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from threading import Thread
from typing import TypeVar

T = TypeVar("T")


def run_async(factory: Callable[[], Awaitable[T]]) -> T:
    """Run an SDK coroutine from both sync code and an active event loop."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())

    result: list[T] = []
    error: list[BaseException] = []

    def worker() -> None:
        try:
            result.append(asyncio.run(factory()))
        except BaseException as exc:  # re-raise the SDK error in the caller
            error.append(exc)

    thread = Thread(target=worker, daemon=True)
    thread.start()
    thread.join()
    if error:
        raise error[0]
    return result[0]
