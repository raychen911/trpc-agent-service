"""Small, safe primitives for external IM delivery.

The Worker execution is deliberately outside this module.  A delivery retry
only repeats an SDK write that the facade proves did not send any bytes.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import AsyncIterator

from trpc_service.transport.models import WorkerErrorCode


class ChannelSendError(Exception):
    """A fixed SDK-facade send failure.

    ``sent`` is intentionally the only detail.  It prevents retrying a write
    that might already be visible to an IM user and never carries SDK text.
    """

    def __init__(self, *, sent: bool) -> None:
        super().__init__("channel send failed")
        self.sent = sent


async def send_with_retry(send: Callable[[], Awaitable[object]]) -> bool:
    """Try an unsent SDK write up to three times, without swallowing cancel.

    Returns whether a write completed.  A ``sent=True`` error is terminal,
    because a second send could duplicate a user-visible reply.
    """
    for attempt in range(3):
        try:
            await send()
            return True
        except asyncio.CancelledError:
            raise
        except ChannelSendError as exc:
            if exc.sent or attempt == 2:
                return False
            await asyncio.sleep(0.1 * (attempt + 1))
    return False


class ChannelExecutionStream(AsyncIterator[object]):
    """Internal public-event stream carrying one already-created task identity.

    The identity becomes available only after admission and task creation;
    external services use it after their *SDK* terminal write for audit.  It
    is intentionally not part of the Console event schema.
    """

    def __init__(self, iterator: AsyncIterator[object]) -> None:
        self._iterator = iterator
        self._nested_iterator: AsyncIterator[object] | None = None
        self.task: object | None = None
        self.delivery_events: str = "all"

    def __aiter__(self) -> "ChannelExecutionStream":
        return self

    async def __anext__(self) -> object:
        return await anext(self._iterator)

    def set_nested_iterator(self, iterator: AsyncIterator[object] | None) -> None:
        """Register the active Worker stream so consumer cancellation closes it now."""
        self._nested_iterator = iterator

    async def aclose(self) -> None:
        nested = self._nested_iterator
        self._nested_iterator = None
        if nested is not None:
            close_nested = getattr(nested, "aclose", None)
            if close_nested is not None:
                await close_nested()
        close = getattr(self._iterator, "aclose", None)
        if close is not None:
            await close()


async def record_terminal_delivery(
    ingress,
    execution: ChannelExecutionStream,
    category: str,
    *,
    failure_categories: frozenset[str],
) -> None:
    """Append the one SDK-terminal delivery audit fact through the ingress.

    Shared by the external IM services.  ``failure_categories`` names THIS
    platform's mid-chain SDK-write failure categories, which map to the
    fixed ``CHANNEL_DELIVERY_FAILED`` code.  Indeterminate categories — a
    broken Worker stream or a missing terminal — prove nothing about
    platform delivery, so they never become a delivered (or failed) audit
    row.  The recorder is an OPTIONAL ingress capability: adapters must
    tolerate an ingress without it.
    """
    recorder = getattr(ingress, "record_external_delivery", None)
    if recorder is None:
        return
    code = WorkerErrorCode.CHANNEL_DELIVERY_FAILED if category in failure_categories else None
    if code is None and category not in {"done", "error"}:
        return
    result = recorder(execution, code)
    if inspect.isawaitable(result):
        await result


class SdkReplySender:
    """One IM reply chain's SDK write policy (shared by all platforms).

    - Exception normalization: unexpected SDK errors become the fixed
      ``ChannelSendError(sent=False)``; cancellation and explicit
      ``ChannelSendError`` pass through unchanged.
    - Retry: only the FIRST write retries (bounded ``send_with_retry``);
      every later write fires exactly once — a partially delivered reply is
      never replayed, so the platform never sees duplicate messages.
    """

    def __init__(self) -> None:
        self._sent = False

    @property
    def sent(self) -> bool:
        """Whether at least one write already reached the platform."""
        return self._sent

    async def write(self, operation: Callable[..., Awaitable[object]], *args, **kwargs) -> bool:
        """Deliver one SDK write (positional and keyword arguments pass
        through to the SDK operation); returns whether it completed."""

        async def _once() -> None:
            try:
                await operation(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except ChannelSendError:
                raise
            except Exception:
                raise ChannelSendError(sent=False) from None

        if self._sent:
            try:
                await _once()
            except ChannelSendError:
                return False
            return True
        if not await send_with_retry(_once):
            return False
        self._sent = True
        return True


__all__ = [
    "ChannelExecutionStream",
    "ChannelSendError",
    "SdkReplySender",
    "record_terminal_delivery",
    "send_with_retry",
]
