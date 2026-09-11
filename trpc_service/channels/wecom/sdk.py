"""WeCom SDK facade — the only module that imports the ``aibot`` package.

External code interacts with :class:`WeComClient` (a Protocol) and
:func:`create_wecom_client` (the factory).  The real SDK types are
re-exported under private names so that tests can monkeypatch them
without importing ``aibot``.
"""

from __future__ import annotations

import logging
import asyncio
from typing import Any, Callable, Protocol, runtime_checkable

from trpc_service.channels.delivery import ChannelSendError
from .settings import WeComSettings

try:
    from aibot import WSClient as _WSClient
    from aibot import WSClientOptions as _WSClientOptions
except ImportError:
    _WSClient = None  # type: ignore[assignment,misc]
    _WSClientOptions = None  # type: ignore[assignment,misc]


class _SafeWeComLogger:
    """SDK-facing logger that never forwards original messages or args.

    The SDK's ``DefaultLogger`` unconditionally prints raw frames (e.g.
    ``ws.py`` logs ``json.dumps(body)`` at DEBUG, ``message_handler.py``
    truncates frames into WARN text), which would leak message bodies,
    external user IDs, message IDs, ``response_url`` and credentials.
    This logger satisfies the SDK ``Logger`` protocol but:

    - ``debug``/``info`` are dropped entirely;
    - ``warn``/``error`` emit fixed category text only — never the input;
    - no state (frames, messages, credentials) is retained.
    """

    def debug(self, message: str, *args: Any) -> None:
        return None

    def info(self, message: str, *args: Any) -> None:
        return None

    def warn(self, message: str, *args: Any) -> None:
        logging.getLogger(__name__).warning("WeCom SDK reported a warning (channel=wecom)")

    def error(self, message: str, *args: Any) -> None:
        logging.getLogger(__name__).error("WeCom SDK reported an error (channel=wecom)")


@runtime_checkable
class WeComClient(Protocol):
    """Protocol for the WeCom SDK facade.

    Implementations wrap the real SDK or a test fake behind a uniform
    async interface.
    """

    async def on_text(self, handler: Callable[[dict[str, Any]], Any]) -> None:
        """Register an async handler for incoming IM message frames."""
        ...

    async def on_authenticated(self, handler: Callable[[], Any]) -> None:
        """Register a callback for when the SDK connection is authenticated."""
        ...

    async def connect(self) -> None:
        """Establish the long-lived connection."""
        ...

    async def close(self) -> None:
        """Close the connection.  Must be idempotent."""
        ...

    async def reply_stream(
        self,
        frame: dict[str, Any],
        stream_id: str,
        text: str,
        finished: bool = False,
    ) -> Any:
        """Send a streaming text reply chunk."""
        ...


class FacadeClient:
    """Concrete facade that delegates to the real SDK ``WSClient``."""

    def __init__(self, sdk_client: Any) -> None:
        self._sdk = sdk_client
        self._closed = False

    async def on_text(self, handler: Callable[[dict[str, Any]], Any]) -> None:
        # The SDK emits this generic event for text and every media kind.
        # The service locally rejects media before ingress; subscribing only
        # to ``message.text`` would silently skip that safety reply.
        self._sdk.on("message", handler)

    async def on_authenticated(self, handler: Callable[[], Any]) -> None:
        self._sdk.on("authenticated", handler)

    async def connect(self) -> None:
        await self._sdk.connect()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._sdk.disconnect()

    async def reply_stream(
        self,
        frame: dict[str, Any],
        stream_id: str,
        text: str,
        finished: bool = False,
    ) -> Any:
        try:
            return await self._sdk.reply_stream(
                frame,
                stream_id=stream_id,
                content=text,
                finish=finished,
            )
        except asyncio.CancelledError:
            raise
        except ChannelSendError:
            raise
        except Exception:
            # The SDK call did not acknowledge the write; do not expose its
            # body/URL/error text to service code or logs.
            raise ChannelSendError(sent=False) from None


def create_wecom_client(settings: WeComSettings) -> WeComClient:
    """Create a :class:`FacadeClient` backed by the real SDK.

    This is the **only** place that constructs ``WSClient`` / ``WSClientOptions``.
    """
    if _WSClient is None or _WSClientOptions is None:
        raise RuntimeError("WeCom SDK is not installed. Install wecom-aibot-python-sdk.")
    # logger must be injected explicitly: without it the SDK falls back to
    # DefaultLogger, which prints raw frames (payload/secret leakage).
    options = _WSClientOptions(
        bot_id=settings.bot_id,
        secret=settings.secret,
        logger=_SafeWeComLogger(),
    )
    sdk_client = _WSClient(options)
    return FacadeClient(sdk_client)


__all__ = [
    "FacadeClient",
    "WeComClient",
    "create_wecom_client",
]
