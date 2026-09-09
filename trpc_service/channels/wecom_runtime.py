"""WeCom intelligent-bot connection lifecycle with an injectable fake client."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any
from typing import Protocol

from trpc_service.storage import SessionExecutionGuard
from trpc_service.channels.base import ChannelTransportError

FrameHandler = Callable[[dict[str, Any]], Awaitable[None]]


class WeComClient(Protocol):

    async def connect(self, handler: FrameHandler) -> None:
        ...

    async def reply_stream(self, request_id: str, text: str, finish: bool) -> str:
        ...

    async def send_message(self, conversation_id: str, text: str) -> str:
        ...

    async def download_file(self, url: str, aes_key: str = "") -> tuple[bytes, str]:
        ...

    async def close(self) -> None:
        ...


class FakeWeComClient:
    """No-network client used by unit, component and takeover tests."""

    def __init__(self) -> None:
        self.handler: FrameHandler | None = None
        self.sent: list[tuple[str, str]] = []
        self.connected = False
        self.files: dict[str, tuple[bytes, str]] = {}
        self.downloads: list[tuple[str, str]] = []
        self.send_error: ChannelTransportError | None = None

    async def connect(self, handler: FrameHandler) -> None:
        self.handler = handler
        self.connected = True

    async def emit(self, frame: dict[str, Any]) -> None:
        if not self.handler:
            raise RuntimeError("fake WeCom client is not connected")
        await self.handler(frame)

    async def reply_stream(self, request_id: str, text: str, finish: bool) -> str:
        if self.send_error:
            error, self.send_error = self.send_error, None
            raise error
        self.sent.append((request_id, text))
        return f"fake-stream:{len(self.sent)}:{int(finish)}"

    async def send_message(self, conversation_id: str, text: str) -> str:
        if self.send_error:
            error, self.send_error = self.send_error, None
            raise error
        self.sent.append((conversation_id, text))
        return f"fake-message:{len(self.sent)}"

    async def download_file(self, url: str, aes_key: str = "") -> tuple[bytes, str]:
        self.downloads.append((url, aes_key))
        return self.files.get(url, (f"fake:{url}".encode(), "wecom-file.bin"))

    async def close(self) -> None:
        self.connected = False


class AibotWeComClient:
    """Thin adapter over WeComTeam ``aibot.WSClient``; import is optional."""

    def __init__(self, bot_id: str, secret: str, frame_ttl_seconds: float = 240) -> None:
        try:
            from aibot import WSClient
            from aibot import WSClientOptions
            from aibot import generate_req_id
        except ImportError as error:
            raise RuntimeError("install WeCom support with: pip install -e '.[wecom]'") from error
        self._generate_req_id = generate_req_id
        self._client = WSClient(WSClientOptions(bot_id=bot_id, secret=secret))
        required = ("connect", "disconnect", "reply_stream", "send_message", "download_file", "on")
        missing = [name for name in required if not callable(getattr(self._client, name, None))]
        if missing:
            raise RuntimeError(f"unsupported WeCom SDK: missing methods {', '.join(missing)}")
        self._frames: dict[str, tuple[dict[str, Any], float]] = {}
        self._frame_ttl_seconds = frame_ttl_seconds
        self._authenticated = asyncio.Event()
        self._client.on("authenticated", lambda: self._authenticated.set())
        self._client.on("disconnected", lambda *args: self._authenticated.clear())
        self._streams: dict[str, str] = {}
        self._handler_registered = False

    @staticmethod
    def _request_id(frame: dict[str, Any]) -> str:
        headers = frame.get("headers") or {}
        return str(
            frame.get("req_id") or frame.get("request_id") or headers.get("req_id") or headers.get("request_id") or "")

    async def connect(self, handler: FrameHandler) -> None:

        async def on_message(frame: dict[str, Any]) -> None:
            now = asyncio.get_running_loop().time()
            expired = [key for key, item in self._frames.items() if item[1] <= now]
            for key in expired:
                self._frames.pop(key, None)
                self._streams.pop(key, None)
            request_id = self._request_id(frame)
            if request_id:
                deadline = asyncio.get_running_loop().time() + self._frame_ttl_seconds
                self._frames[request_id] = (frame, deadline)
            await handler(frame)

        if not self._handler_registered:
            self._client.on("message", on_message)
            self._handler_registered = True
        await self._client.connect()

    async def reply_stream(self, request_id: str, text: str, finish: bool) -> str:
        item = self._frames.get(request_id)
        if item is None or item[1] <= asyncio.get_running_loop().time():
            self._frames.pop(request_id, None)
            raise KeyError("WeCom request frame expired; use active send instead")
        frame = item[0]
        stream_id = self._streams.setdefault(request_id, self._generate_req_id("stream"))
        response = await self._client.reply_stream(frame, stream_id, text, finish)
        if finish:
            self._frames.pop(request_id, None)
            self._streams.pop(request_id, None)
        return self._request_id(response) if isinstance(response, dict) else str(response)

    async def send_message(self, conversation_id: str, text: str) -> str:
        response = await self._client.send_message(conversation_id, {
            "msgtype": "markdown",
            "markdown": {
                "content": text
            }
        })
        return self._request_id(response) if isinstance(response, dict) else str(response)

    async def wait_authenticated(self, timeout_seconds: float = 15) -> None:
        await asyncio.wait_for(self._authenticated.wait(), timeout=timeout_seconds)

    async def download_file(self, url: str, aes_key: str = "") -> tuple[bytes, str]:
        result = await self._client.download_file(url, aes_key or None)
        if isinstance(result, tuple) and len(result) == 2:
            content, name = result
            return bytes(content), str(name or "")
        if isinstance(result, dict) and "buffer" in result:
            return bytes(result["buffer"]), str(result.get("filename") or "")
        raise RuntimeError("unsupported WeCom SDK download_file result")

    async def close(self) -> None:
        self._authenticated.clear()
        self._frames.clear()
        self._streams.clear()
        result = self._client.disconnect()
        if inspect.isawaitable(result):
            await result


class WeComChannelRuntime:
    """Hold one binding lease while the SDK client owns the bot connection."""

    def __init__(self,
                 binding_id: str,
                 client: WeComClient,
                 guard: SessionExecutionGuard,
                 handler: FrameHandler,
                 *,
                 wait_timeout: float = 5) -> None:
        self._binding_id = binding_id
        self._client = client
        self._guard = guard
        self._handler = handler
        self._wait_timeout = wait_timeout
        self.owned = False

    async def run(self, stop: asyncio.Event) -> None:
        lease_key = f"wecom-binding:{self._binding_id}"
        async with self._guard.hold(lease_key, wait_timeout=self._wait_timeout, lease_seconds=30) as lease:
            tasks = []
            try:
                await self._client.connect(self._handler)
                authenticate = getattr(self._client, "wait_authenticated", None)
                if authenticate:
                    await authenticate()
                self.owned = True
                lost = asyncio.create_task(lease.lost.wait())
                stopped = asyncio.create_task(stop.wait())
                tasks = [lost, stopped]
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                lease.assert_owned(lease_key)
            finally:
                self.owned = False
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._client.close()

    async def close(self) -> None:
        await self._client.close()
