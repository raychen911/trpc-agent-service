"""Enterprise WeCom intelligent-bot WebSocket connection."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

LOGGER = logging.getLogger(__name__)


class WeComLongConnection:
    """Own one official WeCom AI-bot client for a tenant."""

    def __init__(
        self,
        bot_id: str,
        secret: str,
        tenant_id: str,
        event_callback: Callable[[str, Mapping[str, Any], Any], Awaitable[bool | dict[str, Any]]],
        *,
        ws_url: str = "wss://openws.work.weixin.qq.com",
    ) -> None:
        self.bot_id = bot_id
        self.secret = secret
        self.tenant_id = tenant_id
        self.event_callback = event_callback
        self.ws_url = ws_url
        self.client: Any | None = None
        self._task: asyncio.Task[Any] | None = None
        self._state = "stopped"
        self._error: str | None = None

    @property
    def status(self) -> dict[str, Any]:
        return {"tenant_id": self.tenant_id, "bot_id": self.bot_id, "state": self._state, "error": self._error}

    async def start(self) -> None:
        if self._task is not None:
            return
        try:
            from aibot import WSClient, WSClientOptions
        except ImportError as exc:
            raise RuntimeError("WeCom long connection requires wecom-aibot-python-sdk") from exc
        self.client = WSClient(WSClientOptions(bot_id=self.bot_id, secret=self.secret, ws_url=self.ws_url))
        self.client.on("message", self._on_message)
        self.client.on("event", self._on_event)
        self.client.on("error", self._on_error)
        self._state = "starting"
        await self.client.connect()
        self._state = "connected"
        self._task = asyncio.current_task()

    async def run(self) -> None:
        await self.start()
        await asyncio.Event().wait()

    async def stop(self) -> None:
        client, task = self.client, self._task
        self._task = None
        if client is not None:
            client.disconnect()
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._state = "stopped"

    def _on_error(self, error: BaseException) -> None:
        self._state = "failed"
        self._error = type(error).__name__
        LOGGER.error("WeCom long connection failed for tenant %s: %s", self.tenant_id, error)

    def _on_message(self, frame: Mapping[str, Any]) -> None:
        asyncio.create_task(self.event_callback(self.tenant_id, frame, self))

    def _on_event(self, frame: Mapping[str, Any]) -> None:
        asyncio.create_task(self.event_callback(self.tenant_id, frame, self))
