"""Feishu official-SDK long-connection lifecycle adapter."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import Awaitable, Callable, Mapping
from queue import Empty, Full, Queue
from typing import Any

LOGGER = logging.getLogger(__name__)


class FeishuLongConnection:
    """Run the blocking official SDK client outside the ASGI event loop."""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        tenant_id: str,
        event_callback: Callable[[str, Mapping[str, Any]], Awaitable[bool | dict[str, Any]]],
        *,
        domain: str = "https://open.feishu.cn",
        event_buffer_size: int = 1000,
        log_level: str = "WARNING",
    ) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_id = tenant_id
        self.event_callback = event_callback
        self.domain = domain.rstrip("/")
        self.log_level = log_level.upper()
        self._event_buffer: Queue[tuple[str, Mapping[str, Any]]] = Queue(maxsize=event_buffer_size)
        self._event_available: asyncio.Event | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._consumer_stopping = False
        self._asyncio_loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._client: Any | None = None
        self._sdk_loop: asyncio.AbstractEventLoop | None = None
        self._stop_requested = False
        self._state = "stopped"
        self._error: str | None = None

    @property
    def status(self) -> dict[str, Any]:
        state = self._state
        if state in {"starting", "running"} and getattr(self._client, "_conn", None) is not None:
            state = "connected"
        return {
            "tenant_id": self.tenant_id,
            "app_id": self.app_id,
            "state": state,
            "error": self._error,
            "buffer_depth": self._event_buffer.qsize(),
        }

    def start(self, asyncio_loop: asyncio.AbstractEventLoop) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._asyncio_loop = asyncio_loop
        self._start_event_consumer(asyncio_loop)
        self._state = "starting"
        self._error = None
        self._stop_requested = False
        self._thread = threading.Thread(
            target=self._run,
            name=f"feishu-ws-{self.tenant_id}",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        sdk_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(sdk_loop)
        self._sdk_loop = sdk_loop
        try:
            from lark_oapi.ws import client as sdk_client

            sdk_client.loop = sdk_loop
            self._client = self._build_client()
            if self._stop_requested:
                self._state = "stopped"
                return
            self._state = "running"
            self._client.start()
        except BaseException as exc:  # The SDK owns this daemon thread and its event loop.
            if self._stop_requested:
                self._state = "stopped"
                return
            self._state = "failed"
            self._error = type(exc).__name__
            LOGGER.exception("Feishu long connection stopped for tenant %s", self.tenant_id)
        finally:
            if not sdk_loop.is_running():
                sdk_loop.close()

    def stop(self, timeout: float = 3.0) -> None:
        """Best-effort shutdown for an SDK that exposes only a blocking start API."""
        thread = self._thread
        if thread is None or not thread.is_alive():
            self._state = "stopped"
            return
        self._stop_requested = True
        self._state = "stopping"
        client = self._client
        sdk_loop = self._sdk_loop
        if client is not None and sdk_loop is not None and sdk_loop.is_running():
            client._auto_reconnect = False
            try:
                future = asyncio.run_coroutine_threadsafe(self._shutdown_sdk_tasks(), sdk_loop)
                future.result(timeout=max(0.1, timeout / 2))
            except Exception:  # noqa: BLE001 - SDK has no public close API
                LOGGER.warning("Feishu long connection did not disconnect cleanly for tenant %s", self.tenant_id)
        thread.join(timeout=timeout)
        if thread.is_alive() and sdk_loop is not None and sdk_loop.is_running():
            sdk_loop.call_soon_threadsafe(sdk_loop.stop)
            thread.join(timeout=max(0.1, timeout / 2))
        if self._asyncio_loop is not None and self._asyncio_loop.is_running():
            try:
                future = asyncio.run_coroutine_threadsafe(self._stop_event_consumer(), self._asyncio_loop)
                future.result(timeout=timeout)
            except Exception:  # noqa: BLE001 - best effort during ASGI shutdown
                LOGGER.warning("Feishu event consumer did not stop cleanly for tenant %s", self.tenant_id)
        if not thread.is_alive():
            self._state = "stopped"

    def _start_event_consumer(self, asyncio_loop: asyncio.AbstractEventLoop) -> None:
        self._consumer_stopping = False
        self._event_available = asyncio.Event()
        self._consumer_task = asyncio_loop.create_task(self._consume_events())

    async def _stop_event_consumer(self) -> None:
        self._consumer_stopping = True
        if self._event_available is not None:
            self._event_available.set()
        if self._consumer_task is not None:
            await self._consumer_task

    async def _consume_events(self) -> None:
        if self._event_available is None:
            return
        while True:
            await self._event_available.wait()
            self._event_available.clear()
            while True:
                try:
                    tenant_id, payload = self._event_buffer.get_nowait()
                except Empty:
                    break
                try:
                    await self.event_callback(tenant_id, payload)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception("Feishu event processing failed for tenant %s", tenant_id)
                finally:
                    self._event_buffer.task_done()
            if self._consumer_stopping and self._event_buffer.empty():
                return

    async def _shutdown_sdk_tasks(self) -> None:
        current = asyncio.current_task()
        tasks = []
        select_tasks = []
        for task in asyncio.all_tasks():
            if task is current:
                continue
            name = getattr(task.get_coro(), "__name__", "")
            if name == "_select":
                select_tasks.append(task)
                continue
            task.cancel()
            tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._client is not None:
            await self._client._disconnect()
        for task in select_tasks:
            task.cancel()

    def _build_client(self) -> Any:
        try:
            import lark_oapi as lark
        except ImportError as exc:
            raise RuntimeError("Feishu websocket mode requires the lark-oapi package") from exc

        handler = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(self._handle_sdk_event)
            .build()
        )
        log_level = getattr(lark.LogLevel, self.log_level, lark.LogLevel.INFO)
        return lark.ws.Client(
            self.app_id,
            self.app_secret,
            event_handler=handler,
            log_level=log_level,
            domain=self.domain,
        )

    def _handle_sdk_event(self, event: Any) -> None:
        try:
            import lark_oapi as lark

            payload = json.loads(lark.JSON.marshal(event))
            if not isinstance(payload, dict):
                raise TypeError("SDK event is not a JSON object")
            loop = self._asyncio_loop
            event_available = self._event_available
            if loop is None or loop.is_closed() or event_available is None:
                raise RuntimeError("service event loop is unavailable")
            try:
                self._event_buffer.put_nowait((self.tenant_id, payload))
            except Full as exc:
                raise RuntimeError("Feishu inbound event buffer is full") from exc
            loop.call_soon_threadsafe(event_available.set)
        except Exception as exc:
            raise RuntimeError("Feishu long-connection event handling failed") from exc
