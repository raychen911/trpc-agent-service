"""Process lifecycle, drain signalling, and local event-loop heartbeats.

The state files are deliberately local to one container/process namespace.
They let Kubernetes exec probes distinguish a live event loop from a process
whose PID still exists, and let a ``preStop`` hook request drain without
opening another network listener.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time
from pathlib import Path


class ProcessLifecycle:
    """Coordinate cooperative shutdown for one runtime role."""

    def __init__(
        self,
        role: str,
        state_dir: Path,
        *,
        heartbeat_interval_seconds: float = 5.0,
    ) -> None:
        if not role:
            raise ValueError("role must not be empty")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat interval must be positive")
        self.role = role
        self.state_dir = state_dir
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.stop_event = asyncio.Event()
        self._heartbeat_task: asyncio.Task[None] | None = None

    @property
    def heartbeat_path(self) -> Path:
        return self.state_dir / f"{self.role}.heartbeat"

    @property
    def drain_path(self) -> Path:
        return self.state_dir / f"{self.role}.draining"

    async def start(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.drain_path.unlink(missing_ok=True)
        self._install_signal_handlers()
        await self._write_heartbeat()
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"lifecycle-heartbeat:{self.role}"
        )

    async def close(self) -> None:
        self.stop_event.set()
        self.drain_path.touch(exist_ok=True)
        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def request_stop(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.drain_path.touch(exist_ok=True)
        self.stop_event.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.add_signal_handler(sig, self.request_stop)

    async def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            if self.drain_path.exists():
                self.stop_event.set()
                return
            await self._write_heartbeat()
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=self.heartbeat_interval_seconds
                )
            except TimeoutError:
                continue

    async def _write_heartbeat(self) -> None:
        # A tiny synchronous replace is preferable to leaving partial content
        # that a concurrently executing probe could misread.
        temporary = self.heartbeat_path.with_suffix(f".tmp.{os.getpid()}")
        temporary.write_text(f"{time.time():.6f}\n", encoding="ascii")
        temporary.replace(self.heartbeat_path)


def request_drain(role: str, state_dir: Path) -> Path:
    """Request drain from a preStop helper process."""

    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / f"{role}.draining"
    path.touch(exist_ok=True)
    return path


def is_process_live(role: str, state_dir: Path, *, max_age_seconds: float) -> bool:
    """Return whether the role's event-loop heartbeat is recent."""

    if max_age_seconds <= 0:
        return False
    path = state_dir / f"{role}.heartbeat"
    try:
        age = time.time() - path.stat().st_mtime
    except OSError:
        return False
    return 0 <= age <= max_age_seconds


def is_process_ready(role: str, state_dir: Path) -> bool:
    """A draining role must stop receiving traffic or new work."""

    return not (state_dir / f"{role}.draining").exists()


__all__ = [
    "ProcessLifecycle",
    "is_process_live",
    "is_process_ready",
    "request_drain",
]
