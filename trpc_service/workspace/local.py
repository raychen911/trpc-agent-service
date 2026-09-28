"""Tenant-isolated local filesystem workspace implementation."""

import asyncio
import fcntl
import os
from pathlib import Path, PurePosixPath
import re
import shutil
from threading import Lock
import time
from typing import BinaryIO

from trpc_service.tenant.context import TenantContext
from trpc_service.workspace.contracts import WorkspaceHandle, WorkspaceKind, WorkspaceProvider

_WORKSPACE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_WORKSPACE_LOCK_FILE = ".workspace.lock"
_CLEANUP_LOCK_FILE = ".workspace-cleanup.lock"


class LocalWorkspaceProvider(WorkspaceProvider):
    """Persist work files beneath a tenant and Agent scoped local root."""

    def __init__(
        self,
        root: Path,
        *,
        retention_seconds: int = 7 * 24 * 60 * 60,
        cleanup_interval_seconds: int = 5 * 60,
    ) -> None:
        if retention_seconds <= 0 or cleanup_interval_seconds < 0:
            raise ValueError("workspace retention and cleanup interval are invalid")
        self._root = root.expanduser().resolve()
        self._retention_seconds = retention_seconds
        self._cleanup_interval_seconds = cleanup_interval_seconds
        self._last_cleanup = 0.0
        self._cleanup_lock = asyncio.Lock()
        self._lease_guard = Lock()
        self._active_leases: dict[str, BinaryIO] = {}

    async def acquire(self, context: TenantContext, workspace_id: str) -> WorkspaceHandle:
        """Create and return one deterministic, tenant-isolated directory."""

        if _WORKSPACE_ID.fullmatch(workspace_id) is None:
            raise ValueError("workspace ID contains unsupported characters")
        await self.cleanup_expired()
        location = (self._root / str(context.tenant_id) / str(context.agent_app_id) /
                    workspace_id).resolve()
        self._require_beneath(location, self._root, label="workspace")
        acquire_task = asyncio.create_task(asyncio.to_thread(self._acquire_directory, location))
        try:
            # Shield the thread Future: cancelling the caller cannot stop a
            # filesystem thread that may already have acquired its flock.
            await asyncio.shield(acquire_task)
        except asyncio.CancelledError:
            try:
                await acquire_task
            except Exception:
                # Acquisition failed before owning a lease, but cancellation
                # remains the caller-visible outcome.
                pass
            else:
                # Never strand a request lease when no Handle was returned.
                await asyncio.shield(asyncio.to_thread(self._release_directory, location))
            raise
        return WorkspaceHandle(
            tenant_id=context.tenant_id,
            agent_app_id=context.agent_app_id,
            workspace_id=workspace_id,
            kind=WorkspaceKind.LOCAL,
            location=str(location),
        )

    async def release(self, handle: WorkspaceHandle) -> None:
        """Release a handle while retaining local data for recovery and audit."""

        await asyncio.to_thread(
            self._release_directory,
            self._validated_workspace_root(handle),
        )

    async def list_files(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        *,
        max_results: int,
        max_entries: int,
    ) -> tuple[str, ...]:
        """Scan a bounded number of local entries outside the event loop."""

        if max_results < 1 or max_entries < max_results:
            raise ValueError("workspace listing limits are invalid")
        return await asyncio.to_thread(
            self._list_files,
            handle,
            relative_path,
            max_results,
            max_entries,
        )

    async def read_text(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        *,
        max_bytes: int,
    ) -> str:
        """Read at most max_bytes plus one sentinel byte outside the event loop."""

        if max_bytes < 1:
            raise ValueError("workspace read limit is invalid")
        return await asyncio.to_thread(self._read_text, handle, relative_path, max_bytes)

    async def cleanup_expired(self) -> int:
        """Opportunistically delete bounded stale request directories."""

        monotonic_now = time.monotonic()
        if monotonic_now - self._last_cleanup < self._cleanup_interval_seconds:
            return 0
        async with self._cleanup_lock:
            monotonic_now = time.monotonic()
            if monotonic_now - self._last_cleanup < self._cleanup_interval_seconds:
                return 0
            self._last_cleanup = monotonic_now
            return await asyncio.to_thread(self._cleanup_expired, time.time())

    def resolve_path(self, handle: WorkspaceHandle, relative_path: str) -> Path:
        """Resolve a user path without allowing absolute or traversal access."""

        candidate = PurePosixPath(relative_path)
        if (not relative_path.strip() or candidate.is_absolute() or ".." in candidate.parts
                or "\x00" in relative_path or _WORKSPACE_LOCK_FILE in candidate.parts):
            raise ValueError("workspace path must be a safe relative path")
        workspace_root = self._validated_workspace_root(handle)
        resolved = workspace_root.joinpath(*candidate.parts).resolve()
        self._require_beneath(resolved, workspace_root, label="workspace path")
        return resolved

    def _validated_workspace_root(self, handle: WorkspaceHandle) -> Path:
        if handle.kind is not WorkspaceKind.LOCAL:
            raise ValueError("local provider requires a local workspace handle")
        expected = (self._root / str(handle.tenant_id) / str(handle.agent_app_id) /
                    handle.workspace_id).resolve()
        actual = Path(handle.location).resolve()
        if actual != expected:
            raise ValueError("workspace handle is outside its tenant scope")
        self._require_beneath(actual, self._root, label="workspace")
        return actual

    def _list_files(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        max_results: int,
        max_entries: int,
    ) -> tuple[str, ...]:
        workspace_root = self._validated_workspace_root(handle)
        target = self.resolve_path(handle, relative_path)
        if not target.exists():
            raise LookupError("workspace path does not exist")
        if not target.is_dir():
            raise ValueError("workspace.list path must be a directory")
        files: list[str] = []
        scanned = 0
        pending = [target]
        while pending and len(files) < max_results:
            directory = pending.pop()
            for candidate in directory.iterdir():
                scanned += 1
                if scanned > max_entries:
                    raise ValueError("workspace directory exceeds the scan limit")
                resolved = candidate.resolve()
                self._require_beneath(resolved, workspace_root, label="workspace path")
                if candidate.name == _WORKSPACE_LOCK_FILE:
                    continue
                if candidate.is_symlink():
                    if resolved.is_file():
                        files.append(candidate.relative_to(workspace_root).as_posix())
                    continue
                if candidate.is_dir():
                    pending.append(candidate)
                elif candidate.is_file():
                    files.append(candidate.relative_to(workspace_root).as_posix())
                if len(files) >= max_results:
                    break
        return tuple(sorted(files))

    def _read_text(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        max_bytes: int,
    ) -> str:
        target = self.resolve_path(handle, relative_path)
        if not target.is_file():
            raise LookupError("workspace file does not exist")
        with target.open("rb") as stream:
            content = stream.read(max_bytes + 1)
        if len(content) > max_bytes:
            raise ValueError("workspace file exceeds the read limit")
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("workspace.read supports UTF-8 text files only") from error

    def _cleanup_expired(self, wall_clock: float) -> int:
        if not self._root.exists():
            return 0
        with (self._root / _CLEANUP_LOCK_FILE).open("a+b") as cleanup_lock:
            # A root-wide lock coordinates independent local WorkNode processes.
            # Active request directories also hold their own exclusive lease.
            fcntl.flock(cleanup_lock, fcntl.LOCK_EX)
            return self._cleanup_expired_locked(wall_clock)

    def _cleanup_expired_locked(self, wall_clock: float) -> int:
        cutoff = wall_clock - self._retention_seconds
        deleted = 0
        scanned = 0
        # The bounded three-level walk mirrors Tenant/Agent/Request exactly and
        # never follows symlinks or recursively scans request contents.
        for tenant in self._root.iterdir():
            scanned += 1
            if scanned > 10_000:
                break
            if tenant.is_symlink() or not tenant.is_dir():
                continue
            for agent in tenant.iterdir():
                scanned += 1
                if scanned > 10_000:
                    return deleted
                if agent.is_symlink() or not agent.is_dir():
                    continue
                for workspace in agent.iterdir():
                    scanned += 1
                    if scanned > 100_000 or deleted >= 10_000:
                        return deleted
                    if workspace.is_symlink() or not workspace.is_dir():
                        continue
                    if self._remove_if_expired(workspace, cutoff):
                        deleted += 1
        return deleted

    def _acquire_directory(self, location: Path) -> None:
        self._root.mkdir(parents=True, exist_ok=True, mode=0o750)
        with (self._root / _CLEANUP_LOCK_FILE).open("a+b") as cleanup_lock:
            fcntl.flock(cleanup_lock, fcntl.LOCK_SH)
            location.mkdir(parents=True, exist_ok=True, mode=0o750)
            # Standard directories keep staged inputs, generated outputs and
            # disposable files separate for future container/sandbox adapters.
            for directory in ("inputs", "outputs", "tmp"):
                (location / directory).mkdir(exist_ok=True, mode=0o750)
            lease = (location / _WORKSPACE_LOCK_FILE).open("a+b")
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                lease.close()
                raise RuntimeError("workspace is already active in another WorkNode") from error
            try:
                with self._lease_guard:
                    if str(location) in self._active_leases:
                        raise RuntimeError("workspace is already active in this WorkNode")
                    self._active_leases[str(location)] = lease
                # Refresh activity before releasing the root-wide shared lock.
                os.utime(location, None)
            except Exception:
                with self._lease_guard:
                    self._active_leases.pop(str(location), None)
                lease.close()
                raise

    def _release_directory(self, workspace_root: Path) -> None:
        with self._lease_guard:
            lease = self._active_leases.pop(str(workspace_root), None)
        # Retain data for failover and make the end of execution the TTL clock.
        if workspace_root.exists():
            os.utime(workspace_root, None)
        if lease is not None:
            fcntl.flock(lease, fcntl.LOCK_UN)
            lease.close()

    def _remove_if_expired(self, workspace: Path, cutoff: float) -> bool:
        try:
            if workspace.stat().st_mtime >= cutoff:
                return False
            lease = (workspace / _WORKSPACE_LOCK_FILE).open("a+b")
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lease.close()
                return False
            try:
                # Recheck after locking because release refreshes mtime before
                # dropping the active lease.
                if workspace.stat().st_mtime >= cutoff:
                    return False
                self._require_beneath(workspace.resolve(), self._root, label="workspace")
                shutil.rmtree(workspace)
                return True
            finally:
                lease.close()
        except FileNotFoundError:
            # Another local Worker may have reclaimed the directory first.
            return False

    @staticmethod
    def _require_beneath(path: Path, root: Path, *, label: str) -> None:
        try:
            path.relative_to(root)
        except ValueError as error:
            raise ValueError(f"{label} escapes its configured root") from error
