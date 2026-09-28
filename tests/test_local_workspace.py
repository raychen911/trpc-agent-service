import asyncio
import os
from pathlib import Path
from threading import Event
import time
from uuid import uuid4

import pytest

from trpc_service.tenant import TenantContext
from trpc_service.workspace import (
    LocalWorkspaceProvider,
    WorkspaceHandle,
    WorkspaceKind,
)


def _context() -> TenantContext:
    return TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )


@pytest.mark.anyio
async def test_local_workspace_is_scoped_by_tenant_agent_and_workspace(tmp_path: Path) -> None:
    provider = LocalWorkspaceProvider(tmp_path)
    context = _context()

    handle = await provider.acquire(context, "run-1")

    expected = tmp_path / str(context.tenant_id) / str(context.agent_app_id) / "run-1"
    assert Path(handle.location) == expected.resolve()
    assert expected.is_dir()
    assert handle.kind is WorkspaceKind.LOCAL


@pytest.mark.anyio
async def test_local_workspace_rejects_traversal_and_cross_scope_paths(tmp_path: Path) -> None:
    provider = LocalWorkspaceProvider(tmp_path)
    handle = await provider.acquire(_context(), "run-1")

    with pytest.raises(ValueError, match="workspace path"):
        provider.resolve_path(handle, "../../other-tenant/secret.txt")
    with pytest.raises(ValueError, match="workspace ID"):
        await provider.acquire(_context(), "../other-workspace")

    for unsafe_path in ("", "/absolute", "a/../b", "bad\x00path", ".workspace.lock"):
        with pytest.raises(ValueError, match="safe relative path"):
            provider.resolve_path(handle, unsafe_path)

    wrong_kind = WorkspaceHandle(
        tenant_id=handle.tenant_id,
        agent_app_id=handle.agent_app_id,
        workspace_id=handle.workspace_id,
        kind=WorkspaceKind.CONTAINER,
        location=handle.location,
    )
    with pytest.raises(ValueError, match="local workspace"):
        provider.resolve_path(wrong_kind, "inputs")

    forged = WorkspaceHandle(
        tenant_id=handle.tenant_id,
        agent_app_id=handle.agent_app_id,
        workspace_id=handle.workspace_id,
        kind=WorkspaceKind.LOCAL,
        location=str(tmp_path / "another-location"),
    )
    with pytest.raises(ValueError, match="tenant scope"):
        provider.resolve_path(forged, "inputs")


def test_local_workspace_rejects_invalid_retention_configuration(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="retention"):
        LocalWorkspaceProvider(tmp_path, retention_seconds=0)
    with pytest.raises(ValueError, match="retention"):
        LocalWorkspaceProvider(tmp_path, cleanup_interval_seconds=-1)


@pytest.mark.anyio
async def test_release_keeps_persistent_local_workspace_data(tmp_path: Path) -> None:
    provider = LocalWorkspaceProvider(tmp_path)
    handle = await provider.acquire(_context(), "run-1")
    file_path = provider.resolve_path(handle, "reports/result.txt")
    file_path.parent.mkdir(parents=True)
    file_path.write_text("done", encoding="utf-8")

    await provider.release(handle)

    assert file_path.read_text(encoding="utf-8") == "done"


@pytest.mark.anyio
async def test_local_worker_nodes_share_one_recoverable_request_workspace(tmp_path: Path, ) -> None:
    """Independent local Worker processes resolve the same request directory."""

    context = _context()
    worker_one = LocalWorkspaceProvider(tmp_path)
    worker_two = LocalWorkspaceProvider(tmp_path)

    first = await worker_one.acquire(context, context.request_id)
    output = worker_one.resolve_path(first, "outputs/result.txt")
    output.write_text("worker-one", encoding="utf-8")
    with pytest.raises(RuntimeError, match="another WorkNode"):
        await worker_two.acquire(context, context.request_id)
    await worker_one.release(first)
    recovered = await worker_two.acquire(context, context.request_id)

    assert recovered.location == first.location
    assert worker_two.resolve_path(recovered,
                                   "outputs/result.txt").read_text(encoding="utf-8") == "worker-one"
    assert all(
        worker_two.resolve_path(recovered, directory).is_dir()
        for directory in ("inputs", "outputs", "tmp"))
    await worker_two.release(recovered)


@pytest.mark.anyio
async def test_local_workspace_reclaims_expired_requests_without_touching_active_one(
        tmp_path: Path) -> None:
    provider = LocalWorkspaceProvider(
        tmp_path,
        retention_seconds=1,
        cleanup_interval_seconds=0,
    )
    context = _context()
    expired = await provider.acquire(context, "expired-request")
    expired_path = Path(expired.location)
    await provider.release(expired)
    old_time = time.time() - 10
    os.utime(expired_path, (old_time, old_time))

    active = await provider.acquire(context, "active-request")
    await provider.release(active)

    assert not expired_path.exists()
    assert Path(active.location).is_dir()


@pytest.mark.anyio
async def test_local_workspace_cleanup_skips_a_lease_held_by_another_worker(tmp_path: Path) -> None:
    worker_one = LocalWorkspaceProvider(
        tmp_path,
        retention_seconds=1,
        cleanup_interval_seconds=0,
    )
    worker_two = LocalWorkspaceProvider(
        tmp_path,
        retention_seconds=1,
        cleanup_interval_seconds=0,
    )
    handle = await worker_one.acquire(_context(), "active-request")
    active_path = Path(handle.location)
    old_time = time.time() - 10
    os.utime(active_path, (old_time, old_time))

    assert await worker_two.cleanup_expired() == 0
    assert active_path.is_dir()

    await worker_one.release(handle)


@pytest.mark.anyio
async def test_local_workspace_bounds_directory_scans(tmp_path: Path) -> None:
    provider = LocalWorkspaceProvider(tmp_path)
    handle = await provider.acquire(_context(), "run-1")
    root = Path(handle.location)
    for index in range(3):
        (root / f"entry-{index}").mkdir()

    with pytest.raises(ValueError, match="scan limit"):
        await provider.list_files(handle, ".", max_results=2, max_entries=2)

    with pytest.raises(ValueError, match="listing limits"):
        await provider.list_files(handle, ".", max_results=0, max_entries=1)
    with pytest.raises(ValueError, match="listing limits"):
        await provider.list_files(handle, ".", max_results=2, max_entries=1)
    with pytest.raises(ValueError, match="read limit"):
        await provider.read_text(handle, "inputs/file.txt", max_bytes=0)


@pytest.mark.anyio
async def test_local_workspace_lists_safe_symlinks_but_rejects_escape(tmp_path: Path) -> None:
    provider = LocalWorkspaceProvider(tmp_path)
    handle = await provider.acquire(_context(), "run-1")
    root = Path(handle.location)
    (root / "inputs" / "source.txt").write_text("source", encoding="utf-8")
    (root / "inputs" / "source-link.txt").symlink_to(root / "inputs" / "source.txt")
    (root / "inputs" / "directory-link").symlink_to(root / "outputs", target_is_directory=True)

    files = await provider.list_files(handle, "inputs", max_results=10, max_entries=10)

    assert files == ("inputs/source-link.txt", "inputs/source.txt")

    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    (root / "tmp" / "escape.txt").symlink_to(outside)
    with pytest.raises(ValueError, match="escapes"):
        await provider.list_files(handle, "tmp", max_results=10, max_entries=10)


@pytest.mark.anyio
async def test_local_workspace_cleanup_is_throttled_and_release_is_idempotent(
        tmp_path: Path) -> None:
    provider = LocalWorkspaceProvider(tmp_path, cleanup_interval_seconds=300)
    handle = await provider.acquire(_context(), "run-1")

    assert await provider.cleanup_expired() == 0
    await provider.release(handle)
    await provider.release(handle)


@pytest.mark.anyio
async def test_cancelled_acquire_does_not_strand_the_cross_worker_lease(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = LocalWorkspaceProvider(tmp_path)
    takeover = LocalWorkspaceProvider(tmp_path)
    context = _context()
    entered = Event()
    proceed = Event()
    original_acquire = provider._acquire_directory

    def delayed_acquire(location: Path) -> None:
        entered.set()
        proceed.wait(timeout=2)
        original_acquire(location)

    monkeypatch.setattr(provider, "_acquire_directory", delayed_acquire)
    task = asyncio.create_task(provider.acquire(context, "cancelled-request"))
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    proceed.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    recovered = await takeover.acquire(context, "cancelled-request")
    await takeover.release(recovered)
