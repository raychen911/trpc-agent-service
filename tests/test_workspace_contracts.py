from uuid import uuid4

import pytest

from trpc_service.tenant import TenantContext
from trpc_service.workspace import WorkspaceHandle, WorkspaceKind, WorkspaceProvider


class RecordingWorkspaceProvider(WorkspaceProvider):
    """Minimal adapter proving implementations can stay outside the Agent core."""

    async def acquire(self, context: TenantContext, workspace_id: str) -> WorkspaceHandle:
        return WorkspaceHandle(
            tenant_id=context.tenant_id,
            agent_app_id=context.agent_app_id,
            workspace_id=workspace_id,
            kind=WorkspaceKind.LOCAL,
            location=f"/tmp/{context.tenant_id}/{workspace_id}",
        )

    async def release(self, handle: WorkspaceHandle) -> None:
        del handle

    async def list_files(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        *,
        max_results: int,
        max_entries: int,
    ) -> tuple[str, ...]:
        del handle, relative_path, max_results, max_entries
        return ()

    async def read_text(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        *,
        max_bytes: int,
    ) -> str:
        del handle, relative_path, max_bytes
        return ""


@pytest.mark.anyio
async def test_workspace_contract_keeps_tenant_scope_in_the_handle() -> None:
    context = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )

    handle = await RecordingWorkspaceProvider().acquire(context, "run-1")

    assert handle.tenant_id == context.tenant_id
    assert handle.agent_app_id == context.agent_app_id
    assert handle.kind is WorkspaceKind.LOCAL


def test_workspace_handle_rejects_empty_identity_or_location() -> None:
    with pytest.raises(ValueError):
        WorkspaceHandle(
            tenant_id=uuid4(),
            agent_app_id=uuid4(),
            workspace_id=" ",
            kind=WorkspaceKind.CONTAINER,
            location="container://worker-1",
        )
