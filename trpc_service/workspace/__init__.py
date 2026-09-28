"""Workspace contracts and built-in local implementation."""

from trpc_service.workspace.contracts import (
    WorkspaceHandle,
    WorkspaceKind,
    WorkspaceProvider,
)
from trpc_service.workspace.local import LocalWorkspaceProvider

__all__ = ["LocalWorkspaceProvider", "WorkspaceHandle", "WorkspaceKind", "WorkspaceProvider"]
