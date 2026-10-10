"""Tenant-isolated workspace management."""

from trpc_service.workspace.manager import TenantWorkspace, WorkspaceManager

__all__ = ["TenantWorkspace", "WorkspaceManager"]
