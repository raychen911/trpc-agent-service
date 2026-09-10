"""租户工作区沙箱。"""
from __future__ import annotations

from pathlib import Path

from trpc_agent_sdk.code_executors import BaseWorkspaceRuntime
from trpc_agent_sdk.code_executors import create_local_workspace_runtime

from trpc_service.config.tenant_config import TenantConfig

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class WorkspaceError(RuntimeError):
    """工作区初始化失败。"""


def tenant_work_root(tenant_id: str) -> Path:
    """租户独立工作目录：data/workspace/<tenant_id>（路径级隔离）。"""
    root = PROJECT_ROOT / "data" / "workspace" / tenant_id
    root.mkdir(parents=True, exist_ok=True)
    return root


def create_tenant_workspace(tenant_config: TenantConfig) -> BaseWorkspaceRuntime:
    """按租户配置创建沙箱 runtime（默认本地目录沙箱）。"""
    if tenant_config.workspace.mode == "container":
        try:
            from trpc_agent_sdk.code_executors import (
                ContainerConfig,
                create_container_workspace_runtime,
            )
        except ImportError as exc:  # pragma: no cover
            raise WorkspaceError(f"容器沙箱依赖不可用: {exc}") from exc
        cfg = ContainerConfig(image=tenant_config.workspace.image)
        try:
            return create_container_workspace_runtime(container_config=cfg, auto_inputs=True)
        except Exception as exc:
            raise WorkspaceError(f"容器沙箱初始化失败（需 docker 环境）: {exc}") from exc
    return create_local_workspace_runtime(
        work_root=str(tenant_work_root(tenant_config.tenant_id))
    )
