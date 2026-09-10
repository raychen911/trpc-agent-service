"""workspace 沙箱测试：本地目录沙箱与租户路径隔离。"""
from trpc_agent_sdk.code_executors import LocalWorkspaceRuntime

from trpc_service.config.tenant_config import TenantConfig, WorkspaceConfig
from trpc_service.workspace import tenant_work_root, create_tenant_workspace


def _tenant(tenant_id="tenant_ws_a", mode="local"):
    return TenantConfig(
        tenant_id=tenant_id, name=tenant_id,
        workspace=WorkspaceConfig(mode=mode),
    )


def test_local_sandbox_per_tenant_root(tmp_path):
    tenant = _tenant()
    runtime = create_tenant_workspace(tenant)
    assert isinstance(runtime, LocalWorkspaceRuntime)
    root = runtime.manager().work_root
    assert str(tenant_work_root(tenant.tenant_id)) == root
    assert tenant.tenant_id in root


def test_tenant_work_roots_isolated():
    root_a = tenant_work_root("tenant_a")
    root_b = tenant_work_root("tenant_b")
    assert root_a != root_b
    assert root_a.exists() and root_b.exists()
    assert "tenant_a" in str(root_a) and "tenant_b" not in str(root_a)


def test_default_workspace_config_is_local():
    """不配置 workspace 时默认本地沙箱（开发/测试零依赖）。"""
    tenant = TenantConfig(tenant_id="t", name="t")
    assert tenant.workspace.mode == "local"
    assert tenant.workspace.image == "python:3.13-slim"
