"""容器沙箱测试：Docker 守护进程可用时验证容器 runtime，不可用时跳过。"""
import shutil
import subprocess

import pytest

from trpc_service.config.tenant_config import TenantConfig, WorkspaceConfig
from trpc_service.workspace import WorkspaceError, create_tenant_workspace


def _docker_daemon_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        r = subprocess.run(
            ["docker", "ps"], capture_output=True, timeout=15
        )
        return r.returncode == 0
    except Exception:
        return False


docker_ready = pytest.mark.skipif(
    not _docker_daemon_available(), reason="Docker daemon 未运行"
)


def _tenant_container() -> TenantConfig:
    return TenantConfig(
        tenant_id="tenant_container_t",
        name="容器沙箱测试",
        workspace=WorkspaceConfig(mode="container", image="python:3.13-slim"),
    )


@docker_ready
def test_container_runtime_created():
    """容器模式创建 ContainerWorkspaceRuntime（不依赖真实拉镜像）。"""
    from trpc_agent_sdk.code_executors import ContainerWorkspaceRuntime

    runtime = create_tenant_workspace(_tenant_container())
    assert isinstance(runtime, ContainerWorkspaceRuntime)


@docker_ready
async def test_container_skill_repository_roundtrip():
    """端到端：容器沙箱下技能仓库可装载 text-stats（真实 docker 环境初始化）。"""
    from trpc_service.skill import create_skill_bundle

    _, _, repository = create_skill_bundle(_tenant_container())
    assert "text-stats" in repository.skill_list()
    assert "text_stats" in repository.path("text-stats")  # 技能目录可解析


def test_container_mode_without_docker_raises():
    """无守护进程时容器初始化抛 WorkspaceError（调用方可降级 local）。"""
    if _docker_daemon_available():
        pytest.skip("Docker 可用，无法测降级路径")
    with pytest.raises(WorkspaceError):
        create_tenant_workspace(_tenant_container())
