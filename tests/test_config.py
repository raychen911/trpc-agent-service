"""配置加载与热加载测试。"""
from pathlib import Path

import pytest

from trpc_service.config.loader import load_config
from trpc_service.config.manager import ConfigManager
from trpc_service.config.tenant_config import TenantConfig

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def test_load_config_returns_both_tenants():
    configs = load_config()
    assert "tenant_001" in configs
    assert "tenant_002" in configs
    # app_name 已被 TenantConfig 校验器规范化（带租户前缀）
    assert configs["tenant_001"].app.app_name == "tenant_001_customer_service"


def test_app_name_normalized():
    from trpc_service.config.tenant_config import TenantConfig

    cfg = TenantConfig(tenant_id="t1", name="x", app={"app_name": "hr"})
    assert cfg.app.app_name == "t1_hr"


def test_app_name_idempotent():
    """重复构造不套前缀。"""
    from trpc_service.config.tenant_config import TenantConfig

    cfg = TenantConfig(tenant_id="t1", name="x", app={"app_name": "t1_hr"})
    assert cfg.app.app_name == "t1_hr"


def test_default_app_name_prefixed():
    from trpc_service.config.tenant_config import TenantConfig

    cfg = TenantConfig(tenant_id="t2", name="x")
    assert cfg.app.app_name == "t2_default_app"


def test_manager_conflict_detected():
    """绕过校验器制造非法状态（validate_assignment 未开启，构造后可直接改字段），。"""
    manager = ConfigManager()
    a = TenantConfig(tenant_id="t1", name="a", app={"app_name": "x"})
    b = TenantConfig(tenant_id="t2", name="b", app={"app_name": "y"})
    manager.register(a)
    manager.register(b)
    b.app.app_name = a.app.app_name  # 模拟非法赋值
    with pytest.raises(ValueError):
        manager._assert_unique_app_name()


def test_load_config_applies_env_key():
    """api_key 为空的租户应被 .env 的 KEY 覆盖。"""
    cfg = load_config()["tenant_001"]
    assert cfg.model.api_key, "环境变量 KEY 应覆盖空 api_key"


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent.yaml")


def test_manager_reload_and_get():
    manager = ConfigManager()
    assert manager.get("tenant_001") is not None
    assert manager.get("nope") is None
    configs = manager.maybe_reload()
    assert set(configs) >= {"tenant_001", "tenant_002"}
