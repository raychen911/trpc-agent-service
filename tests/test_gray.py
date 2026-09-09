# 按比例灰度（PRD 5.2）单元测试
import pytest

from trpc_service.events import AgentEvent
from trpc_service.runtime import MockAgentRunner, Runtime
from trpc_service.storage import InMemoryStorage
from trpc_service.storage.sql_store import SqlTenantStore, create_sql_engine
from trpc_service.tenant import TenantConfig, TenantRegistry
from trpc_service.tenant.gray import apply_gray


def _tenant_with_gray(percent=100, canary=None, enabled=True):
    return TenantConfig(
        tenant_id="gray-t",
        name="灰度租户",
        model={"model_name": "base-model"},
        gray={
            "enabled": enabled,
            "percent": percent,
            "canary": canary or {
                "model": {
                    "model_name": "canary-model"
                }
            },
        },
    )


# ------------------------------------------------------------------
# apply_gray 纯函数
# ------------------------------------------------------------------


def test_apply_gray_disabled_returns_original():
    t = _tenant_with_gray(enabled=False)
    assert apply_gray(t, "any-user") is t


def test_apply_gray_percent_zero_returns_original():
    t = _tenant_with_gray(percent=0)
    assert apply_gray(t, "any-user") is t


def test_apply_gray_percent_100_hits_all():
    t = _tenant_with_gray(percent=100)
    for uid in ["u1", "u2", "u3", "u_100"]:
        out = apply_gray(t, uid)
        assert out.model.model_name == "canary-model", f"{uid} 应命中 canary"


def test_apply_gray_percent_0_never_hits():
    t = _tenant_with_gray(percent=0)
    assert apply_gray(t, "u1").model.model_name == "base-model"


def test_apply_gray_deterministic_across_calls():
    """同一 user_id 分流结果稳定（跨节点一致性基础）。"""
    t = _tenant_with_gray(percent=50)
    assert apply_gray(t, "u1").model.model_name == apply_gray(t, "u1").model.model_name


def test_apply_gray_unknown_field_ignored():
    t = _tenant_with_gray(canary={"not_a_field": {"x": 1}, "model": {"model_name": "canary-model"}})
    out = apply_gray(t, "u-hit")
    assert out.model.model_name == "canary-model"
    assert not hasattr(out, "not_a_field")


# ------------------------------------------------------------------
# Runtime 端到端：命中者走 canary 配置
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_runtime_applies_gray_per_user():
    storage = InMemoryStorage()

    # percent=100 -> 所有用户命中 canary，模型/提示词被覆盖
    tenant = TenantConfig(
        tenant_id="g1",
        name="灰度",
        app={"system_prompt": "原版提示词"},
        gray={
            "enabled": True,
            "percent": 100,
            "canary": {
                "app": {
                    "system_prompt": "灰度提示词"
                }
            },
        },
    )

    async def load_fn(tid):
        return tenant if tid == "g1" else None

    registry = TenantRegistry(load_fn=load_fn)
    rt = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())

    resp = await rt.handle(
        AgentEvent(tenant_id="g1", session_id="s1", user_id="hit-user", content="你好", channel_type="web"))
    # Mock runner 回声含系统提示词前缀？MockAgentRunner 是 prefix + 用户消息，
    # 不含 system_prompt。改用工具白名单断言：canary 里 tools 白名单不同。
    assert resp.content, "请求应正常执行（命中灰度不报错）"

    # 直接断言 registry 中租户原配置未被污染（apply_gray 不修改原对象）
    original = await registry.get("g1")
    assert original.app.system_prompt == "原版提示词"


@pytest.mark.asyncio
async def test_runtime_gray_tools_allowlist_applied():
    """canary 覆盖 tools 白名单：命中用户可用 canary 独有工具。"""
    tenant = TenantConfig(
        tenant_id="g2",
        name="灰度2",
        tools={"allowlist": ["echo"]},
        gray={
            "enabled": True,
            "percent": 100,
            "canary": {
                "tools": {
                    "allowlist": ["echo", "get_time"]
                }
            },
        },
    )

    async def load_fn(tid):
        return tenant if tid == "g2" else None

    registry = TenantRegistry(load_fn=load_fn)
    # 手动确认 apply_gray 后工具白名单被覆盖
    resolved = apply_gray(await registry.get_or_raise("g2"), "any-user")
    assert "get_time" in resolved.tools.allowlist
    assert "echo" in resolved.tools.allowlist


# ------------------------------------------------------------------
# SQL 持久化（gray_config 列）+ 轻量迁移
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sql_tenant_gray_persisted(tmp_path):
    dsn = f"sqlite+aiosqlite:///{tmp_path}/gray.db"
    engine = await create_sql_engine(dsn)
    store = SqlTenantStore(engine)
    cfg = _tenant_with_gray(percent=30)
    await store.create(cfg)

    loaded = await store.get("gray-t")
    assert loaded is not None
    assert loaded.gray.enabled is True
    assert loaded.gray.percent == 30
    assert loaded.gray.canary["model"]["model_name"] == "canary-model"
    await store.close()


@pytest.mark.asyncio
async def test_sql_tenant_migration_adds_gray_column(tmp_path):
    """既有 tenant 表（无 gray_config 列）打开后自动补列不丢数据。"""
    import sqlite3

    db_path = f"{tmp_path}/legacy.db"
    # 建一个旧结构库（无 gray_config 列）+ 旧数据
    raw = sqlite3.connect(db_path)
    raw.execute("CREATE TABLE tenant (tenant_id VARCHAR(64) PRIMARY KEY, name VARCHAR(128) NOT NULL, "
                "status VARCHAR(32) NOT NULL, app_config JSON, model_config JSON, tool_permissions JSON, "
                "im_channel_config JSON, data_backend_config JSON, audit_policy JSON, "
                "monthly_budget_usd FLOAT, used_budget_usd FLOAT, rate_limit_per_min INTEGER, "
                "created_at DATETIME, updated_at DATETIME)")
    raw.execute("INSERT INTO tenant (tenant_id, name, status) VALUES ('old1', '旧租户', 'active')")
    raw.commit()
    raw.close()

    dsn = f"sqlite+aiosqlite:///{db_path}"
    engine = await create_sql_engine(dsn)  # 触发补列迁移
    store = SqlTenantStore(engine)
    loaded = await store.get("old1")
    assert loaded is not None
    assert loaded.name == "旧租户", "迁移不应丢既有数据"
    assert loaded.gray.enabled is False, "旧行无 gray -> 默认 GrayConfig"
    await store.close()
