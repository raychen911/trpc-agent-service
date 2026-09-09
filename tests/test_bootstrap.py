# bootstrap 装配层单测：CLI 之外的装配入口必须可独立验证
# 此前装配逻辑内嵌于 _cli._serve_gateway（274 行，0% 覆盖），
# 装配错误只能靠真实启动暴露——本文件即为此缺口的回归防护。
import asyncio
import os
import warnings

from fastapi.testclient import TestClient

from trpc_service.bootstrap import (
    DEMO_MODEL_BY_RUNNER,
    build_gateway,
    feishu_sdk_env_channel,
    im_channels_from_env,
    wecom_bot_env_channel,
)
from trpc_service.config.settings import PlatformSettings, SqlSettings, StorageSettings


def _isolated_settings(tmp_path) -> PlatformSettings:
    """测试隔离: SQL DSN 指向临时库，避免污染开发者的真实 data/teneuris.db。

    教训: build_gateway 的租户存储创建是无条件的（inmemory 也建 SQL 租户
    存储），默认 DSN 即 data/teneuris.db——曾导致单测把 mock demo 种子
    写进运行库，污染下一次真实启动的租户回源。
    """
    return PlatformSettings(storage=StorageSettings(sql=SqlSettings(dsn=f"sqlite+aiosqlite:///{tmp_path}/test.db")))


def test_build_gateway_inmemory_mock_serves_demo_chat(tmp_path):
    """inmemory+mock 装配产物可直接服务 demo 租户对话（全链路装配自检）。

    app.state.chain/ctx/runtime 在 build_gateway_app 内同步挂载（非 lifespan），
    故装配完成后无需进入事件循环即可用 TestClient 驱动 /chat。
    """
    warnings.simplefilter("ignore")
    bundle = asyncio.run(build_gateway(_isolated_settings(tmp_path), storage_backend="inmemory", runner_kind="mock"))
    assert bundle.storage_manager is None  # inmemory 无按租户懒建
    assert bundle.broadcaster is None  # inmemory 无跨节点广播
    assert bundle.runner_kind == "mock"
    assert bundle.app.state.chain is not None  # 治理链已装配
    client = TestClient(bundle.app)
    resp = client.post("/chat", json={"tenant_id": "demo", "user_id": "u1", "content": "hi"})
    assert resp.status_code == 200
    assert resp.json()["response_type"] == "text"


def test_build_gateway_framework_without_key_exits(tmp_path):
    """framework 装配缺 api_key 时显式退出（不静默回落 mock，Spec 用例 6）。"""
    saved = os.environ.pop("DEEPSEEK_API_KEY", None)
    try:
        exited = False
        try:
            asyncio.run(build_gateway(_isolated_settings(tmp_path), storage_backend="inmemory",
                                      runner_kind="framework"))
        except SystemExit as exc:
            exited = exc.code == 1
        assert exited, "缺 key 应 sys.exit(1)"
    finally:
        if saved is not None:
            os.environ["DEEPSEEK_API_KEY"] = saved


def test_env_channel_helpers_shape():
    """env 通道助手：凭证缺失返回 None；demo 模型表含 mock/framework 两档。"""
    assert set(DEMO_MODEL_BY_RUNNER) == {"mock", "framework"}
    # 无凭证环境（CI/单测）应安全返回 None 而非抛错
    has_wecom = os.environ.get("WECOM_BOT_ID") and os.environ.get("WECOM_BOT_SECRET")
    assert (wecom_bot_env_channel() is not None) == bool(has_wecom)
    has_fs = os.environ.get("FEISHU_APP_ID") and os.environ.get("FEISHU_APP_SECRET")
    assert (feishu_sdk_env_channel() is not None) == bool(has_fs)
    channels = im_channels_from_env()
    assert isinstance(channels, list)
    assert all({"channel_type", "webhook_path"} <= set(c) for c in channels)


# ------------------------------------------------------------------
# 首启自动播种（ensure_demo_tenant）
# ------------------------------------------------------------------


def _tmp_tenant_store(tmp_path):
    """临时 sqlite 租户存储（与生产同一 SqlTenantStore 实现）。"""
    from trpc_service.storage.sql_store import SqlTenantStore, create_sql_engine

    dsn = f"sqlite+aiosqlite:///{tmp_path}/seed.db"
    engine = asyncio.run(create_sql_engine(dsn))
    return SqlTenantStore(engine)


_REDIS_BACKENDS = {"session": "redis", "memory": "redis", "summary": "sql", "audit": "sql", "knowledge": "redis"}


def test_ensure_demo_tenant_seeds_empty_store(tmp_path):
    """空库首启: 播种 demo 租户且配置与内置兜底同构（Admin 可直接热更新）。"""
    from trpc_service.bootstrap import ensure_demo_tenant

    store = _tmp_tenant_store(tmp_path)
    seeded = asyncio.run(ensure_demo_tenant(store, "mock", _REDIS_BACKENDS))
    assert seeded is True

    config = asyncio.run(store.get("demo"))
    assert config is not None
    assert config.name == "演示租户"
    assert config.backends.session == "redis"
    assert config.backends.knowledge == "redis"
    assert "echo" in config.tools.allowlist


def test_ensure_demo_tenant_skips_nonempty_store(tmp_path):
    """非空库不播种: 尊重 Admin 删除 demo 后的运维意图。"""
    from trpc_service.bootstrap import ensure_demo_tenant
    from trpc_service.bootstrap import demo_tenant_dict
    from trpc_service.tenant.models import tenant_from_dict

    store = _tmp_tenant_store(tmp_path)
    other = tenant_from_dict(demo_tenant_dict("mock", _REDIS_BACKENDS))
    other.tenant_id = "real-tenant"
    asyncio.run(store.create(other))

    seeded = asyncio.run(ensure_demo_tenant(store, "mock", _REDIS_BACKENDS))
    assert seeded is False, "库中已有租户时不得补种"
    assert asyncio.run(store.get("demo")) is None


def test_ensure_demo_tenant_race_duplicate_create(tmp_path):
    """并发首启窗口: list 看到空库、create 撞唯一键 → 静默放弃不抛错。"""
    from trpc_service.bootstrap import ensure_demo_tenant

    store = _tmp_tenant_store(tmp_path)
    # 真实库中已有 demo（对方节点抢先播种完成）
    assert asyncio.run(ensure_demo_tenant(store, "mock", _REDIS_BACKENDS)) is True

    class _RacyStore:
        """窗口期替身: list 谎报空库，create 委托真实库（将撞唯一键）。"""

        def __init__(self, inner):
            self._inner = inner

        async def list(self):
            return []  # 窗口期快照: 尚未看到对方写入

        async def create(self, config):
            return await self._inner.create(config)  # 撞唯一键 IntegrityError

    assert asyncio.run(ensure_demo_tenant(_RacyStore(store), "mock", _REDIS_BACKENDS)) is False
    # 库中 demo 仅一份（对方写入的那份），未被破坏
    assert asyncio.run(store.get("demo")) is not None


# ------------------------------------------------------------------
# demo 模型对齐修复（reconcile_demo_model）
# ------------------------------------------------------------------


def test_reconcile_demo_model_repairs_mock_seed(tmp_path):
    """mock 种子 + framework 启动: demo 模型应对齐为 framework 档。"""
    from trpc_service.bootstrap import ensure_demo_tenant, reconcile_demo_model

    store = _tmp_tenant_store(tmp_path)
    assert asyncio.run(ensure_demo_tenant(store, "mock", _REDIS_BACKENDS)) is True
    assert asyncio.run(store.get("demo")).model.provider == "mock"

    repaired = asyncio.run(reconcile_demo_model(store, "framework"))
    assert repaired is True
    model = asyncio.run(store.get("demo")).model
    assert model.provider == "deepseek", "应对齐为 framework 档模型"
    # 幂等: 第二次不再动作
    assert asyncio.run(reconcile_demo_model(store, "framework")) is False


def test_reconcile_demo_model_leaves_custom_model_alone(tmp_path):
    """运营者自定义模型（非 mock）不被触碰；mock runner 启动不触发修复。"""
    from trpc_service.bootstrap import reconcile_demo_model
    from trpc_service.bootstrap import demo_tenant_dict
    from trpc_service.tenant.models import ModelConfig, tenant_from_dict

    store = _tmp_tenant_store(tmp_path)
    custom = tenant_from_dict(demo_tenant_dict("mock", _REDIS_BACKENDS))
    custom.model = ModelConfig(provider="openai", model_name="gpt-4o", api_key_ref="sk-custom")
    asyncio.run(store.create(custom))

    assert asyncio.run(reconcile_demo_model(store, "framework")) is False
    kept = asyncio.run(store.get("demo"))
    # provider 不被触碰（api_key_ref 本就不落库，密钥走环境注入，PRD 4.5）
    assert kept.model.provider == "openai" and kept.model.model_name == "gpt-4o"
