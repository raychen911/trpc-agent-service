"""灰度发布（release_stage + release API）测试。"""
from trpc_service.config.tenant_config import TenantConfig
from trpc_service.tenant.sql_store import SqlTenantStore


def _config(name="v1"):
    return TenantConfig(
        tenant_id="tenant_rel_1",
        name=name,
        app={"app_name": "tenant_rel_1_app", "instruction": f"inst-{name}"},
    )


def test_release_stage_roundtrip(tmp_path):
    from trpc_service.tenant.storage.database import Database

    database = Database(f"sqlite:///{(tmp_path / 'rel.db').as_posix()}")
    database.create_all()
    try:
        store = SqlTenantStore(database)
        store.save_config(_config("v1"))
        store.save_config(_config("v2"))
        # 灰度切到 r1 并标记 canary
        cfg = store.load_revision("tenant_rel_1", 1)
        cfg.release_stage = "canary"
        store.save_config(cfg)
        current = store.load_all_configs()[0]
        assert current.release_stage == "canary"
        assert current.name == "v1"
        # 灰度动作本身是版本化保存（r3 可再回滚）
        assert store.current_revision("tenant_rel_1") == 3
        # 推全量：stage 回 stable
        current.release_stage = "stable"
        store.save_config(current)
        assert store.load_all_configs()[0].release_stage == "stable"
    finally:
        database.dispose()


def test_release_api_validation():
    from fastapi.testclient import TestClient

    from trpc_service.web.app import app

    client = TestClient(app)
    res = client.post(
        "/api/v1/tenants/tenant_001/release",
        json={"revision": 1, "stage": "bogus"},
    )
    assert res.status_code == 422
