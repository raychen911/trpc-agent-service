"""租户 SQL 持久化测试：tenant/agent_app 表读写与恢复。"""
import pytest

from trpc_service.config.tenant_config import TenantConfig
from trpc_service.tenant.sql_store import SqlTenantStore
from trpc_service.tenant.storage.tables import AgentAppRow, TenantRow


@pytest.fixture()
def store(tmp_path):
    from trpc_service.tenant.storage.database import Database

    database = Database(f"sqlite:///{(tmp_path / 'tenant.db').as_posix()}")
    database.create_all()
    yield SqlTenantStore(database)
    database.dispose()


def _config(tenant_id="tenant_sql_1", name="客服租户", instruction="你是客服。"):
    return TenantConfig(
        tenant_id=tenant_id,
        name=name,
        app={"app_name": f"{tenant_id}_cs_app", "instruction": instruction},
    )


def test_save_and_load_roundtrip(store):
    store.save_config(_config())
    configs = store.load_all_configs()
    assert len(configs) == 1
    loaded = configs[0]
    assert loaded.tenant_id == "tenant_sql_1"
    assert loaded.name == "客服租户"
    assert loaded.app.instruction == "你是客服。"


def test_upsert_updates_snapshot(store):
    store.save_config(_config())
    store.save_config(_config(name="改名租户"))
    configs = store.load_all_configs()
    assert len(configs) == 1
    assert configs[0].name == "改名租户"


def test_agent_app_projection(store):
    """agent_app 表登记 app_name 唯一投影，随租户更新。"""
    store.save_config(_config())
    store.save_config(_config(instruction="改提示词"))
    with store._db.session() as s:
        apps = s.query(AgentAppRow).all()
        assert len(apps) == 1  # 更新是替换而非追加
        assert apps[0].app_name == "tenant_sql_1_cs_app"
        assert apps[0].instruction == "改提示词"
        assert s.query(TenantRow).count() == 1


def test_delete_config(store):
    store.save_config(_config())
    assert store.delete_config("tenant_sql_1") is True
    assert store.delete_config("tenant_sql_1") is False
    assert store.load_all_configs() == []


def test_invalid_snapshot_skipped(store, tmp_path):
    """损坏快照跳过不阻断启动。"""
    store.save_config(_config())
    with store._db.session() as s:
        row = s.query(TenantRow).first()
        row.config = {"tenant_id": "broken", "not_a_field": True}  # 缺 name 必填
    configs = store.load_all_configs()
    assert all(c.tenant_id != "broken" for c in configs)
