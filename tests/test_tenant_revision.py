"""租户配置版本化与回滚测试。"""
import pytest

from trpc_service.config.tenant_config import TenantConfig
from trpc_service.tenant.sql_store import SqlTenantStore


@pytest.fixture()
def store(tmp_path):
    from trpc_service.tenant.storage.database import Database

    database = Database(f"sqlite:///{(tmp_path / 'tenant_rev.db').as_posix()}")
    database.create_all()
    yield SqlTenantStore(database)
    database.dispose()


def _config(name="客服租户"):
    return TenantConfig(
        tenant_id="tenant_rev_1",
        name=name,
        app={"app_name": "tenant_rev_1_app", "instruction": f"instruction-{name}"},
    )


def test_save_creates_incremental_revisions(store):
    store.save_config(_config("v1"))
    store.save_config(_config("v2"))
    store.save_config(_config("v3"))
    assert store.current_revision("tenant_rev_1") == 3
    revs = store.list_revisions("tenant_rev_1")
    assert [r["revision"] for r in revs] == [3, 2, 1]  # 新→旧


def test_rollback_restores_old_config_as_new_revision(store):
    store.save_config(_config("v1"))
    store.save_config(_config("v2-bad"))
    restored = store.rollback_config("tenant_rev_1", 1)
    assert restored is not None and restored.name == "v1"
    # 回滚产生新版本 r3（save r1 → save r2 → 回滚 = r3），回滚本身可再回滚
    assert store.current_revision("tenant_rev_1") == 3
    configs = store.load_all_configs()
    assert configs[0].name == "v1"
    again = store.rollback_config("tenant_rev_1", 2)
    assert again is not None and again.name == "v2-bad"
    assert store.current_revision("tenant_rev_1") == 4


def test_rollback_missing_revision_returns_none(store):
    store.save_config(_config())
    assert store.rollback_config("tenant_rev_1", 99) is None


def test_load_revision(store):
    store.save_config(_config("v1"))
    store.save_config(_config("v2"))
    cfg = store.load_revision("tenant_rev_1", 1)
    assert cfg is not None and cfg.name == "v1"
    assert store.load_revision("tenant_rev_1", 42) is None
