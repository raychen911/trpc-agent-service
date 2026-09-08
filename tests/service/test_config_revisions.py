"""Draft/publish/revision binding and persistence tests."""

from __future__ import annotations

import pytest

from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import MySqlTenantRepository
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import tenant_config_checksum


def _tenant(model_name: str = "model-v1") -> Tenant:
    return Tenant(tenant_id="tenant-a", name="A", model=ModelEndpoint(model_name=model_name))


def test_draft_is_inert_until_publish_and_old_revision_remains_resolvable():
    manager = TenantConfigManager()
    manager.register(_tenant())
    draft = manager.stage(_tenant("model-v2"), expected_version=1, by="owner", reason="upgrade")

    assert draft.checksum == tenant_config_checksum(_tenant("model-v2"))
    assert manager.current_version("tenant-a") == 1
    assert manager.get("tenant-a").model.model_name == "model-v1"
    assert manager.get_draft("tenant-a").created_by == "owner"

    published = manager.publish("tenant-a", expected_version=1, by="reviewer")
    assert published.model.model_name == "model-v2"
    assert manager.current_version("tenant-a") == 2
    assert manager.get_version("tenant-a", 1).model.model_name == "model-v1"
    assert manager.get_version("tenant-a", 2).model.model_name == "model-v2"
    assert manager.get_version("missing", 1) is None
    assert manager.get_draft("tenant-a") is None


def test_draft_conflict_preflight_and_discard_leave_active_config_unchanged():
    rejected = []

    def preflight(tenant):
        if tenant.model.model_name == "bad":
            rejected.append(tenant.model.model_name)
            raise ValueError("backend unavailable")

    manager = TenantConfigManager(preflight_checks=[preflight])
    manager.register(_tenant())
    with pytest.raises(ValueError, match="version conflict"):
        manager.stage(_tenant("bad"), expected_version=9)
    manager.stage(_tenant("bad"))
    with pytest.raises(ValueError, match="backend unavailable"):
        manager.publish("tenant-a")
    assert rejected == ["bad"]
    assert manager.get("tenant-a").model.model_name == "model-v1"
    assert manager.get_draft("tenant-a") is not None
    manager.discard_draft("tenant-a")
    with pytest.raises(ValueError, match="no config draft"):
        manager.discard_draft("tenant-a")
    with pytest.raises(ValueError, match="no config draft"):
        manager.publish("tenant-a")
    with pytest.raises(ValueError, match="not found"):
        manager.current_version("missing")


def test_every_activation_path_uses_the_same_preflight_boundary():
    blocked_models = {"blocked"}

    def preflight(tenant):
        if tenant.model.model_name in blocked_models:
            raise ValueError("preflight rejected")

    manager = TenantConfigManager(preflight_checks=[preflight])
    with pytest.raises(ValueError, match="preflight rejected"):
        manager.register(_tenant("blocked"))

    manager.register(_tenant())
    with pytest.raises(ValueError, match="preflight rejected"):
        manager.update(_tenant("blocked"))

    manager.update(_tenant("model-v2"))
    blocked_models.add("model-v1")
    with pytest.raises(ValueError, match="preflight rejected"):
        manager.rollback("tenant-a", 1)
    assert manager.get("tenant-a").model.model_name == "model-v2"

    existing = TenantConfigManager()
    existing.register(_tenant("blocked"))
    with pytest.raises(ValueError, match="preflight rejected"):
        existing.add_preflight_check(preflight)


def test_persistent_draft_survives_restart_and_publish_is_versioned(tmp_path):
    url = f"sqlite:///{tmp_path / 'tenant-config.db'}"
    first = TenantConfigManager(MySqlTenantRepository(url, "encryption-key"))
    first.register(_tenant())
    first.stage(_tenant("model-v2"), by="author")
    first.close()

    second = TenantConfigManager(MySqlTenantRepository(url, "encryption-key"))
    assert second.get_draft("tenant-a").config_snapshot["model"]["model_name"] == "model-v2"
    second.publish("tenant-a", by="publisher")
    assert [item.version for item in second.history("tenant-a")] == [1, 2]
    assert second.get("tenant-a").model.model_name == "model-v2"
    assert second.get_draft("tenant-a") is None
    second.stage(_tenant("model-v3"))
    second.delete("tenant-a")
    assert second.get_draft("tenant-a") is None
    second.close()


def test_historical_revision_cache_miss_reads_authoritative_repository(tmp_path):
    url = f"sqlite:///{tmp_path / 'remote-config.db'}"
    writer = TenantConfigManager(MySqlTenantRepository(url, "encryption-key"))
    writer.register(_tenant())
    reader = TenantConfigManager(MySqlTenantRepository(url, "encryption-key"))

    writer.update(_tenant("model-v2"))
    reader._on_remote_change("tenant-a", 2, "tenant.updated")
    writer.update(_tenant("model-v3"))
    reader._on_remote_change("tenant-a", 3, "tenant.updated")

    assert reader.current_version("tenant-a") == 3
    assert reader.get_version("tenant-a", 2).model.model_name == "model-v2"

    writer.close()
    reader.close()
