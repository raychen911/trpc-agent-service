"""租户配置热加载端点测试。"""
import shutil
from pathlib import Path

from fastapi.testclient import TestClient

from trpc_service.config.loader import DEFAULT_CONFIG_PATH
from trpc_service.web.app import app, state


class SimpleNamespaceClose:
    async def close(self):
        return None


def test_reload_no_change_reports_clean():
    client = TestClient(app)
    res = client.post("/api/v1/tenants/reload")
    assert res.status_code == 200
    body = res.json()
    assert body["reloaded"] is True
    assert body["changed"] == [] and body["removed"] == []
    assert "tenant_001" in state.config_manager.all()


def test_reload_rebuilds_changed_tenant(tmp_path):
    """改 YAML（临时副本指向）→ reload → 配置变化的租户热重建 Runner。"""
    tmp_yaml = tmp_path / "tenants.yaml"
    shutil.copy(DEFAULT_CONFIG_PATH, tmp_yaml)
    manager = state.config_manager
    orig_path, orig_mtime = manager._config_path, manager._mtime
    manager._config_path = Path(tmp_yaml)
    try:
        client = TestClient(app)
        client.post("/api/v1/tenants/reload")  # 以副本为基准建立 mtime

        text = tmp_yaml.read_text(encoding="utf-8")
        text = text.replace('rate_limit_per_minute: 30', 'rate_limit_per_minute: 99')
        tmp_yaml.write_text(text, encoding="utf-8")

        class FakeRunner:
            app_name = "x"
            closed = False

            async def close(self):
                self.closed = True

        fake = FakeRunner()
        state.runners["tenant_002"] = fake
        res = client.post("/api/v1/tenants/reload")
        assert res.status_code == 200
        body = res.json()
        assert "tenant_002" in body["changed"]
        assert fake.closed is True
        assert state.runners["tenant_002"] is not fake
    finally:
        manager._config_path = orig_path
        manager._mtime = orig_mtime
        manager.reload(force=True)
        state.runners.pop("tenant_002", None)


def test_reload_removes_tenant_not_in_yaml():
    client = TestClient(app)
    state.runners["tenant_ghost"] = SimpleNamespaceClose()
    try:
        res = client.post("/api/v1/tenants/reload")
        assert res.status_code == 200
        assert "tenant_ghost" in res.json()["removed"]
        assert "tenant_ghost" not in state.runners
    finally:
        state.runners.pop("tenant_ghost", None)
