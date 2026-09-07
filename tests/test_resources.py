from pathlib import Path

import pytest

from tenant_agent import resources


def test_resources_work_outside_checkout_and_in_wheel_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert resources.default_bootstrap_path().is_file()
    package = tmp_path / "installed" / "tenant_agent"
    bundle = package / "_resources"
    (bundle / "config").mkdir(parents=True)
    (bundle / "alembic.ini").write_text("[alembic]\n", encoding="utf-8")
    (bundle / "config/tenants.example.yaml").write_text("tenants: []\n", encoding="utf-8")
    monkeypatch.setattr(resources, "__file__", str(package / "resources.py"))
    assert resources.resource_root() == bundle
    assert resources.default_bootstrap_path().is_file()
    monkeypatch.setattr(resources, "__file__", str(tmp_path / "broken" / "tenant_agent" / "resources.py"))
    with pytest.raises(RuntimeError, match="missing bundled"):
        resources.resource_root()
