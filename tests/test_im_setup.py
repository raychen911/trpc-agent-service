"""Secret entry is local, exclusive, allowlisted and never echoes invalid values."""

import json
import pytest
from trpc_service.im_setup import save_bundle, load_bundle


def test_local_configuration_preserves_secrets_without_shell_expansion(tmp_path, monkeypatch):
    value = "synthetic-$()-`text`-secret"
    path = tmp_path / "im.json"
    save_bundle(path, {"TRPC_MODEL_API_KEY": value})
    monkeypatch.delenv("TRPC_MODEL_API_KEY", raising=False)
    load_bundle(path)
    import os
    assert os.environ["TRPC_MODEL_API_KEY"] == value
    with pytest.raises(FileExistsError):
        save_bundle(path, {"TRPC_MODEL_API_KEY": "replacement"})
    assert json.loads(path.read_text())["TRPC_MODEL_API_KEY"] == value


def test_secret_bundle_cannot_set_process_control_environment(tmp_path):
    path = tmp_path / "im.json"
    path.write_text(json.dumps({"PYTHONPATH": "secret-value"}))
    with pytest.raises(ValueError) as error:
        load_bundle(path)
    assert "secret-value" not in str(error.value)


def test_feishu_hidden_setup_preserves_existing_bundle(tmp_path, monkeypatch, capsys):
    from trpc_service.im_setup import configure_feishu
    existing = tmp_path / "im.json"
    save_bundle(existing, {"TRPC_IDENTITY_KEY": "unchanged-identity", "TRPC_MODEL_API_KEY": "unchanged-model"})
    before = existing.read_bytes()
    path = tmp_path / "feishu.json"
    secret = "synthetic-$()-`feishu-secret`"
    monkeypatch.setattr("trpc_service.im_setup.getpass.getpass", lambda _: secret)
    configure_feishu(path, app_id="cli_synthetic")
    assert existing.read_bytes() == before
    assert json.loads(path.read_text())["TRPC_FEISHU_APP_SECRET"] == secret
    assert secret not in capsys.readouterr().out
    monkeypatch.delenv("TRPC_FEISHU_APP_ID", raising=False)
    monkeypatch.delenv("TRPC_FEISHU_APP_SECRET", raising=False)
    load_bundle(path)
    import os
    assert os.environ["TRPC_FEISHU_APP_ID"] == "cli_synthetic"
    assert os.environ["TRPC_FEISHU_APP_SECRET"] == secret
    with pytest.raises(ValueError, match="未覆盖"):
        configure_feishu(path, app_id="cli_other")
    assert json.loads(path.read_text())["TRPC_FEISHU_APP_ID"] == "cli_synthetic"


@pytest.mark.parametrize("mode", ["empty", "control", "echoed-terminal"])
def test_feishu_invalid_or_echoed_secret_is_never_saved(tmp_path, monkeypatch, capsys, mode):
    import getpass
    import warnings
    from trpc_service.im_setup import configure_feishu
    path = tmp_path / "feishu.json"

    def read_secret(_):
        if mode == "echoed-terminal":
            warnings.warn("synthetic fallback", getpass.GetPassWarning)
        return "" if mode == "empty" else "synthetic\x00secret"

    monkeypatch.setattr("trpc_service.im_setup.getpass.getpass", read_secret)
    with pytest.raises(ValueError) as error:
        configure_feishu(path, app_id="cli_synthetic")
    assert not path.exists()
    assert "synthetic" not in str(error.value) + capsys.readouterr().out
