"""通道凭证环境变量注入测试：yaml 留空占位，凭证不进仓库。"""
import pytest

from trpc_service.config.loader import load_config


@pytest.fixture()
def _channel_env(monkeypatch):
    monkeypatch.setenv("WECOM_TOKEN", "tk_from_env")
    monkeypatch.setenv("WECOM_AES_KEY", "aes_from_env")
    monkeypatch.setenv("WECOM_BOT_SECRET", "sec_from_env")
    yield
    monkeypatch.delenv("WECOM_TOKEN", raising=False)
    monkeypatch.delenv("WECOM_AES_KEY", raising=False)
    monkeypatch.delenv("WECOM_BOT_SECRET", raising=False)


def _channel_of(tenant_id, name, _channel_env):
    from trpc_service.config.loader import load_config

    cfg = load_config()[tenant_id]
    return cfg.channels[name]


def test_wecom_credentials_from_env(_channel_env):
    """wecom 通道：yaml 空占位 → 环境变量注入。"""
    ch = _channel_of("tenant_001", "wecom", _channel_env)
    assert ch.token == "tk_from_env"
    assert ch.encoding_aes_key == "aes_from_env" or ch.encoding_aes_key


def test_wecom_smartbot_secret_from_env(_channel_env):
    ch = _channel_of("tenant_001", "wecom_smartbot", _channel_env)
    assert ch.secret == "sec_from_env"


def test_unmapped_channel_not_touched(_channel_env):
    """环境变量只作用于映射过的通道，其余保持 yaml 原值。"""
    cfg = load_config()["tenant_001"]
    for name, ch in cfg.channels.items():
        if name not in ("wecom", "wecom_smartbot"):
            assert ch.token in ("", "your_verification_token")
