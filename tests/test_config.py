# config 模块单元测试
import os

import pytest
import yaml

from trpc_service.config import load_settings, redact, redact_secrets, mask_secret, Redactor
from trpc_service.config.loader import _apply_env_overrides


def test_default_settings():
    s = load_settings()
    assert s.env == "dev"
    assert s.gateway.port == 8000
    assert s.storage.redis.dsn.startswith("redis://")


def test_env_reference_and_override(tmp_path):
    os.environ["TENEURIS_TEST_KEY"] = "sk-secret-1234567890"
    os.environ["TENEURIS_GATEWAY_PORT"] = "9001"
    cfg = {
        "env": "test",
        "admin": {
            "api_key": {
                "env": "TENEURIS_TEST_KEY"
            }
        },
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    s = load_settings(path)
    assert s.env == "test"
    assert s.gateway.port == 9001  # 环境变量覆盖
    assert s.admin.api_key is not None


def test_env_override_beats_yaml(tmp_path):
    """yaml 与环境变量冲突时，环境变量必须生效（文档「加载顺序 4」契约）。"""
    os.environ["TENEURIS_GATEWAY_PORT"] = "9002"
    path = tmp_path / "conflict.yaml"
    path.write_text(yaml.safe_dump({"gateway": {"port": 8000}}, allow_unicode=True), encoding="utf-8")
    s = load_settings(path)
    assert s.gateway.port == 9002
    del os.environ["TENEURIS_GATEWAY_PORT"]


def test_env_ref_missing_raises(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump({"admin": {"api_key": {"env": "TENEURIS_NOPE"}}}), encoding="utf-8")
    from trpc_service.config import ConfigError

    with pytest.raises(ConfigError):
        load_settings(path)


def test_pii_redaction_default_rules():
    assert "13812345678" not in redact("手机 13812345678")
    assert redact("联系 a@b.com") == "联系 [REDACTED]"
    assert "sk-abcdef1234567890xyz" not in redact("key=sk-abcdef1234567890xyz")


def test_redact_custom_rules():
    r = Redactor({"phone": r"1[3-9]\d{9}"})
    assert r.redact("13812345678 保留") == "[REDACTED] 保留"


def test_secret_masking():
    assert mask_secret("sk-abcdef1234567890", "api_key").startswith("sk-")
    assert "[REDACTED]" in mask_secret("sk-abcdef1234567890", "api_key")
    assert mask_secret("redis://u:p@h:6379/0") == "redis://u:***@h:6379/0"


def test_redact_secrets_nested():
    out = redact_secrets({"api_key": "sk-1234567890abcdef", "nested": {"password": "p@ss"}})
    assert "sk-1234567890abcdef" not in str(out)
    assert "[REDACTED]" in str(out)


def test_redact_disabled():
    r = Redactor(enabled=False)
    assert r.redact("13812345678") == "13812345678"


# ------------------------------------------------------------------
# 全项目审查回归（2026-09-04 OCR findings）
# ------------------------------------------------------------------


def test_env_override_schema_aware_admin_api_key():
    """TENEURIS_ADMIN_API_KEY 必须映射到 admin.api_key（schema 贪婪匹配）。

    缺陷背景: 朴素按 _ 切分曾把该变量映射到 admin.api.key（三层嵌套），
    导致 Admin 鉴权经环境变量配置静默失效（2026-09-05 联调发现）。
    """
    os.environ["TENEURIS_ADMIN_API_KEY"] = "testkey123"
    try:
        s = load_settings()
        assert s.admin.api_key is not None
        assert s.admin.api_key.get_secret_value() == "testkey123"
    finally:
        del os.environ["TENEURIS_ADMIN_API_KEY"]


def test_env_override_schema_aware_nested_paths():
    """嵌套路径与含下划线的顶层字段都要能经环境变量到达。"""
    os.environ["TENEURIS_LOG_LEVEL"] = "DEBUG"
    os.environ["TENEURIS_DATA_DIR"] = "/tmp/teneuris-itest"
    os.environ["TENEURIS_STORAGE_REDIS_DSN"] = "redis://127.0.0.1:6399/7"
    try:
        s = load_settings()
        assert s.log_level == "DEBUG"
        assert s.data_dir == "/tmp/teneuris-itest"
        assert s.storage.redis.dsn == "redis://127.0.0.1:6399/7"
    finally:
        del os.environ["TENEURIS_LOG_LEVEL"]
        del os.environ["TENEURIS_DATA_DIR"]
        del os.environ["TENEURIS_STORAGE_REDIS_DSN"]


def test_env_override_numeric_secret_stays_string():
    """SecretStr 字段不做 int 转换（纯数字密钥保持字符串语义）。"""
    os.environ["TENEURIS_ADMIN_API_KEY"] = "1234567890"
    try:
        s = load_settings()
        assert s.admin.api_key.get_secret_value() == "1234567890"
    finally:
        del os.environ["TENEURIS_ADMIN_API_KEY"]


def test_env_override_fallback_nested_for_unknown_keys():
    """未匹配 schema 的键回退朴素嵌套（向后兼容）。"""
    os.environ["TENEURIS_TEST_KEY"] = "v1"
    try:
        data = _apply_env_overrides({})
        assert data["test"]["key"] == "v1"
    finally:
        del os.environ["TENEURIS_TEST_KEY"]


def test_env_override_dict_field_pattern():
    """dict 字段剩余 parts 拼为 key 且值保持原始字符串。"""
    os.environ["TENEURIS_PII_PATTERNS_APIKEY"] = r"\d{6}"
    try:
        s = load_settings()
        assert s.pii.patterns["apikey"] == r"\d{6}"
    finally:
        del os.environ["TENEURIS_PII_PATTERNS_APIKEY"]


def test_redaction_covers_dsn_password_only_and_text_path():
    """DSN 内嵌凭据（含无用户名形态）在文本脱敏中被替换（审查 09-04 补漏）。"""
    from trpc_service.config.redaction import redact

    text = "dsn=redis://:secretpwd@host:6379/0 other=redis://bob:pw2@db.internal/0"
    out = redact(text)
    assert "secretpwd" not in out and "pw2" not in out, out
    assert "host:6379/0" in out and "db.internal/0" in out, "应保留主机信息仅脱敏凭据"


def test_json_and_text_formatters_redact_secrets():
    """两种日志格式（JSON/文本）的消息与异常堆栈都必须过脱敏（审查 09-04）。"""
    import io
    import logging

    from trpc_service.log.logger import setup_logging

    for json_output in (True, False):
        buf = io.StringIO()
        setup_logging(level="DEBUG", json_output=json_output)
        root = logging.getLogger("teneuris")
        for h in root.handlers:
            h.stream = buf
        try:
            raise RuntimeError("dsn=redis://bob:topsecret@db/0 key sk-abcdefghijklmnop")
        except Exception:
            root.warning("op failed", exc_info=True)
        out = buf.getvalue()
        assert "topsecret" not in out, f"json_output={json_output} 堆栈泄漏"
        assert "sk-abcdefghijklmnop" not in out, f"json_output={json_output} 消息泄漏"


# ------------------------------------------------------------------
# 生产安全 fail-closed 校验（env=prod 拒绝危险配置启动）
# ------------------------------------------------------------------


def _prod_overrides(**kwargs):
    """prod 合规基线: 在此之上逐项破坏单一条件验证 fail-closed。"""
    base = {
        "env": "prod",
        "log_level": "INFO",
        "admin": {
            "api_key": {
                "env": "TENEURIS_PROD_ADMIN_KEY"
            }
        },
        "storage": {
            "redis": {
                "dsn": "redis://redis.internal:6379/0"
            },
            "sql": {
                "dsn": "mysql+aiomysql://user:pw@db.internal:3306/teneuris"
            },
        },
        "telemetry": {
            "prometheus_enabled": True
        },
        "pii": {
            "enabled": True
        },
    }
    base.update(kwargs)
    return base


@pytest.fixture()
def _prod_admin_key(monkeypatch):
    monkeypatch.setenv("TENEURIS_PROD_ADMIN_KEY", "sk-prod-admin-key-0000000001")


def test_prod_valid_config_passes(_prod_admin_key, tmp_path):
    path = tmp_path / "prod-ok.yaml"
    path.write_text(yaml.safe_dump(_prod_overrides(), allow_unicode=True), encoding="utf-8")
    s = load_settings(path)
    assert s.env == "prod"
    assert s.admin.api_key.get_secret_value() == "sk-prod-admin-key-0000000001"


def test_prod_without_admin_api_key_rejected(tmp_path):
    cfg = _prod_overrides()
    cfg.pop("admin")
    path = tmp_path / "prod.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    with pytest.raises(Exception, match="admin.api_key"):
        load_settings(path)


def test_prod_debug_log_rejected(_prod_admin_key, tmp_path):
    cfg = _prod_overrides(log_level="DEBUG")
    path = tmp_path / "prod.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    with pytest.raises(Exception, match="DEBUG"):
        load_settings(path)


def test_prod_sqlite_dsn_rejected(_prod_admin_key, tmp_path):
    cfg = _prod_overrides(storage={"sql": {"dsn": "sqlite+aiosqlite:///data/teneuris.db"}})
    path = tmp_path / "prod.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    with pytest.raises(Exception, match="sqlite"):
        load_settings(path)


def test_prod_default_redis_dsn_rejected(_prod_admin_key, tmp_path):
    cfg = _prod_overrides(storage={
        "redis": {
            "dsn": "redis://127.0.0.1:6379/0"
        },
        "sql": {
            "dsn": "mysql+aiomysql://user:pw@db.internal:3306/teneuris"
        }
    })
    path = tmp_path / "prod.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    with pytest.raises(Exception, match="Redis"):
        load_settings(path)


def test_prod_pii_disabled_rejected(_prod_admin_key, tmp_path):
    cfg = _prod_overrides(pii={"enabled": False})
    path = tmp_path / "prod.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    with pytest.raises(Exception, match="脱敏"):
        load_settings(path)


def test_prod_prometheus_disabled_rejected(_prod_admin_key, tmp_path):
    cfg = _prod_overrides(telemetry={"prometheus_enabled": False})
    path = tmp_path / "prod.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    with pytest.raises(Exception, match="监控"):
        load_settings(path)


def test_dev_env_skips_production_safety(tmp_path):
    """dev 模式回归: 同样的危险配置必须照常通过（本地开箱即用不受影响）。"""
    path = tmp_path / "dev.yaml"
    path.write_text(yaml.safe_dump({"env": "dev", "log_level": "DEBUG"}, allow_unicode=True), encoding="utf-8")
    s = load_settings(path)
    assert s.log_level == "DEBUG"
