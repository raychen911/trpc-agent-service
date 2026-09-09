# ===================================================================
# config - 配置加载与校验（平台层新增）
# ===================================================================
# 说明: 平台全局配置（服务 / 存储 / 遥测 / PII 脱敏规则），
#   加载顺序: 默认值 -> yaml -> 环境变量(TENEURIS_ 前缀)。
#   密钥字段支持 {env: NAME} 引用，运行时解密到内存，不落盘不入日志。
# 规范: 使用 load_settings() 获取配置，reload_settings() 热更新。
# ===================================================================

from .loader import ConfigError, load_settings, reload_settings, save_example_config
from .redaction import REDACTED, Redactor, mask_secret, redact, redact_secrets
from .settings import (
    AdminSettings,
    GatewaySettings,
    PiiSettings,
    PlatformSettings,
    RedisSettings,
    S3Settings,
    ServerSettings,
    SqlSettings,
    StorageSettings,
    TelemetrySettings,
    VectorSettings,
)

__all__ = [
    "AdminSettings",
    "ConfigError",
    "GatewaySettings",
    "PiiSettings",
    "PlatformSettings",
    "REDACTED",
    "RedisSettings",
    "Redactor",
    "S3Settings",
    "ServerSettings",
    "SqlSettings",
    "StorageSettings",
    "TelemetrySettings",
    "VectorSettings",
    "load_settings",
    "mask_secret",
    "redact",
    "redact_secrets",
    "reload_settings",
    "save_example_config",
]
