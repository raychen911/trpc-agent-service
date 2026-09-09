# ===================================================================
# config.settings - 平台级配置模型
# ===================================================================
# 说明: 平台层全局配置（服务端口 / 存储后端 / 日志 / 遥测），Pydantic 校验。
#   密钥字段一律使用 SecretStr 或经 redaction 脱敏，禁止明文入日志。
# 规范: 与 PRD 0.5 目录结构对应；配置来源: yaml 文件 + 环境变量覆盖
# ===================================================================

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, SecretStr, model_validator


class ServerSettings(BaseModel):
    """服务监听配置（Gateway / Worker / Admin 通用）。"""

    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)
    workers: int = Field(default=1, ge=1, le=64)
    """并发 Worker 协程数 / uvicorn worker 数。"""


class GatewaySettings(ServerSettings):
    """Agent Gateway: 协议接入 + Filter 治理 + 路由，无状态多实例。

    Worker 内嵌于 Gateway（Runtime 编排），无独立 Worker 服务——
    多节点水平扩展靠多副本 Gateway + 共享后端（PRD 1.3）。
    """


class AdminSettings(ServerSettings):
    """Admin API: 租户管理 / 配置下发 / 审计查询 / 灰度控制。"""

    # 显式默认 8002，避免不带 config 时与 Gateway(8000) 撞端口（09-06 联调发现）
    port: int = Field(default=8002, ge=1, le=65535)
    api_key: Optional[SecretStr] = None
    """Admin 接口访问密钥（可选，部署层注入）。"""


class RedisSettings(BaseModel):
    """Redis 后端: Session / 幂等 / 缓存 / 分布式锁。"""

    dsn: str = "redis://127.0.0.1:6379/0"
    pool_size: int = Field(default=16, ge=1, le=256)


class SqlSettings(BaseModel):
    """SQL 后端: 租户 / 审计 / Summary 持久化。"""

    dsn: str = "sqlite+aiosqlite:///data/teneuris.db"
    """支持 sqlite+aiosqlite / mysql+aiomysql / postgresql+asyncpg。"""
    echo: bool = False


class VectorSettings(BaseModel):
    """向量库后端: Knowledge / Memory 检索（默认禁用，本地开发可用空实现）。"""

    dsn: Optional[str] = None
    collection_prefix: str = "tenant_"


class S3Settings(BaseModel):
    """对象存储后端: Artifact / 备份（默认禁用）。"""

    endpoint: Optional[str] = None
    bucket: str = "teneuris"
    access_key: Optional[SecretStr] = None
    secret_key: Optional[SecretStr] = None


class StorageSettings(BaseModel):
    """统一存储配置，对应 PRD 2.x 各数据域后端选择。"""

    redis: RedisSettings = Field(default_factory=RedisSettings)
    sql: SqlSettings = Field(default_factory=SqlSettings)
    vector: VectorSettings = Field(default_factory=VectorSettings)
    s3: S3Settings = Field(default_factory=S3Settings)


class TelemetrySettings(BaseModel):
    """可观测性: Prometheus（tracing 为自研 trace_id 贯穿，见 PRD 4.3）。"""

    prometheus_enabled: bool = True


class PiiSettings(BaseModel):
    """PII 脱敏规则（PRD 1.4 / 4.5）。"""

    enabled: bool = True
    patterns: dict[str, str] = Field(
        default_factory=lambda: {
            "phone": r"1[3-9]\d{9}",
            "id_card": r"\d{17}[\dXx]",
            "bank_card": r"\d{16,19}",
            "email": r"[\w.+-]+@[\w-]+\.[\w.]+",
            "api_key": r"(sk|pk|AKIA)[A-Za-z0-9_-]{16,}",
        })
    """脱敏规则名 -> 正则；命中替换为 [REDACTED]。"""


_DEFAULT_REDIS_DSN = RedisSettings().dsn
"""Redis 默认 DSN（本机开发口径）；prod 环境必须显式覆盖为共享实例。"""


class PlatformSettings(BaseModel):
    """平台全局配置根模型。"""

    service_name: str = "trpc-agent-service"
    env: str = Field(default="dev", pattern="^(dev|test|prod)$")
    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    data_dir: str = "data"
    """本地数据目录（SQLite / 临时文件），对应题目骨架 data/。"""

    gateway: GatewaySettings = Field(default_factory=GatewaySettings)
    admin: AdminSettings = Field(default_factory=AdminSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    telemetry: TelemetrySettings = Field(default_factory=TelemetrySettings)
    pii: PiiSettings = Field(default_factory=PiiSettings)

    @model_validator(mode="after")
    def enforce_production_safety(self) -> "PlatformSettings":
        """生产环境（env=prod）fail-closed：危险配置直接拒启，不带病运行。

        dev / test 不触发，保证本地开箱即用。校验项与依据:
        - admin.api_key 必填: Admin 持有租户配置与审计查询，无鉴权即裸奔（PRD 4.5）
        - log_level 禁 DEBUG: 详尽日志放大敏感信息泄露面（PRD 4.5）
        - pii.enabled 强制开启: 脱敏是生产硬性要求（PRD 4.5）
        - SQL DSN 禁 sqlite: 单文件库与多节点水平扩展主张冲突（PRD 2.2）
        - Redis DSN 禁默认值: 默认指向本机，生产必须显式配置共享实例（PRD 1.3）
        - prometheus_enabled 强制开启: 生产必须有监控打点（PRD 4.2）
        """
        if self.env != "prod":
            return self
        errors: list[str] = []
        if self.admin.api_key is None:
            errors.append("admin.api_key 未配置（Admin 接口生产环境必须开启鉴权）")
        if self.log_level == "DEBUG":
            errors.append("log_level=DEBUG 禁止用于生产（敏感信息泄露面）")
        if not self.pii.enabled:
            errors.append("pii.enabled=false 禁止用于生产（脱敏必须开启）")
        if self.storage.sql.dsn.startswith("sqlite"):
            errors.append("storage.sql.dsn 为 sqlite，生产环境须使用 mysql/postgres 等服务化数据库")
        if self.storage.redis.dsn == _DEFAULT_REDIS_DSN:
            errors.append("storage.redis.dsn 仍为本机默认值，生产环境须显式配置共享 Redis 实例")
        if not self.telemetry.prometheus_enabled:
            errors.append("telemetry.prometheus_enabled=false 禁止用于生产（监控打点必须开启）")
        if errors:
            raise ValueError("生产安全校验未通过（env=prod）:\n  - " + "\n  - ".join(errors))
        return self
