from enum import Enum
from pydantic import BaseModel, Field, model_validator


class ModelProvider(str, Enum):
    """模型提供商"""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    DEEPSEEK = "deepseek"
    QWEN = "qwen"


class ModelConfig(BaseModel):
    """模型配置"""
    provider: ModelProvider = ModelProvider.QWEN
    model_name: str = "qwen3.7-flash"
    api_key: str = ""
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    max_tokens: int = Field(default=4096, ge=1, le=128000)
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)


class StorageBackend(str, Enum):
    """存储后端"""
    IN_MEMORY = "in_memory"
    REDIS = "redis"
    SQL = "sql"


class StorageConfig(BaseModel):
    """数据后端配置"""
    session_backend: StorageBackend = StorageBackend.IN_MEMORY
    memory_backend: StorageBackend = StorageBackend.IN_MEMORY
    redis_url: str = "redis://localhost:6379/0"
    sql_url: str = ""


class ChannelType(str, Enum):
    """通道类型"""
    FEISHU = "feishu"
    WECOM = "wecom"                      # HTTP 回调被动回复（需公网+备案域名）
    WECOM_SMARTBOT = "wecom_smartbot"    # 智能机器人长连接（免公网）
    TELEGRAM = "telegram"
    WEB = "web"


class ChannelConfig(BaseModel):
    """IM 通道配置"""
    enabled: bool = False
    bot_id: str = ""
    secret: str = ""
    token: str = ""  # 企微回调 Token / 飞书事件订阅 verification token
    corp_id: str = ""  # 企业微信 CorpID（回复加密的 receiveid）
    encoding_aes_key: str = ""  # 企业微信回调 EncodingAESKey（43 位）
    # 飞书自建应用凭证
    app_id: str = ""
    app_secret: str = ""
    encrypt_key: str = ""  # 飞书事件订阅 Encrypt Key（配置后强制验签）


class AppConfig(BaseModel):
    """应用配置"""
    app_name: str = "default_app"
    description: str = ""
    instruction: str = "你是一个有帮助的助手。"


class ToolConfig(BaseModel):
    """工具权限配置"""
    allowed_tools: list[str] = Field(default_factory=list)
    blocked_tools: list[str] = Field(default_factory=list)


class WorkspaceMode(str, Enum):
    """沙箱模式"""
    LOCAL = "local"
    CONTAINER = "container"


class WorkspaceConfig(BaseModel):
    """工作区沙箱配置"""
    mode: WorkspaceMode = WorkspaceMode.LOCAL
    image: str = "python:3.13-slim"  # 容器沙箱镜像


class SkillConfig(BaseModel):
    """技能配置"""
    enabled: bool = False


class AuditConfig(BaseModel):
    """审计策略"""
    enabled: bool = True
    log_level: str = "info"
    mask_pii: bool = True
    retention_days: int = 90


class TenantConfig(BaseModel):
    """租户配置"""
    tenant_id: str
    name: str
    status: str = "active"
    app: AppConfig = Field(default_factory=AppConfig)
    model: ModelConfig = Field(default_factory=ModelConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    channels: dict[str, ChannelConfig] = Field(default_factory=dict)
    tools: ToolConfig = Field(default_factory=ToolConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)
    workspace: WorkspaceConfig = Field(default_factory=WorkspaceConfig)
    skills: SkillConfig = Field(default_factory=SkillConfig)
    daily_api_calls: int = 10000
    daily_token_budget: int = 1000000
    rate_limit_per_minute: int = 0  # 每用户每分钟消息上限，0 = 不限制（IM 洪峰防护）
    release_stage: str = "stable"  # 灰度发布阶段：canary（灰度先行）| stable（全量）

    @model_validator(mode="after")
    def _normalize_app_name(self):
        """app_name 强制带租户前缀，防止不同租户共享框架数据键空间。"""
        app_name = self.app.app_name or "default_app"
        prefix = f"{self.tenant_id}_"
        if not app_name.startswith(prefix):
            self.app.app_name = prefix + app_name
        return self
