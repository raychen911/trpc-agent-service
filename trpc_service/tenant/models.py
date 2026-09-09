# ===================================================================
# tenant.models - 租户模型（平台层新增）
# ===================================================================
# 说明: 租户是平台一等公民（PRD 1.1），TenantConfig 与 SQL 表一一对应。
#   app_config / model_config / tool_permissions / im_channel_config /
#   data_backend_config / audit_policy 使用 dict 承载，与 JSON 列存储一致。
# 规范: 所有跨租户查询必须携带 tenant_id（行级隔离，PRD 1.4）。
# ===================================================================

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, SecretStr

TenantStatus = Literal["active", "suspended", "deleted"]
ChannelType = Literal["wechat_work", "feishu", "web", "wecom_bot", "feishu_sdk"]


class ModelConfig(BaseModel):
    """租户模型配置（PRD 1.1 model_config 结构化）。"""

    provider: str = "openai"
    """模型提供商: openai / anthropic / deepseek / custom。"""
    model_name: str = "gpt-4o-mini"
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(default=1024, ge=1, le=128000)
    api_key_ref: Optional[SecretStr] = None
    """模型 API key（密钥环境注入，不落盘不入日志，PRD 4.5）。"""
    base_url: Optional[str] = None
    """自定义 provider 的 endpoint（可选）。"""
    input_price_per_1m_usd: float = Field(default=0.0, ge=0.0)
    """输入 token 单价（USD / 每百万 token）；0 表示不计成本（tokens 照常计数）。"""
    output_price_per_1m_usd: float = Field(default=0.0, ge=0.0)
    """输出 token 单价（USD / 每百万 token）；0 表示不计成本（tokens 照常计数）。"""


class AppConfig(BaseModel):
    """Agent 应用配置（PRD 1.1 app_config 结构化）。"""

    agent_type: Literal["llm", "chain", "graph"] = "llm"
    system_prompt: str = "你是一个乐于助人的 AI 助手。"
    max_rounds: int = Field(default=10, ge=1, le=100)
    """单次会话最大推理轮数（防失控，PRD 6-10）。"""


class ToolPermissions(BaseModel):
    """工具权限（PRD 1.1 tool_permissions 结构化）。"""

    allowlist: list[str] = Field(default_factory=list)
    """允许的工具名白名单；为空表示不限制（配合平台默认集）。"""
    blocklist: list[str] = Field(default_factory=list)
    """黑名单，优先级高于白名单。"""
    dangerous_tools: list[str] = Field(default_factory=list)
    """危险工具集，命中需二次确认（PRD 4.1）。"""
    require_confirmation: bool = True
    """危险工具二次确认总开关。"""

    def is_allowed(self, tool_name: str) -> bool:
        """白名单/黑名单判定。"""
        if tool_name in self.blocklist:
            return False
        return not self.allowlist or tool_name in self.allowlist

    def requires_confirmation(self, tool_name: str) -> bool:
        """是否命中危险工具二次确认。"""
        return self.require_confirmation and tool_name in self.dangerous_tools


class UserAcl(BaseModel):
    """IM 用户级权限（PRD 4.1 UserAuthFilter，区别于租户级鉴权）。

    同一租户下按 allowlist / blocklist 判定该 IM 用户能否使用 bot
    （如内部员工白名单、黑名单拉黑）；空 allowlist 表示不限制。
    """

    enabled: bool = True
    allowlist: list[str] = Field(default_factory=list)
    """允许的 user_id 白名单；为空表示不限制。"""
    blocklist: list[str] = Field(default_factory=list)
    """拉黑名单，优先级高于白名单。"""

    def is_allowed(self, user_id: str) -> bool:
        if not self.enabled:
            return True
        if user_id in self.blocklist:
            return False
        return not self.allowlist or user_id in self.allowlist


class ImChannelConfig(BaseModel):
    """IM 通道绑定配置（PRD 1.1 im_channel_config 结构化 / 3.4）。

    企微自建应用（wechat_work）凭证字段:
      - app_id: 企微 corp_id
      - agent_id: 企微自建应用 AgentId
      - token_ref: 回调验签 Token
      - secret_ref: 应用 Secret（corpsecret）
      - aes_key_ref: 回调 EncodingAESKey

    飞书 webhook（feishu）凭证字段:
      - app_id: 飞书自建应用 AppID；secret_ref: AppSecret
      - token_ref（verification_token）/ aes_key_ref（encrypt_key）

    企微智能机器人·长连接（wecom_bot，PRD 3.3 第二接入形态）:
      - app_id: 智能机器人 BotID
      - secret_ref: 智能机器人 Secret（长连接专用，非 corpsecret）
      - 无 token_ref / aes_key_ref / webhook_path（WSS 出站连接，非 HTTP 回调）

    飞书官方 SDK·长连接（feishu_sdk，PRD 3.3 第二实现形态）:
      - app_id: 飞书自建应用 AppID；secret_ref: AppSecret
      - aes_key_ref / token_ref（verification）仅 webhook 形态用，长连接不需要
      - default_target: 兜底 open_id（可选）
    """

    channel_type: ChannelType = "web"
    webhook_path: str = ""
    """如 /webhook/wechat_work/demo__wecom，binding_id 隐含 tenant_id。"""
    app_id: str = ""
    """企微 corp_id / 飞书 AppID。"""
    agent_id: str = ""
    """企微自建应用 AgentId。"""
    token_ref: Optional[SecretStr] = None
    """回调验签 Token（企微 / 飞书 verification_token）。"""
    secret_ref: Optional[SecretStr] = None
    """应用 Secret（企微 corpsecret / 飞书 AppSecret），获取 access_token 用。"""
    aes_key_ref: Optional[SecretStr] = None
    """回调 EncodingAESKey（企微安全模式加解密 / 飞书 encrypt_key）。"""
    user_id_mapping: dict[str, str] = Field(default_factory=dict)
    """外部字段名 -> 内部 user_id 字段名（PRD 3.4 身份映射）。"""
    user_acl: UserAcl = Field(default_factory=UserAcl)
    """IM 用户级黑白名单（PRD 4.1 UserAuthFilter）。"""
    default_target: Optional[str] = None
    """默认发送目标（open_id / user_id / chat_id 等），缺省时由消息 metadata 提供。
    演示租户从环境变量（如 FEISHU_DEMO_USER_OPEN_ID）注入，方便换测试账号只改 .env。"""


class DataBackendConfig(BaseModel):
    """数据后端选择（PRD 2.1，各数据域独立后端）。"""

    session: Literal["inmemory", "redis", "sql"] = "redis"
    memory: Literal["inmemory", "redis", "vector"] = "redis"
    summary: Literal["inmemory", "redis", "sql"] = "sql"
    artifact: Literal["inmemory", "s3"] = "inmemory"
    knowledge: Literal["inmemory", "redis", "vector"] = "inmemory"
    audit: Literal["inmemory", "sql"] = "sql"


class AuditPolicy(BaseModel):
    """审计策略（PRD 1.1 audit_policy 结构化 / 4.4）。"""

    enabled: bool = True
    retention_days: int = Field(default=180, ge=1, le=3650)
    sensitive_fields: list[str] = Field(default_factory=lambda: ["content", "tool_input"])
    """入库前需脱敏的字段名列表。"""
    mask_regex: dict[str, str] = Field(default_factory=dict)
    """自定义脱敏规则（叠加平台默认 PII 规则）。"""


class GrayConfig(BaseModel):
    """租户级按比例灰度配置（PRD 5.2 灰度发布 / §4 附录）。

    请求级选版：Gateway 按 `hash(user_id) % 100 < percent` 决定该用户走
    canary（覆盖后配置）还是原配置。canary 只覆盖**顶层子配置**（如
    {"model": {...}} / {"app": {...}}），不做任意深层 JSON 合并（避免语义
    爆炸）；未命中即原配置。跨节点一致性由「配置共享 + 请求级纯函数」保证。
    """

    enabled: bool = False
    percent: int = Field(default=0, ge=0, le=100)
    """命中 canary 的用户百分比（0-100）。"""
    canary: dict[str, Any] = Field(default_factory=dict)
    """canary 覆盖的子配置（如 {"model": {"model_name": "gpt-4o"}}）。"""

    def hit(self, user_id: str) -> bool:
        """用户是否命中 canary（稳定哈希分流，跨进程/节点结果一致）。

        不用内置 hash()（PYTHONHASHSEED 随机化导致跨进程不稳定），
        用 sha256 取整稳定分流。
        """
        if not self.enabled or self.percent <= 0:
            return False
        import hashlib

        digest = int(hashlib.sha256(user_id.encode("utf-8")).hexdigest(), 16)
        return (digest % 100) < self.percent


class TenantConfig(BaseModel):
    """租户核心模型，与 SQL 表 tenant 一一对应（PRD 1.1 / 2.5）。"""

    tenant_id: str = Field(min_length=1, max_length=64)
    name: str = Field(max_length=128)
    status: TenantStatus = "active"

    app: AppConfig = Field(default_factory=AppConfig)
    model: ModelConfig = Field(default_factory=ModelConfig)
    tools: ToolPermissions = Field(default_factory=ToolPermissions)
    im: list[ImChannelConfig] = Field(default_factory=list)
    backends: DataBackendConfig = Field(default_factory=DataBackendConfig)
    audit: AuditPolicy = Field(default_factory=AuditPolicy)
    gray: GrayConfig = Field(default_factory=GrayConfig)
    """按比例灰度（PRD 5.2）：enabled 时按 user_id 哈希分流到 canary 覆盖配置。"""

    # 成本预算（PRD 4.2 tenant_cost_usd / 6-10 成本失控）
    monthly_budget_usd: float = Field(default=0.0, ge=0.0)
    """月度预算上限（USD），0 表示不限。"""
    used_budget_usd: float = Field(default=0.0, ge=0.0)
    """已用预算（运行时累计，BudgetFilter 读取）。"""
    rate_limit_per_min: int = Field(default=60, ge=0)
    """租户级每分钟请求上限（0 表示不限）。"""

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    def budget_exceeded(self) -> bool:
        """是否超出月度预算（BudgetFilter 硬限）。"""
        return 0 < self.monthly_budget_usd <= self.used_budget_usd

    def find_channel(self, channel_type: ChannelType, webhook_path: str = "") -> Optional[ImChannelConfig]:
        """按通道类型 / webhook path 查找绑定配置。

        webhook_path 非空时要求与配置声明的 webhook_path 精确一致（绑定校验，
        PRD 3.4）——webhook 入口以此拒绝未声明的 binding 后缀；为空时返回该
        通道类型的第一个绑定（治理 Filter 等内部调用方用法）。
        """
        for cfg in self.im:
            if cfg.channel_type != channel_type:
                continue
            if webhook_path and cfg.webhook_path != webhook_path:
                continue
            return cfg
        return None


# 兼容 PRD 1.1 的扁平 dict 写法: 允许直接传 app_config 等 key 自动展开
_TENANT_ALIASES: dict[str, str] = {
    "app_config": "app",
    "model_config": "model",
    "tool_permissions": "tools",
    "im_channel_config": "im",
    "data_backend_config": "backends",
    "audit_policy": "audit",
    "gray_config": "gray",
}


def tenant_from_dict(data: dict[str, Any]) -> TenantConfig:
    """从存储层（SQL/Redis）读出的扁平 dict 构建 TenantConfig。

    兼容 PRD 1.1 的嵌套命名（app_config 等）与结构化命名（app 等）。
    """
    normalized = dict(data)
    for alias, field in _TENANT_ALIASES.items():
        if alias in normalized and field not in normalized:
            normalized[field] = normalized.pop(alias)
    return TenantConfig.model_validate(normalized)
