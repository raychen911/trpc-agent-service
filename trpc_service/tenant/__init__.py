# ===================================================================
# tenant - 多租户模型与解析（平台层新增）
# ===================================================================
# 说明: 租户是平台一等公民（PRD 1.x）。
#   - models: TenantConfig 等 Pydantic 模型，与 SQL 表一一对应
#   - resolver: 从 Header/子域名/webhook path 解析 tenant_id，生成 session_id
#   - registry: 租户配置本地 LRU 缓存 + 异步回源
# 规范: 所有跨租户访问必须携带 tenant_id（行级隔离），密钥字段不落盘。
# ===================================================================

from .models import (
    AppConfig,
    AuditPolicy,
    ChannelType,
    DataBackendConfig,
    ImChannelConfig,
    ModelConfig,
    TenantConfig,
    TenantStatus,
    ToolPermissions,
    tenant_from_dict,
)
from .registry import LoadFn, TenantRegistry
from .resolver import (
    HEADER_TENANT_ID,
    ResolvedRequest,
    generate_session_id,
    parse_webhook_path,
    resolve_tenant_id,
)

__all__ = [
    "AppConfig",
    "AuditPolicy",
    "ChannelType",
    "DataBackendConfig",
    "HEADER_TENANT_ID",
    "ImChannelConfig",
    "LoadFn",
    "ModelConfig",
    "ResolvedRequest",
    "TenantConfig",
    "TenantRegistry",
    "TenantStatus",
    "ToolPermissions",
    "generate_session_id",
    "parse_webhook_path",
    "resolve_tenant_id",
    "tenant_from_dict",
]
