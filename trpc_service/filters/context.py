# ===================================================================
# filters.context - Gateway 上下文
# ===================================================================
# 说明: Filter 链执行期间的共享上下文（PRD 0.3-2），持有租户配置、
#   存储后端、指标、脱敏器等依赖，由 Gateway 创建并贯穿链路。
# 规范: ctx 只读共享；Filter 需写入的字段写入 AgentEvent/metadata。
# ===================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from ..metrics.metrics import Metrics
from ..storage.base import Storage
from ..tenant.models import TenantConfig
from ..tenant.registry import TenantRegistry


@dataclass
class GatewayContext:
    """Filter 链共享上下文。"""

    registry: Optional[TenantRegistry] = None
    """租户配置注册表（LRU 缓存 + 回源）。"""
    storage: Optional[Storage] = None
    """存储后端（幂等 / 锁 / 审计）。单例模式下即唯一后端；manager 模式为兜底。"""
    storage_manager: Any = None
    """按租户懒建 Storage 的容器（PRD 2.1）；None 时所有租户共用 `storage`。"""
    metrics: Optional[Metrics] = None
    """指标注册表。"""
    tenant: Optional[TenantConfig] = None
    """已解析的租户配置（TenantResolveFilter 写入）。"""
    rate_limiter: Any = None
    """租户限流器（RateLimitFilter 使用）。"""
    redactor: Any = None
    """PII 脱敏器（PIIFilter / AuditFilter 使用）。"""
    signature_verifiers: dict[str, Any] = field(default_factory=dict)
    """channel_type -> 验签可调用对象 (body, signature) -> bool。"""
    extras: dict[str, Any] = field(default_factory=dict)
    """扩展依赖注入点（测试 / 自定义 Filter）。"""

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default) or self.extras.get(key, default)

    async def resolve_storage(self, tenant: Optional[TenantConfig] = None) -> Optional[Storage]:
        """按租户解析 Storage（PRD 2.1）。

        有 storage_manager 且给出 tenant 时，按其 backends 懒建/取缓存；
        否则回落单例 storage（幂等 / 锁等共享基础设施语义）。
        """
        if self.storage_manager is not None and tenant is not None:
            return await self.storage_manager.get(tenant)
        return self.storage
