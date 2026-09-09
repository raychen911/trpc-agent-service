# ===================================================================
# tenant.gray - 按用户比例灰度选版（纯函数）
# ===================================================================
# 说明: PRD 5.2 灰度发布——租户配置可带 GrayConfig（enabled/percent/canary），
#   Gateway 每请求按 `hash(user_id) % 100 < percent` 决定该用户走 canary
#   覆盖后的配置还是原配置。
#   - 纯函数 + 确定性哈希：跨节点、跨进程分流结果一致（共享配置即可）
#   - canary 覆盖 TenantConfig 顶层**单值子配置**（model/app/tools/audit/
#     backends：dict -> 子模型 model_validate）与标量（预算/限流等）；
#     im（列表）等复合字段不做覆盖（语义复杂，避免误配置）
#   - 未启用 / percent=0 / 未命中 → 返回原配置
# 规范: 在租户上下文解析后、执行前调用一次；结果仅作用于本次请求。
# ===================================================================

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from .models import TenantConfig

# canary 可覆盖的字段（单 BaseModel 子配置）: 值须为 dict，经子模型校验
_SINGLE_MODEL_FIELDS = {"app", "model", "tools", "audit", "backends"}
# canary 可覆盖的标量字段（预算 / 限流）
_SCALAR_FIELDS = {"monthly_budget_usd", "used_budget_usd", "rate_limit_per_min"}


def _build_update(tenant: TenantConfig, canary: dict[str, Any]) -> dict[str, Any]:
    """把 canary dict 转成可安全 model_copy 的顶层更新（子模型重新校验）。"""
    updates: dict[str, Any] = {}
    for field_name, value in canary.items():
        if field_name in _SINGLE_MODEL_FIELDS and isinstance(value, dict):
            current = getattr(tenant, field_name)
            if isinstance(current, BaseModel):
                updates[field_name] = type(current).model_validate(value)
        elif field_name in _SCALAR_FIELDS and not isinstance(value, (dict, list)):
            updates[field_name] = value
        # 其余字段忽略（如 im 列表、未知字段）——覆盖语义需显式，不静默部分应用
    return updates


def apply_gray(tenant: TenantConfig, user_id: str) -> TenantConfig:
    """按用户命中灰度则返回 canary 覆盖后的租户配置，否则原配置。

    Args:
        tenant: 租户配置（含 gray 段）
        user_id: 内部用户标识（已身份映射）

    Returns:
        TenantConfig: 命中 canary 时为新配置实例；未命中/未启用返回原实例。
    """
    gray = tenant.gray
    if not gray.enabled or gray.percent <= 0 or not gray.canary:
        return tenant
    if not gray.hit(user_id):
        return tenant
    updates = _build_update(tenant, gray.canary)
    if not updates:
        return tenant
    return tenant.model_copy(update=updates)
