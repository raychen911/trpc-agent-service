# ===================================================================
# web - FastAPI 服务化（平台层新增）
# ===================================================================
# 说明: Agent Gateway（webhook + Filter 链 + Runtime）与 Admin API
#   （租户管理 / 审计查询 / 配置下发）两个 FastAPI 应用（PRD 0.2）。
#   Web UI IM 自测页内嵌于 Gateway（/ 与 /chat）。
# 规范: Gateway 无状态可水平扩展；Admin 独立部署内网访问。
# ===================================================================

from .admin import TenantRepository, build_admin_app
from .app import build_gateway_app

__all__ = [
    "TenantRepository",
    "build_admin_app",
    "build_gateway_app",
]
