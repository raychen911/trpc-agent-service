# ===================================================================
# metrics - Prometheus 指标（平台层扩展）
# ===================================================================
# 说明: 平台统一指标注册表（PRD 4.2），全部带 tenant_id label:
#   agent_requests_total / llm_latency_seconds / llm_tokens_*_total /
#   tool_calls_total / tool_latency_seconds / im_delivery_*_total /
#   tenant_cost_usd / tenant_budget_usd / agent_errors_total
# 规范: get_metrics() 获取全局实例；/metrics 端点由 web 模块暴露。
# ===================================================================

from .metrics import Metrics, get_metrics, reset_metrics

__all__ = ["Metrics", "get_metrics", "reset_metrics"]
