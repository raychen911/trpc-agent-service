# ===================================================================
# metrics.metrics - Prometheus 指标（tenant 维度）
# ===================================================================
# 说明: 平台统一指标注册表，对应 PRD 4.2 指标表，全部带 tenant_id label。
#   指标类型: Counter（请求/错误/成本累计）/ Histogram（延迟）/ Gauge（预算、活跃数）。
# 规范: 使用 get_metrics() 获取全局实例；metric 名统一前缀 agent_/llm_/tool_/im_。
# ===================================================================

from __future__ import annotations

from typing import Optional

from prometheus_client import Counter, Gauge, Histogram

# 默认 label 常量
_LABEL_TENANT = "tenant_id"
_LABEL_CHANNEL = "channel"
_LABEL_MODEL = "model"
_LABEL_TOOL = "tool_name"
_LABEL_ERROR = "error_type"
_LABEL_STAGE = "stage"
_LABEL_STATUS = "status"
_LABEL_MSG_TYPE = "msg_type"
_LABEL_COST_TYPE = "cost_type"
_LABEL_OP = "op"
_LABEL_BACKEND = "backend"


class Metrics:
    """平台指标注册表（PRD 4.2 全部指标）。"""

    def __init__(self, namespace: str = "teneuris") -> None:
        ns = namespace

        # 请求量
        self.agent_requests = Counter(
            f"{ns}_agent_requests_total",
            "Agent 请求总数",
            [_LABEL_TENANT, _LABEL_CHANNEL, "agent_name"],
        )
        self.agent_errors = Counter(
            f"{ns}_agent_errors_total",
            "Agent 错误总数",
            [_LABEL_TENANT, _LABEL_ERROR, _LABEL_STAGE],
        )

        # 模型
        self.llm_latency = Histogram(
            f"{ns}_llm_latency_seconds",
            "模型调用延迟（秒）",
            [_LABEL_TENANT, _LABEL_MODEL, _LABEL_STATUS],
            buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60),
        )
        self.llm_tokens_input = Counter(
            f"{ns}_llm_tokens_input_total",
            "模型输入 token 累计",
            [_LABEL_TENANT, _LABEL_MODEL],
        )
        self.llm_tokens_output = Counter(
            f"{ns}_llm_tokens_output_total",
            "模型输出 token 累计",
            [_LABEL_TENANT, _LABEL_MODEL],
        )

        # 工具
        self.tool_calls = Counter(
            f"{ns}_tool_calls_total",
            "工具调用总数",
            [_LABEL_TENANT, _LABEL_TOOL, _LABEL_STATUS],
        )
        self.tool_latency = Histogram(
            f"{ns}_tool_latency_seconds",
            "工具调用延迟（秒）",
            [_LABEL_TENANT, _LABEL_TOOL],
            buckets=(0.01, 0.05, 0.1, 0.5, 1, 5, 10),
        )

        # IM 投递
        self.im_delivery_success = Counter(
            f"{ns}_im_delivery_success_total",
            "IM 回复投递成功数",
            [_LABEL_TENANT, _LABEL_CHANNEL, _LABEL_MSG_TYPE],
        )
        self.im_delivery_failed = Counter(
            f"{ns}_im_delivery_failed_total",
            "IM 回复投递失败数",
            [_LABEL_TENANT, _LABEL_CHANNEL, _LABEL_MSG_TYPE],
        )
        self.im_delivery_retry = Counter(
            f"{ns}_im_delivery_retry_total",
            "IM 回复投递重试数（未送达类连接错误）",
            [_LABEL_TENANT, _LABEL_CHANNEL, _LABEL_MSG_TYPE],
        )

        # 成本（PRD 4.2 tenant_cost_usd / tenant_budget_usd）
        self.tenant_cost = Counter(
            f"{ns}_tenant_cost_usd",
            "租户累计成本（USD）",
            [_LABEL_TENANT, _LABEL_COST_TYPE],
        )
        self.tenant_budget = Gauge(
            f"{ns}_tenant_budget_usd",
            "租户月度预算（USD）",
            [_LABEL_TENANT],
        )

        # 活跃 session（Gauge，运维观测）
        self.active_sessions = Gauge(
            f"{ns}_active_sessions",
            "当前活跃 session 数",
            [_LABEL_TENANT],
        )

        # Session 后端延迟（PRD 4.2 点名指标：Session 后端延迟）
        self.session_backend_latency = Histogram(
            f"{ns}_session_backend_latency_seconds",
            "Session 后端读写延迟（秒）",
            [_LABEL_OP, _LABEL_BACKEND],
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
        )


# 全局单例（进程内共享）
_instance: Optional[Metrics] = None


def get_metrics(namespace: str = "teneuris") -> Metrics:
    """获取全局指标实例（幂等）。"""
    global _instance
    if _instance is None:
        _instance = Metrics(namespace)
    return _instance


def reset_metrics() -> None:
    """重置全局实例（测试用）。"""
    global _instance
    _instance = None
