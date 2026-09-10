"""平台内置示例工具。"""
from __future__ import annotations

from datetime import datetime, timedelta

from trpc_agent_sdk.tools import FunctionTool

_DANGEROUS_TOOLS = {"modify_order"}


def query_order(order_id: str) -> dict:
    """根据订单号查询电商订单的状态与物流信息。"""
    if not order_id or len(order_id) < 4:
        return {"error": "订单号无效"}
    return {
        "order_id": order_id,
        "status": "已发货",
        "eta": (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
    }


def check_logistics(order_id: str) -> dict:
    """查询订单的物流轨迹。"""
    return {
        "order_id": order_id,
        "traces": ["已揽收", "运输中", "派送中"],
    }


def search_code(keyword: str) -> dict:
    """在内部代码仓库中搜索代码片段。"""
    return {"keyword": keyword, "files": [f"src/{keyword}/impl.py", f"src/{keyword}/utils.py"]}


def query_docs(keyword: str) -> dict:
    """检索内部技术文档。"""
    return {"keyword": keyword, "docs": [f"docs/{keyword}.md"]}


def modify_order(order_id: str, action: str, confirm: bool = False) -> dict:
    """修改订单状态（危险操作，用户确认后携带 confirm=true 执行）。"""
    if not confirm:
        return {"error": "confirmation_required", "message": "请先向用户确认后再执行"}
    return {"order_id": order_id, "action": action, "result": "已执行"}


def is_dangerous(name: str) -> bool:
    """判断工具是否属于危险操作名单。"""
    return name in _DANGEROUS_TOOLS


def build_example_tools(app_name: str, filters_name: list | None = None) -> list[FunctionTool]:
    """按应用名返回该租户可用的示例工具集（可选挂载治理过滤链）。"""
    return [
        FunctionTool(func=func, filters_name=list(filters_name) if filters_name else None)
        for func in TOOL_FUNCS_BY_APP.get(app_name, [])
    ]


TOOL_FUNCS_BY_APP = {
    "customer_service": [query_order, check_logistics, modify_order],
    "dev_assistant": [search_code, query_docs],
}
