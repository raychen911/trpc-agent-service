# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Demo tools for the 3-tenant SaaS customer-service example.

Each tenant's ``tool_permissions`` whitelist determines which of these tools its
agent may call; ``cancel_order`` is marked dangerous so it triggers the HITL
confirmation flow.
"""

from __future__ import annotations

from trpc_agent_sdk.tools import FunctionTool


def query_order(order_id: str) -> str:
    """查询订单状态。"""
    return f"订单 {order_id}：已发货，预计 2 天内送达。"


def query_logistics(order_id: str) -> str:
    """查询物流轨迹。"""
    return f"订单 {order_id}：包裹正在派送中。"


def query_balance(account: str) -> str:
    """查询账户余额。"""
    return f"账户 {account}：余额 1234.56 元。"


def cancel_order(order_id: str) -> str:
    """取消订单（危险操作，需二次确认）。"""
    return f"订单 {order_id}：已取消。"


_ALL_TOOLS = {
    "query_order": query_order,
    "query_logistics": query_logistics,
    "query_balance": query_balance,
    "cancel_order": cancel_order,
}


def build_tools(tenant) -> list[FunctionTool]:
    """Return the tool set allowed for the tenant (whitelist governs)."""
    whitelist = set(tenant.tool_permissions.tool_whitelist)
    selected = [name for name in _ALL_TOOLS if not whitelist or name in whitelist]
    return [FunctionTool(_ALL_TOOLS[name]) for name in selected]
