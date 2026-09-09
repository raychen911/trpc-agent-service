# ===================================================================
# tool.registry - 工具注册表与权限过滤（平台层新增）
# ===================================================================
# 说明: 平台内置工具集中注册，运行时按租户 tool_permissions
#   （allowlist / blocklist / dangerous_tools）过滤（PRD 1.4 / 4.1）。
#   框架 FunctionTool 需要 InvocationContext，故工具实现保持
#   纯 async 函数签名（**kwargs），由 builder 适配为框架工具。
# 规范: 危险工具标记 dangerous=True，触发二次确认（PRD 4.1）。
# ===================================================================

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from ..tenant.models import ToolPermissions

ToolFunc = Callable[..., Awaitable[Any]]
"""工具实现: async fn(*, tenant_id, user_id, session_id, **args) -> Any"""


@dataclass
class ToolSpec:
    """平台工具定义。"""

    name: str
    description: str
    func: ToolFunc
    parameters: dict[str, Any] = field(default_factory=dict)
    """JSON Schema 参数描述（框架 FunctionTool 声明用）。"""
    dangerous: bool = False
    """危险工具标记（二次确认）。"""


class ToolRegistry:
    """工具注册表（进程内单例模式）。"""

    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> "ToolRegistry":
        if spec.name in self._tools:
            raise ValueError(f"工具重复注册: {spec.name}")
        self._tools[spec.name] = spec
        return self

    def unregister(self, name: str) -> None:
        self._tools.pop(name, None)

    def get(self, name: str) -> Optional[ToolSpec]:
        return self._tools.get(name)

    def all(self) -> list[ToolSpec]:
        return list(self._tools.values())

    def filter_by_permissions(self, perms: ToolPermissions) -> list[ToolSpec]:
        """按租户工具权限过滤（白名单 > 黑名单 > 危险标记，PRD 1.4）。"""
        result: list[ToolSpec] = []
        for spec in self._tools.values():
            if not perms.is_allowed(spec.name):
                continue
            result.append(spec)
        return result


# ---------------------------------------------------------------------------
# 内置工具
# ---------------------------------------------------------------------------


async def _echo(*, content: str, **kwargs: Any) -> Any:
    """回显工具（本地自测）。"""
    return {"echo": content}


async def _now(**kwargs: Any) -> Any:
    """获取当前时间戳。"""
    return {"timestamp": int(time.time()), "iso": time.strftime("%Y-%m-%d %H:%M:%S")}


async def _calculator(*, expression: str, **kwargs: Any) -> Any:
    """安全计算器: 仅支持数字与四则运算（防注入）。"""
    import re

    # 限长 + 禁幂运算（**），防超大整数 DoS
    if len(expression) > 128 or "**" in expression:
        return {"error": "expression too long or exponentiation not allowed"}
    if not re.fullmatch(r"[0-9+\-*/().\s]+", expression):
        return {"error": "expression contains invalid characters"}
    # 用 Python 内置求值但先白名单校验（已拒绝字母/关键字）
    try:
        value = eval(expression, {"__builtins__": {}}, {})  # noqa: S307 - 白名单后求值
        return {"result": value}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


async def _web_search(*, query: str, **kwargs: Any) -> Any:
    """Web 搜索（占位实现，生产接入搜索 API）。"""
    return {"query": query, "results": [], "notice": "web_search 为占位实现，请配置搜索后端"}


async def _delete_file(*, path: str, **kwargs: Any) -> Any:
    """删除文件（危险工具示例: 需二次确认）。"""
    import os

    if os.path.isabs(path) and path.startswith("/etc"):
        return {"error": "refusing to delete system path"}
    # 结果诚实化（审查 09-04）：演示工具不实际删除文件，此前返回
    # {"deleted": path} 对 LLM 撒谎——"声称已删"比"拒绝删"更危险。
    return {"simulated": True, "path": path, "note": "演示工具，未实际删除文件"}


def build_default_registry() -> ToolRegistry:
    """创建含内置工具的注册表。"""
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="echo",
            description="回显输入内容（本地自测用）",
            func=_echo,
            parameters={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string"
                    }
                }
            },
        ))
    registry.register(
        ToolSpec(
            name="get_time",
            description="获取当前时间",
            func=_now,
            parameters={
                "type": "object",
                "properties": {}
            },
        ))
    registry.register(
        ToolSpec(
            name="calculator",
            description="执行数学计算（四则运算）",
            func=_calculator,
            parameters={
                "type": "object",
                "properties": {
                    "expression": {
                        "type": "string"
                    }
                }
            },
        ))
    registry.register(
        ToolSpec(
            name="web_search",
            description="搜索互联网获取最新信息",
            func=_web_search,
            parameters={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string"
                    }
                }
            },
        ))
    registry.register(
        ToolSpec(
            name="delete_file",
            description="删除指定路径文件（危险操作）",
            func=_delete_file,
            parameters={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string"
                    }
                }
            },
            dangerous=True,
        ))
    return registry


# 全局默认注册表（可被测试替换）
_default_registry: Optional[ToolRegistry] = None


def get_default_registry() -> ToolRegistry:
    """获取全局默认工具注册表（懒初始化）。"""
    global _default_registry
    if _default_registry is None:
        _default_registry = build_default_registry()
    return _default_registry


def reset_default_registry() -> None:
    """重置全局注册表（测试用）。"""
    global _default_registry
    _default_registry = None


async def execute_tool(spec: ToolSpec, *, tenant_id: str, user_id: str, session_id: str, args: dict[str, Any]) -> Any:
    """统一工具执行入口（带租户上下文注入，PRD 0.3 工具调用）。"""
    return await spec.func(tenant_id=tenant_id, user_id=user_id, session_id=session_id, **args)
