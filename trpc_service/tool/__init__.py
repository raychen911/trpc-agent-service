# ===================================================================
# tool - 工具注册、租户过滤与框架适配（平台层新增，框架复用为主）
# ===================================================================
# 说明: PRD 0.4「Tool / MCP 复用框架 FunctionTool」——平台层负责:
#   - ToolRegistry: 内置工具注册（echo/get_time/calculator/web_search/delete_file）
#   - select_tools: 按租户 tool_permissions 过滤（白名单/黑名单/危险）
#   - make_framework_tool: 平台 ToolSpec -> 框架 FunctionTool（含危险工具门控）
#   - ToolContext: 自测用工具执行上下文
#
# Agent 组装（模型 + 工具集 + 指令 -> LlmAgent）在 agent/builder.py，
# 依赖方向单向 agent -> tool，本包不引用 agent。
# 规范: 工具实现为纯 async 函数，租户上下文经 kwargs 注入。
# ===================================================================

from .builder import (
    ToolContext,
    make_framework_tool,
    make_knowledge_search_spec,
    run_tool_with_context,
    select_tools,
    tool_confirmation_required,
)
from .registry import (
    ToolFunc,
    ToolRegistry,
    ToolSpec,
    build_default_registry,
    execute_tool,
    get_default_registry,
    reset_default_registry,
)

__all__ = [
    "ToolContext",
    "ToolFunc",
    "ToolRegistry",
    "ToolSpec",
    "build_default_registry",
    "execute_tool",
    "get_default_registry",
    "make_framework_tool",
    "make_knowledge_search_spec",
    "reset_default_registry",
    "run_tool_with_context",
    "select_tools",
    "tool_confirmation_required",
]
