# ===================================================================
# agent - Agent 构建与编排（复用为主，平台层薄封装）
# ===================================================================
# 说明: PRD 0.3「复用框架 Runner」——本包是 Agent 相关能力的归属:
#   - agent/builder.py:        build_framework_agent（模型 + 工具集 + 指令 -> LlmAgent）
#   - agent/model_factory.py:  provider -> 框架 LLMModel 实例
#   - agent/summarizer.py:     会话摘要（与 Agent 同源复用模型工厂）
#   执行编排在 runtime/runner.py（FrameworkAgentRunner）。
#
# 依赖方向: agent -> tool（单向）。工具定义/过滤/框架适配属 tool 包，
#   本包不再转出 tool 的符号，避免 agent / tool 双向导入成环。
# ===================================================================

from .builder import build_framework_agent

__all__ = [
    "build_framework_agent",
]
