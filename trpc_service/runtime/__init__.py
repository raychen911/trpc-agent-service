# ===================================================================
# runtime - 运行时编排（平台层新增）
# ===================================================================
# 说明: 在框架 Runner 之外补上多租户上下文注入与「事件流 -> IM 回复」桥接:
#   - Runtime: 编排器（租户配置注入 / Session-Memory 读写 / 事件流消费）
#   - AgentRunner: Runner 抽象（FrameworkAgentRunner 生产 / MockAgentRunner 本地自测）
#   - RunnerEvent: 归一化事件（content / tool_call / tool_result / done / error）
# 规范: Worker 无状态，状态经 Storage Adapter 读写共享后端。
# ===================================================================

from .events import RunnerEvent
from .runner import AgentRunner, FrameworkAgentRunner, MockAgentRunner
from .runtime import Runtime

__all__ = [
    "AgentRunner",
    "FrameworkAgentRunner",
    "MockAgentRunner",
    "RunnerEvent",
    "Runtime",
]
