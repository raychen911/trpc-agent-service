# 项目记忆

## 项目信息
- 项目名：trpc-agent-service
- 路径：D:\trpc\trpc-agent-service
- 框架：trpc_agent_sdk（pip 包名 trpc-agent-py）
- Python 路径：.venv\Scripts\python.exe

## 框架能力（直接用）
| 能力 | 模块 | 说明 |
|------|------|------|
| Agent 编排 | agents/ | LlmAgent, ChainAgent, ParallelAgent 等 |
| Session 管理 | sessions/ | InMemory/Redis/SQL SessionService |
| Memory 管理 | memory/ | InMemory/Redis/SQL MemoryService |
| 模型调用 | models/ | OpenAIModel, AnthropicModel 等 |
| Tool 系统 | tools/ | FunctionTool, MCPToolset 等 |
| Runner | runners.py | 执行 Agent，管理 Session/Memory |
| 文件上传 | types/Part | Part.from_uri, Part.from_bytes |

## 我们需要搭建
| 能力 | 模块 | 说明 |
|------|------|------|
| 多租户管理 | tenant/ | 框架没有 |
| 存储工厂 | store/ | 根据配置选择后端 |
| 审计日志 | log/ | 租户级审计 |
| Admin API | web/ | 管理接口 |
| Web UI | web/ | 聊天界面 |

## API 文档位置
- docs/framework_api.md - 框架 API 使用手册
- docs/framework_capabilities.md - 框架能力清单

## 常用导入
```python
# Session
from trpc_agent_sdk.sessions import InMemorySessionService, RedisSessionService, SqlSessionService

# Memory
from trpc_agent_sdk.memory import InMemoryMemoryService, RedisMemoryService, SqlMemoryService

# Model
from trpc_agent_sdk.models import OpenAIModel

# Agent
from trpc_agent_sdk.agents import LlmAgent

# Runner
from trpc_agent_sdk.runners import Runner

# Types
from trpc_agent_sdk.types import Content, Part
```

## 常用初始化
```python
# Session
session_service = InMemorySessionService()
session_service = RedisSessionService(db_url="redis://localhost:6379/0")
session_service = SqlSessionService(db_url="sqlite:///data.db")

# Memory
memory_service = InMemoryMemoryService(enabled=True)
memory_service = RedisMemoryService(db_url="redis://localhost:6379/0", enabled=True)
memory_service = SqlMemoryService(db_url="sqlite:///memory.db", enabled=True)

# Model
model = OpenAIModel(model_name="qwen3.7-flash", api_key="xxx", base_url="https://...")

# Agent
agent = LlmAgent(model=model, instruction="你是一个助手")

# Runner
runner = Runner(app_name="my_app", agent=agent, session_service=session_service, memory_service=memory_service)
```
