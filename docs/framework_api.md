# trpc_agent_sdk 框架 API 使用手册（官方文档版）

## 1. Session 管理

### 1.1 Session vs Memory

| 特性 | Session | Memory |
|-----|---------|--------|
| **作用域** | 单个会话（session） | 跨会话（所有 session 共享） |
| **生命周期** | 随会话创建和销毁 | 独立于会话，由 TTL 控制 |
| **存储内容** | 当前会话的完整对话历史 | 关键事件和知识片段 |
| **访问方式** | 自动加载到上下文 | 通过 `load_memory` 工具检索 |
| **典型用途** | 单次对话的上下文 | 长期记忆、用户画像、知识积累 |

### 1.2 可用服务

| 服务类 | 初始化参数 | 用途 |
|--------|-----------|------|
| InMemorySessionService | `session_config=None` | 内存版，开发测试用 |
| RedisSessionService | `db_url: str, is_async=True, session_config=None` | Redis 版，生产用 |
| SqlSessionService | `db_url: str, is_async=True, session_config=None` | SQL 版，生产用 |

### 1.3 使用示例

```python
from trpc_agent_sdk.sessions import InMemorySessionService, SessionServiceConfig

# 配置
session_config = SessionServiceConfig(
    event_ttl_seconds=3600,  # 事件 TTL：1 小时
    max_events=100,          # 最大事件数：100
    ttl=SessionServiceConfig.create_ttl_config(
        enable=True,
        ttl_seconds=86400,              # 会话过期时间：24 小时
        cleanup_interval_seconds=3600,  # 清理间隔：1 小时
    ),
)

# 内存版
session_service = InMemorySessionService(session_config=session_config)

# Redis 版
session_service = RedisSessionService(
    db_url="redis://localhost:6379/0",
    is_async=True,
    session_config=session_config,
)

# SQL 版（MySQL）
session_service = SqlSessionService(
    db_url="mysql+pymysql://user:pass@localhost/db?charset=utf8mb4",
    is_async=True,
    session_config=session_config,
)
```

### 1.4 核心方法

```python
# 创建会话
session = await session_service.create_session(
    app_name="my_app",
    user_id="user_001",
    session_id="session_001",  # 可选
    state={"initial_key": "initial_value"}  # 可选
)

# 获取会话
session = await session_service.get_session(
    app_name="my_app",
    user_id="user_001",
    session_id="session_001"
)

# 列出会话
session_list = await session_service.list_sessions(
    app_name="my_app",
    user_id="user_001"
)

# 删除会话
await session_service.delete_session(
    app_name="my_app",
    user_id="user_001",
    session_id="session_001"
)

# 追加事件
await session_service.append_event(session=session, event=event)

# 更新会话
await session_service.update_session(session=session)
```

### 1.5 状态作用域

| 作用域 | 前缀 | 存储位置 | 生命周期 | 示例 |
|-------|------|---------|---------|------|
| **Session State** | 无前缀 | `session.state` | 随会话 | `{"current_topic": "天气"}` |
| **User State** | `user:` | `SessionService` | 跨会话，用户级别 | `{"user:name": "Alice"}` |
| **App State** | `app:` | `SessionService` | 跨会话，应用级别 | `{"app:version": "1.0"}` |
| **Temp State** | `temp:` | 内存 | 临时，不持久化 | `{"temp:cache": "..."}` |

---

## 2. Memory 管理

### 2.1 可用服务

| 服务类 | 初始化参数 | 用途 |
|--------|-----------|------|
| InMemoryMemoryService | `memory_service_config=None, enabled=False` | 内存版 |
| RedisMemoryService | `db_url: str, enabled=False, is_async=True` | Redis 版 |
| SqlMemoryService | `db_url: str, is_async=True, enabled=False` | SQL 版 |

### 2.2 使用示例

```python
from trpc_agent_sdk.memory import InMemoryMemoryService, MemoryServiceConfig

# 配置
memory_service_config = MemoryServiceConfig(
    enabled=True,
    ttl=MemoryServiceConfig.create_ttl_config(
        enable=True,
        ttl_seconds=86400,              # 记忆过期时间：24 小时
        cleanup_interval_seconds=3600,  # 清理间隔：1 小时
    ),
)

# 内存版
memory_service = InMemoryMemoryService(memory_service_config=memory_service_config)

# Redis 版
memory_service = RedisMemoryService(
    db_url="redis://localhost:6379/0",
    is_async=True,
    memory_service_config=memory_service_config,
    enabled=True,
)

# SQL 版
memory_service = SqlMemoryService(
    db_url="mysql+pymysql://user:pass@localhost/db?charset=utf8mb4",
    is_async=True,
    memory_service_config=memory_service_config,
    enabled=True,
)
```

### 2.3 核心方法

```python
# 存储会话到记忆
await memory_service.store_session(session=session)

# 搜索记忆
search_key = f"{app_name}/{user_id}"
response = await memory_service.search_memory(
    key=search_key,
    query="用户的名字",
    limit=10
)
```

---

## 3. 模型配置

### 3.1 可用模型

| 模型类 | 用途 |
|--------|------|
| OpenAIModel | OpenAI 及兼容 API（阿里云、DeepSeek 等） |
| AnthropicModel | Claude 模型 |
| LiteLLMModel | 统一接口调用多种模型 |

### 3.2 使用示例

```python
from trpc_agent_sdk.models import OpenAIModel

# 阿里云百炼
model = OpenAIModel(
    model_name="qwen3.7-flash",
    api_key="your-api-key",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)

# DeepSeek
model = OpenAIModel(
    model_name="deepseek-chat",
    api_key="your-api-key",
    base_url="https://api.deepseek.com/v1"
)

# OpenAI
model = OpenAIModel(
    model_name="gpt-4o",
    api_key="sk-xxx",
    base_url="https://api.openai.com/v1"
)
```

---

## 4. Agent 创建

### 4.1 可用 Agent

| Agent 类 | 用途 |
|----------|------|
| LlmAgent | 基础 Agent，调用 LLM |
| ChainAgent | 串联多个 Agent |
| ParallelAgent | 并行执行多个 Agent |
| TransferAgent | Agent 之间切换 |
| CycleAgent | 循环执行 Agent |

### 4.2 使用示例

```python
from trpc_agent_sdk.agents import LlmAgent

# 创建 Agent
agent = LlmAgent(
    name="assistant",  # Agent 名称
    description="A helpful assistant",  # 描述
    model=model,  # OpenAIModel 实例
    instruction="你是一个有帮助的助手。",  # 系统指令
    tools=[],  # 工具列表（可选）
)
```

---

## 5. Runner 使用

### 5.1 初始化

```python
from trpc_agent_sdk.runners import Runner

runner = Runner(
    app_name="my_app",           # 应用名
    agent=agent,                 # Agent 对象
    session_service=session_service,  # 会话服务
    memory_service=memory_service,    # 记忆服务（可选）
)
```

### 5.2 运行 Agent

```python
from trpc_agent_sdk.types import Content, Part

# 构建消息
content = Content(parts=[Part(text="你好")])

# 带图片的消息
content = Content(parts=[
    Part(text="看看这张图"),
    Part.from_uri(file_uri="https://example.com/image.jpg", mime_type="image/jpeg")
])

# 运行
async for event in runner.run_async(
    user_id="user_001",
    session_id="session_001",
    new_message=content,
):
    if event.content and event.content.parts:
        for part in event.content.parts:
            if part.text:
                print(part.text)
```

### 5.3 取消运行

```python
await runner.cancel_run_async(
    user_id="user_001",
    session_id="session_001"
)
```

---

## 6. 文件上传（多模态）

### 6.1 支持的类型

| 类型 | 方法 |
|------|------|
| 文字 | `Part(text="...")` |
| 图片（URL） | `Part.from_uri(file_uri="...", mime_type="image/jpeg")` |
| 图片（Base64） | `Part.from_bytes(data=..., mime_type="image/jpeg")` |
| PDF | `Part.from_bytes(data=..., mime_type="application/pdf")` |

### 6.2 示例

```python
from trpc_agent_sdk.types import Content, Part

# 纯文字
content = Content(parts=[Part(text="你好")])

# 文字 + 图片URL
content = Content(parts=[
    Part(text="描述这张图"),
    Part.from_uri(file_uri="https://example.com/cat.jpg", mime_type="image/jpeg")
])

# 文字 + Base64图片
import base64
with open("image.jpg", "rb") as f:
    img_base64 = base64.b64encode(f.read()).decode()

content = Content(parts=[
    Part(text="描述这张图"),
    Part.from_bytes(data=img_base64, mime_type="image/jpeg")
])
```

---

## 7. 工具系统

### 7.1 可用工具

| 工具类 | 用途 |
|--------|------|
| FunctionTool | 自定义函数工具 |
| WebSearchTool | 网络搜索 |
| WebFetchTool | 网页抓取 |
| FileToolSet | 文件操作 |
| MCPToolset | MCP 协议工具 |
| LoadMemoryTool | 加载记忆 |

### 7.2 创建自定义工具

```python
from trpc_agent_sdk.tools import FunctionTool

def query_order(order_id: str) -> dict:
    """查询订单状态"""
    return {"order_id": order_id, "status": "已发货"}

# 创建工具
tool = FunctionTool(func=query_order)

# 添加到 Agent
agent = LlmAgent(
    model=model,
    instruction="你是客服助手",
    tools=[tool]
)
```

---

## 8. 完整示例

```python
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import InMemorySessionService, SessionServiceConfig
from trpc_agent_sdk.memory import InMemoryMemoryService, MemoryServiceConfig
from trpc_agent_sdk.types import Content, Part

# 1. 创建模型
model = OpenAIModel(
    model_name="qwen3.7-flash",
    api_key="your-api-key",
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
)

# 2. 创建 Agent
agent = LlmAgent(
    model=model,
    instruction="你是一个有帮助的助手。"
)

# 3. 创建服务
session_config = SessionServiceConfig(
    event_ttl_seconds=3600,
    max_events=100,
)
session_service = InMemorySessionService(session_config=session_config)

memory_service_config = MemoryServiceConfig(
    enabled=True,
    ttl=MemoryServiceConfig.create_ttl_config(enable=True, ttl_seconds=86400),
)
memory_service = InMemoryMemoryService(memory_service_config=memory_service_config)

# 4. 创建 Runner
runner = Runner(
    app_name="my_app",
    agent=agent,
    session_service=session_service,
    memory_service=memory_service,
)

# 5. 运行
async def chat(user_id: str, session_id: str, message: str):
    content = Content(parts=[Part(text=message)])
    reply = ""
    
    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=content,
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    reply += part.text
    
    return reply
```

---

## 参考资料

- 官方文档：`D:\trpc\trpc-agent-python-jasindev-docs\docs\mkdocs\zh\`
- Session 文档：`session.md`
- Memory 文档：`memory.md`
- 模型文档：`model.md`
- Agent 文档：`llm_agent.md`
- 工具文档：`tool.md`
