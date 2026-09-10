# Research: tRPC-Agent SDK 最小集成验证

**Date**: 2026-09-03
**SDK reference**: tRPC-Agent-Python `1.1.19`, local reference commit
`909998e`

## Decision 1: Runtime and dependency baseline

**Decision**: 使用 Python 3.12，并将运行依赖精确固定为
`trpc-agent-py==1.1.19`。使用 uv 生成锁文件；测试依赖为 pytest 和
pytest-asyncio。

**Rationale**: 当前开发机已安装 Python 3.12.7 和 uv 0.11.21；SDK 的
`pyproject.toml` 声明支持 Python 3.10 及以上。本地参考源码
`trpc_agent_sdk/version.py` 声明版本 1.1.19。

**Alternatives considered**:

- 不固定 SDK 版本：拒绝，因为无法形成可重复的兼容性基线。
- 直接依赖相邻源码目录：拒绝，因为换机或提交后无法复现。
- 复制 SDK 源码：拒绝，违反 Framework-First 原则。

## Decision 2: Deterministic model integration

**Decision**: 实现最小 `LLMModel` 子类，并以模型实例形式传给
`LlmAgent`；不注册到全局 `ModelRegistry`。

**Rationale**: 官方测试证明 `LlmAgent` 接受 `LLMModel` 实例；
`_generate_async_impl` 可以产生确定性的 `LlmResponse`。实例注入没有
全局注册表残留，适合重复测试和隔离运行。

**Alternatives considered**:

- 使用真实 OpenAI 兼容模型：拒绝，因为需要网络、凭据和费用。
- 注册临时模型名称到全局 registry：可行但会产生跨测试共享状态。
- 完全伪造 Agent 或 Runner：拒绝，因为无法证明官方执行链路。

## Decision 3: Runner and Session lifecycle

**Decision**: 使用同一个 `Runner` 和 `InMemorySessionService` 完成同一
session 的两轮执行；不同 session 使用独立标识但共享服务实例。每次验证运行创建
全新的 Runner/Session 服务，并在结束时关闭资源。

**Rationale**: SDK `Runner.run_async` 会先按 app、user、session 查询会话，
不存在时自动创建，存在时继续使用；这正好验证会话连续性。InMemory 实现适用于
开发和测试，且符合本功能明确的非生产边界。

**Alternatives considered**:

- 每轮创建新的 Session 服务：拒绝，因为无法验证历史连续性。
- 使用 Redis 或 SQL：拒绝，因为会把后端集成引入首个最小验证。
- 预先手工创建 Session：不必要；Runner 的自动创建路径也是需要验证的能力。

## Decision 4: Event collection and final response

**Decision**: 收集 Runner 产生的全部可见 Event；使用
`Event.is_final_response()` 识别最终事件，使用 `Event.get_text()` 提取文本。
最终事件必须唯一且文本非空。

**Rationale**: 这些是 SDK 公开提供的事件语义。依赖 `partial` 或作者字段自行
推断最终回复容易在 SDK 行为变化时产生假阳性。

**Alternatives considered**:

- 直接取最后一个 Event：拒绝，最后事件未必就是最终用户回复。
- 仅检查事件数量：拒绝，无法证明获得了有效回复。
- 访问 Event 私有字段：拒绝，会放大升级兼容风险。

## Decision 5: Two-turn continuity and session isolation

**Decision**: 第一轮要求模型记住固定 token，第二轮询问该 token。确定性模型仅在
收到的请求历史中找到第一轮 token 时才能正确回答。第二个 session 使用不同 token，
并断言不会读取第一个 session 的内容。

**Rationale**: 该方法验证的是 Runner 和 Session 向下一轮模型请求提供了正确历史，
而不是在测试代码的全局变量中模拟记忆。

**Alternatives considered**:

- 只检查 Session 对象存在：拒绝，不能证明第二轮真正使用历史。
- 让 mock 模型自行保存 token：拒绝，这会绕过 Session 能力。
- 只测一个 session：拒绝，无法发现状态串扰。

## Decision 6: Offline and secret-safety boundary

**Decision**: 验证入口不读取模型供应商凭据；自动化测试清除常见模型密钥环境变量，
并在验证执行阶段阻断标准 socket 连接。报告只包含允许字段和清理后的异常类型/
阶段，不输出环境快照或完整请求载荷。

**Rationale**: “使用 mock”本身不能证明没有意外网络路径；运行期网络阻断和安全
输出模型提供可执行证据，同时不增加第三方测试插件。

**Alternatives considered**:

- 仅在文档中声明离线：拒绝，缺少自动化证据。
- 引入网络阻断插件：暂不采用，标准库 fixture 已满足最小范围。
- 打印完整异常和请求：拒绝，可能泄露凭据或测试内容。

## Decision 7: Validation entrypoint and report contract

**Decision**: 提供 `python -m trpc_service.agent.sdk_validation` 入口，默认输出
可读摘要，`--json` 输出单个稳定 JSON 文档。成功退出码为 0，验证失败为 1，
调用参数错误为 2。

**Rationale**: 同一个编排函数可同时服务 CLI 和 pytest，避免“演示路径”和“测试
路径”分叉；JSON 便于契约测试和将来 CI 使用。

**Alternatives considered**:

- 只提供 pytest：拒绝，不利于答辩或开发者快速演示。
- 先提供 HTTP API：拒绝，超出本 Spec。
- 输出自由格式日志：拒绝，难以稳定判断成功与失败。

## Decision 8: Failure-stage diagnostics

**Decision**: 阶段固定为 version、initialization、single_turn、event_finalization、
session_continuity、session_isolation 和 offline_safety。报告保存每阶段状态与安全
消息；版本或初始化失败时停止依赖它们的场景，其余场景尽可能独立报告。

**Rationale**: 固定阶段直接覆盖 FR-011 和 SC-006，并允许评审者在一次运行中定位
失败，而无需读取堆栈或 SDK 内部日志。

**Alternatives considered**:

- 仅返回布尔值：拒绝，无法诊断。
- 暴露完整异常堆栈：仅允许在开发调试日志中受控使用，不作为稳定 CLI 契约。
- 为每个 SDK 内部步骤建阶段：拒绝，过度绑定内部实现。

## Audit Sources

- `../trpc-agent-python-reference/pyproject.toml`
- `../trpc-agent-python-reference/trpc_agent_sdk/version.py`
- `../trpc-agent-python-reference/trpc_agent_sdk/models/_llm_model.py`
- `../trpc-agent-python-reference/trpc_agent_sdk/runners.py`
- `../trpc-agent-python-reference/trpc_agent_sdk/events/_event.py`
- `../trpc-agent-python-reference/trpc_agent_sdk/sessions/_in_memory_session_service.py`
- `../trpc-agent-python-reference/tests/agents/test_llm_agent.py`
- `../trpc-agent-python-reference/tests/test_runner.py`
