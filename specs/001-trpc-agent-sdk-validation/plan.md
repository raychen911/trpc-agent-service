# Implementation Plan: tRPC-Agent SDK 最小集成验证

**Branch**: `001-trpc-agent-sdk-validation`（逻辑功能名；当前目录无 Git 元数据）
| **Date**: 2026-09-03
| **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from
`/specs/001-trpc-agent-sdk-validation/spec.md`

## Summary

在作业仓库中建立可复现的 Python 项目依赖，并通过 tRPC-Agent-Python
`1.1.19` 的正式 `LlmAgent -> Runner -> Event -> InMemorySessionService`
路径完成离线验证。验证入口使用确定性模型替代真实供应商，输出结构化阶段结果；
自动化测试覆盖版本核对、单轮最终回复、双轮上下文连续性、跨会话隔离、重复运行、
无外部模型访问和失败阶段诊断。

本功能只证明 SDK 集成基线，直接支撑 README 验收标准 7 中的“框架能力复用”
部分；多租户平台新增能力、HTTP、IM、多后端和生产部署由后续 Spec 实现。

## Technical Context

**Language/Version**: Python 3.12；SDK 官方最低要求为 Python 3.10

**Primary Dependencies**: `trpc-agent-py==1.1.19`；标准库
`asyncio`、`dataclasses`、`enum`、`importlib.metadata`、`json`、
`uuid`；开发依赖 `pytest`、`pytest-asyncio`

**Storage**: `InMemorySessionService`，仅用于本地 SDK 验证；不产生持久化业务数据

**Testing**: pytest + pytest-asyncio；标准库网络阻断 fixture；模块与 CLI 契约测试

**Target Platform**: 本地开发环境；Windows PowerShell 为当前验证环境，Python
模块和测试保持 Windows/Linux 可运行

**Project Type**: 单 Python 包，提供内部验证模块和开发者 CLI

**Performance Goals**: 依赖安装完成后，完整 SDK 验证测试在普通开发机上 10 秒内
结束；新开发者按 quickstart 在 10 分钟内得到明确通过或失败结果

**Constraints**: 运行验证不需要模型 API Key，不调用真实模型供应商，不产生费用；
相同输入产生相同结论；输出不得包含环境秘密；只使用官方 SDK 的 Agent、Runner、
Event 和 Session 路径

**Scale/Scope**: 1 个确定性 Agent、2 轮连续消息、至少 2 个隔离 session、3 类
故障诊断场景；完整 CLI 验证固定执行两个 session、每个两轮，共 4 个最终回复；
不进行并发、吞吐或生产容量验证

## Constitution Check

*GATE: Phase 0 前检查，并在 Phase 1 设计后复核。*

| Constitution rule | Design response | Status |
|---|---|---|
| I. Framework-First | 固定正式发行版本，直接使用官方 Agent、Runner、Event、Session；验证逻辑集中在单一模块，不复制参考源码 | PASS |
| II. Tenant Isolation | 本功能明确不引入 tenant 模型；使用两个 session 验证状态不串扰，不将其虚报为租户隔离证明 | PASS |
| III. Stateless Workers | InMemory 仅限本地验证，Plan 和 Spec 均排除生产/多节点声明 | PASS |
| IV. Contract-First | 对唯一外部入口定义 CLI 契约；不引入 IM 或存储供应商专属载荷 | PASS |
| V. Security by Default | 使用无凭据确定性模型，测试期阻断网络连接，输出采用允许字段列表 | PASS |
| VI. Observability | 每次验证生成 run_id，阶段结果可定位版本、初始化、执行、事件、连续性和隔离失败 | PASS |
| VII. Spec-Driven Evidence | 设计从现有 Spec 派生，生成研究、模型、契约和 quickstart；测试任务先于实现任务 | PASS |
| README traceability | 本功能映射验收标准 7；其他总体验收项明确留给后续 Spec | PASS |

**Version-control condition**: 当前作业目录缺少 Git 元数据。依据 Constitution 的
临时例外条款，本 Plan 记录原因，并选择唯一恢复路径：实现前在项目根目录执行
`git init -b 001-trpc-agent-sdk-validation`，以本地功能分支开始追踪；远程
origin 只有在用户提供并核对准确 fork URL 后才绑定，不得猜测。在 T001 完成前
不得修改应用源码或宣称功能已实现。

**Post-design re-check**: Phase 1 产物保持相同边界；未引入真实外部服务、持久化
后端、租户语义或平台功能。所有 Constitution gates 继续通过。

## Phase 0: Research Decisions

研究结果记录在 [research.md](./research.md)。已解决的关键决策包括：

- 运行时和依赖版本选择；
- 确定性模型替代接口及其隔离方式；
- Runner 自动创建/复用 Session 的使用路径；
- 最终 Event 的识别和安全摘要；
- 双轮上下文连续性与跨 Session 隔离测试方法；
- 无网络、无凭据和可重复运行的验证边界；
- CLI 输出、退出码和失败阶段契约。

不存在剩余的 `NEEDS CLARIFICATION`。

## Phase 1: Design and Contracts

### Runtime design

1. `SdkValidationRunner` 创建唯一 validation run，先核对发行包版本和模块版本。
2. `DeterministicValidationModel` 作为 `LLMModel` 实例直接传给
   `LlmAgent`，避免修改全局 `ModelRegistry`。
3. `Runner` 与同一个 `InMemorySessionService` 在两轮消息间复用。
4. 收集所有可见 Event，只以 `is_final_response()` 判断最终结果，并通过
   `get_text()` 提取文本。
5. 完整验证包含 session A 和 session B，每个 session 各执行两轮：第一轮写入
   不同 token，第二轮查询该 token。US1 的单轮场景复用 session A 第一轮，因此
   完整 CLI 运行共处理 4 轮并产生 4 个最终回复。
6. 每个阶段形成 `StageResult`，最终汇总为 `ValidationReport`；失败时返回
   非零退出码和安全诊断，不输出原始环境或完整请求。
7. 运行结束显式关闭 Runner/Session 资源，测试每次创建全新实例。

### Design artifacts

- [research.md](./research.md)：SDK 1.1.19 API 调研与选择依据。
- [data-model.md](./data-model.md)：验证运行、阶段、会话和事件观察模型。
- [contracts/sdk-validation-cli.md](./contracts/sdk-validation-cli.md)：开发者 CLI
  输入、输出、退出码和安全边界。
- [quickstart.md](./quickstart.md)：安装、运行、测试和期望结果。
- `validation-results.md`：实现完成后记录运行耗时、CLI 结果、测试结果和
  FR/SC 验收矩阵；由 Tasks 阶段创建。

## Project Structure

### Documentation (this feature)

```text
specs/001-trpc-agent-sdk-validation/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── validation-results.md # 实现后由验收任务生成
├── contracts/
│   └── sdk-validation-cli.md
├── checklists/
│   └── requirements.md
└── tasks.md
```

### Source Code (repository root)

```text
pyproject.toml
uv.lock

trpc_service/
└── agent/
    ├── __init__.py
    ├── _deterministic_validation_model.py
    └── sdk_validation.py

tests/
└── sdk_validation/
    ├── conftest.py
    ├── test_sdk_validation.py
    └── test_sdk_validation_cli.py
```

**Structure Decision**: 保持现有单包结构，把框架集成边界放入
`trpc_service.agent`。确定性模型使用私有模块，避免被误认为生产模型；
CLI、阶段模型和编排集中在 `sdk_validation.py`。测试独立放在
`tests/sdk_validation`，以便单独运行并为未来 SDK 升级提供回归基线。

## Verification Strategy

- **Version tests**: 发行包元数据和 SDK 模块版本均必须等于 `1.1.19`。
- **Unit tests**: 阶段状态转换、最终 Event 选择、安全错误摘要和退出码映射。
- **Integration tests**: 真实构造官方 `LlmAgent`、`Runner` 和
  `InMemorySessionService`，完成单轮、双轮和跨 Session 场景。
- **Offline guard**: 验证运行期间拦截网络连接；清除已知模型凭据环境变量后测试。
- **Contract tests**: JSON 字段、状态、退出码及 stdout/stderr 责任符合 CLI 契约。
- **Repeatability**: 在全新实例上连续执行两次，比较规范化后的报告内容。
- **Failure tests**: 版本不匹配、无最终 Event、上下文丢失分别映射到唯一阶段。
- **Timing evidence**: quickstart 验收记录干净环境从依赖同步到获得报告的总耗时，
  以及依赖完成后测试套件耗时；分别验证 10 分钟和 10 秒目标。

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| 当前目录暂未纳入 Git | 工作区缺少 `.git`，且不能安全猜测远程 fork URL | 忽略会违反可追溯性；实现前使用明确的本地功能分支初始化，远程待准确 URL 后绑定 |
