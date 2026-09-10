# Tasks: tRPC-Agent SDK 最小集成验证

**Input**: Design documents from
`/specs/001-trpc-agent-sdk-validation/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md),
[research.md](./research.md), [data-model.md](./data-model.md),
[CLI contract](./contracts/sdk-validation-cli.md), [quickstart.md](./quickstart.md)

**Tests**: 本功能明确要求自动化、离线、可重复验证，因此每个用户故事都先编写失败
测试，再实现对应行为。

**Organization**: 任务按用户故事组织。描述中的 FR/SC 编号用于需求覆盖追踪。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可与同阶段其他标记任务并行，且不修改同一文件
- **[Story]**: 对应 spec.md 中的 US1、US2 或 US3

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 建立可追溯、可复现的本地 Python 项目。

- [X] T001 在项目根目录执行 `git init -b 001-trpc-agent-sdk-validation`，并通过 `.git/HEAD` 验证当前本地功能分支；不得猜测或写入 origin，准确 fork URL 在发布前另行核对
- [X] T002 创建 `pyproject.toml`，声明 Python 3.12、`trpc-agent-py==1.1.19`、pytest、pytest-asyncio 和验证模块入口（FR-001、FR-010）
- [X] T003 基于 `pyproject.toml` 生成并核对 `uv.lock`，确认解析出的 `trpc-agent-py` 精确为 1.1.19（FR-001、SC-003）

**Checkpoint**: Git 本地分支可追踪、依赖可同步，才可进入功能实现。

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 建立所有用户故事共享的安全测试边界和验证结果模型。

**CRITICAL**: 本阶段完成前不得开始任一用户故事。

- [X] T004 [P] 在 `tests/sdk_validation/conftest.py` 创建每次测试使用的全新运行环境，清除常见模型凭据并阻断执行期间的外部 socket 连接（FR-003、FR-009、FR-012、SC-004、SC-007）
- [X] T005 在 `trpc_service/agent/sdk_validation.py` 定义 SdkBaseline、ValidationRun、StageResult、ValidationConversation、ValidationTurn 和 EventObservation，以及固定阶段和安全错误分类（FR-011、FR-012）

**Checkpoint**: 测试环境和共享结果契约准备完成。

---

## Phase 3: User Story 1 - 验证官方 SDK 最小执行链路 (Priority: P1) MVP

**Goal**: 在无真实凭据和外部模型调用的条件下，通过官方 SDK 产生事件和唯一最终
回复，并核对运行版本为 1.1.19。

**Independent Test**: 执行 session A 第一轮固定消息，断言发行包/模块版本、至少
一个 Event、唯一非空最终回复、零凭据要求和零外部模型调用。

### Tests for User Story 1

> 先完成 T006 并确认测试失败，再开始实现。

- [X] T006 [US1] 在 `tests/sdk_validation/test_sdk_validation.py` 编写版本不匹配、官方 Runner 单轮执行、Event 收集、唯一最终回复和空回复失败测试（FR-001、FR-002、FR-005、FR-006、SC-002、SC-003）

### Implementation for User Story 1

- [X] T007 [US1] 在 `trpc_service/agent/_deterministic_validation_model.py` 实现无网络、无凭据且只返回确定性 LlmResponse 的 LLMModel 测试替代（FR-003、FR-004、SC-004）
- [X] T008 [US1] 在 `trpc_service/agent/sdk_validation.py` 构造官方 LlmAgent、Runner 与 InMemorySessionService，并实现 session A 第一轮消息执行和资源关闭（FR-002、FR-005）
- [X] T009 [US1] 在 `trpc_service/agent/sdk_validation.py` 实现发行包/模块双版本核对、可见 Event 收集、is_final_response 判定及 get_text 安全提取，使 T006 全部通过（FR-001、FR-006、SC-003）

**Checkpoint**: US1 可以独立运行，证明 SDK 最小执行链路成立。

---

## Phase 4: User Story 2 - 验证同一会话的连续执行 (Priority: P2)

**Goal**: 证明同一 session 的第二轮能使用第一轮历史，且不同 session 不串扰。

**Independent Test**: 在全新验证实例中运行 session A 和 session B；每个 session
使用不同固定 token 完成两轮对话，第二轮只返回本 session 的 token。完整运行共
4 轮和 4 个最终回复。

### Tests for User Story 2

> 先完成 T010 并确认连续性、隔离和四轮计数测试失败，再实现会话场景。

- [X] T010 [US2] 在 `tests/sdk_validation/test_sdk_validation.py` 添加同 session 双轮上下文、不同 session token 隔离、完整运行四个最终回复和全新实例无残留测试（FR-007、FR-008、FR-009、SC-002）

### Implementation for User Story 2

- [X] T011 [US2] 扩展 `trpc_service/agent/_deterministic_validation_model.py`，仅根据 SDK 传入的请求历史识别当前 session 的第一轮 validation token，不在模型实例保存会话状态（FR-004、FR-007、FR-008）
- [X] T012 [US2] 在 `trpc_service/agent/sdk_validation.py` 实现同一 Runner 和 InMemorySessionService 上 session A 的第二轮连续性场景（FR-007）
- [X] T013 [US2] 在 `trpc_service/agent/sdk_validation.py` 实现 session B 两轮隔离场景和每次 ValidationRun 的全新生命周期，使 T010 全部通过（FR-008、FR-009、SC-005）

**Checkpoint**: US1 与 US2 均通过，完整运行产生 4 个最终回复，且没有把 session
隔离虚报为租户隔离。

---

## Phase 5: User Story 3 - 获得可重复、可诊断的验证结果 (Priority: P3)

**Goal**: 提供稳定 CLI、失败阶段和可比较报告，供开发者、评审者与自动化环境复用。

**Independent Test**: 两次运行得到相同规范化结果；人为制造版本不匹配、无最终
Event 和上下文丢失时，均返回退出码 1 并标识唯一失败阶段。

### Tests for User Story 3

> T014 与 T015 可并行编写；两者确认失败后再实现 CLI 和报告。

- [X] T014 [P] [US3] 在 `tests/sdk_validation/test_sdk_validation_cli.py` 编写 --help、human/JSON stdout、schema、七阶段顺序、四个最终回复、退出码、重复运行规范化和未知参数契约测试（FR-010、FR-013、SC-001、SC-005）
- [X] T015 [P] [US3] 在 `tests/sdk_validation/test_sdk_validation.py` 添加版本不匹配、无最终 Event、上下文丢失的阶段诊断、stdout/stderr 敏感值扫描和 socket 阻断生效测试（FR-003、FR-011、FR-012、SC-004、SC-006、SC-007）

### Implementation for User Story 3

- [X] T016 [US3] 在 `trpc_service/agent/sdk_validation.py` 实现七阶段编排、passed/failed/skipped 状态转换、依赖阶段短路和允许字段错误摘要（FR-011、FR-012、SC-006）
- [X] T017 [US3] 在 `trpc_service/agent/sdk_validation.py` 实现 human 与 --json 报告、schema_version=1、四个最终回复计数、stdout/stderr 分工及 0/1/2 退出码（FR-010、FR-013、SC-001）
- [X] T018 [US3] 在 `trpc_service/agent/sdk_validation.py` 实现排除 run_id、时间戳和 SDK 自动标识的规范化比较，并确保连续两次运行结论一致（FR-004、FR-009、SC-005）

**Checkpoint**: 三个用户故事均可独立验证，CLI、安全和失败契约测试全部通过。

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: 同步文档、执行耗时验收并留下可审查证据。

- [X] T019 [P] 在 `README.md` 增加 SDK 验证入口，并链接 `specs/001-trpc-agent-sdk-validation/quickstart.md`，明确它只证明 README 验收标准 7 的框架复用基线（FR-010、SC-001）
- [X] T020 按 `specs/001-trpc-agent-sdk-validation/quickstart.md` 执行 human、JSON、pytest 和 Timing acceptance，并将版本、退出码、测试数量、首次准备总耗时、依赖完成后测试耗时和非敏感结果记录到 `specs/001-trpc-agent-sdk-validation/validation-results.md`（FR-013、SC-001、SC-002）
- [X] T021 在 `specs/001-trpc-agent-sdk-validation/validation-results.md` 记录连续两次规范化结果、四轮计数、三类故障诊断结果及零外部模型调用证据，并对照 FR-001 至 FR-013 和 SC-001 至 SC-007 完成验收矩阵（FR-013、SC-004、SC-005、SC-006、SC-007）

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 Setup**: 立即开始；T001 阻断所有源码修改，T002 完成后才能执行 T003。
- **Phase 2 Foundational**: 依赖 Phase 1，阻断所有用户故事。
- **US1**: 依赖 Phase 2，是可演示 MVP。
- **US2**: 依赖 US1 提供的官方执行链路，但拥有独立验收场景。
- **US3**: 依赖 US1/US2 提供场景结果；T014 与 T015 可并行。
- **Polish**: 依赖计划交付的所有用户故事。

### User Story Dependencies

```text
Setup -> Foundation -> US1 (MVP) -> US2 -> US3 -> Polish
```

- **US1**: 证明固定 SDK 的单轮执行和最终 Event。
- **US2**: 复用 US1 的 session A 第一轮，新增 session A 第二轮和 session B
  两轮隔离。
- **US3**: 汇总 US1/US2 的四轮结果，新增稳定 CLI、诊断和重复性证据。

### Within Each User Story

- 测试必须先写并确认失败。
- 确定性模型行为先于 Runner 场景编排。
- 场景编排先于 CLI 汇总。
- 每个 Checkpoint 必须独立通过后才进入下一优先级。

### Parallel Opportunities

- T004 可与 T005 的数据结构设计并行，且修改不同文件。
- T014 与 T015 修改不同测试文件，可并行。
- T019 可与最终验收准备并行，但 T020 只有在所有测试实现完成后执行。

---

## Parallel Example: User Story 3

```text
Task T014: 编写 CLI、四轮计数和重复运行契约测试
Task T015: 编写失败阶段、敏感信息和网络阻断测试
```

两项完成并确认失败后，再顺序执行 T016、T017、T018。

## Implementation Strategy

### MVP First

1. 完成 Phase 1 和 Phase 2。
2. 完成 T006 至 T009。
3. 只运行 US1 测试并演示固定版本、单轮 Event 和最终回复。
4. US1 未通过时停止，不进入 Session 或 CLI 扩展。

### Incremental Delivery

1. US1：证明“框架可以真实运行”。
2. US2：证明“框架 Session 可以支撑连续对话且不串 session”。
3. US3：证明“结果可重复、可诊断、可自动消费”。
4. Polish：同步 README、测量耗时并留下验收矩阵。

## Notes

- 所有任务均包含明确文件路径。
- `[P]` 只用于不存在文件冲突和未完成依赖的任务。
- 参考源码只读，不得在 `../trpc-agent-python-reference` 中完成任务。
- 本功能不实现 HTTP、租户、IM、Redis、SQL 或生产部署。
- 每个逻辑任务或小组完成后应形成清晰 Git 提交。
