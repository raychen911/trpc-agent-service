# 总体验收项追踪矩阵（第八阶段收口，FR-036，SC-011）

七项 README 总体验收标准 → 阶段 → FR/NFR/SC → 设计文档 → 测试 → 证据。
状态：`real`=真实环境验证、`automated`=自动化离线验证、`design`=仅设计建议。

| # | 验收标准 | 阶段 | 追溯 | 设计文档 | 测试 | 证据 | 状态 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 架构覆盖多租户/节点化/数据同步/多后端/IM/治理监控/故障恢复 | 2~8 | FR-001~FR-036 | plan.md, deployment-topology.md, ../../ARCHITECTURE.md | 全量回归 | validation-results.md | automated + real local evidence |
| 2 | 数据模型表达 tenant/agent/channel binding/session/event/memory/summary/audit log | 2~7 | data-model.md 各阶段 | data-model.md | tests/unit/data, tests/contract/data | Phase 2~7 validation-results | automated |
| 3 | ≥2 种 IM 通道接入差异（含微信/企业微信） | 4/5 | FR-channels | 第四阶段外部验证、specs/005 | tests/contract/channels, tests/integration/channels, tests/e2e/data, tests/e2e/governance | Phase 4/5 记录 | real（飞书/企业微信客户端） |
| 4 | ≥3 类后端存储与同步策略（Redis/SQL/向量/对象） | 3/7 | FR-data | specs/003, 007 | tests/contract/data, tests/integration/shared | Phase 3/7/8 记录 | Redis/SQL real；Vector/Object contract/design |
| 5 | 完整消息链路时序（trace_id 贯穿） | 8/US1 | FR-001~FR-007 | ../../ARCHITECTURE.md, contracts/observability-* | tests/e2e/observability/test_end_to_end_trace.py | US1 GREEN | automated |
| 6 | ≥8 个生产风险与缓解 | 8/US7 | FR-030, SC-010/011 | risk-register.md（9 项） | tests/e2e/operations/test_fault_exercise.py | US7 GREEN + 演练引用 | automated |
| 7 | 框架复用 vs 平台新增明确 | 1/8 | FR-036, SC-011 | 阶段成果记录.md | tests/sdk_validation | Phase 1 + US8 收口 | automated |

覆盖率：7/7 = 100%。
- `real` 边界：飞书/企业微信真实客户端，以及共享 Redis/PostgreSQL 和第八阶段
  Docker Compose 均已完成本机验收；最终结果为 `620 passed, 0 skipped, 0 failed`。
- `design` 边界：deployment-topology.md 的生产推荐拓扑为设计建议，不声明生产 HA/SLA。
- `automated` 边界：离线 harness（内存权威、LocalObservableOverlay、
  FaultExerciseHarness）证据均为自动化可重复，不等同生产环境；Vector/Object
  当前为契约与确定性替身，不宣称已接入生产产品。
