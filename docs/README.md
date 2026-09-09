# 📚 文档导航

> 本目录是本平台的全部交付文档，按 AI-Native SDLC 的 artifact 链组织：
> `Problem.md`（意图）→ `PRD.md`（规格）→ 代码 + 测试 → `VERIFICATION.md`（验证）→ `DEVELOPMENT_LOG`（过程）。

## 🎯 评审入口

| 文档 | 回答什么问题 |
| ---- | ------------ |
| [`PRD.md`](PRD.md) | **主设计文档（spec）**：架构图、六大板块决策、风险清单、验收标准映射 |
| [`VERIFICATION.md`](VERIFICATION.md) | 验证记录：两轮全链路实测证据与复现命令（282 测试 / 84% 覆盖） |

## 📐 设计详设（spec 深度层，按题目板块）

| 文档 | 覆盖板块 |
| ---- | -------- |
| [`DESIGN-ARCHITECTURE.md`](DESIGN-ARCHITECTURE.md) | 总体架构、运行时模型、企微全链路时序、关键决策 |
| [`DESIGN-MULTI-TENANT.md`](DESIGN-MULTI-TENANT.md) | 租户模型、节点部署、无 sticky 路由、隔离与首启自举 |
| [`DESIGN-DATA-SYNC.md`](DESIGN-DATA-SYNC.md) | 八域存储抽象、并发一致性、迁移、幂等、数据模型 DDL |
| [`DESIGN-IM-CHANNELS.md`](DESIGN-IM-CHANNELS.md) | 企微/飞书四通道、验签去重、session 规则、平台限制 |
| [`DESIGN-GOVERNANCE.md`](DESIGN-GOVERNANCE.md) | Filter 治理链、监控指标、审计字段、脱敏、生产安全校验 |
| [`DESIGN-OPERATIONS.md`](DESIGN-OPERATIONS.md) | 降级策略、灰度回滚、容量评估、依赖锁定、K8s 部署 |

## 🧭 理解与过程

| 文档 | 回答什么问题 |
| ---- | ------------ |
| [`OVERVIEW.md`](OVERVIEW.md) | 系统概貌导读：设计哲学、代码地图、一条消息的完整旅程 |
| `Problem.md` | 题目要求与验收标准（最高锚点） |
