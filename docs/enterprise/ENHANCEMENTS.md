# 可选增强功能方案对比

> 本文档对当前已交付基础上仍可选的**生产级增强项**逐一做方案对比与选型建议。
> 每项包含「目标 / 现状 / 方案对比 / 推荐」四部分，最后给出汇总与建议实施顺序。

---

## 0. 总览

> 状态说明：✅ = 已作为 P0 增强完成（见 `P0_CHECKLIST.md`）。

| # | 增强项 | 推荐方案 | 复杂度 | 优先级 | 收益 | 状态 |
|---|---|---|---|---|---|---|
| 1 | 网关/Worker 网络分离 | Redis Streams | 中 | P1 | 独立扩缩容、削峰 | ✅ |
| 2 | 企业微信原生流式卡片 | Stream API + TemplateCard | 中 | P1 | 首字延迟、体验 | ✅（Stream API 部分） |
| 3 | 预算按日重置 | 日期前缀键（无重置） | 低 | P1 | 正确性、零运维 | ✅ |
| 4 | Admin API HTTP 化 | FastAPI + 数据库事实源 | 低 | P2 | 租户自助管理 | ✅ |
| 5 | OTel 导出实测 | Jaeger + Prometheus | 低 | P2 | 可观测闭环 | ✅（Trace/Metrics） |
| 6 | HITL 卡片交互 | TemplateCard + Inline Keyboard | 中 | P2 | 确认转化率 | 待做 |

---

## 1. 网关 / Worker 网络分离 ✅ 已完成

> 已按推荐方案 B（Redis Streams）实现，见 `_queue.py` / `_consumer.py` / `run_worker.py`。

**目标**：让 Gateway 与 Worker 独立部署、独立扩缩容，Gateway 只做接入与路由，Worker
专注执行，中间以队列削峰解耦。

**现状**：Gateway 与 Worker 同进程（`create_gateway_app` 直接 `worker.handle()`），
扩缩容需整体扩容，无法对"接入"与"执行"分别伸缩。

| 方案 | 实现 | 延迟 | 可靠性 | 扩缩容 | 运维成本 | 适用 |
|---|---|---|---|---|---|---|
| A 同进程（现状） | 直接函数调用 | 最低 | 单点 | 一体 | 低 | 最小部署、PoC |
| B Redis Streams | `XADD`/`XREADGROUP` + 消费组 ACK | 低 | 高（ACK 重投） | 独立 | 中（复用已有 Redis） | **推荐** |
| C RabbitMQ/Kafka | 独立消息中间件 | 中 | 最高 | 独立 | 高（新增组件） | 高吞吐、多消费者 |
| D tRPC/gRPC | 同步 RPC 调用 | 低 | 中（需重试/熔断） | 独立 | 中 | 强一致、低延迟场景 |

**推荐**：**B（Redis Streams）**。复用现有 Redis 依赖、消费组自带 ACK/重投（配合
已有 `SETNX` 幂等可做到 at-least-once + 幂等 = exactly-once 效果）；吞吐不足时再平滑
升级到 C。

---

## 2. 企业微信原生流式卡片 ✅ 已完成（Stream API 部分）

> Stream API 已实现（`WecomAdapter.send_stream` 走 `msgtype=stream` 开流/追加/结束）；
> TemplateCard 结构化卡片作为后续可选。

**目标**：实现真正的流式回复（低首字延迟 + 用户可见打字过程）。

**原现状（已改进）**：`WecomAdapter.send_stream` 曾为"分段多消息"渐进发送，首字延迟受整块
累积影响，且每条消息都是独立气泡。现已改为原生 Stream API（方案 B），分段多消息仅作降级。

| 方案 | 实现 | 首字延迟 | 体验 | 复杂度 | 适用 |
|---|---|---|---|---|---|
| A 分段多消息（降级） | 逐段 `send` | 中 | 多气泡 | 低 | 兜底 |
| B Stream API ✅ | `msgtype=stream` + `message/update` 开流/追加/结束 | 低 | 单气泡流式 | 中 | **已实现** |
| C TemplateCard 更新 | 先发卡片，多次 `update` | 低 | 卡片式 | 中 | 结构化展示 |

**推荐**：**B 为主，C 为辅**。B 是官方流式能力（单气泡持续更新）；当结果需要结构化
（按钮/列表）时用 C 的 TemplateCard。

---

## 3. 预算按日重置 ✅ 已完成

> 已按推荐方案 B（日期前缀键）实现，`BudgetTracker` 按 `(tenant_id, date_str)` 分桶，
> 无需任何重置任务。

**目标**：`BudgetTracker` 的日预算正确按自然日滚动，不累积、不泄漏。

**现状**：`BudgetTracker` 计数仅手动 `reset()`，无按日滚动。

| 方案 | 实现 | 正确性 | 复杂度 | 运维 | 适用 |
|---|---|---|---|---|---|
| A 每节点定时任务 | `asyncio` 定时 `reset` | 多节点重复/漏 | 低 | 有坑 | 单节点 |
| B 日期前缀键 | key 带 `{date}`，查询时只读当日 | 天然正确 | 低 | 零 | **推荐** |
| C 分布式调度 | APScheduler/Celery beat + 锁 | 正确 | 中 | 中 | 已有调度体系 |

**推荐**：**B（日期前缀键）**。把 `usage` 按 `tenant_id + YYYY-MM-DD` 分桶，预算检查与
扣费只读/写当日桶，**无需重置任务**，天然解决跨日与多节点问题；历史桶可留作账单。

---

## 4. Admin API HTTP 化

**目标**：把租户 CRUD / 版本回滚 / 审计查询暴露为 HTTP，实现租户自助与运营管理。

**现状**：已提供 FastAPI Admin API、管理页面、MySQL 配置事实源、版本历史和回滚；文件/YAML 用于启动引导。

| 方案 | 实现 | 一致性 | 复杂度 | 运维 | 适用 |
|---|---|---|---|---|---|
| A FastAPI + 文件持久化 | Admin 服务写 YAML/JSON + 内存注册表 | 单节点一致 | 低 | 低 | **推荐起步** |
| B 配置中心（Nacos/Consul/etcd） | Admin 写配置中心，Worker 订阅 | 强一致 | 中 | 中 | 多租户多节点 |
| C 数据库作配置源 | tenant 表 + Admin CRUD + Worker 轮询 | 强一致 | 中 | 中 | 需要审计/复杂查询 |

**推荐**：**A 起步，演进到 B**。最小交付先做 FastAPI Admin + 文件持久化（复用
`TenantConfigManager` 的版本/回滚），多节点规模再引入配置中心做发布订阅。

---

## 5. OTel 导出实测

**目标**：验证 trace/metrics 能端到端导出并可视化，闭环可观测。

**现状**：已配置 OTel TracerProvider/MeterProvider、OTLP HTTP exporter、跨 Redis Streams trace carrier、
Collector traces/metrics pipeline、Prometheus 抓取与基础告警。指标目录和调试方法见 `METRICS.md`。

| 方案 | 组件 | 场景 | 复杂度 | 适用 |
|---|---|---|---|---|
| A Jaeger | `jaeger` 容器 + OTLP 4317 | 通用 trace | 低 | **推荐** |
| B Grafana Tempo | Tempo + Grafana | 与监控面板统一 | 中 | 已有 Grafana |
| C Langfuse | Langfuse 自托管 | LLM 层（token/成本/评估） | 中 | LLM 专项 |
| D Prometheus metrics | `prometheus` + gen_ai semconv | 指标告警 | 低 | 指标侧 |

**推荐**：**A（或 B）做 trace + D 做指标 + C 做 LLM 专项**。三者互补而非互斥；
trace 用 Jaeger/Tempo，指标用 Prometheus，LLM 成本/质量评估交给 Langfuse。

---

## 6. HITL 卡片交互

**目标**：危险工具确认从"文本回显 token"升级为"点按钮确认"，提升转化率。

**现状**：危险工具签发 token 后需用户手动回显 `确认 <token>`。

| 方案 | 实现 | 体验 | 复杂度 | 适用 |
|---|---|---|---|---|
| A 文本回显（现状） | 回显 token | 一般 | 低 | 兜底 |
| B 企业微信 TemplateCard 按钮 | 按钮回调 → `resolve(token)` | 好 | 中 | 企业微信 |

**推荐**：**B + C 按平台实现**。复用现有 `ConfirmationManager.resolve`，把回调事件里的
`token` 走同一条确认链路，只新增"按钮→回调解析"的适配层。

---

## 7. 建议实施顺序

```
已完成（P0）：
  ✅ 3 预算日期分桶 → ✅ 1 网关/Worker 分离 → ✅ 2 企业微信流式

已完成（P2，运维/可观测）：
  ✅ 4 Admin API → ✅ 5 OTel Trace/Metrics 导出

待做：
  6 HITL 卡片
```

理由：**3** 改动最小、收益最确定（修复跨日正确性）；**1** 是水平扩展的关键前提；**2**
直接改善终端体验；三项均已完成。P2 属锦上添花，可按资源逐步推进。
