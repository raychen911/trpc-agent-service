# US5 容量验收结果（local_evidence）

- 日期：2026-09-11；环境：本机开发环境（非共享 Docker），证据类别 `local_evidence`
- 正式场景：2 tenant / 2 Worker / 100 并发 Session / 每 Session 10 条消息 = 1,000 条（seed=2026091100，warm-up 1 轮，测量 9 轮中位数取位）
- 命令：`uv run python`（LocalCapacityHarness 正式运行，见 capacity-report.json）；测试命令见 validation-results.md

## 双门禁判定（DEC-005）

- 正确性门禁（零容忍）：lost_results=0, cross_tenant_leaks=0, unexplained_duplicates=0 → **通过**
- 相对性能门禁（≤10%）：吞吐 -1.26%、p50 +2.61%、p95 +0.00%、p99 +3.32% → **通过**
- 总判定：**pass**（详见 capacity-report.json 的 verdict/gates）

## 已测量事实

- 吞吐：baseline(off) 9722.53 msg/s → enabled(on) 9600.21 msg/s
- 会话批量延迟：p50/p95/p99 见 capacity-report.json measured_facts
- 本环境 CPU/内存/Redis/PostgreSQL 峰值采样为进程内合成负载（0.0），真实后端压力留待共享环境补测

## 明确排除（未覆盖因素）

- 真实模型供应商延迟（本场景不调用外部模型 API）
- 真实 IM 渠道限流
- 生产基础设施拓扑与多区域网络差异
- 本报告不声明任何生产吞吐/延迟/SLA 绝对值；环境不等价时判定 invalid 且不放宽 10% 阈值
