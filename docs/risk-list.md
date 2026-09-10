# 生产风险清单（12 项）

| # | 风险 | 影响 | 缓解措施 |
|---|------|------|---------|
| 1 | **多节点并发写同一 session 冲突** | 会话历史错乱、state 互相覆盖 | append + seq 唯一索引（并发实测：不丢不重）；state_delta 键值合并天然可交换（实测结论见 sync-and-idempotency.md）；同 session 双端极端并发由前端串行化兜底 |
| 2 | **IM 消息重复投递/乱序** | 重复执行工具、重复扣预算、回复错乱 | 三层幂等（内存 TTL → Redis SETNX → SQL 唯一索引）；重复消息直接 ACK |
| 3 | **模型 API key / IM secret 泄漏** | 资损与安全事件 | 密钥只经环境变量注入；日志/trace/审计入库前 redact（键名整值遮蔽 + PII 打码）；错误报告统一脱敏 |
| 4 | **单租户预算耗尽/刷量** | 全局资源被挤占、超额账单 | 网关前置校验 + TOOL 层二次拦截双层预算（多节点 Redis INCRBY 共享计数，故障永久降级内存）；daily_api_calls / daily_token_budget / rate_limit_per_minute 按租户封顶；超限 429 / 限流提示 |
| 5 | **单点 Redis / SQL 故障** | 全部租户会话不可用 | Redis 哨兵/集群；SQL 主从；审计 JSONL 文件兜底保证审计不丢；网关对存储超时快速失败 + 兜底话术 |
| 6 | **模型超时/限流/错误率飙升** | 回复失败率上升 | 韧性层：超时类错误指数退避重试（LLM_RETRY_MAX）+ 租户级熔断快速失败；统一兜底话术 + error_type 审计归因；RunConfig.max_llm_calls 防止无限循环 |
| 7 | **危险工具误执行** | 业务数据被误改/误删 | dangerous_confirm 过滤器强制二次确认（confirm=true 或会话内确认）；危险名单独立维护；审计记录 decision |
| 8 | **PII 进入日志/trace/对话** | 合规风险 | mask_pii 租户开关 + 正则打码（手机/身份证/邮箱/密钥）；审计与输出双侧脱敏；trace 属性不含原文 |
| 9 | **配置热加载错误配置全网生效** | 全部租户异常 | YAML 加载失败保持旧配置；租户配置版本化（tenant / tenant_revision 表）支持按 revision 回滚并热重建 Runner；Admin API 变更留痕 |
| 10 | **IM 回调验签被绕过** | 伪造消息注入 | 飞书：SHA256 验签 + verification token；企微：SHA1 验签 + AES-256-CBC；均做协议回环测试，不符一律 403；企微 HTTP 回调依赖备案域名，长连接形态免公网可作替代 |
| 11 | **trace 无法串联定位问题** | 故障排查时长不可控 | trace_id 入口生成注入 AgentContext，贯穿 gateway→runner→tool→存储→IM 回复并写回审计；OTLP 上报 Jaeger |
| 12 | **节点故障时在途请求丢失** | 用户感知回复丢失 | 队列模式（QUEUE_MODE=redis）：BRPOPLPUSH 保证崩溃任务留存 processing 列表可重放，结果键 TTL 回传；IM 场景依赖事件重投 + 三层幂等；Worker 无状态，重启无恢复成本 |

## 补充说明

- 风险 1/6 的「降级」与风险 9 的「回滚」联动：配置回滚后 Runner 热重建，秒级生效。
- 风险 4 的多节点计数：已实现 Redis INCRBY 原子共享（BUDGET_REDIS_URL），运行期 Redis 故障永久降级内存不中断服务。
- 风险 12 的异步队列：已实现（worker.py，QUEUE_MODE=redis 开启），默认 inline 单进程模式零外部依赖。
- 模型/工具失败的韧性层：超时类错误指数退避重试（LLM_RETRY_MAX），租户级熔断（连续失败 ≥ CIRCUIT_FAILURE_THRESHOLD 快速失败，冷却后 half-open 恢复）。
- 灰度发布与风险 6/9 联动：canary 租户先切新 revision，观察指标异常即 rollback（release_stage 机制见 architecture.md）。
