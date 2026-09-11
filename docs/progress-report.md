# 四周实施进展说明

> 2026-09-10 复核说明：本文为阶段记录，“已完成”仅指已有代码基线，不代表全量生产功能。准确边界以 [原题逐条检查](requirements-audit.md) 为准：固定的是入队消息 revision；自动灰度、自动搬迁、完整 token 流和沙箱未集成，token/cost 为估算。下方 75 项/覆盖率为历史记录，本次验证结果见检查记录。

> 更新时间：2026-09-09
> 项目：基于 tRPC-Agent-Python 的多租户节点化 Agent 服务
> 当前状态：四周代码和本地自动化验证已完成；真实基础设施、平台凭据、容量和故障演练待目标环境验收

## 1. 总体结论

仓库已从前三周原型推进到完整生产代码基线，形成以下闭环：

```text
企业微信 / 飞书
  -> Gateway 验签、身份校验、去重、租户限流
  -> PostgreSQL Inbox + Outbox
  -> Redis Streams consumer group
  -> Worker Session lease/fencing
  -> tRPC-Agent Runner + Model fallback + FunctionTool/MCP
  -> Session/Event/Memory/Audit 事务状态
  -> Summary/Knowledge/Vector 异步投影 + S3 Artifact
  -> IM 投递账本、重试、DLQ 与人工恢复
```

InMemory 仍保留用于离线 demo 和单元测试。生产模式会拒绝 InMemory、弱 Session key、`literal://` Secret、自动 migration、Echo 模型和 IM dry-run。

## 2. 分周完成情况

### 第 1 周：需求与领域基线

- 分析 tRPC-Agent-Python 的 Runner、Agent、Session、Memory、Knowledge、Tool/MCP、Filter、Telemetry 和服务化能力。
- 确定 Gateway/Worker 分层、PostgreSQL 权威事实源、Redis 通知层、Outbox、lease/fencing 和版本投影语义。
- 完成架构图、核心时序图、数据模型、一致性、IM、安全、运维和 16 项风险清单。
- 完成 `TenantConfig`、`AgentApp`、`ChannelBinding`、`TenantPolicy`、`AuditPolicy`、`StorageProfile` 和配置 revision。
- 配置发布、回滚使用单调版本与 ETag/If-Match CAS；租户、Session、存储 key、审计和 trace 全链路带 tenant scope。

### 第 2 周：Agent、治理与状态链路

- 完成无状态 `AgentService`、队列消费、Session 串行写入、Memory、Audit 和幂等状态机。
- 完成 `TRPCAgentExecutor`，将文本和已校验 Artifact 转成 tRPC `Content/Part`，只采用 final response。
- 支持租户 App 独立模型名称/API endpoint/API Key SecretRef/timeout、主模型 transport/error fallback、Redis tRPC Session 以及生产环境禁用 Echo。
- FunctionTool/MCP 显式注册并受租户/app allowlist 约束；生产 MCP 强制 HTTPS。
- 危险工具使用绑定租户、用户、Session、工具和参数的一次性确认 token；非幂等工具未知结果进入人工处理。
- 完成 PostgreSQL token 预算预留/结算、工具执行账本、确认账本、输出/日志/trace 脱敏。

### 第 3 周：IM 接入和服务入口

- 企业微信：普通/加密 XML、SHA1 验签、EncodingAESKey 解密、URL 校验、应用消息发送、媒体下载与退避。
- 飞书：HTTP Webhook、加密回调、官方 SDK 长连接、App Secret 换 token、文本/图片/文件、卡片、主动发送和撤回。
- 所有外部消息生成 HMAC Session ID；伪造的内部 Artifact 元数据会被清除，真实媒体落入租户 S3 前缀并校验 checksum。
- FastAPI 提供 health、metrics、webhook、Admin 配置/审计/知识库/恢复 API；飞书长连接快速确认后异步入队。

### 第 4 周：共享后端、恢复、观测和部署

- PostgreSQL：Inbox/Outbox、Session/Event、Memory/Summary、Knowledge、Artifact、Audit、Budget、Tool、配置 revision。
- Redis：Streams consumer group、`XAUTOCLAIM`、heartbeat、源消息 ACK 后清理、Stream/DLQ 有界保留、原子租户限流、可选 Session/Memory 后端和 Summary 缓存。
- Session 写入采用 lease owner + fencing epoch + version CAS；投影只读取不超过 `source_version` 的事件。
- Outbox 支持投影 handler、指数退避、死信、stranded Inbox 补偿扫描和人工重放。
- IM 投递失败保留 `prepared` 回复，人工重放不会再次调用模型或重复提交 Session。
- Knowledge 使用 SQL 文档事实源、Outbox 异步 embedding、按 tenant/collection 的 pgvector 检索，并注入对应 App 上下文。
- Prometheus 提供入站/出站/工具/队列/fencing、callback/model/storage/tool 延迟、token 和成本指标；OTel span 关联消息 trace id，并由 Compose/Kubernetes Collector 接收。
- Gateway、Worker、Admin、Migration 四角色独立；Admin 仅需控制库，使用服务端绑定的 viewer/operator/admin token。
- 提供 Dockerfile、Compose、Kustomize、HPA、PDB、NetworkPolicy、Migration Job、Prometheus 和 OTel Collector 配置。

## 3. 关键状态

| 能力 | 当前实现 | 状态 |
|---|---|---|
| 多租户配置 | 不可变 revision、CAS、回滚、SecretRef、AuditPolicy | 已完成 |
| Gateway | 企业微信与飞书接入、验签、binding 校验、重复识别和原子限流 | 已有代码基线 |
| Worker | Redis 消费、reclaim/heartbeat/DLQ、Session fencing、固定 revision | 已完成 |
| tRPC Agent | LlmAgent/Runner、租户级模型配置、Redis Session、多模态 Part、timeout/fallback | 已完成 |
| Tool/MCP | 显式注册、allowlist、确认、执行账本、审计 | 已完成 |
| 多后端 | PostgreSQL、Redis、pgvector、S3/MinIO、动态 StorageProfile | 已完成 |
| Knowledge | collection 配置、文档事实源、Outbox 索引、检索注入 | 已完成 |
| IM 投递 | 分段、媒体、重试、状态账本、ambiguous/人工恢复 | 已完成 |
| 观测 | OTel、Prometheus、低基数 tenant 标签、延迟/token/cost | 已完成 |
| Admin | 配置 CRUD、RBAC、审计保留、Knowledge、Inbox/Outbox 恢复 | 已完成 |
| 部署 | Compose、Gateway/Worker/Admin、Migration Job、HPA/PDB/NetworkPolicy | 已完成 |

## 4. 自动化验证

本地验证命令：

```bash
bash build.sh
bash lint_flake8.sh
bash coverage.sh
uv run python -m compileall -q trpc_service tests
uv build
```

早期检查记录为 75 项测试通过，总体语句覆盖率 67%，Redis 状态后端覆盖率 90%。2026 年 9 月 11 日通过 `coverage.sh` 重新验证后，82 项测试全部通过，总体语句覆盖率仍为 67%，详见 [逐条检查清单](requirements-audit.md)。

测试范围包括 IM 回调与媒体、重复受理、跨租户隔离、队列恢复、Outbox、RLS 迁移契约、Redis 状态与 fencing，以及 prepared 回复恢复。其他用例覆盖版本投影、Knowledge、Artifact、模型超时与主备切换、工具确认、配置版本、Admin 权限和生产启动检查。

## 5. 仍需目标环境验收

以下不是未实现代码，而是当前机器缺少 Docker/Kubernetes、真实数据库和 IM/模型凭据，无法伪造的上线证据：

1. PostgreSQL RLS、runtime/control/migration 三账号权限和真实 migration 验证。
2. Redis 重启、Stream 丢失、Worker SIGTERM、pending reclaim 和 fencing 故障演练。
3. pgvector embedding 维度、MinIO/S3 checksum、对象生命周期与孤儿清理验证。
4. 企业微信与飞书的真实应用权限、平台回执和限流联调。
5. 真实模型、FunctionTool/MCP、fallback、费用单价和 token 统计校准。
6. callback P95 < 500 ms、IM 正常投递率 >= 99.9% 和单 Worker 容量压测。
7. 镜像/依赖安全扫描、备份恢复、Secret 轮换和租户配置灰度回滚演练。

## 6. 结论

原始 README 要求的多租户、节点部署、共享状态、多后端、两类以上 IM、治理、安全、审计、观测、恢复、灰度、容量方法和部署交付物均已有代码或文档对应。当前版本可以进入目标环境集成验收，但在第 5 节证据完成前不应宣称达到生产 SLO。
