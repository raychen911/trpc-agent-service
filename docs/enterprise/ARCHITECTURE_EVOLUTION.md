# PR7 / PR8 / PR9 架构整合说明

本轮重构吸收三份评审文档中可落地的共同优点，旧环境变量和旧管理接口不保留兼容层。目标是让业务能力留在高内聚模块中，让进程、数据库和渠道只依赖稳定的小接口。

## 组件边界

| 组件 | 唯一职责 | 允许依赖 | 禁止承担 |
| --- | --- | --- | --- |
| Gateway | 鉴权、规范化入站消息、入队 | TenantManager、ChannelRegistry、TaskQueue | Agent 推理、出站发送 |
| Worker | 消费任务并完成一个 Agent turn | TenantWorker、MessageStore | Webhook 协议、渠道重试 |
| Outbox | 租约领取并投递一个消息分片 | MessageStore、DeliveryTransport | Agent 推理、配置修改 |
| Admin | 草稿、发布、回滚与审计 | TenantConfigManager | 直接改运行中对象 |
| Config | 设置、SecretRef、发布前检查 | Pydantic、SecretResolver | HTTP、队列和 Provider SDK |
| Messaging | receipt、租约、fencing、Outbox | SQLAlchemy、抽象 Transport | 具体 IM SDK |
| Runtime | 节点心跳与稳定路由 | Redis 抽象 | Agent 与渠道逻辑 |

依赖方向是 `entrypoint -> application service -> domain boundary -> adapter`。Gateway 的渠道构造已移入 `ChannelRegistry`，测试入口移入独立 router，出站投递通过 `DeliveryTransportABC` 隔离渠道 SDK。

## 一致性边界

入站消息以 `(tenant_id, channel, message_id)` 去重。Worker 在执行前领取带过期时间的 receipt 租约，并在长任务期间随队列 ownership 一起续租；租约每次重新领取都会提升 fencing token，任一续租失败都会取消旧 Worker，旧 token 也无法提交结果。Agent 成功后，receipt 完成状态和 Outbox 意图在同一数据库事务提交，Redis ACK 只能发生在事务之后。

这提供的是业务边界内的 effectively-once：Agent turn 对同一入站消息至多提交一次。外部 IM Provider 不参与本地事务，因此不能宣称端到端 exactly-once；出站采用租约、指数退避、死信和“每个分片成功后立刻 checkpoint”，把重复窗口收敛到一次 Provider 调用。人工重放只重置死信记录，不会重新执行 Agent。

## 配置与密钥

- 仅接受 `TRPC_SERVICE_*` 服务变量；布尔值严格解析，启动时按 role 校验依赖。
- 租户配置使用 `draft -> publish -> historical revision`。发布在单事务内写版本、激活配置和配置 Outbox；任务记录 revision，Worker 与 Outbox 均读取该历史版本。
- `env://` 与受根目录限制的 `file://` 由 `SecretResolver` 解析，其他后端可以注册为插件。普通 URL 不会被误判成密钥引用。
- 日志脱敏器登记解析后的密钥精确值；数据库持久化 SecretRef 本身，内联密钥才加密。
- production 发布前检查拒绝内存向量库、本地对象存储和内联密钥。

## 数据、路由与部署

- MySQL 保存配置版本、草稿、receipt、Outbox、投递尝试和审计；迁移由有 ledger 的独立 Job 执行，运行进程不负责变更 schema。
- 节点目录使用 TTL 心跳，稳定路由采用 rendezvous hashing，并按 capacity/load 过滤节点；节点消失后无需清理业务映射即可重选。
- Kustomize 分为 base、production 与 performance。production 默认拒绝网络、非 root、只读根文件系统、最小 capabilities，并提供一个不接公网 Ingress 的 canary Service。
- Compose 将 Gateway、Worker、Outbox、Migration 拆为独立 role；`fault-stage-runtime.override.yml` 可重复注入 Redis/MySQL 中断。

## 发布与恢复 Runbook

1. 先运行 `python -m trpc_service.migrations.run --check` 查看待执行迁移，再由 migration Job 应用迁移。
2. 通过 Admin API 写 draft，检查 checksum 与 production preflight，再 publish；不要直接编辑活动配置。
3. 将新镜像先部署到 `agent-gateway-canary`，只向内部 canary Service 发送探测流量。
4. 验证 `/readyz`、端到端 IM 探针、错误率、P95、预算拒绝、DLQ 增量和 Outbox oldest-age；通过后替换稳定版本的不可变镜像摘要。
5. 配置异常时回滚到历史 revision；镜像异常时撤回稳定 Deployment 镜像。任务仍按已记录 revision 完成，避免一轮对话读到两套配置。
6. Redis 中断时停止接入或等待队列恢复；MySQL 中断时 receipt/Outbox 事务不能提交，绝不提前 ACK。恢复后检查 pending/claim、receipt lease 和 Outbox retry。
7. 死信重放前先修复 Provider 或配置原因；仅重放 Outbox，不重新执行 Agent。全过程记录操作者、原因、message/turn/revision 和 delivery attempt。

本地故障阶段：

```bash
docker compose -f deploy/docker-compose.minimal.yml \
  -f deploy/fault-stage-runtime.override.yml up --build
FAULT_SECONDS=10 deploy/run-fault-stage.sh
```

验收门禁同时要求单测、增量覆盖率、Compose/Kustomize 渲染、迁移检查及 runtime gate 证据。模板见 `deploy/runtime-gate.example.yaml`。
