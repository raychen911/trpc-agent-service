# tRPC Agent 多租户部署平台

基于 [tRPC-Agent-Python](https://github.com/trpc-group/trpc-agent-python) 构建的多租户 Agent 服务平台，支持企业微信和飞书接入、多 Worker 水平扩展、共享 Session 与 Memory、多后端存储、租户治理、审计和故障恢复。

## 已实现能力

- 多租户配置、Agent 应用、模型、工具权限、IM 绑定和审计策略
- 企业微信智能机器人与飞书企业自建应用机器人真实接入
- 无状态 Agent Worker 和基于租户及 Session 的消息路由
- PostgreSQL、Redis、Qdrant、MinIO 与 InMemory 后端适配
- Session lease、fencing token、revision CAS、Inbox/Outbox 和消息幂等
- Memory、Summary、Artifact、Knowledge、MCP 和危险工具二次确认
- 租户预算、权限校验、日志脱敏、审计、指标和全链路 Trace
- 配置灰度、回滚、数据迁移、备份恢复和故障演练

完整架构、数据模型、时序图、同步策略、测试结果和验收标准见 [最终架构设计与验收报告](docs/PR最终架构设计与验收说明.md)。

## 快速运行

### Docker Compose

```bash
docker compose up -d --build gateway worker-1 worker-2 post-turn
```

默认 Compose 使用模拟模型和测试租户，启动 PostgreSQL、Redis、Qdrant、MinIO、Gateway、两个 Worker、Post-turn Worker 和可观测组件，适合本地功能验证。

停止服务：

```bash
docker compose down
```

### 本地测试

```bash
python -m pip install -e ".[dev,im]"
python -m pytest -q
flake8 trpc_service tests
```

### 真实 IM 验收

真实验收需要可用的 PostgreSQL、Redis、Qdrant、MinIO、模型凭据，以及企业微信和飞书机器人凭据。密钥放在 Git 忽略的 `.secrets/` 中，不要写入配置正文或提交到仓库。

```bash
python -m trpc_service._cli live-acceptance --test-timeout 600
```

机器人首次配置见 [真实 IM 配置指南](docs/真实IM首次配置指南-2026-09-06.md)。

## 项目结构

```text
trpc_service/
  agent/          Agent 与 Runner 接线
  channels/       企业微信和飞书适配
  tenant/         多租户模型与路由
  storage/        Session Memory Artifact Knowledge 适配
  reliability/    Inbox Outbox Worker 与恢复
  governance/     权限 预算 审批 审计与脱敏
  telemetry/      Trace 日志与指标
  migration/      Session 和向量数据迁移
  operations/     发布 备份 恢复与容量管理
tests/             单元 集成 故障与端到端测试
deploy/            部署配置和运维规则
docs/              架构和运维文档
reports/           测试与真实验收证据
```

## 当前验收状态

企业微信与飞书真实私聊链路已通过。双租户双 Worker 测试已覆盖 Session、Memory、Summary、工具调用、Artifact、Knowledge、成本结算、回复投递和跨进程 Trace；飞书另完成群聊、跨群和回复线程隔离、图片、文件及消息撤回验证。

真实平台限频、长时间断网、ACK 丢失和生产峰值容量仍需在目标生产环境专项演练。详细边界以最终验收报告为准。
