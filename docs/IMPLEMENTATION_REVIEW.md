# 实现审查

本文档汇总题目要求在本分支中的实现状态，以及仓库内可核实的实测证据。

## 总体结论

本分支已经覆盖题目的主干要求：

- 多租户与节点化部署
- 数据同步与多后端支持
- 至少两类 IM 接入
- 治理、监控与安全
- 故障恢复与运维
- 迁移、幂等与补偿

仓库内还提供压测、真实模型集成测试、PostgreSQL RLS 集成测试和故障注入脚本，
用于区分纯单元测试、协议级集成验证和需要外部账号的真实平台联调。

## 题目要求对照

### 已实现

1. 多租户模型
   - 包含 `tenant_id`、应用配置、模型配置、工具权限、IM 通道配置、数据后端配置、审计策略。
   - 见 [trpc_service/tenant/models.py](../trpc_service/tenant/models.py) 和 [docs/DATA_MODEL.md](./DATA_MODEL.md)。

2. 节点化部署
   - 已实现 Gateway、Worker、Channel Adapter、Storage Adapter、Admin API、Telemetry Collector 的协作说明。
   - 见 [docs/ARCHITECTURE.md](./ARCHITECTURE.md) 和 [deployment/kubernetes/platform.yaml](../deployment/kubernetes/platform.yaml)。

3. 多节点水平扩展
   - Gateway/Worker 无状态化，Session / Memory / Summary / Idempotency 依赖共享后端。
   - 见 [trpc_service/gateway/router.py](../trpc_service/gateway/router.py) 和 [trpc_service/storage/locking.py](../trpc_service/storage/locking.py)。

4. 数据同步与多后端
   - 支持 Redis、SQLite、PostgreSQL、向量库、对象存储、外部 Memory 服务。
   - 见 [trpc_service/storage/factory.py](../trpc_service/storage/factory.py) 和 [docs/MIGRATION.md](./MIGRATION.md)。

5. IM 接入
   - 已实现企业微信、微信公众号、微信客服、Telegram、Web UI。
   - 见 [trpc_service/channels/wecom.py](../trpc_service/channels/wecom.py)、[trpc_service/channels/wechat_official_account.py](../trpc_service/channels/wechat_official_account.py)、[trpc_service/channels/telegram.py](../trpc_service/channels/telegram.py)、[trpc_service/channels/web.py](../trpc_service/channels/web.py)。

6. 治理与监控
   - 已实现租户 Filter、配额、脱敏、审计、OTel tracing、Prometheus metrics。
   - 见 [trpc_service/policy/tenant_filter.py](../trpc_service/policy/tenant_filter.py)、[trpc_service/policy/quota.py](../trpc_service/policy/quota.py)、[trpc_service/telemetry/tracing.py](../trpc_service/telemetry/tracing.py)、[trpc_service/telemetry/metrics.py](../trpc_service/telemetry/metrics.py)。

7. 故障恢复
   - 已实现补偿队列、幂等恢复、Webhook/Worker 重试、Redis/SQL 故障恢复。
   - 见 [trpc_service/storage/compensation.py](../trpc_service/storage/compensation.py) 和 [trpc_service/gateway/worker_queue.py](../trpc_service/gateway/worker_queue.py)。

8. 灰度与回滚
   - 已实现租户配置版本、发布、回滚和灰度分流。
   - 见 [trpc_service/tenant/service.py](../trpc_service/tenant/service.py)。

## 实测证据

### 0. 2026-09-01 当前分支验收

本次验收直接针对当前工作区执行，未复用历史报告结论：

- `python -m unittest discover -s tests -v`：107 条通过，常规测试仅跳过两个显式 opt-in 的外部集成用例。
- `python -m compileall -q trpc_service tests`：通过。
- `python -m flake8 trpc_service tests`：通过。
- 真实 Responses API：通过，服务完成鉴权、模型发现和实际文本生成。
- 默认 `TRPC_AGENT_RUNTIME_MODE=trpc`：通过完整 `TrpcAgentWorker.run()`、Session lease、模型调用及状态写入。
- PostgreSQL 16 RLS：在一次性容器中验证 app 角色跨租户不可见、admin 角色可按权限访问，测试通过后删除容器。
- 生产 Compose：启用 PostgreSQL RLS、`DURABLE_INBOX_OUTBOX=1`、Redis 远程 Worker 队列和两个 Worker；健康检查、真实模型回复、重复消息幂等回放、Inbox/Outbox 表检查均通过。
- 企业微信和 Telegram：7 条协议级组合测试通过，覆盖 SDK 路径、XML/签名/AES、Webhook secret、图片/文件和消息幂等。

本次同时修复了 `TrpcAgentWorker._run_unlocked` 未接收 `SessionLease` 导致默认
tRPC 运行模式在模型调用前抛出 `TypeError` 的问题，并加入通过公开 `run()` 路径
执行的回归测试。

### 1. 本地压测

`data/load-test-report.json` 记录了本地 Web UI 压测：

- 时间：2026-08-27
- 请求数：100
- 并发数：10
- 成功数：100
- 吞吐：15.85 QPS
- 延迟：p50 610.19 ms，p95 666.96 ms，p99 669.46 ms

这说明 fallback 模式下的完整主链路可稳定运行。

### 2. 真实模型小并发测试（历史记录）

`data/load-test-real-model-report.json` 记录了真实模型链路：

- 时间：2026-08-27
- 请求数：5
- 并发数：2
- 成功数：5
- 延迟：p95 142891.1 ms

该文件说明真实模型链路曾经跑通，但外部模型耗时明显高于本地
fallback。当前测试默认不访问外部模型，必须由 reviewer 显式提供临时凭据。

### 3. PostgreSQL RLS 真实集成测试

2026-08-31 在独立的 PostgreSQL 16 临时容器中执行：

```text
POSTGRES_RLS_TEST_DSN=postgresql://postgres:<temporary-password>@127.0.0.1:15433/trpc_rls_test
POSTGRES_RLS_TEST_ALLOW_DESTRUCTIVE=1
python -m unittest tests.test_postgres_rls -v
```

结果为 `Ran 6 tests ... OK`。测试使用唯一角色名和唯一数据前缀，结束后删除临时
角色、策略、表上的 RLS 状态和容器，不影响项目 Compose 数据库。

### 4. 故障注入

`data/fault-injection-report.json` 记录了 Redis 和 PostgreSQL 停机恢复：

- Redis 停机期间请求返回 500
- Redis 恢复后 `PONG`
- PostgreSQL 停机期间请求返回 500
- PostgreSQL 恢复后 `accepting connections`

这说明故障恢复和重新探活链路可用。

## Reviewer 复现命令

真实模型测试需要一个新生成的、可撤销的测试 Key；不要把 Key 提交到仓库：

```powershell
$env:RUN_REAL_MODEL_TESTS="1"
$env:CPA_BASE_URL="https://<provider>/v1"
$env:CPA_MODEL="<model-name>"
$env:CPA_WIRE_API="responses"
$env:OPENAI_API_KEY="<fresh-test-key>"
py -3.12 -m unittest tests.test_real_model_integration -v
```

Linux/macOS 等价命令：

```bash
RUN_REAL_MODEL_TESTS=1 CPA_BASE_URL=https://<provider>/v1 \
CPA_MODEL=<model-name> CPA_WIRE_API=responses OPENAI_API_KEY=<fresh-test-key> \
python -m unittest tests.test_real_model_integration -v
```

RLS reviewer 可以使用任意可丢弃 PostgreSQL 16 实例。Windows PowerShell 示例：

```powershell
docker run --rm --name trpc-agent-rls-review `
  -e POSTGRES_PASSWORD=local-only `
  -e POSTGRES_DB=trpc_rls_test -p 15433:5432 -d postgres:16-alpine
# 等待 pg_isready 成功后执行测试
$env:POSTGRES_RLS_TEST_DSN="postgresql://postgres:local-only@127.0.0.1:15433/trpc_rls_test"
$env:POSTGRES_RLS_TEST_ALLOW_DESTRUCTIVE="1"
py -3.12 -m unittest tests.test_postgres_rls -v
docker rm -f trpc-agent-rls-review
```

## 验收结论

当前实现已通过不依赖第三方 IM 账号的核心验收，包括真实模型、默认 tRPC-Agent
运行时、PostgreSQL RLS 和生产 Compose 多节点链路。仍需要目标环境提供真实平台
账号后完成的外部验收是：

- 真实企业微信 / 微信公众号 / 微信客服账号配置
- 真实 Telegram Bot 或另一类 IM 账号配置
- 生产级 Redis / PostgreSQL / 向量库 / 对象存储连通性
- 更大规模容量压测
- 生产 OTel exporter 落地

当前机器的 Process、User、Machine 环境以及项目 `.env` 中均未配置任何真实 IM
凭据，因此不能把协议级测试表述为真实平台收发成功。真实联调步骤和所需密钥名
见 [IM_INTEGRATION.md](./IM_INTEGRATION.md)。模型凭据只在验收子进程环境中使用，
未写入仓库、日志或测试数据。
