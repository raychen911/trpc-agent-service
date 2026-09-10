# Quickstart: 共享状态多节点消息闭环

**Feature**: 003-shared-state-multinode-flow
**Purpose**: 实现完成后的本地双节点运行、故障演示和验收指南
**Status**: Implemented and validated on 2026-09-07

## 1. Prerequisites

- Python 3.12
- uv
- Docker Desktop 与 Docker Compose
- PowerShell
- 当前目录为仓库根目录
- 端口 6379、5432、8001、8002 未被占用
- 不需要真实 IM 账号、真实模型 API Key、Kubernetes 或外部模型网络

~~~powershell
cd E:/grad_files/2026trpc-agent/trpc-agent-service-submit
git branch --show-current
uv sync --group dev
uv run python -m trpc_service.agent.sdk_validation
~~~

SDK 基线最后应输出 RESULT: PASS。分支仍为 feature/luwenjie。

## 2. Runtime Secrets

为以下环境变量设置本地临时生成、互不相同的安全值：

~~~powershell
$env:TRPC_DEMO_ALPHA_SECRET = "<runtime-generated-alpha-secret>"
$env:TRPC_DEMO_BETA_SECRET = "<runtime-generated-beta-secret>"
$env:TRPC_DEMO_REDIS_PASSWORD = "<runtime-generated-alphanumeric-password>"
$env:TRPC_DEMO_POSTGRES_PASSWORD = "<different-runtime-generated-alphanumeric-password>"
~~~

尖括号内容必须替换，不能作为真实秘密。不要把变量值写入仓库、截图、测试结果或答辩
材料。服务与 sender 所在 PowerShell 会话需要相同的 Channel secrets。

共享连接通过运行时引用构造：

~~~powershell
$env:TRPC_SHARED_REDIS_URL = "redis://:$($env:TRPC_DEMO_REDIS_PASSWORD)@127.0.0.1:6379/0"
$env:TRPC_SHARED_DATABASE_URL = "postgresql+asyncpg://trpc_agent:$($env:TRPC_DEMO_POSTGRES_PASSWORD)@127.0.0.1:5432/trpc_agent"
$env:TRPC_RUNTIME_PROFILE = "shared"
~~~

演示密码应限制为 URL-safe 字母数字，避免把未编码特殊字符直接放入 URL。

## 3. Start Shared Backends

目标 compose 文件固定使用 redis:7.4.11-alpine3.21 与
postgres:17.11-alpine3.24，并只映射 loopback：

~~~powershell
docker compose -f deploy/local-shared/compose.yaml up -d
docker compose -f deploy/local-shared/compose.yaml ps
~~~

预期 Redis 与 PostgreSQL 均为 healthy。初始化 schema 和两个 demo tenant：

~~~powershell
uv run trpc-agent-shared-init
~~~

该命令必须幂等，只执行已知 migration 和 demo seed；不得打印密码或完整连接 URL。

## 4. Start Two Independent Workers

在 PowerShell A：

~~~powershell
uv run trpc-agent-shared-serve --node-id worker-a --host 127.0.0.1 --port 8001
~~~

在 PowerShell B：

~~~powershell
uv run trpc-agent-shared-serve --node-id worker-b --host 127.0.0.1 --port 8002
~~~

两个进程必须分别创建 Runtime，不能通过同一 Python 对象共享 dict、Session 或 lock。

检查：

~~~powershell
Invoke-RestMethod http://127.0.0.1:8001/healthz
Invoke-RestMethod http://127.0.0.1:8002/healthz
Invoke-RestMethod http://127.0.0.1:8001/readyz
Invoke-RestMethod http://127.0.0.1:8002/readyz
~~~

healthz 预期 status=ok，readyz 预期 status=ready。响应不能泄露后端信息。

## 5. Cross-Node Multi-Turn Session

第一轮发往 worker-a：

~~~powershell
uv run trpc-agent-local-send --url http://127.0.0.1:8001 --binding-id binding-alpha --secret-env TRPC_DEMO_ALPHA_SECRET --external-message-id shared-alpha-001 --external-user-id shared-user --conversation-type direct --external-conversation-id shared-conversation --text "Remember validation token ALPHA."
~~~

第二轮发往 worker-b：

~~~powershell
uv run trpc-agent-local-send --url http://127.0.0.1:8002 --binding-id binding-alpha --secret-env TRPC_DEMO_ALPHA_SECRET --external-message-id shared-alpha-002 --external-user-id shared-user --conversation-type direct --external-conversation-id shared-conversation --text "Recall the validation token."
~~~

预期：

- 两次响应 platform_session_id 相同。
- 第二轮仅召回 ALPHA。
- 两次 trace_id 不同，Audit 可关联各自 node_id。
- worker-a 可以停止，worker-b 仍能继续第三轮。

## 6. Tenant Isolation

使用 binding-beta、相同 external user/conversation 在任一节点保存 BRAVO，再从另一节点
召回。预期 beta 只召回 BRAVO，alpha 仍只召回 ALPHA，两个 platform_session_id 不同。

所有 Redis/SQL 查询和测试断言必须带 tenant scope，不能只比较响应文本。

## 7. Cross-Node Duplicate

准备同一 external_message_id、相同正文和相同 Binding 的请求，同时发往 8001/8002。
推荐使用自动化测试，避免手工启动时序不稳定：

~~~powershell
uv run pytest tests/integration/shared/test_cross_node_idempotency.py -q
~~~

预期每组：

- Agent call count = 1。
- Session business event count = 1。
- delivery_action=deliver 数量 = 1。
- 另一响应为 processing 或 cached terminal，不能再次 deliver。
- 同 ID 不同正文返回 409 conflict。

## 8. Same-Session Serialization

~~~powershell
uv run pytest tests/integration/shared/test_session_serialization.py -q
~~~

预期 50 组同 Session 并发的最大执行并发为 1、迟到写成功数为 0；20 组不同 Session
能够观察到重叠执行。

## 9. Node Failure and Fencing

~~~powershell
uv run pytest tests/integration/shared/test_node_takeover.py -q
uv run pytest tests/integration/shared/test_fencing.py -q
~~~

测试必须覆盖：

- EXECUTION_STARTED 前停止 owner：租约失效后新 generation 安全接管。
- EXECUTION_STARTED 后停止 owner：outcome_unknown，禁止重放。
- 续期/到期/接管竞争：任一时刻有效 owner <= 1。
- 旧 generation 恢复后写 Session/Event/terminal/business Audit：全部拒绝。
- 当前平台身份追加独立 late_write_rejected diagnostic Audit。

## 10. Backend Outage and Recovery

自动化故障测试负责安全地停止/恢复测试后端：

~~~powershell
uv run pytest tests/integration/shared/test_backend_outages.py -q
uv run pytest tests/integration/shared/test_partial_commit_recovery.py -q
~~~

预期：

- Redis 在开始前不可用：backend_unavailable，Agent 不执行。
- PostgreSQL 配置不可用：authorization_unavailable；旧正向 cache 也不能放行。
- Agent 开始后丢失 lease 或终态不确定：outcome_unknown，不自动重放。
- final Audit + Recovery Marker 已成功但 Redis terminal 不确定：reconciler 只补齐原
  ExecutionResult，Agent call count 不增加。
- 错误 generation/result digest：进入 conflict_review，不覆盖当前终态。

## 11. Contract and Regression Suites

先运行不要求 Docker 的快速基线：

~~~powershell
uv run pytest tests/unit -q
uv run pytest tests/contract -m "not shared_backend" -q
uv run pytest tests/sdk_validation -q
~~~

后端运行时执行共享契约与完整验收：

~~~powershell
uv run pytest tests/contract -m shared_backend -q
uv run pytest tests/integration/shared -q
uv run pytest tests/e2e/test_two_worker_processes.py -q
uv run pytest -q
~~~

同一核心契约测试必须分别报告 InMemory、Redis/PostgreSQL 两套 Adapter 通过；供应商
专属断言只允许位于 Adapter setup/fault tests。

## 12. Ten-Minute Demonstration

计时顺序：

1. docker compose up 和 shared-init。
2. 启动 worker-a、worker-b 并检查 readyz。
3. A 保存、B 召回同一 Session。
4. 双节点并发发送同一消息并展示一次执行/一次投递。
5. 运行节点接管与 fencing 的定向测试。
6. 展示按 tenant/session/trace 查询的 Audit 与 Recovery 断言。

从第 1 步到明确 PASS/FAIL 不超过 10 分钟。记录真实耗时、机器环境、镜像版本、测试数
和失败注入点，不记录秘密。

## 13. Evidence to Capture

- 两个独立进程 PID/node_id 和两个 ready 响应。
- 跨节点 20 轮同 Session 结果。
- 50 组并发重复的 Agent/event/delivery 计数。
- 同 Session serial 与不同 Session parallel 峰值。
- 五项澄清决策对应测试名称与输出。
- worker kill 前后 generation、owner/execution trace 关系。
- late-write diagnostic Audit。
- terminal_pending -> reconciled 过程且 Agent count 不增加。
- 配置 outage 下旧 cache 被拒绝。
- 完整 pytest 通过数、耗时和秘密扫描结果。

## 14. Cleanup

先停止两个 Worker，再停止 compose：

~~~powershell
docker compose -f deploy/local-shared/compose.yaml down
Remove-Item Env:TRPC_DEMO_ALPHA_SECRET
Remove-Item Env:TRPC_DEMO_BETA_SECRET
Remove-Item Env:TRPC_DEMO_REDIS_PASSWORD
Remove-Item Env:TRPC_DEMO_POSTGRES_PASSWORD
Remove-Item Env:TRPC_SHARED_REDIS_URL
Remove-Item Env:TRPC_SHARED_DATABASE_URL
Remove-Item Env:TRPC_RUNTIME_PROFILE
~~~

普通 down 保留 named volumes 便于 Worker restart 验证。只有明确重置 demo 数据时才使用
实现提供的受保护 reset 流程；不得误删非测试数据库或生产数据。

## 15. Known Limits

- 仅本地两个 Worker 与单 Redis/PostgreSQL 实例，不证明生产 HA。
- 不提供跨 Redis/PostgreSQL 分布式事务或生产 exactly-once。
- Redis AOF/volume 只用于本地恢复验证。
- 无真实企业微信、第二 IM、真实模型、Tool、Memory、向量库或 Kubernetes。
- Metrics 仍为 node-scoped 验收数据，不是完整生产 Telemetry。
- 配置后端不可用时主动牺牲可用性并默认拒绝。

## 16. Validated Result

2026-09-07 在 Windows、Python 3.12、Docker Engine 29.6.2 上使用固定 Redis/PostgreSQL
镜像完成最终验证：全量 `uv run pytest -q` 为 165 passed in 31.67s；SDK 定向回归为
18 passed in 3.73s；200 次确定性随机路由为 0 错误、200 次执行，p95 45.57ms。
这些数字是本地功能证据，不是生产容量承诺。
