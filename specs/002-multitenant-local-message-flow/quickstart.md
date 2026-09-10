# Quickstart: 多租户本地消息闭环

**Feature**: `002-multitenant-local-message-flow`
**Purpose**: 实现完成后的本地运行、演示和验收指南

> 本文命令已在第二阶段实现完成后按真实 loopback 进程验证。客户端明确禁用系统代理，
> 避免本机代理环境截获 `127.0.0.1` 请求。

## 1. Prerequisites

- Windows PowerShell
- Python 3.12
- uv
- 当前目录为仓库根目录
- 不需要真实 IM 账号、模型 API Key、Redis、SQL 或外部网络

```powershell
cd E:\grad_files\2026trpc-agent\trpc-agent-service-submit
git branch --show-current
uv sync --group dev
```

分支应为 `feature/luwenjie`。依赖同步后先验证第一阶段基线：

```powershell
uv run python -m trpc_service.agent.sdk_validation
```

预期最后一行为 `RESULT: PASS`。

## 2. Runtime Secret Setup

每个 Channel Binding 使用独立运行时秘密。不要把秘密写入仓库、配置样例或命令输出
记录。以下变量的值应由密码生成器在本地临时生成，并在启动服务和调用客户端的两个
PowerShell 会话中设置为相同值：

```powershell
$env:TRPC_DEMO_ALPHA_SECRET = "<runtime-generated-secret>"
$env:TRPC_DEMO_BETA_SECRET = "<different-runtime-generated-secret>"
```

尖括号内容是说明性占位符，不是可用秘密。两个绑定不得共用同一秘密。演示结束后：

```powershell
Remove-Item Env:TRPC_DEMO_ALPHA_SECRET
Remove-Item Env:TRPC_DEMO_BETA_SECRET
```

## 3. Start the Local Service

目标启动命令：

```powershell
uv run trpc-agent-local-serve --host 127.0.0.1 --port 8000
```

预期服务只监听 loopback，并显示不包含消息正文、签名或秘密的启动信息。

健康检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/healthz
```

预期：

```json
{"status":"ok"}
```

## 4. Send a Signed Message

目标客户端命令会按照
[local-message-http.md](./contracts/local-message-http.md) 生成 HMAC：

```powershell
uv run trpc-agent-local-send --url http://127.0.0.1:8000 --binding-id binding-alpha --secret-env TRPC_DEMO_ALPHA_SECRET --external-message-id alpha-001 --external-user-id shared-user --conversation-type direct --external-conversation-id shared-conversation --text "Remember validation token ALPHA."
```

预期：

- HTTP 200。
- `status=succeeded`。
- `delivery_action=deliver`。
- 返回非空 `trace_id` 与 tenant-scoped `platform_session_id`。
- 回复文本为确定性 Agent 的正式最终结果。

第二轮使用新消息标识和相同用户/会话：

```powershell
uv run trpc-agent-local-send --url http://127.0.0.1:8000 --binding-id binding-alpha --secret-env TRPC_DEMO_ALPHA_SECRET --external-message-id alpha-002 --external-user-id shared-user --conversation-type direct --external-conversation-id shared-conversation --text "Recall the validation token."
```

预期回复仅召回 `ALPHA`。

## 5. Verify Tenant Isolation

使用 beta 绑定、相同外部用户和相同外部会话保存不同标记：

```powershell
uv run trpc-agent-local-send --url http://127.0.0.1:8000 --binding-id binding-beta --secret-env TRPC_DEMO_BETA_SECRET --external-message-id beta-001 --external-user-id shared-user --conversation-type direct --external-conversation-id shared-conversation --text "Remember validation token BRAVO."
```

再以 beta 绑定发送 recall。预期 beta 只召回 `BRAVO`，alpha 仍只召回 `ALPHA`，
且两个响应的 `platform_session_id` 不同。

## 6. Verify Duplicate Delivery

使用与 alpha-001 完全相同的参数再次发送。

预期：

- HTTP 200，`status=duplicate`。
- 文本与首次结果相同。
- `delivery_action=suppress`。
- 当前 `trace_id` 可关联本次投递。
- `original_trace_id` 指向首次执行。
- Agent 执行计数、Session 业务事件和业务回复投递均不增加。

随后复用 alpha-001，但修改正文。预期 HTTP 409、
`error.code=idempotency_conflict`，且首次结果保持不变。

## 7. Verify Authentication Rejection

在客户端会话中临时使用与服务端不同的秘密：

```powershell
$savedSecret = $env:TRPC_DEMO_ALPHA_SECRET
$env:TRPC_DEMO_ALPHA_SECRET = "<different-generated-value>"
uv run trpc-agent-local-send --url http://127.0.0.1:8000 --binding-id binding-alpha --secret-env TRPC_DEMO_ALPHA_SECRET --external-message-id rejected-001 --external-user-id shared-user --conversation-type direct --external-conversation-id shared-conversation --text "This must be rejected."
$env:TRPC_DEMO_ALPHA_SECRET = $savedSecret
```

预期 HTTP 401、`error.code=unauthorized`，没有 Agent 执行或 Session 写入。
未知 binding、过期时间戳和错误签名的公开响应必须不可区分。

## 8. Automated Acceptance

```powershell
uv run pytest tests/unit -q
uv run pytest tests/contract -q
uv run pytest tests/integration/test_multitenant_message_flow.py -q
uv run pytest tests/integration/test_acceptance_scale.py -q
uv run pytest tests/sdk_validation -q
uv run pytest -q
```

必须覆盖：

- 两租户相同外部标识无串话。
- 同租户双轮连续。
- 100 次顺序重复只执行一次。
- 20 组并发重复每组只执行一次。
- 同一会话不同消息串行，不同会话可并行。
- 开始前失败允许重试，开始后失败和结果未知不重放。
- HMAC 正常、篡改、未知绑定和 ±300 秒边界。
- trace_id 与 original_trace_id 关联。
- 审计失败显式可见。
- RUNNING 前准备失败可重试，RUNNING 后超时/取消保存为 OUTCOME_UNKNOWN 且释放锁。
- 审计按租户、会话和 trace 查询不会跨租户；PreAuthScope 无法访问租户记录。
- 租户指标快照准确记录请求/错误/延迟/投递，离线模型、工具和真实 IM 指标为零或
  not_applicable。
- stdout、stderr、审计和错误响应无秘密及完整正文。
- 无外部模型调用。

从执行启动命令开始计时，到首次成功响应和审计查询完成为止必须不超过 5 分钟；
将实际耗时写入 validation-results.md。Repository、Audit 和 Metrics 契约测试还要
针对 InMemory Adapter 与最小 Fake Adapter 运行同一组行为断言，证明上层语义不依赖
本地专属数据表示。

## 9. Evidence to Capture

为阶段成果记录和答辩保存以下非敏感证据：

1. 健康检查和一条成功响应。
2. alpha、beta 使用相同外部标识但返回不同上下文的结果。
3. duplicate 响应中的 current/original trace 关系。
4. 错误签名的统一 401 响应。
5. pytest 总通过数和耗时。
6. 决策记录中 D-001 至 D-008 对应的测试名称。
7. 首次演示总耗时、租户作用域审计查询和 MetricsSnapshot。
8. 同一套契约测试在 InMemory 与最小 Fake Adapter 上的结果。

不得截图、提交或复制运行时秘密和完整签名。

## 10. Known Limits

- 仅单进程 InMemory，重启后数据消失。
- 幂等保证只覆盖当前进程生命周期；服务重启后五分钟签名窗口内的旧请求不会被本阶段
  的 InMemory 状态去重，因此不得宣称跨重启 exactly-once。
- 不支持多 Worker 或跨节点 Session。
- 不是真实企业微信或第二种 IM 联调。
- 不包含 Redis、SQL、向量库或对象存储。
- 使用确定性离线模型，不证明真实模型质量或供应商稳定性。
- 不包含生产密钥管理、公开审计/指标查询 API、限流或 Kubernetes；本阶段指标仅为
  进程内验收快照，不代表生产 Telemetry。

这些限制必须在演示和最终报告中明确说明。
