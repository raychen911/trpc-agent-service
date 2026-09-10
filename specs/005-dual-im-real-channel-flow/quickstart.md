# 第五阶段快速验证指南

**功能**：005-dual-im-real-channel-flow
**用途**：在实现完成后重复验证飞书和企业微信真实消息闭环

> 当前状态：第五阶段离线实现与自动化门禁已完成；2026-09-08 的结果为
> `218 passed, 28 skipped`。28 个 skip、真实双客户端和人工接管仍需在具备
> Redis/PostgreSQL 与轮换后凭证的环境执行，不得写成已通过。

## 1. 实现基线准备

当前实现仓库为 trpc-agent-service-submit，功能分支
005-dual-im-real-channel-flow 已继承第三阶段提交 1bbf202，且第五阶段目录已迁入。
开始每次实现前仍应确认当前分支和基线：

~~~powershell
git branch --show-current
git merge-base --is-ancestor 1bbf202 HEAD
uv sync --group dev
uv run pytest -q
~~~

预期：分支为 005-dual-im-real-channel-flow，merge-base 检查成功，001、002、003、005
测试无失败。当前离线回归为 218 passed、28 skipped；共享后端用例需启动
Redis/PostgreSQL 后补跑。若回归失败，不开始真实客户端验收。

## 2. 安全前置条件

在真实连接前完成：

1. 轮换曾出现在独立 Echo 测试脚本注释中的飞书和企业微信凭证。
2. 删除所有源码、文档、命令示例和测试夹具中的真实值。
3. 确保 .env、日志、截图原件和本地凭证文件不进入 Git。
4. 运行工作区与 Git 历史敏感信息扫描。
5. Channel Binding 只保存 Secret 环境变量名称或 Secret Provider 引用。

任何一项未满足都不得启动真实 Adapter 或提交代码。

## 3. 安装与配置

第五阶段目标依赖：

~~~text
lark-channel-sdk==1.4.0
wecom-aibot-python-sdk==1.0.2
~~~

实现完成后：

~~~powershell
uv sync --group dev
~~~

真实凭证只在当前终端或受控 Secret Provider 中设置。以下函数通过安全提示读取值，
不会把输入回显到屏幕或写入 PowerShell 命令历史；不要把占位符替换成真实值后保存脚本。

~~~powershell
function Set-ProcessSecret([string]$Name) {
    $secureValue = Read-Host "输入 $Name" -AsSecureString
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureValue)
    try {
        [Environment]::SetEnvironmentVariable(
            $Name,
            [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer),
            "Process"
        )
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
        $secureValue.Dispose()
    }
}

Set-ProcessSecret "TRPC_DEMO_REDIS_PASSWORD"
Set-ProcessSecret "TRPC_DEMO_POSTGRES_PASSWORD"
Set-ProcessSecret "LARK_TENANT_KEY"
Set-ProcessSecret "LARK_APP_ID"
Set-ProcessSecret "LARK_APP_SECRET"
Set-ProcessSecret "WECOM_CORP_ID"
Set-ProcessSecret "WECOM_BOT_ID"
Set-ProcessSecret "WECOM_BOT_SECRET"

$redisPassword = [Uri]::EscapeDataString($env:TRPC_DEMO_REDIS_PASSWORD)
$postgresPassword = [Uri]::EscapeDataString($env:TRPC_DEMO_POSTGRES_PASSWORD)
$env:TRPC_SHARED_REDIS_URL = "redis://:$redisPassword@127.0.0.1:6379/0"
$env:TRPC_SHARED_DATABASE_URL = "postgresql+asyncpg://trpc_agent:$postgresPassword@127.0.0.1:5432/trpc_agent"
Remove-Variable redisPassword, postgresPassword -ErrorAction SilentlyContinue
~~~

关闭终端前清除凭证环境变量：

~~~powershell
Remove-Item Env:LARK_APP_ID -ErrorAction SilentlyContinue
Remove-Item Env:LARK_APP_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:LARK_TENANT_KEY -ErrorAction SilentlyContinue
Remove-Item Env:WECOM_BOT_ID -ErrorAction SilentlyContinue
Remove-Item Env:WECOM_BOT_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:WECOM_CORP_ID -ErrorAction SilentlyContinue
Remove-Item Env:TRPC_SHARED_REDIS_URL -ErrorAction SilentlyContinue
Remove-Item Env:TRPC_SHARED_DATABASE_URL -ErrorAction SilentlyContinue
Remove-Item Env:TRPC_DEMO_REDIS_PASSWORD -ErrorAction SilentlyContinue
Remove-Item Env:TRPC_DEMO_POSTGRES_PASSWORD -ErrorAction SilentlyContinue
~~~

不要把真实值粘贴到源码、README、聊天截图或 Git 提交中。

## 4. 自动化测试

以下命令由 tasks/implementation 阶段落实。

### 4.1 单元测试

~~~powershell
uv run pytest tests/unit/channels -q
~~~

验证：

- 可信复合 Channel Identity。
- 飞书/企业微信字段标准化。
- 群聊结构化 @ 和自身消息过滤。
- 单聊/群聊 Session key。
- Delivery 状态机和错误分类。
- Secret redaction。

### 4.2 Adapter 契约测试

~~~powershell
uv run pytest tests/contract/channels -q
~~~

预期：FeishuChannelAdapter 和 WeComChannelAdapter 对同一行为矩阵全部通过。

### 4.3 共享后端集成测试

Docker Desktop Engine 必须已经启动。全部命令必须在第 3 节注入变量的同一个
PowerShell 窗口执行；另一个窗口设置的进程级环境变量不会自动传入当前窗口。

~~~powershell
docker info
docker compose -f deploy/local-shared/compose.yaml up -d --wait --wait-timeout 120
docker compose -f deploy/local-shared/compose.yaml ps

$python = (Resolve-Path .\.venv\Scripts\python.exe).Path
& $python -m trpc_service._cli shared-init
if ($LASTEXITCODE -ne 0) { throw "shared-init failed" }

$feishuBinding = & $python -m trpc_service._cli shared-channel-init --channel feishu | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw "Feishu binding initialization failed" }
$wecomBinding = & $python -m trpc_service._cli shared-channel-init --channel wecom | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw "WeCom binding initialization failed" }
~~~

`shared-channel-init` 只把 `LARK_APP_SECRET` / `WECOM_BOT_SECRET` 变量名作为
`secret_ref` 写入 PostgreSQL，不写入凭证值；输出只包含渠道、固定 Binding ID、
identity digest 和状态。然后先运行 T078 的 28 个共享后端测试：

~~~powershell
& $python -m pytest -q -p no:cacheprovider -m shared_backend
~~~

预期为 `28 passed`、退出码 `0`、无 skip。再运行渠道集成、双 Worker e2e 和全量回归：

~~~powershell
& $python -m pytest -q -p no:cacheprovider tests/integration/channels tests/e2e
& $python -m pytest -q -p no:cacheprovider
~~~

验证：

- 不同节点重复收到同 message_id/msgid，Agent 调用 <= 1。
- 同一 Session 跨 Worker 多轮连续。
- 同群不同成员 Session 隔离。
- Delivery 恢复不增加 Agent 调用。
- Adapter 主动/备用接管和旧 fence 拒绝。

### 4.4 全量回归

~~~powershell
uv run pytest -q
~~~

预期：001、002、003 和 005 全部通过，既有 HTTP v1 不变。

也可以使用仓库内的一键门禁脚本；脚本使用现有 `.venv`，不会触发联网安装：

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File specs/005-dual-im-real-channel-flow/scripts/run-automated-validation.ps1
~~~

安全扫描命令只输出命中数量和文件路径，不回显疑似敏感值：

~~~powershell
powershell -NoProfile -ExecutionPolicy Bypass -File specs/005-dual-im-real-channel-flow/scripts/scan-sensitive-material.ps1
~~~

## 5. SDK 测试替身故障矩阵

| Case | Expected |
|---|---|
| 连接中断后平台重放 | 重新进入共享幂等，Agent <=1 |
| Secret 无效 | Adapter not_ready，日志无 Secret |
| 未知/禁用 Binding | 默认拒绝，Agent 0 |
| 自身消息 | 静默忽略，Session/Agent/reply 均 0 |
| 身份字段缺失 | 安全拒绝并审计，Agent 0 |
| 群聊未 @ | 不调用 Agent，不回复 |
| 临时发送失败 | 1、2、4 秒后重试，最多 3 次 |
| 永久发送失败 | delivery_failed，重试 0 |
| ACK 超时 | delivery_unknown，自动重发 0 |
| 旧 Adapter fence | 发送和状态写入均拒绝 |

测试用虚拟时钟验证退避，不等待真实 7 秒。

## 6. 启动 Adapter

在完成 T078 后，在同一个安全 PowerShell 中启动两个 Worker 和每个渠道的两个
Adapter。子进程继承当前终端环境变量，日志只写入本机临时目录：

~~~powershell
$repoRoot = (Get-Location).Path
$logDir = Join-Path $env:TEMP ("trpc-real-acceptance-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
New-Item -ItemType Directory -Path $logDir | Out-Null

function Start-TrpcProcess([string]$Name, [string[]]$ProcessArgs) {
    Start-Process -FilePath $python -WorkingDirectory $repoRoot `
        -ArgumentList $ProcessArgs -PassThru `
        -RedirectStandardOutput (Join-Path $logDir "$Name.out.log") `
        -RedirectStandardError (Join-Path $logDir "$Name.err.log")
}

$workerA = Start-TrpcProcess "worker-a" @("-m", "trpc_service._cli", "shared-serve", "--node-id", "worker-a", "--port", "8001")
$workerB = Start-TrpcProcess "worker-b" @("-m", "trpc_service._cli", "shared-serve", "--node-id", "worker-b", "--port", "8002")
$feishuA = Start-TrpcProcess "feishu-a" @("-m", "trpc_service._cli", "channel-serve", "--channel", "feishu", "--node-id", "feishu-a")
$feishuB = Start-TrpcProcess "feishu-b" @("-m", "trpc_service._cli", "channel-serve", "--channel", "feishu", "--node-id", "feishu-b")
$wecomA = Start-TrpcProcess "wecom-a" @("-m", "trpc_service._cli", "channel-serve", "--channel", "wecom", "--node-id", "wecom-a")
$wecomB = Start-TrpcProcess "wecom-b" @("-m", "trpc_service._cli", "channel-serve", "--channel", "wecom", "--node-id", "wecom-b")

Start-Sleep -Seconds 5
Invoke-RestMethod http://127.0.0.1:8001/readyz
Invoke-RestMethod http://127.0.0.1:8002/readyz
$feishuReadiness = Invoke-RestMethod "http://127.0.0.1:8001/v1/channels/readiness?identity_digest=$($feishuBinding.identity_digest)"
$wecomReadiness = Invoke-RestMethod "http://127.0.0.1:8001/v1/channels/readiness?identity_digest=$($wecomBinding.identity_digest)"
$feishuReadiness
$wecomReadiness
$logDir
~~~

预期两个 Worker 均为 `ready`，每个 Channel Identity 的共享 readiness 为 `ready`，
且只报告一个 `owner_node_id` 和一个 generation。另一个 Adapter 进程保持 standby。

Worker 的安全状态接口可使用 Channel Identity 摘要查询 ownership；响应不会回显摘要：

~~~text
GET /v1/channels/readiness?identity_digest=<64位小写十六进制摘要>
~~~

按租户和 trace 查询脱敏链路摘要：

~~~powershell
uv run trpc-agent-trace-diagnose --tenant-id tenant-alpha --trace-id <trace-uuid>
~~~

输出仅包含租户摘要、trace 角色、Session 摘要、Adapter 节点/generation、
Delivery ID/attempt/status 和稳定错误类型，不包含原始用户、消息、凭证或连接 URL。

## 7. 飞书真实客户端验收

前置条件：

- 自建应用已发布到测试企业。
- 机器人消息接收/发送和长连接事件权限已生效。
- 测试用户在应用可用范围内。
- PostgreSQL 已配置唯一 active Channel Binding。

步骤：

1. 单聊发送第一轮文本，确认机器人回复。
2. 连续发送第二、第三轮，确认上下文延续。
3. 在群聊中发送未 @ 消息，确认无 Agent 回复。
4. 明确 @ 机器人发送文本，确认回复到原群。
5. 由同群另一用户 @，确认不读取第一位用户历史。
6. 重放同一 message_id 的测试事件，确认 Agent 不重复执行。
7. 记录脱敏 trace_id，并查询 Audit 与 Delivery 状态。

## 8. 企业微信真实客户端验收

前置条件：

- API 模式智能机器人使用长连接。
- 新 Bot ID/Secret 已轮换并只存在于安全运行环境。
- 消息权限已授权，机器人对测试用户可用。
- PostgreSQL 已配置唯一 active Channel Binding。

步骤与飞书相同，同时额外确认：

- 业务幂等 ID 使用 msgid。
- req_id 仅用于协议回复上下文。
- ACK 超时测试进入 delivery_unknown，不自动重发。

## 9. 主动/备用接管验收

1. 同一机器人启动两个 Adapter 节点。
2. 查询 readiness，确认 ready 数量为 1。
3. 停止活动节点，等待租约到期和备用接管。
4. 发送新消息，确认新节点可以处理。
5. 模拟旧节点恢复，确认旧 generation 无法发送或写入。
6. 检查 takeover、lease_lost 和 stale_adapter_rejected 审计。

验收目标：同一 Channel Identity 同时 ready 数 <=1，旧节点成功写入/发送次数为 0。

## 10. 验收证据模板

每个渠道记录：

~~~text
渠道：
验收时间：
应用/机器人（仅非敏感名称或摘要）：
Binding ID 摘要：
Adapter 节点与 generation：
消息场景：单聊 / 群聊@ / 未@ / 重复 / 故障
trace_id：
execution_trace_id：
platform_session_id：
Agent 调用次数：
Delivery 状态与 attempt 数：
Audit 查询结果：
客户端截图（已脱敏）：
结论：通过 / 未通过
限制说明：
~~~

不得记录 Secret、token、ticket、access_key、完整 WebSocket URL、原始 SDK frame
或包含凭证的终端截图。

## 11. 完成判定

只有同时满足以下条件才可声明第五阶段完成：

- 自动化测试与 001/002/003 回归全部通过。
- 飞书、企业微信真实客户端各完成 3 轮单聊和群聊 @。
- 重复投递 Agent 执行次数 <=1。
- 未知/禁用/错误 Binding、self-message、身份不确定均 Agent 0。
- 临时/永久/未知交付语义与 DEC-003 一致。
- 主动/备用 ready 数 <=1，旧 fence 成功操作 0。
- trace/Audit/Delivery 证据完整且 Secret 扫描为 0。
