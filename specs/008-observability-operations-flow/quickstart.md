# Quickstart：第八阶段可观测性与运维验收

本指南描述当前实现的可重复验收顺序。它不包含生产 Secret，测试细节见
[data-model.md](./data-model.md) 与 [contracts/](./contracts/)。以下命令已在
2026-09-11 的真实本地 Docker 环境中执行通过，最终证据见
[validation-results.md](./validation-results.md)。

## 1. 前置条件

- Windows PowerShell 7 或 Linux shell；以下示例使用 PowerShell。
- Python 3.12、`uv`、Docker Engine 与 Compose v2。
- 仓库：`E:\grad_files\2026trpc-agent\trpc-agent-service-submit`。
- 真实 IM 验收是可选 profile；默认测试使用 SDK 替身，不要求真实凭证。
- PostgreSQL/Redis 密码只在当前进程或 `.env.local` 中设置；`.env.local` 必须被 Git 忽略。不要把值贴到终端日志、文档或聊天。

```powershell
Set-Location E:\grad_files\2026trpc-agent\trpc-agent-service-submit
uv sync
git status -sb
```

预期：依赖安装成功，当前分支为 `feature/luwenjie`；第八阶段工作可见且没有 Secret 文件进入 Git 候选。

## 2. 快速契约验证

```powershell
uv run pytest tests/unit/observability tests/unit/operations -q -p no:cacheprovider
uv run pytest tests/contract/observability tests/contract/operations -q -p no:cacheprovider
```

必须证明：

- 官方 Runner span 通过出口前 allowlist，预置正文/Token/URL/身份原文为 0 命中。
- 关键异常采样决策 100% keep，普通成功按稳定 hash 采样。
- exporter 故障不改变业务结果；缓冲有界、有限重试、普通成功优先丢弃。
- 各角色 readiness 矩阵、告警去重/恢复、release 状态机和稳定错误契约通过。

## 3. 共享后端准备

在当前进程安全设置以下变量，实际值不要回显：

```powershell
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
    }
}

Set-ProcessSecret "TRPC_DEMO_REDIS_PASSWORD"
Set-ProcessSecret "TRPC_DEMO_POSTGRES_PASSWORD"

$env:TRPC_SHARED_REDIS_URL = "redis://:" + [uri]::EscapeDataString($env:TRPC_DEMO_REDIS_PASSWORD) + "@127.0.0.1:6379/0"
$env:TRPC_SHARED_DATABASE_URL = "postgresql+asyncpg://trpc_agent:" + [uri]::EscapeDataString($env:TRPC_DEMO_POSTGRES_PASSWORD) + "@127.0.0.1:5432/trpc_agent"
```

为本次运行使用唯一且可识别的 Compose project name：

```powershell
$testProject = "trpc-agent-v8-" + (Get-Date -Format "yyyyMMddHHmmss")
$composeBase = "deploy/local-shared/compose.yaml"

docker compose -p $testProject --env-file .env.local -f $composeBase up -d --wait
uv run trpc-agent-shared-init
docker compose -p $testProject --env-file .env.local -f $composeBase ps
```

预期：Redis/PostgreSQL 均为 `healthy`，schema version 已升级到实现声明的版本。若没有 `.env.local`，可省略 `--env-file .env.local`，Compose 会读取当前进程中的两个密码变量。

## 4. PostgreSQL/Redis 集成验证

```powershell
uv run pytest tests/integration/observability tests/integration/operations -q -m shared_backend -p no:cacheprovider
```

必须证明：

- 两节点告警只创建一个逻辑 incident state version，恢复只产生一个逻辑 resolved version。
- 两节点并发推进/回滚 release 只有合法 revision/fence 成功，旧节点写入被拒。
- 硬门槛在普通 telemetry 故障时仍能 latch 并让新请求走 last-good。
- 回滚后新请求使用 last-good；已开始请求和恢复节点继续使用原 `ExecutionConfigPin`。
- PostgreSQL 权威不可用时配置解析 fail closed，Redis 缓存不得旁路。

## 5. 最小可观察部署

实现后使用独立叠加文件启动 Gateway、两个 Worker 和 Collector：

```powershell
$composeOps = "deploy/local-observable/compose.yaml"
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps up -d --wait
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps ps
```

依次检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8080/health/live
Invoke-RestMethod http://127.0.0.1:8080/health/ready
uv run pytest tests/e2e/observability tests/e2e/operations -q -p no:cacheprovider
```

预期：核心服务 ready；Collector debug 输出能按安全 trace reference 定位 Adapter/Gateway/Worker/官方 Runner/数据/投递阶段，且不含测试敏感标记。

### 遥测出口故障

```powershell
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps stop otel-collector
uv run pytest tests/e2e/observability/test_exporter_outage.py -q -p no:cacheprovider
Invoke-RestMethod http://127.0.0.1:8080/health/ready
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps start otel-collector
```

预期：消息业务仍成功，平台为 degraded；缓冲不超过上限，drop counter 可见；Collector 恢复后健康状态在规格时限内恢复。正式 Audit 行为不得改变。

### 安全排空

```powershell
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps stop worker-a
uv run pytest tests/e2e/operations/test_worker_drain_takeover.py -q -p no:cacheprovider
```

预期：Worker-A 先撤 readiness，不再接新 claim；在途执行完成、由更高 fence 接管或明确标记 unknown，Worker-B 持续服务，非幂等未知副作用不自动重放。

## 6. 灰度与回滚验收

```powershell
uv run pytest tests/e2e/operations/test_tenant_canary_release.py -q -p no:cacheprovider
uv run pytest tests/e2e/operations/test_hard_gate_rollback.py -q -p no:cacheprovider
uv run pytest tests/e2e/operations/test_quality_gate_pause.py -q -p no:cacheprovider
```

预期：范围外 tenant 始终使用 stable；硬门槛首次命中即停止 candidate 新请求并自动回滚；质量门槛达到最小样本后越线只暂停，等待授权决定；Audit 故障时发布命令整体回滚。

## 7. 容量双门禁

```powershell
uv run pytest tests/performance/test_observability_capacity_gate.py -q -p no:cacheprovider
```

测试必须使用同一 workload manifest 分别得到 baseline/off 与 telemetry/on 结果，并生成不含正文或 Secret 的机器可读报告。通过条件：

- 2 tenants、2 Workers、100 concurrent Sessions、1,000 messages。
- 0 丢失、0 跨租户串用、0 不可解释重复。
- telemetry on 相对 baseline 的 throughput 下降、p50/p95/p99 延迟增幅均不超过 10%。
- 报告明确标记为本地相对证据，不宣称生产 SLA。

## 8. 全量回归与安全检查

```powershell
uv run pytest -q -p no:cacheprovider
git diff --check
uv run pytest tests/security -q -p no:cacheprovider
git status --short
```

必须记录 passed/failed/skipped 和环境原因。任何未解释失败、以 skip 代替 pass、敏感标记命中或 `git diff --check` 错误都会阻断完成声明。

## 9. 停止环境

先核对项目名，只停止本次明确创建的环境：

```powershell
$testProject
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps ps
docker compose -p $testProject --env-file .env.local -f $composeBase -f $composeOps down
```

上述命令保留数据卷。只有确认 `$testProject` 正是本次临时测试环境且数据可删除时，才由操作者另行执行带 `-v` 的清理；不要对默认项目或不明确目标使用递归/卷删除。

## 10. 验收产物

- `validation-results.md`：命令、时间、环境摘要、结果和跳过原因。
- `capacity-report.json` 与 `capacity-results.md`：baseline 与 telemetry-on 的事实数据。
- `risk-register.md`：至少八项风险。
- `deployment-topology.md` 与 runbook：最小/推荐拓扑、排空、故障演练和回滚。
- README traceability：README → FR/NFR/SC → 设计 → 测试 → 证据 100% 映射。
