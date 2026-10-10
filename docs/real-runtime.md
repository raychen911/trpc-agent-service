# 真实多进程与故障注入验收

`scripts/real_runtime_gate.py` 是生产验收工具，不是虚拟测试。它只在同时满足
`TRPC_RUN_REAL_MULTINODE=1` 和 `--execute` 时访问 Docker、PostgreSQL、Redis 与
Toxiproxy；缺少任意前置条件时只写入 `gate=not_run`，不会把模拟结果升级为生产通过。

## 验收前环境

需要 Docker Engine、Docker Compose v2、运行中的本项目镜像和已完成的数据库迁移。
使用运行账号（不是 PostgreSQL owner）连接检查数据库，并准备下面的本地 Secret 环境变量；
值不会写入报告：

所有真实验收 Compose 命令都必须把 `deploy/acceptance-runtime.override.yml` 作为最后一个
`-f` 文件。它只作用于验收项目，不改变普通 `docker-compose.yml` 的生产默认策略；其中
`restart=no` 防止 Docker Desktop 在主机重启后自动恢复验收容器，CPU/内存/PID 与
`json-file` 日志轮转上限用于限制本机压力。`real_runtime_gate.py --compose-up` 会强制注入
该 override，并拒绝接管已有 Compose 容器。

```powershell
$env:TRPC_RUN_REAL_MULTINODE = "1"
$env:TRPC_REAL_DATABASE_DSN = "postgresql+asyncpg://trpc_runtime:<password>@127.0.0.1:5432/trpc_service"
$env:TRPC_REAL_REDIS_URL = "redis://:<password>@127.0.0.1:6379/0"
$env:TRPC_REAL_TENANT_ID = "<已存在的验收租户>"
$env:TRPC_REAL_BINDING_ID = "<该租户的已启用飞书或企业微信 binding>"
$env:TRPC_REAL_SESSION_HMAC_KEY = "<至少 32 字节的测试密钥>"
# 生产门禁必须单独检查跨租户 SQL 角色；不能复用 TRPC_REAL_DATABASE_DSN 的普通租户角色。
$env:TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN = "postgresql+asyncpg://trpc_worker:<worker-password>@127.0.0.1:5432/trpc_service"
$env:TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE = "trpc_worker"
$env:TRPC_REAL_RUNTIME_DATABASE_ROLE = "trpc_runtime"
```

租户必须已经有可被 Worker 处理的配置 revision。若模型配置指向真实模型，另外需要在
Compose Secret 中注入对应 API key；离线 deterministic agent 不能冒充真实多进程生产验收。
验收结束后不要执行 `docker compose down -v`，以保留 PostgreSQL、Redis、MinIO 和
Prometheus 卷。

## Canary 与正式门槛

开发或部署变更后可以先做小样本 canary。canary 固定使用 `messages=20`、
`duplicates=2`、`fault-messages=4`（仍建议使用 4 个 Worker），输出到独立的
`real-runtime-canary.json`。即使全部运行阶段成功，canary 也只能得到
`gate=pass`、`production_gate=not_run`；它不能替代正式验收，也不能复制或重命名为
`real-runtime.json`：

```powershell
.venv\Scripts\python.exe scripts\real_runtime_gate.py `
  --execute --phase all --use-toxiproxy --workers 4 --messages 20 `
  --duplicates 2 --fault-messages 4 --kill-worker --allow-process-kill `
  --republish-probe --output runs/multitenant/real-runtime-canary.json
```

正式运行时最低门槛固定为：`phase=all`、至少 4 个独立 Worker、至少 200 条消息、
至少 20 条重复消息、至少 8 条故障消息，启用 Toxiproxy，真实终止一个 Worker，
并启用 `--republish-probe` 验证重复 Redis 发布只产生一个 turn。只有满足这些条件且
所有阶段证据通过，才可能得到 `production_gate=pass`。

## 多进程负载与 fencing

先确认 Compose 已经运行至少四个独立 Worker；需要脚本负责拉起/扩容时才加
`--compose-up`：

```powershell
.venv\Scripts\python.exe scripts\real_runtime_gate.py `
  --execute --phase load --workers 4 --messages 200 --duplicates 20 `
  --kill-worker --allow-process-kill `
  --output runs/multitenant/real-runtime-load.json
```

脚本会生成同一 Session 的重复和乱序消息，观察 PostgreSQL Inbox、Session lease、连续
event sequence、Worker 容器 PID 和被杀 Worker 后的接管。只有在杀死发生时确实观察到
processing turn，并且之后看到重试/接管证据时，fencing 子门禁才会通过；否则报告为
`not_run`，不会因最终状态看起来正确而虚报 fencing 通过。

## Redis/PostgreSQL Toxiproxy 故障

基础 `docker-compose.yml` 直连 PostgreSQL/Redis，不能用于证明代理切断生效。必须用下面
的 override 启动应用角色，使它们在容器网络内连接 `toxiproxy:15432` 和
`toxiproxy:16379`：

```powershell
docker compose -f docker-compose.yml -f deploy/toxiproxy-runtime.override.yml `
  -f deploy/acceptance-runtime.override.yml `
  -p trpc-agent-service up -d --no-build --scale worker=4 `
  postgres redis toxiproxy worker outbox-dispatcher channel-dispatcher
```

然后运行完整的真实门禁，并显式确认允许进程终止：

```powershell
.venv\Scripts\python.exe scripts\real_runtime_gate.py `
  --execute --phase all --use-toxiproxy --workers 4 --messages 200 `
  --duplicates 20 --fault-messages 8 --kill-worker --allow-process-kill `
  --republish-probe `
  --output runs/multitenant/real-runtime.json --require-production
```

Redis 故障阶段应观察 PostgreSQL Outbox 保持未发布，恢复后补投并完成 Session；
PostgreSQL 故障阶段应观察依赖恢复前不能提交，恢复后 Outbox/Worker 完成。脚本还会向
一个不存在的 binding 写入一次真实 Outbound Outbox 记录，并验证运行中的 Channel
Dispatcher 在重试上限后产生 `dead_letters`；这是持久 DLQ 路径测试，不等同于真实 IM
供应商失败验收。

## 专用 fault-stage Compose 与进程终止验收

进程终止的三个精确边界（enqueue、tool、commit）必须在独立的
`trpc-fault-stage-*` Compose 项目中执行。正常 Redis/PostgreSQL/Toxiproxy 运行项目可以
复用上节的 `trpc-agent-service`（或一个独立的 `trpc-fault-runtime-*` 项目），但不能与
fault-stage 项目同名；`trpc-perf-*` 性能项目也不能直接作为 `real_runtime_gate.py`
的正常运行项目。候选镜像、source fingerprint、`TRPC_RELEASE_ID`/`TRPC_RELEASE_NONCE`
必须是同一份；数据库、Redis、MinIO 卷和 fixture 必须分别创建，不能跨项目复用。

下面的顺序从空闲本机创建一个正常 Toxiproxy 项目和一个 fault-stage 项目。端口明确
分开：正常项目使用 35432/36379/39000/39001，fault-stage 项目使用
45432/46379/49000/49001；正常项目的 Toxiproxy 代理容器监听仍是
15432/16379/19000，宿主机映射分别由 `TOXIPROXY_POSTGRES_PORT`、
`TOXIPROXY_REDIS_PORT`、`TOXIPROXY_S3_PORT` 配置，API 映射由
`TOXIPROXY_API_PORT` 配置。若这些端口已被其他项目占用，
先停止那个项目并保留其卷；不要把两个验收项目映射到同一宿主机端口。

```powershell
$ErrorActionPreference = "Stop"
Set-Location C:\path\to\trpc-agent-service
$env:COMPOSE_DISABLE_ENV_FILE = "1"
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runtimeProject = "trpc-fault-runtime-$runStamp"
$stageProject = "trpc-fault-stage-$runStamp"
if ($runtimeProject -eq $stageProject) { throw "runtime and fault-stage projects must differ" }
if (-not $env:TRPC_RELEASE_ID -or -not $env:TRPC_RELEASE_NONCE) {
  throw "export the current TRPC_RELEASE_ID and TRPC_RELEASE_NONCE before starting"
}

function New-SyntheticHexSecret {
  $bytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  [Convert]::ToHexString($bytes).ToLowerInvariant()
}

function New-SyntheticBase64UrlSecret {
  $bytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  ([Convert]::ToBase64String($bytes)).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

$env:TRPC_RUN_REAL_MULTINODE = "1"
$env:TRPC_SERVICE_ENVIRONMENT = "test"
$env:TRPC_RUNTIME_USER = "trpc_runtime"
$env:TRPC_WORKER_USER = "trpc_worker"
$env:TRPC_MIGRATION_USER = "trpc_migration"
$env:POSTGRES_DB = "trpc_service"
$env:POSTGRES_USER = "trpc"
$env:MINIO_ROOT_USER = "trpc-minio"
$env:MINIO_BUCKET = "trpc-artifacts"

$sourceFingerprint = .venv\Scripts\python.exe -c "from pathlib import Path; from scripts.evidence_lineage import source_fingerprint; print(source_fingerprint(Path.cwd())['value'])"
if ($LASTEXITCODE -ne 0 -or $sourceFingerprint -notmatch '^[0-9a-f]{64}$') { throw "source fingerprint is unavailable" }
$env:TRPC_SERVICE_IMAGE = "trpc-agent-service:real-$sourceFingerprint"
docker build --provenance=false --build-arg "TRPC_SOURCE_FINGERPRINT=$sourceFingerprint" --tag $env:TRPC_SERVICE_IMAGE .
if ($LASTEXITCODE -ne 0) { throw "candidate image build failed" }

# Normal runtime/Toxiproxy project credentials and host ports.
$runtimePostgresPassword = New-SyntheticHexSecret
$runtimeDatabasePassword = New-SyntheticHexSecret
$runtimeWorkerDatabasePassword = New-SyntheticHexSecret
$runtimeMigrationPassword = New-SyntheticHexSecret
$runtimeRedisPassword = New-SyntheticHexSecret
$runtimeMinioPassword = New-SyntheticHexSecret
$runtimeSessionHmacKey = New-SyntheticBase64UrlSecret
$runtimeEmergencyQueueKey = New-SyntheticBase64UrlSecret
$runtimeDevelopmentToken = New-SyntheticHexSecret
$runtimeDbPort = "35432"
$runtimeRedisPort = "36379"
$runtimeMinioPort = "39000"
$runtimeMinioConsolePort = "39001"
$runtimeGatewayPort = "38080"
$runtimeAdminPort = "38081"
$runtimeToxiproxyApiPort = "38474"
$env:POSTGRES_PORT = $runtimeDbPort
$env:REDIS_PORT = $runtimeRedisPort
$env:MINIO_PORT = $runtimeMinioPort
$env:MINIO_CONSOLE_PORT = $runtimeMinioConsolePort
$env:GATEWAY_PORT = $runtimeGatewayPort
$env:ADMIN_PORT = $runtimeAdminPort
$env:TOXIPROXY_API_PORT = $runtimeToxiproxyApiPort
$env:POSTGRES_PASSWORD = $runtimePostgresPassword
$env:RUNTIME_DATABASE_PASSWORD = $runtimeDatabasePassword
$env:WORKER_DATABASE_PASSWORD = $runtimeWorkerDatabasePassword
$env:MIGRATION_DATABASE_PASSWORD = $runtimeMigrationPassword
$env:REDIS_PASSWORD = $runtimeRedisPassword
$env:MINIO_ROOT_PASSWORD = $runtimeMinioPassword
$env:SESSION_HMAC_KEY = $runtimeSessionHmacKey
$env:EMERGENCY_QUEUE_KEY = $runtimeEmergencyQueueKey
$env:DEVELOPMENT_TOKEN = $runtimeDevelopmentToken
$env:TRPC_REAL_COMPOSE_PROJECT = $runtimeProject
$env:TRPC_REAL_TOXIPROXY_API = "http://127.0.0.1:$runtimeToxiproxyApiPort"
$normalCompose = @(
  "-f", "docker-compose.yml", "-f", "deploy/toxiproxy-runtime.override.yml",
  "-f", "deploy/acceptance-runtime.override.yml",
  "-p", $runtimeProject
)
docker compose @normalCompose config --quiet
if ($LASTEXITCODE -ne 0) { throw "normal runtime Compose configuration is invalid" }
docker compose @normalCompose up -d --no-build --scale worker=4 `
  postgres redis minio minio-init migrate gateway toxiproxy worker `
  outbox-dispatcher channel-dispatcher post-turn-projector session-recovery
if ($LASTEXITCODE -ne 0) { throw "normal runtime Compose startup failed" }

function Get-HealthyWorkerInventory {
  param(
    [object[]]$ComposeArgs,
    [string]$Project,
    [string]$ExpectedSource,
    [string]$ExpectedImage = ""
  )
  $ids = @(docker compose @ComposeArgs ps -q worker | ForEach-Object { $_.Trim() } | Where-Object { $_ })
  if ($LASTEXITCODE -ne 0 -or $ids.Count -ne 4) {
    throw "$Project must expose exactly four worker container IDs"
  }
  $format = '{{.Id}}|{{index .Config.Labels "io.trpc.agent-service.source-fingerprint"}}|{{.Image}}|{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}'
  $rows = @($ids | ForEach-Object { docker inspect --format $format $_ })
  if ($LASTEXITCODE -ne 0 -or $rows.Count -ne 4) { throw "$Project worker inspect failed" }
  $imageIds = @()
  foreach ($row in $rows) {
    $parts = $row -split '\|', 5
    if ($parts.Count -ne 5 -or $parts[0] -notmatch '^[0-9a-f]{64}$' -or
        $parts[1] -ne $ExpectedSource -or $parts[2] -notmatch '^sha256:[0-9a-f]{64}$' -or
        $parts[3] -ne "running" -or $parts[4] -ne "healthy") {
      throw "$Project has a worker without full ID/current source/healthy state"
    }
    $imageIds += $parts[2]
  }
  if (@($imageIds | Sort-Object -Unique).Count -ne 1) { throw "$Project workers use mixed images" }
  if ($ExpectedImage -and $imageIds[0] -ne $ExpectedImage) { throw "$Project image differs from candidate" }
  return $ids
}

function Wait-HealthyWorkerInventory {
  param(
    [object[]]$ComposeArgs,
    [string]$Project,
    [string]$ExpectedSource,
    [string]$ExpectedImage = ""
  )
  for ($attempt = 0; $attempt -lt 60; $attempt++) {
    try {
      return Get-HealthyWorkerInventory -ComposeArgs $ComposeArgs -Project $Project `
        -ExpectedSource $ExpectedSource -ExpectedImage $ExpectedImage
    } catch {
      if ($attempt -eq 59) { throw }
      Start-Sleep -Seconds 2
    }
  }
  throw "$Project worker health did not converge"
}

$normalWorkerIds = @(Wait-HealthyWorkerInventory -ComposeArgs $normalCompose `
  -Project $runtimeProject -ExpectedSource $sourceFingerprint)
$normalImageDigest = (docker inspect --format '{{.Image}}' $normalWorkerIds[0]).Trim()
if ($normalImageDigest -notmatch '^sha256:[0-9a-f]{64}$') { throw "normal candidate image ID is unavailable" }
$env:TRPC_REAL_IMAGE_DIGEST = $normalImageDigest
$env:TRPC_REAL_WORKER_IDENTITIES = $normalWorkerIds -join ","

# Create a normal-runtime synthetic tenant against this runtime project's DB;
# never use a tenant report from the trpc-perf-* volume.
$env:TRPC_PERF_DATABASE_DSN = "postgresql://$env:TRPC_RUNTIME_USER`:$runtimeDatabasePassword@127.0.0.1`:$runtimeDbPort/$env:POSTGRES_DB"
$env:TRPC_PERF_FIXTURE_CONFIRM = "I_UNDERSTAND_PERFORMANCE_FIXTURE"
$normalFixtureReport = "runs/multitenant/fault-runtime-fixture-$runStamp.json"
.venv\Scripts\python.exe scripts/performance_fixture.py create --execute --output $normalFixtureReport
if ($LASTEXITCODE -ne 0) { throw "normal runtime fixture creation failed" }
$normalFixture = Get-Content $normalFixtureReport -Raw | ConvertFrom-Json
if ($normalFixture.gate -ne "pass" -or $normalFixture.synthetic -ne $true) { throw "normal fixture is not synthetic/pass" }
$env:TRPC_REAL_TENANT_ID = [string]$normalFixture.tenant_id
$env:TRPC_REAL_BINDING_ID = [string]$normalFixture.binding_id
# Runtime scenarios use Toxiproxy; role evidence uses the direct DB port.
$env:TRPC_REAL_DATABASE_DSN = "postgresql+asyncpg://$env:TRPC_RUNTIME_USER`:$runtimeDatabasePassword@127.0.0.1:15432/$env:POSTGRES_DB"
$env:TRPC_REAL_REDIS_URL = "redis://:$runtimeRedisPassword@127.0.0.1:16379/0"
$env:TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN = "postgresql+asyncpg://$env:TRPC_WORKER_USER`:$runtimeWorkerDatabasePassword@127.0.0.1`:$runtimeDbPort/$env:POSTGRES_DB"
$env:TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE = $env:TRPC_WORKER_USER
$env:TRPC_REAL_RUNTIME_DATABASE_ROLE = $env:TRPC_RUNTIME_USER
$env:TRPC_REAL_SESSION_HMAC_KEY = $runtimeSessionHmacKey

# Dedicated fault-stage credentials, ports, and run identity.  This Compose
# project has its own named volumes and never shares the normal project's DB.
$stagePostgresPassword = New-SyntheticHexSecret
$stageDatabasePassword = New-SyntheticHexSecret
$stageWorkerDatabasePassword = New-SyntheticHexSecret
$stageMigrationPassword = New-SyntheticHexSecret
$stageRedisPassword = New-SyntheticHexSecret
$stageMinioPassword = New-SyntheticHexSecret
$stageSessionHmacKey = New-SyntheticBase64UrlSecret
$stageEmergencyQueueKey = New-SyntheticBase64UrlSecret
$stageDevelopmentToken = New-SyntheticHexSecret
$stageDbPort = "45432"
$stageRedisPort = "46379"
$stageMinioPort = "49000"
$stageMinioConsolePort = "49001"
$stageRunId = "fault-stage-$runStamp"
$stageRunToken = New-SyntheticHexSecret
$env:POSTGRES_PORT = $stageDbPort
$env:REDIS_PORT = $stageRedisPort
$env:MINIO_PORT = $stageMinioPort
$env:MINIO_CONSOLE_PORT = $stageMinioConsolePort
$env:POSTGRES_PASSWORD = $stagePostgresPassword
$env:RUNTIME_DATABASE_PASSWORD = $stageDatabasePassword
$env:WORKER_DATABASE_PASSWORD = $stageWorkerDatabasePassword
$env:MIGRATION_DATABASE_PASSWORD = $stageMigrationPassword
$env:REDIS_PASSWORD = $stageRedisPassword
$env:MINIO_ROOT_PASSWORD = $stageMinioPassword
$env:SESSION_HMAC_KEY = $stageSessionHmacKey
$env:EMERGENCY_QUEUE_KEY = $stageEmergencyQueueKey
$env:DEVELOPMENT_TOKEN = $stageDevelopmentToken
$env:TRPC_FAULT_RUN_ID = $stageRunId
$env:TRPC_FAULT_RUN_TOKEN = $stageRunToken
$env:TRPC_FAULT_SESSION_HMAC_KEY = $stageSessionHmacKey
$env:TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS = "0.5"
$env:TRPC_FAULT_COMPOSE_PROJECT = $stageProject
$stageCompose = @(
  "-f", "docker-compose.yml", "-f", "deploy/fault-stage-runtime.override.yml",
  "-f", "deploy/acceptance-runtime.override.yml",
  "-p", $stageProject
)
docker compose @stageCompose config --quiet
if ($LASTEXITCODE -ne 0) { throw "fault-stage Compose configuration is invalid" }
docker compose @stageCompose up -d --no-build --scale worker=4 `
  postgres redis minio minio-init migrate worker outbox-dispatcher session-recovery
if ($LASTEXITCODE -ne 0) { throw "fault-stage Compose startup failed" }
$stageWorkerIds = @(Wait-HealthyWorkerInventory -ComposeArgs $stageCompose `
  -Project $stageProject -ExpectedSource $sourceFingerprint -ExpectedImage $normalImageDigest)
$env:TRPC_FAULT_WORKER_CONTAINER = $stageWorkerIds[0] # full 64-hex ID; never a service name
$env:TRPC_FAULT_DATABASE_DSN = "postgresql+asyncpg://$env:TRPC_RUNTIME_USER`:$stageDatabasePassword@127.0.0.1`:$stageDbPort/$env:POSTGRES_DB"
$env:TRPC_FAULT_REDIS_URL = "redis://:$stageRedisPassword@127.0.0.1`:$stageRedisPort/0"
$env:TRPC_FAULT_BINDING_SEED = "binding-$stageRunId"
$env:TRPC_RUN_FAULT_STAGE_ACCEPTANCE = "1"
$env:TRPC_FAULT_STAGE_ALLOW_KILL = "1"

$faultReport = "runs/multitenant/fault-injection-$runStamp.json"
.venv\Scripts\python.exe scripts/fault_injection_gate.py `
  --execute --scenario all --require-production --allow-process-kill `
  --project $runtimeProject --fault-project $stageProject `
  --fault-worker-container $env:TRPC_FAULT_WORKER_CONTAINER `
  --workers 4 --messages 200 --duplicates 20 --fault-messages 8 `
  --toxiproxy-api $env:TRPC_REAL_TOXIPROXY_API --output $faultReport
if ($LASTEXITCODE -ne 0) { throw "fault-injection production gate failed" }
$fault = Get-Content $faultReport -Raw | ConvertFrom-Json
if ($fault.gate -ne "pass" -or $fault.production_gate -ne "pass") {
  throw "fault report is not gate=pass and production_gate=pass"
}

# Validate every exact process-kill marker and its timestamp before cleanup.
$requiredFaultStageMarkers = @{
  worker_enqueue = @(
    "preflight.workers_verified", "acceptance.persisted", "control.armed", "marker.entered",
    "worker.terminated", "worker.survivors_observed", "v2.claim_before_observed",
    "turn.single_contiguous_verified", "turn.commit_verified", "outbound.intent_verified"
  )
  worker_tool = @(
    "preflight.workers_verified", "acceptance.persisted", "turn.processing_observed", "control.armed",
    "marker.entered", "worker.terminated", "worker.survivors_observed", "stale_token_rejection_verified",
    "turn.single_contiguous_verified", "turn.commit_verified", "tool.idempotent_execution_verified",
    "outbound.intent_verified", "v2.ack_before_execute"
  )
  worker_commit = @(
    "preflight.workers_verified", "acceptance.persisted", "turn.processing_observed", "control.armed",
    "marker.entered", "worker.terminated", "worker.survivors_observed", "stale_token_rejection_verified",
    "turn.single_contiguous_verified", "turn.commit_verified", "outbound.intent_verified",
    "v2.ack_before_execute"
  )
}
foreach ($scenarioName in $requiredFaultStageMarkers.Keys) {
  $scenario = $fault.candidate.scenarios.PSObject.Properties[$scenarioName].Value
  if (-not $scenario -or $scenario.status -ne "pass") { throw "$scenarioName did not pass" }
  foreach ($markerName in $requiredFaultStageMarkers[$scenarioName]) {
    $matches = @($scenario.stage_markers | Where-Object { $_.name -eq $markerName })
    if ($matches.Count -ne 1 -or $matches[0].status -ne "pass" -or
        -not ([string]$matches[0].observed_at).Trim()) {
      throw "$scenarioName marker is missing, duplicated, or not pass: $markerName"
    }
  }
  $childPath = [string]$scenario.child_report
  if (-not $childPath -or -not (Test-Path -LiteralPath $childPath -PathType Leaf)) {
    throw "$scenarioName child report is missing"
  }
}
```

`TRPC_FAULT_*` 的来源是专用 fault-stage 项目：`TRPC_FAULT_DATABASE_DSN`、
`TRPC_FAULT_REDIS_URL` 使用 45432/46379，`TRPC_FAULT_BINDING_SEED` 和
`TRPC_FAULT_RUN_ID` 是本次随机运行身份，`TRPC_FAULT_RUN_TOKEN` 与
`TRPC_FAULT_SESSION_HMAC_KEY` 只在当前 PowerShell 会话中生成，
`TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS` 固定为 0.5，
`TRPC_RUN_FAULT_STAGE_ACCEPTANCE=1`、`TRPC_FAULT_STAGE_ALLOW_KILL=1` 和
`TRPC_SERVICE_ENVIRONMENT=test` 是明确的安全开关。父 wrapper 会把
`TRPC_FAULT_PROJECT`、`TRPC_FAULT_WORKER_CONTAINER`、`TRPC_FAULT_SCHEDULER_VERSION`、
`TRPC_FAULT_REDIS_STREAM`、`TRPC_FAULT_REDIS_GROUP` 和一次性证据 nonce 注入 child；
不要手工改写这些 provenance 变量。`WORKER_DATABASE_PASSWORD` 必须与
`TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF` 对应的 `worker_database_password` secret
一致；普通 `RUNTIME_DATABASE_PASSWORD` 不能代替它。

每个 fault-stage case 会在 child 的 `finally` 路径按本次 `run_id`、tenant ownership、
固定表 allowlist 和精确 Redis stream entry 清理；不要用租户名猜测或执行全库删除。先
清理 normal runtime fixture，再停止两个项目，保留卷和所有 JSON 证据：

```powershell
$fixture = Get-Content $normalFixtureReport -Raw | ConvertFrom-Json
$env:TRPC_PERF_DATABASE_DSN = "postgresql://$env:TRPC_RUNTIME_USER`:$runtimeDatabasePassword@127.0.0.1`:$runtimeDbPort/$env:POSTGRES_DB"
.venv\Scripts\python.exe scripts/performance_fixture.py cleanup --execute `
  --report $normalFixtureReport --tenant-id $fixture.tenant_id --run-id $fixture.run_id `
  --output "runs/multitenant/fault-runtime-fixture-cleanup-$runStamp.json"
if ($LASTEXITCODE -ne 0) { throw "normal runtime fixture cleanup failed; retain the DB for audit" }

docker compose @stageCompose down
if ($LASTEXITCODE -ne 0) { throw "fault-stage Compose cleanup failed; do not delete volumes" }

# Restore normal Compose secrets before resolving its project configuration.
$env:POSTGRES_PORT = $runtimeDbPort
$env:REDIS_PORT = $runtimeRedisPort
$env:MINIO_PORT = $runtimeMinioPort
$env:MINIO_CONSOLE_PORT = $runtimeMinioConsolePort
$env:POSTGRES_PASSWORD = $runtimePostgresPassword
$env:RUNTIME_DATABASE_PASSWORD = $runtimeDatabasePassword
$env:WORKER_DATABASE_PASSWORD = $runtimeWorkerDatabasePassword
$env:MIGRATION_DATABASE_PASSWORD = $runtimeMigrationPassword
$env:REDIS_PASSWORD = $runtimeRedisPassword
$env:MINIO_ROOT_PASSWORD = $runtimeMinioPassword
$env:SESSION_HMAC_KEY = $runtimeSessionHmacKey
$env:EMERGENCY_QUEUE_KEY = $runtimeEmergencyQueueKey
$env:DEVELOPMENT_TOKEN = $runtimeDevelopmentToken
docker compose @normalCompose down
if ($LASTEXITCODE -ne 0) { throw "normal runtime Compose cleanup failed; do not delete volumes" }
```

两条 `down` 都明确不带 `-v`；PostgreSQL、Redis、MinIO 和 Prometheus 卷必须保留。若
任一 child report 表示 case cleanup 或 worker restore 未确认，保留对应项目、卷和报告
供审计，不能扩大删除范围或改写 marker。

## 报告解释

报告先写 JSON 到 `runs/multitenant/`，包含 worker 容器 ID/PID、批次状态、事件序号、
故障切断/恢复和 DLQ 证据，但不包含 DSN、密码、Token 或消息正文。`--phase load` 或
`--phase fault` 是范围受限证据，报告的 `production_gate` 仍为 `not_run`；完整的生产
发布门禁仍需真实迁移、Kubernetes 运行态，以及企业微信与飞书真实凭证验收。

## 受控性能拓扑：100 回调/秒与 200 个 Agent turn

正式性能门禁使用单独的 Compose override。它启用一个只接收合成 Feishu HTTP 回调的
Gateway，并把 Gateway、4 个 Worker 和 1 个 Outbox Dispatcher 固定到同一份候选镜像；
Gateway 只绑定调用方显式指定的本机回环端口。Channel Dispatcher、企业微信连接器、
任何真实 IM outbound 以及 OTel/Prometheus/Jaeger 等观测大栈仍被 profile 禁用，因此
该拓扑不会连接或发送到真实飞书/企业微信。

`deploy/performance-runtime.override.yml` 固定 4 个 Worker，每个 Worker 的并发上限为
50；因此 200 个独立 Session turn 可以在同一批次内真实重叠。每个 Worker 的
PostgreSQL pool 固定为 `min=2,max=8`，容器上限为 1 CPU/1 GiB；唯一的 Outbox Dispatcher
固定为 `min=2,max=4` 和 0.5 CPU/512 MiB。Gateway 的 PostgreSQL pool 在就绪前预热为
`min=20,max=24`。若门禁进程使用默认 32 连接，加上四个 Worker 的最多 32 个连接、
Dispatcher 的最多 4 个连接、Recovery 的最多 2 个连接和 8 个探针预留，最坏预算为
102；正式命令把门禁 pool 固定为 16，因此锁定预算为 86，并能覆盖 32 个 inflight
请求中并发等待持久化的部分。Worker 被终止后不会被
Compose 自动拉起。离线 Agent 延迟在拓扑中固定为 `3.0` 秒，不开放操作员覆盖；200 条
消息按门禁默认的 105 回调/秒提交约需 1.9 秒，因此 3 秒处理窗口能够覆盖完整提交窗口，
确保 200 个 turn 有真实重叠。该值在应用允许的 `0..5` 秒范围内，拓扑不会使用超过上限
的值。

验收租户和 binding 必须是专用合成数据，并且已经激活不可变配置版本：
`model.provider=offline`、`model=deterministic`，不配置真实模型 API key。这个
override 只允许 `postgres`、`redis`、`minio`、`minio-init`、`migrate`、Gateway、
`outbox-dispatcher` 和 `worker`；Admin、Channel Dispatcher、Projector、企业微信连接器、
Toxiproxy 和 OTel/Prometheus/Jaeger 都被 profile 禁用。Gateway 的合成回调路径为
`/v1/channels/feishu/{binding_id}/callback`，只应由本次性能测试的合成驱动访问；性能
脚本的内部 Runtime 阶段仍需单独标注为内部验收，不能把它冒充真实 IM 网络延迟。

Windows PowerShell 示例（先按当前 checkout 计算 fingerprint，再构建候选镜像）：

```powershell
$env:COMPOSE_DISABLE_ENV_FILE = "1"
$project = "trpc-perf-20260822-01"
$env:TRPC_PERF_COMPOSE_PROJECT = $project
$env:TRPC_REAL_COMPOSE_PROJECT = $project
$env:TRPC_RUN_REAL_MULTINODE = "1"
$env:TRPC_PERF_GATEWAY_PORT = "18080"  # 仅绑定 127.0.0.1，必须是未占用的本机端口

function New-SyntheticSecret {
  $bytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  return ([Convert]::ToBase64String($bytes)).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}
# Compose 的 environment-backed secrets 也必须在同一会话中存在；hex 只包含 URL 安全字符。
function New-SyntheticHexSecret {
  $bytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  [Convert]::ToHexString($bytes).ToLowerInvariant()
}
function New-SyntheticBase64UrlSecret {
  $bytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  ([Convert]::ToBase64String($bytes)).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}
$env:POSTGRES_DB = "trpc_service"
$env:POSTGRES_USER = "trpc"
$env:TRPC_RUNTIME_USER = "trpc_runtime"
$env:MINIO_ROOT_USER = "trpc-minio"
$env:MINIO_BUCKET = "trpc-artifacts"
$env:POSTGRES_PASSWORD = New-SyntheticHexSecret
$env:RUNTIME_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:WORKER_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:MIGRATION_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:REDIS_PASSWORD = New-SyntheticHexSecret
$env:MINIO_ROOT_PASSWORD = New-SyntheticHexSecret
$env:SESSION_HMAC_KEY = New-SyntheticBase64UrlSecret # 解码后正好 32 字节
$env:EMERGENCY_QUEUE_KEY = New-SyntheticBase64UrlSecret  # decodes to exactly 32 bytes
$env:DEVELOPMENT_TOKEN = New-SyntheticHexSecret
# 这些值只在当前 PowerShell 进程中存在；不要使用真实飞书凭证、不要写入 .env。
$env:TRPC_PERF_FIXTURE_UNUSED_APP_SECRET = New-SyntheticSecret
$env:TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN = New-SyntheticSecret
$env:TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY = New-SyntheticSecret

$sourceFingerprint = .venv\Scripts\python.exe -c "from pathlib import Path; from scripts.evidence_lineage import source_fingerprint; print(source_fingerprint(Path.cwd())['value'])"
if ($LASTEXITCODE -ne 0 -or $sourceFingerprint -notmatch '^[0-9a-f]{64}$') { throw "source fingerprint is unavailable" }
$env:TRPC_PERF_IMAGE = "trpc-agent-service:perf-$sourceFingerprint"

docker build --provenance=false --build-arg "TRPC_SOURCE_FINGERPRINT=$sourceFingerprint" `
  --tag $env:TRPC_PERF_IMAGE .

docker compose -f docker-compose.yml -f deploy/performance-runtime.override.yml `
  -f deploy/acceptance-runtime.override.yml `
  -p $env:TRPC_PERF_COMPOSE_PROJECT config --quiet

docker compose -f docker-compose.yml -f deploy/performance-runtime.override.yml `
  -f deploy/acceptance-runtime.override.yml `
  -p $env:TRPC_PERF_COMPOSE_PROJECT up -d --no-build --scale worker=4 --scale outbox-dispatcher=1 `
  postgres redis minio minio-init migrate gateway outbox-dispatcher session-recovery worker
```

这里的三个 `TRPC_PERF_FIXTURE_UNUSED_*` 是随机生成的合成值，仅用于 fixture 中的
`env://TRPC_PERF_FIXTURE_UNUSED_*` SecretRef；它们不会联系飞书，也绝不能替换为真实
飞书 AppSecret、Verification Token 或 Encrypt Key。若需要访问 HTTP 入口，只能使用
`http://127.0.0.1:$env:TRPC_PERF_GATEWAY_PORT/v1/channels/feishu/<binding_id>/callback`，
不要把端口发布到公网。性能门禁使用专用合成租户和飞书 binding。先用独立的运行账号 DSN 创建 fixture；该命令
只有同时带 `--execute`、`TRPC_RUN_REAL_MULTINODE=1` 和精确确认值才会连接 PostgreSQL，
默认只接受 `localhost`、`127.0.0.1` 或 `::1`。它只写入离线 deterministic 配置和不存在的
测试 SecretRef，不会向飞书发送消息：

```powershell
$env:TRPC_PERF_DATABASE_DSN = "postgresql://$env:TRPC_RUNTIME_USER`:$env:RUNTIME_DATABASE_PASSWORD@127.0.0.1:5432/$env:POSTGRES_DB"
$env:TRPC_PERF_FIXTURE_CONFIRM = "I_UNDERSTAND_PERFORMANCE_FIXTURE"
$fixtureReport = "runs/multitenant/performance-fixture.json"
.venv\Scripts\python.exe scripts/performance_fixture.py create --execute `
  --output $fixtureReport
```

将报告中的 `tenant_id` 和 `run_id` 原样传给性能门禁使用；完成验收后，必须使用同一份报告
和这两个身份字段清理。清理只在事务内按固定 tenant-first allowlist 删除该 `perf-` 租户的
行，不删除 schema、其他租户或 Compose volume：

```powershell
$fixture = Get-Content $fixtureReport -Raw | ConvertFrom-Json
.venv\Scripts\python.exe scripts/performance_fixture.py cleanup --execute `
  --report $fixtureReport --tenant-id $fixture.tenant_id --run-id $fixture.run_id `
  --output runs/multitenant/performance-fixture-cleanup.json
```

如果数据库不在回环地址，还必须额外传 `--allow-remote` 并设置
`TRPC_PERF_FIXTURE_REMOTE_CONFIRM=I_UNDERSTAND_REMOTE_PERFORMANCE_FIXTURE`；不要把服务 DSN
或 `TRPC_REAL_DATABASE_DSN` 作为 fixture DSN。创建/清理失败或缺少确认时均会 fail-closed，
报告只记录错误类型，不记录 DSN、SecretRef 内容或消息正文。

如果本机已有其他 Compose 项目占用 5432、6379、9000 或 9001，应先在运行命令中
通过基础 Compose 文件支持的 `POSTGRES_PORT`、`REDIS_PORT`、`MINIO_PORT` 和
`MINIO_CONSOLE_PORT` 选择未占用端口，并据此设置门禁进程的 DSN/Redis URL；容器间
仍使用 Compose 内部端口。性能 override 不会自动改写这些发布端口，避免出现重复
端口映射。

在确认四个独立 Worker 健康并完成迁移后，再按 `scripts/real_performance_gate.py`
的双重确认要求执行正式门禁。不要启动 profile 为 `performance-disabled` 的服务，
也不要执行 `docker compose down -v`；停止时只使用 `docker compose ... down`，以保留
PostgreSQL、Redis、MinIO 和 Prometheus 数据卷。上述 `config --quiet` 只做 Compose
配置渲染，不会启动容器、发送回调或连接外部 IM。

## 正式性能命令（唯一允许的实际负载入口）

下面是一次完整的、可审计的顺序。`real_performance_gate.py` 不会启动、停止、扩容或
杀死被测服务；它只连接已经启动的本地 Compose 栈，并由一个受监督的子进程提交负载。
正式持续阶段是 **200 个加密合成 Feishu HTTP callback，以 105 callback/s 提交**；
随后是 **200 个独立 Session turn 的 burst**，由 override 中的 **4 个 Worker** 处理。
持续阶段只访问 `127.0.0.1` 上的本项目 Gateway，不访问真实飞书；“Feishu”在这里仅指
使用同一套签名/加密/解析协议的本地合成入口。

### 1. 当前 PowerShell 会话和合成密钥

使用一个全新的 PowerShell 窗口，并为这次运行设置唯一项目名、未占用回环端口和临时
合成 Secret。下面的 7 个 Compose Secret、3 个 Feishu fixture Secret 都由本机会话随机
生成；它们只供本地性能栈使用，绝不能填入真实飞书 AppSecret、Verification Token 或
Encrypt Key：

```powershell
$ErrorActionPreference = "Stop"
$env:COMPOSE_DISABLE_ENV_FILE = "1"        # 禁止 Compose 自动加载 .env；本次只用当前会话的合成值
$project = "trpc-perf-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
$env:TRPC_PERF_COMPOSE_PROJECT = $project
$env:TRPC_REAL_COMPOSE_PROJECT = $project  # 性能门禁只检查同一 Compose 项目的 Worker
$env:TRPC_PERF_GATEWAY_PORT = "18080"       # 若占用，换成其他未占用的本机端口
$env:TRPC_RUN_REAL_MULTINODE = "1"

function New-SyntheticSecret {
  $bytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  ([Convert]::ToBase64String($bytes)).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

function New-SyntheticHexSecret {
  $bytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  [Convert]::ToHexString($bytes).ToLowerInvariant()
}

function New-SyntheticBase64UrlSecret {
  $bytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
  ([Convert]::ToBase64String($bytes)).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

# 保留并显式固定 Compose 的非敏感默认值。
$env:POSTGRES_DB = "trpc_service"
$env:POSTGRES_USER = "trpc"
$env:TRPC_RUNTIME_USER = "trpc_runtime"
$env:MINIO_ROOT_USER = "trpc-minio"
$env:MINIO_BUCKET = "trpc-artifacts"

# docker-compose.yml 的全部 environment-backed secrets；不要回显或写入文件。
$env:POSTGRES_PASSWORD = New-SyntheticHexSecret
$env:RUNTIME_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:WORKER_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:MIGRATION_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:REDIS_PASSWORD = New-SyntheticHexSecret
$env:MINIO_ROOT_PASSWORD = New-SyntheticHexSecret
$env:SESSION_HMAC_KEY = New-SyntheticBase64UrlSecret # 解码后正好 32 字节
$env:EMERGENCY_QUEUE_KEY = New-SyntheticBase64UrlSecret # 解码后正好 32 字节
$env:DEVELOPMENT_TOKEN = New-SyntheticHexSecret

$env:TRPC_PERF_FIXTURE_UNUSED_APP_SECRET = New-SyntheticSecret
$env:TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN = New-SyntheticSecret
$env:TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY = New-SyntheticSecret
```

不要在终端回显上述变量，也不要把它们写入 `.env`、报告、截图或 Git。若本机已有
其他 Compose 项目占用 PostgreSQL/Redis/MinIO 端口，先选择未占用端口并在同一会话中
设置 `POSTGRES_PORT`、`REDIS_PORT`、`MINIO_PORT`、`MINIO_CONSOLE_PORT`；后续 DSN/URL
必须使用相同的宿主机端口。

### 2. 构建并证明 source-attested 镜像

每次正式运行都从当前 checkout 重新计算指纹，并把指纹写入镜像 label。不要复用未
证明来源的 `:dev` 镜像；也不要与 pytest、另一个 build 或另一个负载进程并行。正式门禁使用
单平台 manifest，使 Docker 与 Kubernetes CRI 观测到同一个不可变摘要；供应链来源仍由源码
fingerprint、SBOM 和独立签名报告证明：

```powershell
$sourceFingerprint = .venv\Scripts\python.exe -c "from pathlib import Path; from scripts.evidence_lineage import source_fingerprint; print(source_fingerprint(Path.cwd())['value'])"
if ($LASTEXITCODE -ne 0 -or $sourceFingerprint -notmatch '^[0-9a-f]{64}$') { throw "source fingerprint is unavailable" }
$env:TRPC_PERF_IMAGE = "trpc-agent-service:perf-$sourceFingerprint"

docker build --provenance=false --build-arg "TRPC_SOURCE_FINGERPRINT=$sourceFingerprint" `
  --tag $env:TRPC_PERF_IMAGE .
if ($LASTEXITCODE -ne 0) { throw "candidate image build failed" }
```

### 3. 渲染并启动最小性能栈

`config --quiet` 是静态校验；它不会启动容器。通过后才启动 PostgreSQL、Redis、MinIO、
迁移、Gateway、一个 Outbox Dispatcher、一个 Session Recovery 和四个 Worker。Channel Dispatcher、企业微信
连接器、真实 IM outbound、Toxiproxy 和观测大栈不在这次性能拓扑中：

```powershell
$compose = @("-f", "docker-compose.yml", "-f", "deploy/performance-runtime.override.yml", "-f", "deploy/acceptance-runtime.override.yml", "-p", $env:TRPC_PERF_COMPOSE_PROJECT)
docker compose @compose config --quiet
if ($LASTEXITCODE -ne 0) { throw "performance Compose configuration is invalid" }

docker compose @compose up -d --no-build --scale worker=4 --scale outbox-dispatcher=1 `
  postgres redis minio minio-init migrate gateway outbox-dispatcher session-recovery worker
if ($LASTEXITCODE -ne 0) { throw "performance Compose startup failed" }

docker compose @compose ps
```

必须确认迁移已成功、Gateway/Redis/PostgreSQL/MinIO/Session Recovery 健康且显示 4 个不同 Worker。若任一
服务未就绪，停止并修复环境，不要继续发送负载。

### 4. 创建并核对专用 fixture

fixture 使用 PostgreSQL **运行账号**，不是 owner/迁移账号。上一步已经在当前 PowerShell
会话生成 `RUNTIME_DATABASE_PASSWORD`、`WORKER_DATABASE_PASSWORD`、`REDIS_PASSWORD` 和
`SESSION_HMAC_KEY`；其中
`SESSION_HMAC_KEY` 是无填充 base64url、解码后正好 32 字节，不需要人工复制、回显或再填写生产凭证。数据库、Redis 和 Session
HMAC 后续直接复用这些 environment-backed Secret。

```powershell
# 通过密码管理器/安全环境注入，不要把真实值提交或打印。
$dbPort = if ($env:POSTGRES_PORT) { $env:POSTGRES_PORT } else { "5432" }
$redisPort = if ($env:REDIS_PORT) { $env:REDIS_PORT } else { "6379" }
$dbName = $env:POSTGRES_DB
$runtimeUser = $env:TRPC_RUNTIME_USER
foreach ($name in @("POSTGRES_PASSWORD", "RUNTIME_DATABASE_PASSWORD", "WORKER_DATABASE_PASSWORD", "MIGRATION_DATABASE_PASSWORD", "REDIS_PASSWORD", "MINIO_ROOT_PASSWORD", "SESSION_HMAC_KEY", "EMERGENCY_QUEUE_KEY", "DEVELOPMENT_TOKEN")) {
  if (-not [Environment]::GetEnvironmentVariable($name)) { throw "Compose secret was not generated: $name" }
}

$env:TRPC_PERF_DATABASE_DSN = "postgresql://$runtimeUser`:$env:RUNTIME_DATABASE_PASSWORD@127.0.0.1`:$dbPort/$dbName"
$env:TRPC_PERF_FIXTURE_CONFIRM = "I_UNDERSTAND_PERFORMANCE_FIXTURE"
$fixtureReport = "runs/multitenant/performance-fixture.json"

.venv\Scripts\python.exe scripts/performance_fixture.py create --execute `
  --output $fixtureReport
if ($LASTEXITCODE -ne 0) { throw "performance fixture creation failed" }

$fixture = Get-Content $fixtureReport -Raw | ConvertFrom-Json
if ($fixture.gate -ne "pass" -or $fixture.synthetic -ne $true) { throw "fixture is not a successful synthetic fixture" }
foreach ($field in @("tenant_id", "binding_id", "run_id", "channel", "manifest_checksum")) {
  if (-not $fixture.$field) { throw "fixture field missing: $field" }
}
if ($fixture.channel -ne "feishu") { throw "fixture channel is not Feishu" }
```

创建报告中的 `tenant_id`、`binding_id`、`run_id` 和 `manifest_checksum` 是本次运行唯一
身份。不要手工改写它们，也不要把旧报告混用到新 Compose 项目。

### 5. 注入运行账号和 fixture 路由身份

性能门禁要求把数据库、Redis 和 Session HMAC 明确作为运行时输入；它不会从迁移账号或
旧报告推断这些值。上一步生成的 URL-safe hex Secret 可以直接放入 DSN/Redis URL，
不需要人工复制或回显：

```powershell
$env:TRPC_REAL_DATABASE_DSN = $env:TRPC_PERF_DATABASE_DSN
$env:TRPC_REAL_REDIS_URL = "redis://:$env:REDIS_PASSWORD@127.0.0.1`:$redisPort/0"
$env:TRPC_REAL_SESSION_HMAC_KEY = $env:SESSION_HMAC_KEY
$env:TRPC_REAL_TENANT_ID = [string]$fixture.tenant_id
$env:TRPC_REAL_BINDING_ID = [string]$fixture.binding_id
$env:TRPC_REAL_RUN_ID = [string]$fixture.run_id
$env:TRPC_PERF_GATEWAY_BASE_URL = "http://127.0.0.1:$env:TRPC_PERF_GATEWAY_PORT"
$formalReport = "runs/multitenant/real-performance.json"
```

`TRPC_PERF_GATEWAY_BASE_URL` 必须是本机回环地址，不得带 query、账号或密码；脚本会拒绝
公网地址。`TRPC_REAL_TENANT_ID` 只能来自本次成功 fixture 报告，不能由 callback 请求体
声明或手工指定其他租户。

### 6. 双重真实负载确认与正式运行

正式命令必须同时满足以下两类确认，缺一则脚本只写 `production_gate=not_run`，不会发起
数据库、Redis 或 HTTP 负载：

1. CLI 确认：`--execute --confirm-real-load`；
2. 环境确认：`TRPC_RUN_REAL_MULTINODE=1` 和
   `TRPC_REAL_PERFORMANCE_CONFIRM=I_UNDERSTAND_REAL_LOAD`，并且仍需使用
   `TRPC_REAL_DATABASE_DSN`、`TRPC_REAL_REDIS_URL`、`TRPC_REAL_SESSION_HMAC_KEY`、
   `TRPC_REAL_TENANT_ID`、`TRPC_REAL_BINDING_ID`、`TRPC_REAL_COMPOSE_PROJECT`、Gateway
   合成加密 Secret。`TRPC_REAL_COMPOSE_PROJECT` 与 `TRPC_PERF_COMPOSE_PROJECT` 必须相同。

确认无误后，只运行下面这一条正式命令；默认参数也锁定为 200 callback、105 callback/s、
200 burst turns、至少 4 个 Worker：

```powershell
$env:TRPC_REAL_PERFORMANCE_CONFIRM = "I_UNDERSTAND_REAL_LOAD"
if ($env:TRPC_REAL_COMPOSE_PROJECT -ne $env:TRPC_PERF_COMPOSE_PROJECT) {
  throw "performance Compose project identities do not match"
}

.venv\Scripts\python.exe scripts/real_performance_gate.py `
  --execute --confirm-real-load `
  --callbacks 200 --callback-rate 105 --burst-turns 200 `
  --max-inflight 32 --db-pool-size 16 --min-workers 4 `
  --output $formalReport --require-production
```

成功条件全部满足时，`$formalReport` 的 `gate` 和 `production_gate` 才能为 `pass`：
持续 HTTP callback 全部返回成功、HTTP p95 小于 200ms、实际提交速率至少 100/s、每个
callback 都能在 tenant/channel/account 约束下找到唯一 PostgreSQL `inbound_messages`；
burst 需要 200 个唯一 Session、实际 turn overlap 达到 200、事件/turn 全部提交、Redis
最终 pending 回到 0，并且 preflight 证明是同一 source-attested 镜像上的至少四个独立
Worker。报告只保存聚合指标、ID、hash 和安全元数据，不保存密钥、正文或完整 URL。

任何失败、超时、服务不健康、Worker 少于 4 个、镜像 fingerprint 不一致、fixture 报告
不完整或确认值错误，都必须把该次结果视为 `not_run`/失败证据，不得通过修改报告或降低
参数宣称生产通过。该命令不会连接真实飞书，也不会执行企业微信或飞书真实凭证验收。

### 7. 精确清理与保留卷

无论正式门禁通过还是失败，只能用同一份 fixture 报告中的身份清理；清理前不要删除报告。
清理会验证报告 checksum、tenant ownership 和固定表 allowlist：

```powershell
.venv\Scripts\python.exe scripts/performance_fixture.py cleanup --execute `
  --report $fixtureReport `
  --tenant-id $fixture.tenant_id `
  --run-id $fixture.run_id `
  --output runs/multitenant/performance-fixture-cleanup.json
if ($LASTEXITCODE -ne 0) { throw "fixture cleanup failed; stop and inspect the cleanup report" }

docker compose @compose down
```

只允许删除本次 `perf-` fixture 租户的记录；不要执行 `docker compose down -v`，不要手工
删除 PostgreSQL、Redis、MinIO 或 Prometheus volume。若 cleanup 失败，应保留数据库和
报告供审计，禁止扩大删除范围。完成后检查没有遗留本次性能负载子进程或容器；下一次运行
必须使用新的项目名、端口、随机 Secret 和 fixture。

上述步骤不可与 pytest、Docker build 或另一轮负载并行执行。它仍然只完成“合成 Feishu HTTP
入口 + 本地多进程 Worker”的性能证据；真实迁移、Toxiproxy/进程终止、Kubernetes 运行态、
企业微信真实凭证和飞书真实凭证验收仍分别保持 `not_run`，直到各自的真实环境门禁完成。
