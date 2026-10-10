$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
Set-Location $projectRoot
$env:COMPOSE_DISABLE_ENV_FILE = "1"

function Get-DockerInspectDocument {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )
    $raw = docker inspect @Arguments | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "docker inspect failed"
    }
    try {
        $documents = @(($raw -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop)
    }
    catch {
        throw "docker inspect returned invalid JSON"
    }
    if ($documents.Count -ne 1) {
        throw "docker inspect returned an unexpected JSON document count"
    }
    return $documents[0]
}

$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runtimeProject = "trpc-fault-runtime-$runStamp"
$stageProject = "trpc-fault-stage-$runStamp"
if ($runtimeProject -eq $stageProject) {
    throw "runtime and fault-stage projects must differ"
}
if (-not $env:TRPC_RELEASE_ID -or -not $env:TRPC_RELEASE_NONCE) {
    throw "release binding missing"
}

& .venv\Scripts\python.exe scripts/candidate_lock.py verify | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "candidate lock verification failed"
}
$lockedCandidate = Get-Content -LiteralPath "runs/multitenant/candidate-lock.json" -Raw | ConvertFrom-Json
$sourceFingerprint = [string]$lockedCandidate.source_fingerprint.value
if ($sourceFingerprint -ne $fp) {
    throw "source fingerprint changed after lock"
}
$env:TRPC_SERVICE_IMAGE = $initial
$imageDocument = Get-DockerInspectDocument -Arguments @($env:TRPC_SERVICE_IMAGE)
$imageEvidence = "{0}|{1}" -f [string]$imageDocument.Id, [string]$imageDocument.Config.Labels.'io.trpc.agent-service.source-fingerprint'
if ($imageEvidence -ne "$initialId|$fp") {
    throw "candidate image binding mismatch"
}

function New-SyntheticHexSecret {
    $bytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($bytes) } finally { $generator.Dispose() }
    (([BitConverter]::ToString($bytes)) -replace "-", "").ToLowerInvariant()
}

function New-SyntheticBase64UrlSecret {
    $bytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($bytes) } finally { $generator.Dispose() }
    ([Convert]::ToBase64String($bytes)).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

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
    $rows = @(
        $ids | ForEach-Object {
            $document = Get-DockerInspectDocument -Arguments @($_)
            $healthStatus = if ($document.State.Health) {
                [string]$document.State.Health.Status
            }
            else {
                "none"
            }
            [pscustomobject]@{
                Id = [string]$document.Id
                SourceFingerprint = [string]$document.Config.Labels.'io.trpc.agent-service.source-fingerprint'
                Image = [string]$document.Image
                StateStatus = [string]$document.State.Status
                HealthStatus = $healthStatus
            }
        }
    )
    if ($rows.Count -ne 4) {
        throw "$Project worker inspect failed"
    }
    $imageIds = @()
    foreach ($row in $rows) {
        if (
            $row.Id -notmatch '^[0-9a-f]{64}$' -or
            $row.SourceFingerprint -ne $ExpectedSource -or
            $row.Image -notmatch '^sha256:[0-9a-f]{64}$' -or
            $row.StateStatus -ne "running" -or
            $row.HealthStatus -ne "healthy"
        ) {
            throw "$Project worker attestation/health mismatch"
        }
        $imageIds += $row.Image
    }
    if (@($imageIds | Sort-Object -Unique).Count -ne 1) {
        throw "$Project workers use mixed images"
    }
    if ($ExpectedImage -and $imageIds[0] -ne $ExpectedImage) {
        throw "$Project image differs from candidate"
    }
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
            return Get-HealthyWorkerInventory -ComposeArgs $ComposeArgs -Project $Project -ExpectedSource $ExpectedSource -ExpectedImage $ExpectedImage
        }
        catch {
            if ($attempt -eq 59) {
                throw
            }
            Start-Sleep -Seconds 2
        }
    }
    throw "$Project worker health did not converge"
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

# Normal runtime/Toxiproxy project.
$runtimePostgresPassword = New-SyntheticHexSecret
$runtimeDatabasePassword = New-SyntheticHexSecret
$runtimeWorkerDatabasePassword = New-SyntheticHexSecret
$runtimeMigrationPassword = New-SyntheticHexSecret
$runtimeMetricsDatabasePassword = New-SyntheticHexSecret
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
$env:METRICS_DATABASE_PASSWORD = $runtimeMetricsDatabasePassword
$env:REDIS_PASSWORD = $runtimeRedisPassword
$env:MINIO_ROOT_PASSWORD = $runtimeMinioPassword
$env:SESSION_HMAC_KEY = $runtimeSessionHmacKey
$env:EMERGENCY_QUEUE_KEY = $runtimeEmergencyQueueKey
$env:DEVELOPMENT_TOKEN = $runtimeDevelopmentToken
$env:TRPC_REAL_COMPOSE_PROJECT = $runtimeProject
$env:TRPC_REAL_TOXIPROXY_API = "http://127.0.0.1:$runtimeToxiproxyApiPort"
$normalCompose = @(
    "-f", "docker-compose.yml",
    "-f", "deploy/toxiproxy-runtime.override.yml",
    "-f", "deploy/acceptance-runtime.override.yml",
    "-p", $runtimeProject
)

docker compose @normalCompose config --quiet
if ($LASTEXITCODE -ne 0) {
    throw "normal runtime Compose configuration is invalid"
}
Write-Output "FAULT_GATE_PHASE normal_start project=$runtimeProject"
docker compose @normalCompose up -d --no-build --scale worker=4 postgres redis minio minio-init migrate gateway toxiproxy worker outbox-dispatcher channel-dispatcher post-turn-projector session-recovery
if ($LASTEXITCODE -ne 0) {
    throw "normal runtime Compose startup failed"
}

$normalWorkerIds = @(Wait-HealthyWorkerInventory -ComposeArgs $normalCompose -Project $runtimeProject -ExpectedSource $sourceFingerprint -ExpectedImage $initialId)
$normalImageDocument = Get-DockerInspectDocument -Arguments @($normalWorkerIds[0])
$normalImageDigest = [string]$normalImageDocument.Image
if ($normalImageDigest -ne $initialId) {
    throw "normal candidate image ID mismatch"
}
$env:TRPC_REAL_IMAGE_DIGEST = $normalImageDigest
$env:TRPC_REAL_WORKER_IDENTITIES = $normalWorkerIds -join ","

$env:TRPC_PERF_DATABASE_DSN = "postgresql://{0}:{1}@127.0.0.1:{2}/{3}" -f $env:TRPC_RUNTIME_USER, $runtimeDatabasePassword, $runtimeDbPort, $env:POSTGRES_DB
$env:TRPC_PERF_FIXTURE_CONFIRM = "I_UNDERSTAND_PERFORMANCE_FIXTURE"
$normalFixtureReport = "runs/multitenant/fault-runtime-fixture-$runStamp.json"
.venv\Scripts\python.exe scripts/performance_fixture.py create --execute --output $normalFixtureReport
if ($LASTEXITCODE -ne 0) {
    throw "normal runtime fixture creation failed"
}
$normalFixture = Get-Content $normalFixtureReport -Raw | ConvertFrom-Json
if ($normalFixture.gate -ne "pass" -or $normalFixture.synthetic -ne $true) {
    throw "normal fixture is not synthetic/pass"
}

$env:TRPC_REAL_TENANT_ID = [string]$normalFixture.tenant_id
$env:TRPC_REAL_BINDING_ID = [string]$normalFixture.binding_id
$env:TRPC_REAL_DATABASE_DSN = "postgresql+asyncpg://{0}:{1}@127.0.0.1:{2}/{3}" -f $env:TRPC_RUNTIME_USER, $runtimeDatabasePassword, $runtimeDbPort, $env:POSTGRES_DB
$env:TRPC_REAL_REDIS_URL = "redis://:{0}@127.0.0.1:{1}/0" -f $runtimeRedisPassword, $runtimeRedisPort
$env:TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN = "postgresql+asyncpg://{0}:{1}@127.0.0.1:{2}/{3}" -f $env:TRPC_WORKER_USER, $runtimeWorkerDatabasePassword, $runtimeDbPort, $env:POSTGRES_DB
$env:TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE = $env:TRPC_WORKER_USER
$env:TRPC_REAL_RUNTIME_DATABASE_ROLE = $env:TRPC_RUNTIME_USER
$env:TRPC_REAL_SESSION_HMAC_KEY = $runtimeSessionHmacKey

# Dedicated fault-stage project.
$stagePostgresPassword = New-SyntheticHexSecret
$stageDatabasePassword = New-SyntheticHexSecret
$stageWorkerDatabasePassword = New-SyntheticHexSecret
$stageMigrationPassword = New-SyntheticHexSecret
$stageMetricsDatabasePassword = New-SyntheticHexSecret
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
$env:METRICS_DATABASE_PASSWORD = $stageMetricsDatabasePassword
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
    "-f", "docker-compose.yml",
    "-f", "deploy/fault-stage-runtime.override.yml",
    "-f", "deploy/acceptance-runtime.override.yml",
    "-p", $stageProject
)

docker compose @stageCompose config --quiet
if ($LASTEXITCODE -ne 0) {
    throw "fault-stage Compose configuration is invalid"
}
Write-Output "FAULT_GATE_PHASE stage_start project=$stageProject"
docker compose @stageCompose up -d --no-build --scale worker=4 postgres redis minio minio-init migrate worker outbox-dispatcher session-recovery
if ($LASTEXITCODE -ne 0) {
    throw "fault-stage Compose startup failed"
}

$stageWorkerIds = @(Wait-HealthyWorkerInventory -ComposeArgs $stageCompose -Project $stageProject -ExpectedSource $sourceFingerprint -ExpectedImage $normalImageDigest)
$env:TRPC_FAULT_WORKER_CONTAINER = $stageWorkerIds[0]
$env:TRPC_FAULT_DATABASE_DSN = "postgresql+asyncpg://{0}:{1}@127.0.0.1:{2}/{3}" -f $env:TRPC_RUNTIME_USER, $stageDatabasePassword, $stageDbPort, $env:POSTGRES_DB
$env:TRPC_FAULT_REDIS_URL = "redis://:{0}@127.0.0.1:{1}/0" -f $stageRedisPassword, $stageRedisPort
$env:TRPC_FAULT_BINDING_SEED = "binding-$stageRunId"
$env:TRPC_RUN_FAULT_STAGE_ACCEPTANCE = "1"
$env:TRPC_FAULT_STAGE_ALLOW_KILL = "1"

$faultReport = "runs/multitenant/fault-injection.json"
Write-Output "FAULT_GATE_PHASE execute report=$faultReport"
.venv\Scripts\python.exe scripts/fault_injection_gate.py --execute --require-production --scenario all --allow-process-kill --project $runtimeProject --fault-project $stageProject --fault-worker-container $env:TRPC_FAULT_WORKER_CONTAINER --workers 4 --messages 200 --duplicates 20 --fault-messages 8 --toxiproxy-api $env:TRPC_REAL_TOXIPROXY_API --output $faultReport
if ($LASTEXITCODE -ne 0) {
    throw "fault-injection execution failed; projects retained"
}
$fault = Get-Content $faultReport -Raw | ConvertFrom-Json
if ($fault.gate -ne "pass" -or $fault.production_gate -ne "pass") {
    throw "fault-injection report did not pass the complete production gate; projects retained"
}
$requiredExecutedScenarios = @(
    "redis_interrupt", "worker_enqueue", "worker_tool", "worker_commit",
    "fencing", "republish", "dlq"
)
foreach ($scenarioName in $requiredExecutedScenarios) {
    $executedScenario = $fault.candidate.scenarios.PSObject.Properties[$scenarioName].Value
    if (-not $executedScenario -or $executedScenario.status -ne "pass") {
        throw "required real scenario did not pass: $scenarioName; projects retained"
    }
}
$ambiguousScenario = $fault.candidate.scenarios.PSObject.Properties["ambiguous"].Value
if (-not $ambiguousScenario -or $ambiguousScenario.status -ne "pass") {
    throw "ambiguous provider scenario did not pass the production gate; projects retained"
}

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
    if (-not $scenario -or $scenario.status -ne "pass") {
        throw "$scenarioName did not pass"
    }
    foreach ($markerName in $requiredFaultStageMarkers[$scenarioName]) {
        $matches = @($scenario.stage_markers | Where-Object { $_.name -eq $markerName })
        if ($matches.Count -ne 1 -or $matches[0].status -ne "pass" -or -not ([string]$matches[0].observed_at).Trim()) {
            throw "$scenarioName marker invalid: $markerName"
        }
    }
    $childPath = [string]$scenario.child_report
    if (-not $childPath -or -not (Test-Path -LiteralPath $childPath -PathType Leaf)) {
        throw "$scenarioName child report missing"
    }
}

$fixture = Get-Content $normalFixtureReport -Raw | ConvertFrom-Json
$env:TRPC_PERF_DATABASE_DSN = "postgresql://{0}:{1}@127.0.0.1:{2}/{3}" -f $env:TRPC_RUNTIME_USER, $runtimeDatabasePassword, $runtimeDbPort, $env:POSTGRES_DB
$fixtureCleanupReport = "runs/multitenant/fault-runtime-fixture-cleanup-$runStamp.json"
.venv\Scripts\python.exe scripts/performance_fixture.py cleanup --execute --report $normalFixtureReport --tenant-id $fixture.tenant_id --run-id $fixture.run_id --output $fixtureCleanupReport
if ($LASTEXITCODE -ne 0) {
    throw "normal runtime fixture cleanup failed; projects retained"
}

docker compose @stageCompose down --volumes
if ($LASTEXITCODE -ne 0) {
    throw "fault-stage Compose cleanup failed; volumes retained"
}

# Restore normal project settings before resolving and stopping it.
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
$env:METRICS_DATABASE_PASSWORD = $runtimeMetricsDatabasePassword
$env:REDIS_PASSWORD = $runtimeRedisPassword
$env:MINIO_ROOT_PASSWORD = $runtimeMinioPassword
$env:SESSION_HMAC_KEY = $runtimeSessionHmacKey
$env:EMERGENCY_QUEUE_KEY = $runtimeEmergencyQueueKey
$env:DEVELOPMENT_TOKEN = $runtimeDevelopmentToken
docker compose @normalCompose down --volumes
if ($LASTEXITCODE -ne 0) {
    throw "normal runtime Compose cleanup failed; volumes retained"
}

Write-Output ("FAULT_GATE_RESULT gate={0}|production={1}|report={2}|run={3}" -f $fault.gate, $fault.production_gate, $faultReport, $runStamp)
