$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
Set-Location $projectRoot
$env:COMPOSE_DISABLE_ENV_FILE = "1"

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
$imageInspectionRaw = docker image inspect $initial
$imageInspectExit = $LASTEXITCODE
$imageInspection = @()
if ($imageInspectExit -eq 0) {
    try {
        $imageInspection = @(($imageInspectionRaw -join [Environment]::NewLine) | ConvertFrom-Json)
    }
    catch {
        $imageInspection = @()
    }
}
$imageEvidence = ""
if ($imageInspection.Count -eq 1) {
    $imageDocument = $imageInspection[0]
    $imageEvidence = "{0}|{1}" -f [string]$imageDocument.Id, [string]$imageDocument.Config.Labels.'io.trpc.agent-service.source-fingerprint'
}
if ($imageInspectExit -ne 0 -or $imageInspection.Count -ne 1 -or $imageEvidence -ne "$initialId|$fp") {
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

function New-LoopbackPort {
    param([int[]]$Reserved = @())
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        $candidate = Get-Random -Minimum 20000 -Maximum 45000
        if ($Reserved -contains $candidate) {
            continue
        }
        $listener = [System.Net.Sockets.TcpListener]::new(
            [System.Net.IPAddress]::Loopback,
            $candidate
        )
        try {
            $listener.Start()
            return $candidate
        }
        catch {
            # Retry when the port is occupied or excluded by the host.
        }
        finally {
            $listener.Stop()
        }
    }
    throw "could not reserve a free loopback port"
}

$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$project = "trpc-migration-$runStamp"
$env:TRPC_SERVICE_IMAGE = $initial
$reservedPorts = @()
$env:POSTGRES_PORT = [string](New-LoopbackPort -Reserved $reservedPorts)
$reservedPorts += [int]$env:POSTGRES_PORT
$env:REDIS_PORT = [string](New-LoopbackPort -Reserved $reservedPorts)
$reservedPorts += [int]$env:REDIS_PORT
$env:MINIO_PORT = [string](New-LoopbackPort -Reserved $reservedPorts)
$reservedPorts += [int]$env:MINIO_PORT
$env:MINIO_CONSOLE_PORT = [string](New-LoopbackPort -Reserved $reservedPorts)
$env:POSTGRES_DB = "trpc_service"
$env:POSTGRES_USER = "trpc"
$env:TRPC_RUNTIME_USER = "trpc_runtime"
$env:TRPC_WORKER_USER = "trpc_worker"
$env:TRPC_MIGRATION_USER = "trpc_migration"
$env:MINIO_ROOT_USER = "trpc-minio"
$env:MINIO_BUCKET = "trpc-artifacts"
$env:POSTGRES_PASSWORD = New-SyntheticHexSecret
$env:RUNTIME_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:WORKER_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:MIGRATION_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:METRICS_DATABASE_PASSWORD = New-SyntheticHexSecret
$env:REDIS_PASSWORD = New-SyntheticHexSecret
$env:MINIO_ROOT_PASSWORD = New-SyntheticHexSecret
$env:SESSION_HMAC_KEY = New-SyntheticBase64UrlSecret
$env:EMERGENCY_QUEUE_KEY = New-SyntheticBase64UrlSecret
$env:DEVELOPMENT_TOKEN = New-SyntheticHexSecret

$compose = @(
    "-f", "docker-compose.yml",
    "-f", "deploy/acceptance-runtime.override.yml",
    "-p", $project
)
docker compose @compose config --quiet
if ($LASTEXITCODE -ne 0) { throw "migration Compose configuration is invalid" }
docker compose @compose up -d --no-build postgres redis migrate
if ($LASTEXITCODE -ne 0) { throw "migration Compose startup failed; project retained" }

$migrateId = (docker compose @compose ps -aq migrate | Select-Object -First 1).Trim()
if (-not $migrateId) { throw "migration container is missing; project retained" }
$migrateDeadline = (Get-Date).AddSeconds(120)
do {
    $migrateInspectionRaw = docker inspect $migrateId
    $migrateInspectExit = $LASTEXITCODE
    $migrateInspection = @()
    if ($migrateInspectExit -eq 0) {
        try {
            $migrateInspection = @(($migrateInspectionRaw -join [Environment]::NewLine) | ConvertFrom-Json)
        }
        catch {
            $migrateInspection = @()
        }
    }
    $migrateState = ""
    if ($migrateInspection.Count -eq 1) {
        $migrateDocument = $migrateInspection[0]
        if ([string]$migrateDocument.Image -ne [string]$initialId) {
            throw "schema migration image binding mismatch; project retained"
        }
        $migrateState = "{0}|{1}" -f [string]$migrateDocument.State.Status, [string]$migrateDocument.State.ExitCode
    }
    $migrateParts = $migrateState.Split('|', 2)
    if ($migrateParts[0] -eq "exited") { break }
    if ($migrateParts[0] -notin @("created", "running")) {
        throw "schema migration entered unexpected state $($migrateParts[0]); project retained"
    }
    Start-Sleep -Milliseconds 250
} while ((Get-Date) -lt $migrateDeadline)
if ($migrateParts[0] -ne "exited") { throw "schema migration timed out; project retained" }
$migrateExit = $migrateParts[1]
if ($migrateExit -ne "0") { throw "schema migration failed; project retained" }

$env:TRPC_RUN_REAL_MIGRATION = "1"
$env:TRPC_MIGRATION_PROVISION = "1"
$env:TRPC_MIGRATION_PROVISION_CONFIRMATION = "I_UNDERSTAND_CREATE_NEW_PRODUCTION_CANARY"
$env:TRPC_MIGRATION_KINDS = "session,memory"
$env:TRPC_MIGRATION_SOURCE_REDIS_URL = "redis://:$env:REDIS_PASSWORD@127.0.0.1`:$env:REDIS_PORT/0"
$env:TRPC_MIGRATION_TARGET_DATABASE_DSN = "postgresql+asyncpg://$env:TRPC_RUNTIME_USER`:$env:RUNTIME_DATABASE_PASSWORD@127.0.0.1`:$env:POSTGRES_PORT/$env:POSTGRES_DB"
$env:TRPC_MIGRATION_TENANT_ID = "production-canary-tenant-$runStamp"
$env:TRPC_MIGRATION_ID = "production-canary-migration-$runStamp"
$env:TRPC_MIGRATION_APP_ID = "production-canary-app-$runStamp"
$env:TRPC_MIGRATION_APP_REVISION = "1"
$env:TRPC_MIGRATION_CONFIG_VERSION = "1"
$env:TRPC_MIGRATION_BINDING_ID = "production-canary-binding-$runStamp"
$env:TRPC_MIGRATION_BINDING_REVISION = "1"
$env:TRPC_MIGRATION_OPERATOR_ID = "production-canary-operator-$runStamp"
$env:TRPC_MIGRATION_CHANGE_TICKET = "production-canary-ticket-$runStamp"
$env:TRPC_MIGRATION_IMAGE_DIGEST = $initialId

$canaryReport = "runs/multitenant/migration-production-canary-bootstrap.json"
.venv\Scripts\python.exe scripts/migration_production_canary_bootstrap.py --output $canaryReport
if ($LASTEXITCODE -ne 0) { throw "production canary provisioning failed; project retained" }
$canary = Get-Content -LiteralPath $canaryReport -Raw | ConvertFrom-Json
if ($canary.status -ne "pass" -or $canary.production_gate -ne "not_run" -or $canary.credentials_emitted -ne $false -or $canary.source.source_count -ne 4 -or $canary.target.target_preflight -ne "empty") {
    throw "production canary report contract failed; project retained"
}

$env:TRPC_MIGRATION_OWNER_ID = "migration-operator-$runStamp"
$env:TRPC_MIGRATION_PRODUCTION_CONFIRMATION = "I_UNDERSTAND_REAL_MIGRATION"
$env:TRPC_MIGRATION_CONTROL_FACTORY = "trpc_service.storage.production_migration_control:create"
$migrationReport = "runs/multitenant/migration-live.json"
.venv\Scripts\python.exe scripts/migrate_data.py --production-confirm --output $migrationReport
if ($LASTEXITCODE -ne 0) { throw "production migration failed; project retained" }
$migration = Get-Content -LiteralPath $migrationReport -Raw | ConvertFrom-Json
if ($migration.gate -ne "pass" -or $migration.production_gate -ne "pass") {
    throw "production migration report did not pass; project retained"
}
$verification = $migration.candidate.verification
if (
    -not $verification -or
    $verification.status -ne "pass" -or
    $verification.source_count -ne $verification.target_count -or
    $verification.source_checksum -ne $verification.target_checksum -or
    @($verification.differences).Count -ne 0
) {
    throw "production migration verification checksum/differences mismatch; project retained"
}

docker compose @compose down --volumes
if ($LASTEXITCODE -ne 0) { throw "migration Compose cleanup failed; volumes retained" }
Write-Output ("MIGRATION_RESULT gate={0}|production={1}|report={2}|run={3}" -f $migration.gate, $migration.production_gate, $migrationReport, $runStamp)
