$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
Set-Location $projectRoot
$env:COMPOSE_DISABLE_ENV_FILE = "1"

# This wrapper is intentionally dot-sourced by the release shell.  The caller
# owns the locked image tag/digest/fingerprint variables and release binding.
foreach ($name in @("initial", "initialId", "fp")) {
    $variable = Get-Variable -Name $name -ErrorAction SilentlyContinue
    if ($null -eq $variable -or [string]::IsNullOrWhiteSpace([string]$variable.Value)) {
        throw "caller variable `$${name} is required; dot-source this wrapper after locking the candidate"
    }
}
$candidateImage = [string]$initial
$candidateImageId = [string]$initialId
$candidateFingerprint = [string]$fp
if ($candidateFingerprint -notmatch '^[0-9a-f]{64}$') {
    throw "caller fingerprint is not a canonical sha256 value"
}
if ($candidateImageId -notmatch '^sha256:[0-9a-f]{64}$') {
    throw "caller image ID is not an immutable sha256 digest"
}
if (-not $env:TRPC_RELEASE_ID -or -not $env:TRPC_RELEASE_NONCE) {
    throw "release binding missing"
}

function Get-Sha256Hex {
    param([Parameter(Mandatory = $true)][string]$Value)
    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try {
        $digest = $algorithm.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($Value))
        return (([BitConverter]::ToString($digest)) -replace "-", "").ToLowerInvariant()
    }
    finally {
        $algorithm.Dispose()
    }
}

function New-SyntheticHexSecret {
    $bytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($bytes) } finally { $generator.Dispose() }
    return (([BitConverter]::ToString($bytes)) -replace "-", "").ToLowerInvariant()
}

function New-SyntheticBase64UrlSecret {
    $bytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($bytes) } finally { $generator.Dispose() }
    return ([Convert]::ToBase64String($bytes)).TrimEnd('=').Replace('+', '-').Replace('/', '_')
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
            # A concurrent process may claim a candidate between selection and
            # bind.  Try another candidate without exposing the local error.
        }
        finally {
            $listener.Stop()
        }
    }
    throw "could not reserve a free loopback port"
}

function Assert-ReleaseReport {
    param(
        [Parameter(Mandatory = $true)][object]$Report,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$ExpectedImage,
        [Parameter(Mandatory = $true)][string]$ExpectedFingerprint,
        [Parameter(Mandatory = $true)][string]$ExpectedReleaseId,
        [Parameter(Mandatory = $true)][string]$ExpectedNonceHash
    )
    if ($Report.gate -ne "pass" -or $Report.production_gate -ne "pass") {
        throw "$Label gate/production_gate did not pass"
    }
    $evidence = $Report.evidence
    $binding = if ($null -eq $evidence) { $null } else { $evidence.release_binding }
    if (
        $null -eq $binding -or
        $binding.release_id -ne $ExpectedReleaseId -or
        $binding.nonce_sha256 -ne $ExpectedNonceHash
    ) {
        throw "$Label release binding does not match the caller"
    }
    if ($null -eq $evidence.source_fingerprint -or $evidence.source_fingerprint.value -ne $ExpectedFingerprint) {
        throw "$Label source fingerprint evidence does not match the caller"
    }
}

function Get-HealthyWorkerInventory {
    param(
        [object[]]$ComposeArgs,
        [string]$Project,
        [string]$ExpectedSource,
        [string]$ExpectedImage
    )
    $rawIds = docker compose @ComposeArgs ps -q worker
    if ($LASTEXITCODE -ne 0) {
        throw "$Project worker discovery failed"
    }
    $ids = @($rawIds | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    if ($ids.Count -ne 4) {
        throw "$Project must expose exactly four worker containers"
    }
    $images = @()
    foreach ($id in $ids) {
        $rawInspection = docker inspect $id
        if ($LASTEXITCODE -ne 0) {
            throw "$Project worker inspection failed"
        }
        $documents = @((($rawInspection -join [Environment]::NewLine) | ConvertFrom-Json))
        if ($documents.Count -ne 1) {
            throw "$Project worker inspection failed"
        }
        $document = $documents[0]
        $health = if ($null -eq $document.State.Health) { "none" } else { [string]$document.State.Health.Status }
        $parts = @(
            [string]$document.Id,
            [string]$document.Config.Labels.'io.trpc.agent-service.source-fingerprint',
            [string]$document.Image,
            [string]$document.State.Status,
            $health
        )
        if (
            $parts.Count -ne 5 -or
            $parts[0] -notmatch '^[0-9a-f]{64}$' -or
            $parts[1] -ne $ExpectedSource -or
            $parts[2] -ne $ExpectedImage -or
            $parts[3] -ne "running" -or
            $parts[4] -ne "healthy"
        ) {
            throw "$Project worker source/image/health attestation failed"
        }
        $images += $parts[2]
    }
    if (@($images | Sort-Object -Unique).Count -ne 1) {
        throw "$Project workers use mixed candidate images"
    }
    return $ids
}

function Wait-HealthyWorkerInventory {
    param(
        [object[]]$ComposeArgs,
        [string]$Project,
        [string]$ExpectedSource,
        [string]$ExpectedImage
    )
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        try {
            return Get-HealthyWorkerInventory -ComposeArgs $ComposeArgs -Project $Project -ExpectedSource $ExpectedSource -ExpectedImage $ExpectedImage
        }
        catch {
            if ($attempt -eq 89) {
                throw
            }
            Start-Sleep -Seconds 2
        }
    }
    throw "worker health did not converge"
}

function Wait-HealthyService {
    param([object[]]$ComposeArgs, [string]$Project, [string]$Service)
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        $rawIds = docker compose @ComposeArgs ps -q $Service
        if ($LASTEXITCODE -eq 0) {
            $ids = @($rawIds | ForEach-Object { $_.Trim() } | Where-Object { $_ })
            if ($ids.Count -ge 1) {
                $unhealthy = @($ids | Where-Object {
                    $rawInspection = docker inspect $_
                    if ($LASTEXITCODE -ne 0) {
                        return $true
                    }
                    $documents = @((($rawInspection -join [Environment]::NewLine) | ConvertFrom-Json))
                    if ($documents.Count -ne 1) {
                        return $true
                    }
                    $document = $documents[0]
                    $health = if ($null -eq $document.State.Health) { "none" } else { [string]$document.State.Health.Status }
                    return $document.State.Status -ne "running" -or $health -ne "healthy"
                })
                if ($unhealthy.Count -eq 0) {
                    return $ids
                }
            }
        }
        if ($attempt -eq 89) {
            throw "$Project $Service health did not converge"
        }
        Start-Sleep -Seconds 2
    }
    throw "$Project $Service health did not converge"
}

function Wait-SuccessfulOneShot {
    param([object[]]$ComposeArgs, [string]$Project, [string]$Service)
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        $rawIds = docker compose @ComposeArgs ps -q -a $Service
        if ($LASTEXITCODE -eq 0) {
            $ids = @($rawIds | ForEach-Object { $_.Trim() } | Where-Object { $_ })
            if ($ids.Count -eq 1) {
                $rawInspection = docker inspect $ids[0]
                if ($LASTEXITCODE -eq 0) {
                    $document = @((($rawInspection -join [Environment]::NewLine) | ConvertFrom-Json))[0]
                    if ($document.State.Status -eq "exited") {
                        if ([int]$document.State.ExitCode -eq 0) {
                            return
                        }
                        throw "$Project $Service exited unsuccessfully"
                    }
                }
            }
        }
        if ($attempt -eq 89) {
            throw "$Project $Service did not complete"
        }
        Start-Sleep -Seconds 2
    }
    throw "$Project $Service did not complete"
}

function Assert-RoleEvidence {
    param([Parameter(Mandatory = $true)][object]$Report)
    $roles = $Report.candidate.database_role_evidence
    if ($null -eq $roles -or $roles.status -ne "pass") {
        throw "real-runtime database role evidence did not pass"
    }
    $runtime = $roles.runtime
    $worker = $roles.global_worker
    if (
        $null -eq $runtime -or
        $runtime.expected_role -ne "trpc_runtime" -or
        $runtime.role_snapshot.current_user -ne "trpc_runtime" -or
        $runtime.global_function_probe.expected_access -ne "denied" -or
        $runtime.global_function_probe.observed_access -ne "denied" -or
        $runtime.global_function_probe.denied -ne $true
    ) {
        throw "ordinary trpc_runtime role evidence did not prove denied global access"
    }
    if (
        $null -eq $worker -or
        $worker.expected_role -ne "trpc_worker" -or
        $worker.role_snapshot.current_user -ne "trpc_worker" -or
        $worker.global_function_probe.expected_access -ne "allowed" -or
        $worker.global_function_probe.observed_access -ne "allowed" -or
        $worker.global_function_probe.denied -ne $false
    ) {
        throw "global trpc_worker role evidence did not prove allowed global access"
    }
}

$releaseNonceHash = Get-Sha256Hex -Value ([string]$env:TRPC_RELEASE_NONCE)
& .venv\Scripts\python.exe scripts/candidate_lock.py verify | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "candidate lock verification failed"
}
$lockedCandidate = Get-Content -LiteralPath "runs/multitenant/candidate-lock.json" -Raw | ConvertFrom-Json
$currentSourceFingerprint = [string]$lockedCandidate.source_fingerprint.value
if ($currentSourceFingerprint -ne $candidateFingerprint) {
    throw "source fingerprint changed after lock"
}
$rawImageInspection = docker image inspect $candidateImage
if ($LASTEXITCODE -ne 0) {
    throw "candidate image binding mismatch"
}
$imageDocuments = @((($rawImageInspection -join [Environment]::NewLine) | ConvertFrom-Json))
if ($imageDocuments.Count -ne 1) {
    throw "candidate image binding mismatch"
}
$imageDocument = $imageDocuments[0]
$imageEvidence = "{0}|{1}" -f $imageDocument.Id, $imageDocument.Config.Labels.'io.trpc.agent-service.source-fingerprint'
if ($imageEvidence -ne "$candidateImageId|$candidateFingerprint") {
    throw "candidate image binding mismatch"
}
if (-not (Test-Path -LiteralPath '.venv\Scripts\python.exe' -PathType Leaf)) {
    throw "repository virtualenv interpreter is missing"
}
foreach ($path in @(
    'docker-compose.yml',
    'deploy/toxiproxy-runtime.override.yml',
    'deploy/acceptance-runtime.override.yml',
    'scripts/contract_gate.py',
    'scripts/performance_fixture.py',
    'scripts/real_runtime_gate.py'
)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "required runtime gate input is missing: $path"
    }
}

$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runSuffix = ([Guid]::NewGuid().ToString('N')).Substring(0, 8)
$project = "trpc-fault-runtime-$runStamp-$runSuffix"
$runtimeDbPort = New-LoopbackPort -Reserved @(15432, 16379, 19000)
$runtimeRedisPort = New-LoopbackPort -Reserved @($runtimeDbPort, 15432, 16379, 19000)
$runtimeMinioPort = New-LoopbackPort -Reserved @($runtimeDbPort, $runtimeRedisPort, 15432, 16379, 19000)
$runtimeMinioConsolePort = New-LoopbackPort -Reserved @($runtimeDbPort, $runtimeRedisPort, $runtimeMinioPort, 15432, 16379, 19000)
$runtimeGatewayPort = New-LoopbackPort -Reserved @($runtimeDbPort, $runtimeRedisPort, $runtimeMinioPort, $runtimeMinioConsolePort, 15432, 16379, 19000)
$runtimeAdminPort = New-LoopbackPort -Reserved @($runtimeDbPort, $runtimeRedisPort, $runtimeMinioPort, $runtimeMinioConsolePort, $runtimeGatewayPort, 15432, 16379, 19000)
$toxiproxyApiPort = New-LoopbackPort -Reserved @($runtimeDbPort, $runtimeRedisPort, $runtimeMinioPort, $runtimeMinioConsolePort, $runtimeGatewayPort, $runtimeAdminPort, 15432, 16379, 19000)

$env:TRPC_RUN_REAL_MULTINODE = "1"
$env:TRPC_SERVICE_IMAGE = $candidateImage
$env:TRPC_SERVICE_ENVIRONMENT = "test"
$env:TRPC_SERVICE_SCHEDULER_VERSION = "v2"
$env:TRPC_SERVICE_REDIS_STREAM = "trpc:session-ready:v2"
$env:TRPC_SERVICE_REDIS_CONSUMER_GROUP = "trpc-session-ready-v2"
$env:TRPC_SERVICE_CAPTURE_CONTENT = "false"
$env:TRPC_SERVICE_WORKER_CONCURRENCY = "1"
$env:TRPC_SERVICE_OFFLINE_AGENT_DELAY_SECONDS = "0.5"
$env:TRPC_RUNTIME_USER = "trpc_runtime"
$env:TRPC_WORKER_USER = "trpc_worker"
$env:TRPC_MIGRATION_USER = "trpc_migration"
$env:POSTGRES_DB = "trpc_service"
$env:POSTGRES_USER = "trpc"
$env:MINIO_ROOT_USER = "trpc-minio"
$env:MINIO_BUCKET = "trpc-artifacts"
$env:POSTGRES_PORT = [string]$runtimeDbPort
$env:REDIS_PORT = [string]$runtimeRedisPort
$env:MINIO_PORT = [string]$runtimeMinioPort
$env:MINIO_CONSOLE_PORT = [string]$runtimeMinioConsolePort
$env:GATEWAY_PORT = [string]$runtimeGatewayPort
$env:ADMIN_PORT = [string]$runtimeAdminPort
$env:TOXIPROXY_API_PORT = [string]$toxiproxyApiPort
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
$env:TRPC_REAL_COMPOSE_PROJECT = $project
$env:TRPC_REAL_TOXIPROXY_API = "http://127.0.0.1:$toxiproxyApiPort"

$directRuntimeDsn = "postgresql://{0}:{1}@127.0.0.1:{2}/{3}" -f $env:TRPC_RUNTIME_USER, $env:RUNTIME_DATABASE_PASSWORD, $runtimeDbPort, $env:POSTGRES_DB
$directWorkerDsn = "postgresql://{0}:{1}@127.0.0.1:{2}/{3}" -f $env:TRPC_WORKER_USER, $env:WORKER_DATABASE_PASSWORD, $runtimeDbPort, $env:POSTGRES_DB
$proxiedRuntimeDsn = "postgresql+asyncpg://{0}:{1}@127.0.0.1:15432/{2}" -f $env:TRPC_RUNTIME_USER, $env:RUNTIME_DATABASE_PASSWORD, $env:POSTGRES_DB
$proxiedRedisUrl = "redis://:{0}@127.0.0.1:16379/0" -f $env:REDIS_PASSWORD
$compose = @(
    "-f", "docker-compose.yml",
    "-f", "deploy/toxiproxy-runtime.override.yml",
    "-f", "deploy/acceptance-runtime.override.yml",
    "-p", $project
)

$backendReport = "runs/multitenant/backend-compose.json"
$realRuntimeReport = "runs/multitenant/real-runtime.json"
$fixtureReport = "runs/multitenant/runtime-fixture-$runStamp-$runSuffix.json"
$fixtureCleanupReport = "runs/multitenant/runtime-fixture-cleanup-$runStamp-$runSuffix.json"
$stackStarted = $false
$runSucceeded = $false
try {
    # Static Compose and ownership checks happen before any container creation.
    docker compose @compose config --quiet
    if ($LASTEXITCODE -ne 0) {
        throw "runtime Compose configuration is invalid"
    }
    $existing = docker compose @compose ps -aq
    if ($LASTEXITCODE -ne 0) {
        throw "runtime Compose ownership check failed"
    }
    if (@($existing | Where-Object { $_.Trim() }).Count -ne 0) {
        throw "refusing to reuse a Compose project with existing containers: $project"
    }

    $stackStarted = $true
    # Keep application consumers stopped while the backend suite exercises
    # PostgreSQL outbox/lease transitions directly.  A live dispatcher or
    # worker would race those tests even though their Redis streams are unique.
    docker compose @compose up -d --no-build postgres redis minio minio-init migrate toxiproxy
    if ($LASTEXITCODE -ne 0) {
        throw "runtime Compose startup failed; project retained"
    }
    $null = Wait-HealthyService -ComposeArgs $compose -Project $project -Service "toxiproxy"
    Wait-SuccessfulOneShot -ComposeArgs $compose -Project $project -Service "migrate"
    Wait-SuccessfulOneShot -ComposeArgs $compose -Project $project -Service "minio-init"

    # Backend contract tests use direct host ports and both database roles.
    $env:TRPC_TEST_POSTGRES_DSN = $directRuntimeDsn
    $env:TRPC_TEST_POSTGRES_WORKER_DSN = $directWorkerDsn
    $env:TRPC_TEST_REDIS_URL = "redis://:$env:REDIS_PASSWORD@127.0.0.1`:$runtimeRedisPort/0"
    $env:TRPC_TEST_S3_ENDPOINT = "http://127.0.0.1:$runtimeMinioPort"
    $env:TRPC_TEST_S3_ACCESS_KEY = $env:MINIO_ROOT_USER
    $env:TRPC_TEST_S3_SECRET_KEY = $env:MINIO_ROOT_PASSWORD
    $env:TRPC_TEST_S3_BUCKET = $env:MINIO_BUCKET
    $env:TRPC_TEST_IMAGE_DIGEST = $candidateImageId

    # The backend contract intentionally includes the isolated live migration
    # tests.  Provision fresh migration-acceptance scopes in this owned stack
    # so the suite executes all cases instead of reporting skips.
    $migrationScope = "migration-acceptance-$runStamp-$runSuffix"
    $env:TRPC_MIGRATION_BACKEND_CONTRACT = "1"
    $env:TRPC_RUN_REAL_MIGRATION = "1"
    $env:TRPC_MIGRATION_FULL_ACCEPTANCE = "1"
    $env:TRPC_MIGRATION_BOOTSTRAP = "1"
    $env:TRPC_MIGRATION_SOURCE_REDIS_URL = $env:TRPC_TEST_REDIS_URL
    $env:TRPC_MIGRATION_TARGET_DATABASE_DSN = $directRuntimeDsn
    $env:TRPC_MIGRATION_TENANT_ID = "$migrationScope-tenant"
    $env:TRPC_MIGRATION_ID = "$migrationScope-migration"
    $env:TRPC_MIGRATION_APP_ID = "$migrationScope-app"
    $env:TRPC_MIGRATION_APP_REVISION = "1"
    $env:TRPC_MIGRATION_CONFIG_VERSION = "1"
    $env:TRPC_MIGRATION_BINDING_ID = "$migrationScope-binding"
    $env:TRPC_MIGRATION_BINDING_REVISION = "1"
    $env:TRPC_MIGRATION_PHASE_TENANT_ID = "$migrationScope-phase-tenant"
    $env:TRPC_MIGRATION_PHASE_ID = "$migrationScope-phase-migration"
    $env:TRPC_MIGRATION_PHASE_APP_ID = "$migrationScope-phase-app"
    $env:TRPC_MIGRATION_EXPECTED_RECORDS = "200"
    $env:TRPC_MIGRATION_CONTROL_FACTORY = "trpc_service.storage.production_migration_control:create"
    $env:TRPC_MIGRATION_IMAGE_DIGEST = $candidateImageId
    $bootstrapOutput = & ".venv\Scripts\python.exe" "scripts/migration_acceptance_bootstrap.py"
    if ($LASTEXITCODE -ne 0) {
        throw "backend migration acceptance bootstrap failed; project retained"
    }
    $bootstrap = $bootstrapOutput | ConvertFrom-Json
    if (
        $bootstrap.status -ne "pass" -or
        [int]$bootstrap.expected_records_per_scope -ne 200 -or
        [int]$bootstrap.scopes.base.seeded_records -ne 200 -or
        [int]$bootstrap.scopes.phase.seeded_records -ne 200
    ) {
        throw "backend migration acceptance bootstrap contract failed; project retained"
    }
    & ".venv\Scripts\python.exe" "scripts/contract_gate.py" backend --output $backendReport
    if ($LASTEXITCODE -ne 0) {
        throw "backend contract gate failed; project retained"
    }
    $backend = Get-Content -LiteralPath $backendReport -Raw | ConvertFrom-Json
    Assert-ReleaseReport -Report $backend -Label "backend-compose" -ExpectedImage $candidateImageId -ExpectedFingerprint $candidateFingerprint -ExpectedReleaseId $env:TRPC_RELEASE_ID -ExpectedNonceHash $releaseNonceHash
    if (
        $backend.candidate.runtime_attestation.status -ne "pass" -or
        $backend.candidate.runtime_attestation.image_digest -ne $candidateImageId -or
        $backend.candidate.lineage.status -ne "pass" -or
        $backend.candidate.lineage.image_digest -ne $candidateImageId -or
        [int]$backend.candidate.runtime_attestation.junit_counts.skipped -ne 0
    ) {
        throw "backend-compose runtime/image/test attestation failed"
    }

    docker compose @compose up -d --no-build --scale worker=4 --scale outbox-dispatcher=1 gateway worker outbox-dispatcher channel-dispatcher post-turn-projector session-recovery
    if ($LASTEXITCODE -ne 0) {
        throw "runtime application startup failed; project retained"
    }
    $null = Wait-HealthyWorkerInventory -ComposeArgs $compose -Project $project -ExpectedSource $candidateFingerprint -ExpectedImage $candidateImageId

    # Create the owned synthetic tenant/binding required by real-runtime.py.
    $env:TRPC_PERF_DATABASE_DSN = $directRuntimeDsn
    $env:TRPC_PERF_FIXTURE_CONFIRM = "I_UNDERSTAND_PERFORMANCE_FIXTURE"
    & ".venv\Scripts\python.exe" "scripts/performance_fixture.py" create --execute --output $fixtureReport
    if ($LASTEXITCODE -ne 0) {
        throw "runtime fixture creation failed; project retained"
    }
    $fixture = Get-Content -LiteralPath $fixtureReport -Raw | ConvertFrom-Json
    if ($fixture.gate -ne "pass" -or $fixture.production_gate -ne "not_run" -or $fixture.synthetic -ne $true) {
        throw "runtime fixture contract failed; project retained"
    }

    $env:TRPC_REAL_DATABASE_DSN = $proxiedRuntimeDsn
    $env:TRPC_REAL_REDIS_URL = $proxiedRedisUrl
    $env:TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN = "postgresql+asyncpg://{0}:{1}@127.0.0.1:{2}/{3}" -f $env:TRPC_WORKER_USER, $env:WORKER_DATABASE_PASSWORD, $runtimeDbPort, $env:POSTGRES_DB
    $env:TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE = $env:TRPC_WORKER_USER
    $env:TRPC_REAL_RUNTIME_DATABASE_ROLE = $env:TRPC_RUNTIME_USER
    $env:TRPC_REAL_SESSION_HMAC_KEY = $env:SESSION_HMAC_KEY
    $env:TRPC_REAL_TENANT_ID = [string]$fixture.tenant_id
    $env:TRPC_REAL_BINDING_ID = [string]$fixture.binding_id
    $env:TRPC_REAL_RUN_ID = [string]$fixture.run_id
    $env:TRPC_REAL_IMAGE_DIGEST = $candidateImageId
    & ".venv\Scripts\python.exe" "scripts/real_runtime_gate.py" --execute --phase all --project $project --compose-prestarted --workers 4 --messages 200 --duplicates 20 --fault-messages 8 --kill-worker --allow-process-kill --use-toxiproxy --republish-probe --toxiproxy-api $env:TRPC_REAL_TOXIPROXY_API --output $realRuntimeReport --require-production
    if ($LASTEXITCODE -ne 0) {
        throw "real-runtime gate failed; project retained"
    }
    $realRuntime = Get-Content -LiteralPath $realRuntimeReport -Raw | ConvertFrom-Json
    Assert-ReleaseReport -Report $realRuntime -Label "real-runtime" -ExpectedImage $candidateImageId -ExpectedFingerprint $candidateFingerprint -ExpectedReleaseId $env:TRPC_RELEASE_ID -ExpectedNonceHash $releaseNonceHash
    if (
        $realRuntime.candidate.preflight.status -ne "pass" -or
        $realRuntime.candidate.preflight.image_attestation.status -ne "pass" -or
        $realRuntime.candidate.preflight.image_attestation.image_id -ne $candidateImageId -or
        $realRuntime.candidate.preflight.image_attestation.source_fingerprint -ne $candidateFingerprint -or
        [int]$realRuntime.candidate.preflight.image_attestation.worker_count -ne 4
    ) {
        throw "real-runtime worker source/image attestation failed"
    }
    Assert-RoleEvidence -Report $realRuntime

    # Cleanup the exact synthetic tenant before removing the Compose project.
    $env:TRPC_PERF_DATABASE_DSN = $directRuntimeDsn
    & ".venv\Scripts\python.exe" "scripts/performance_fixture.py" cleanup --execute --report $fixtureReport --tenant-id $fixture.tenant_id --run-id $fixture.run_id --output $fixtureCleanupReport
    if ($LASTEXITCODE -ne 0) {
        throw "runtime fixture cleanup failed; project retained"
    }
    $fixtureCleanup = Get-Content -LiteralPath $fixtureCleanupReport -Raw | ConvertFrom-Json
    if ($fixtureCleanup.gate -ne "pass" -or $fixtureCleanup.production_gate -ne "not_run") {
        throw "runtime fixture cleanup contract failed; project retained"
    }
    $runSucceeded = $true
}
finally {
    if ($runSucceeded -and $stackStarted) {
        docker compose @compose down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) {
            throw "runtime Compose cleanup failed; project retained"
        }
    }
    elseif ($stackStarted) {
        Write-Output "RUNTIME_GATE_PROJECT_RETAINED project=$project"
    }
}

Write-Output ("RUNTIME_GATE_RESULT backend_gate={0}|backend_production={1}|gate={2}|production={3}|report={4}|project={5}|release={6}" -f $backend.gate, $backend.production_gate, $realRuntime.gate, $realRuntime.production_gate, $realRuntimeReport, $project, $env:TRPC_RELEASE_ID)
