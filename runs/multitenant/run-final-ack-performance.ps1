param(
    [string]$KubeconfigPath = $env:TRPC_ACK_KUBECONFIG,
    [string]$KubeContext = $env:TRPC_ACK_CONTEXT
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Get-OptionalPropertyValue {
    param(
        [AllowNull()]
        [object]$Value,
        [Parameter(Mandatory = $true)]
        [string[]]$Path
    )

    $current = $Value
    foreach ($segment in $Path) {
        if ($null -eq $current) {
            return $null
        }
        $property = $current.PSObject.Properties[$segment]
        if ($null -eq $property) {
            return $null
        }
        $current = $property.Value
    }
    return $current
}

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
$kubeconfig = if ([string]::IsNullOrWhiteSpace($KubeconfigPath)) {
    throw "ACK kubeconfig is required; pass -KubeconfigPath or set TRPC_ACK_KUBECONFIG"
} else {
    (Resolve-Path -LiteralPath $KubeconfigPath -ErrorAction Stop).Path
}
if ([string]::IsNullOrWhiteSpace($KubeContext)) {
    throw "ACK context is required; pass -KubeContext or set TRPC_ACK_CONTEXT"
}
$context = $KubeContext
Set-Location $projectRoot

$python = Join-Path $projectRoot ".venv/Scripts/python.exe"
$kubectl = (Get-Command kubectl -ErrorAction Stop).Source
$namespace = "trpc-service"
$supportNamespace = "trpc-runtime-support"
$configPath = Join-Path $projectRoot "deploy/runtime-gate.yaml"
$bindingPath = Join-Path $projectRoot "runs/multitenant/registry-image-binding.json"
$lockPath = Join-Path $projectRoot "runs/multitenant/candidate-lock.json"
$secretManifest = Join-Path $projectRoot "runs/multitenant/.ack-runtime-private/runtime-secrets-with-pull.yaml"
$reportPath = Join-Path $projectRoot "runs/multitenant/real-performance.json"
$fixturePath = Join-Path $projectRoot "runs/multitenant/performance-fixture-ack.json"
$cleanupPath = Join-Path $projectRoot "runs/multitenant/performance-fixture-cleanup-ack.json"

function Invoke-Kubectl {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    & $kubectl --kubeconfig $kubeconfig --context $context @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl failed: $($Arguments -join ' ')"
    }
}

function New-RandomUrlToken {
    $bytes = [byte[]]::new(32)
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }
    return [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "project Python environment is unavailable"
}
if (-not (Test-Path -LiteralPath $secretManifest -PathType Leaf)) {
    throw "ACK runtime Secret manifest is unavailable"
}

$candidateLock = Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json
$candidateBindingSha256 = [string]$candidateLock.binding_sha256
$expectedSource = [string]$candidateLock.source_fingerprint.value
$imageDigest = [string]$candidateLock.images.initial.digest
$upgradeDigest = [string]$candidateLock.images.upgrade.digest
$lockedInitialReference = [string]$candidateLock.images.initial.reference
$releaseId = [string]$candidateLock.release_binding.release_id
if (
    $candidateBindingSha256 -notmatch '^[0-9a-f]{64}$' -or
    $expectedSource -notmatch '^[0-9a-f]{64}$' -or
    $imageDigest -notmatch '^sha256:[0-9a-f]{64}$' -or
    $upgradeDigest -notmatch '^sha256:[0-9a-f]{64}$' -or
    $lockedInitialReference -notmatch '@sha256:[0-9a-f]{64}$' -or
    -not $lockedInitialReference.EndsWith("@$imageDigest") -or
    [string]::IsNullOrWhiteSpace($releaseId)
) {
    throw "candidate lock identity is incomplete"
}
$candidateName = $expectedSource.Substring(0, 12)
$renderedSupport = Join-Path $projectRoot "runs/multitenant/rendered/support-$candidateName"
$renderedPerformance = Join-Path $projectRoot "runs/multitenant/rendered/performance-$candidateName"
$privateReleaseContext = Join-Path $projectRoot "runs/multitenant/.ack-runtime-private/release-context-$releaseId-amd64.json"
$publicReleaseContext = Join-Path $projectRoot "runs/multitenant/release-context-binding-$releaseId-amd64.json"

& $python scripts/release_context.py verify `
    --private-context $privateReleaseContext `
    --release-id $releaseId `
    --source-fingerprint $expectedSource `
    --initial-digest $imageDigest `
    --upgrade-digest $upgradeDigest | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "private release context could not be verified"
}
$releaseContext = Get-Content -LiteralPath $privateReleaseContext -Raw | ConvertFrom-Json
if (
    $releaseContext.release_id -ne $releaseId -or
    $releaseContext.source_fingerprint -ne $expectedSource -or
    $releaseContext.images.initial -ne $imageDigest -or
    $releaseContext.images.upgrade -ne $upgradeDigest -or
    [string]::IsNullOrWhiteSpace([string]$releaseContext.nonce)
) {
    throw "private release context does not match the current candidate"
}

$env:TRPC_RELEASE_ID = $releaseId
$env:TRPC_RELEASE_NONCE = [string]$releaseContext.nonce
& $python scripts/candidate_lock.py verify --binding $bindingPath --lock $lockPath | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "candidate lock does not match the checkout, registry binding, or verified release context"
}
$verifiedCandidateLock = Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json
if ([string]$verifiedCandidateLock.binding_sha256 -ne $candidateBindingSha256) {
    throw "candidate lock changed while its release context was being verified"
}
& $python scripts/release_context.py verify `
    --private-context $privateReleaseContext `
    --public-output $publicReleaseContext `
    --release-id $releaseId `
    --source-fingerprint $expectedSource `
    --initial-digest $imageDigest `
    --upgrade-digest $upgradeDigest | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "verified release context could not be published"
}

& $python scripts/render_runtime_support.py `
    --config $configPath `
    --output-dir $renderedSupport `
    --performance-output-dir $renderedPerformance
if ($LASTEXITCODE -ne 0) {
    throw "runtime manifest render failed"
}

$rendered = (& $kubectl kustomize $renderedPerformance)
if ($LASTEXITCODE -ne 0) {
    throw "rendered performance kustomization is invalid"
}
$renderedText = $rendered -join "`n"
if ($renderedText -notmatch [regex]::Escape($lockedInitialReference)) {
    throw "rendered performance workload is not pinned to the canonical Docker Hub image"
}
$migrationManifest = Join-Path $renderedPerformance "stage1-migration.yaml"
$headCheckManifest = Join-Path $renderedPerformance "stage1-schema-head-check.yaml"
$runtimeManifest = Join-Path $renderedPerformance "stage1-runtime.yaml"
$renderedText | & $python -m scripts.split_kubernetes_runtime_manifest `
    --namespace $namespace `
    --migration-output $migrationManifest `
    --head-check-output $headCheckManifest `
    --runtime-output $runtimeManifest
if ($LASTEXITCODE -ne 0) {
    throw "rendered performance manifest split failed"
}

Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $renderedPerformance "namespace.yaml"))

$env:TRPC_PERF_FIXTURE_UNUSED_APP_SECRET = New-RandomUrlToken
$env:TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN = New-RandomUrlToken
$env:TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY = New-RandomUrlToken
$runtimeSecretsYaml = & $python runs/multitenant/project_kubernetes_secrets.py `
    --manifest $secretManifest --namespace $namespace --profile runtime
if ($LASTEXITCODE -ne 0) {
    throw "runtime Secret projection failed"
}
$runtimeSecretsYaml | & $kubectl --kubeconfig $kubeconfig --context $context apply -f -
if ($LASTEXITCODE -ne 0) {
    throw "runtime Secret apply failed"
}

& $kubectl --kubeconfig $kubeconfig --context $context create namespace $supportNamespace `
    --dry-run=client -o yaml |
    & $kubectl --kubeconfig $kubeconfig --context $context apply -f -
if ($LASTEXITCODE -ne 0) {
    throw "runtime support namespace apply failed"
}

$supportSecretsYaml = & $python runs/multitenant/project_kubernetes_secrets.py `
    --manifest $secretManifest --namespace $supportNamespace --profile support
if ($LASTEXITCODE -ne 0) {
    throw "runtime support Secret projection failed"
}
$supportSecretsYaml | & $kubectl --kubeconfig $kubeconfig --context $context apply -f -
if ($LASTEXITCODE -ne 0) {
    throw "runtime support Secret apply failed"
}

Invoke-Kubectl -Arguments @("--namespace", $supportNamespace, "delete", "job/postgres-bootstrap", "job/minio-bucket-bootstrap", "--ignore-not-found=true", "--wait=true")
Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $renderedSupport "ack-runtime-support.yaml"))
Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $renderedSupport "ack-runtime-minio.yaml"))
Invoke-Kubectl -Arguments @("--namespace", $supportNamespace, "wait", "--for=condition=complete", "job/postgres-bootstrap", "--timeout=600s")
Invoke-Kubectl -Arguments @("--namespace", $supportNamespace, "wait", "--for=condition=complete", "job/minio-bucket-bootstrap", "--timeout=600s")
foreach ($supportDeployment in @("postgres", "redis", "minio", "backlog-metric-source", "prometheus", "prometheus-adapter")) {
    Invoke-Kubectl -Arguments @("--namespace", $supportNamespace, "rollout", "status", "deployment/$supportDeployment", "--timeout=600s")
}

Invoke-Kubectl -Arguments @(
    "--namespace", $namespace, "delete",
    "job/trpc-schema-migration", "job/trpc-schema-head-check",
    "--ignore-not-found=true", "--wait=true"
)
Invoke-Kubectl -Arguments @("apply", "-f", $migrationManifest)
Invoke-Kubectl -Arguments @("--namespace", $namespace, "wait", "--for=condition=complete", "job/trpc-schema-migration", "--timeout=600s")
Invoke-Kubectl -Arguments @("apply", "-f", $headCheckManifest)
Invoke-Kubectl -Arguments @("--namespace", $namespace, "wait", "--for=condition=complete", "job/trpc-schema-head-check", "--timeout=600s")
Invoke-Kubectl -Arguments @("apply", "-f", $runtimeManifest)

# Remove one-time literal fixture variables used by the previous bootstrap.
# Placement is already rendered exclusively from runtime-gate.yaml.
Invoke-Kubectl -Arguments @(
    "--namespace", $namespace, "set", "env", "deployment/trpc-gateway",
    "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET-",
    "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN-",
    "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY-"
)

foreach ($deployment in @("trpc-gateway", "trpc-worker", "trpc-outbox-dispatcher", "trpc-session-recovery", "trpc-backlog-exporter")) {
    Invoke-Kubectl -Arguments @("--namespace", $namespace, "rollout", "status", "deployment/$deployment", "--timeout=600s")
}

$externalMetricReady = $false
for ($attempt = 1; $attempt -le 60; $attempt++) {
    $externalRaw = & $kubectl --kubeconfig $kubeconfig --context $context get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/$namespace/trpc_session_ready_backlog" 2>$null
    if ($LASTEXITCODE -eq 0) {
        try {
            $external = $externalRaw | ConvertFrom-Json
            $items = @($external.items)
            $hpa = & $kubectl --kubeconfig $kubeconfig --context $context --namespace $namespace get hpa trpc-worker -o json | ConvertFrom-Json
            $scalingActive = @($hpa.status.conditions | Where-Object { $_.type -eq "ScalingActive" -and $_.status -eq "True" }).Count -eq 1
            if (
                $external.kind -eq "ExternalMetricValueList" -and
                $items.Count -eq 1 -and
                $items[0].metricName -eq "trpc_session_ready_backlog" -and
                $items[0].metricLabels.namespace -eq $namespace -and
                $null -ne ($items[0].value -as [double]) -and
                $scalingActive
            ) {
                $externalMetricReady = $true
                break
            }
        } catch {
            # Adapter discovery and HPA reconciliation are eventually consistent.
        }
    }
    Start-Sleep -Seconds 5
}
if (-not $externalMetricReady) {
    throw "external backlog metric did not become namespace-bound and ScalingActive"
}

$metricsReady = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    $metricsRaw = & $kubectl --kubeconfig $kubeconfig --context $context get --raw "/apis/metrics.k8s.io/v1beta1/namespaces/$namespace/pods" 2>$null
    if ($LASTEXITCODE -eq 0) {
        try {
            $metrics = $metricsRaw | ConvertFrom-Json
            $workerMetrics = @($metrics.items | Where-Object { $_.metadata.name -like "trpc-worker-*" }).Count
            $outboxMetrics = @($metrics.items | Where-Object { $_.metadata.name -like "trpc-outbox-dispatcher-*" }).Count
            if ($workerMetrics -eq 4 -and $outboxMetrics -ge 1) {
                $metricsReady = $true
                break
            }
        } catch {
            # Metrics may be momentarily incomplete while new Pods are first scraped.
        }
    }
    Start-Sleep -Seconds 5
}
if (-not $metricsReady) {
    throw "metrics-server did not publish complete worker/outbox evidence"
}

Invoke-Kubectl -Arguments @("--namespace", $namespace, "get", "pods", "-o", "wide")

& $python scripts/ack_performance_acceptance.py `
    --config $configPath `
    --output $reportPath `
    --fixture-output $fixturePath `
    --cleanup-output $cleanupPath
$gateExit = $LASTEXITCODE

if (Test-Path -LiteralPath $reportPath -PathType Leaf) {
    $performanceReport = Get-Content -LiteralPath $reportPath -Raw | ConvertFrom-Json
    [ordered]@{
        gate = $performanceReport.gate
        production_gate = $performanceReport.production_gate
        rejection_reasons = @($performanceReport.production_rejection_reasons)
        burst_p95_ms = Get-OptionalPropertyValue -Value $performanceReport -Path @("candidate", "burst", "ack_p95_ms")
        sustained_p95_ms = Get-OptionalPropertyValue -Value $performanceReport -Path @("candidate", "sustained", "ack_p95_ms")
        max_parallel = Get-OptionalPropertyValue -Value $performanceReport -Path @("candidate", "burst", "max_turn_overlap_observed")
    } | ConvertTo-Json -Compress
}
if ($gateExit -ne 0) {
    throw "formal ACK Performance gate failed; no automatic rerun was attempted"
}
