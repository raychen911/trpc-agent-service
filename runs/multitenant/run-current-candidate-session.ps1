param(
    [string]$Repository = $env:TRPC_CANDIDATE_REPOSITORY
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($Repository)) {
    throw "candidate repository is required; pass -Repository or set TRPC_CANDIDATE_REPOSITORY"
}

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
Set-Location $projectRoot
$python = Join-Path $projectRoot ".venv/Scripts/python.exe"

$sessionJson = & $python -m scripts.candidate_session publish `
    --repository $Repository `
    --output "runs/multitenant/registry-image-binding.json" `
    --lock-output "runs/multitenant/candidate-lock.json" `
    --private-directory "runs/multitenant/.ack-runtime-private" `
    --public-directory "runs/multitenant"
if ($LASTEXITCODE -ne 0) {
    throw "candidate publication session failed"
}
$session = $sessionJson | ConvertFrom-Json
$releaseContext = Get-Content -LiteralPath ([string]$session.private_context) -Raw | ConvertFrom-Json
$env:TRPC_RELEASE_ID = [string]$releaseContext.release_id
$env:TRPC_RELEASE_NONCE = [string]$releaseContext.nonce

& $python scripts/candidate_lock.py verify `
    --binding "runs/multitenant/registry-image-binding.json" `
    --lock "runs/multitenant/candidate-lock.json" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "published candidate lock verification failed"
}

Write-Output "CANDIDATE_READY release=$([string]$session.release_id) source=$([string]$session.source_fingerprint) lock=$([string]$session.lock) context=$([string]$session.private_context)"
