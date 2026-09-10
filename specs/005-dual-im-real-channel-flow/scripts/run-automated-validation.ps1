[CmdletBinding()]
param(
    [string]$PythonPath = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
if (-not $PythonPath) {
    $PythonPath = Join-Path $repoRoot ".venv\Scripts\python.exe"
}
if (-not (Test-Path $PythonPath)) {
    throw "The project virtual-environment Python executable is unavailable."
}

$gates = @(
    @(
        "Phase 6 fault recovery",
        "-m", "pytest", "-q", "-p", "no:cacheprovider",
        "tests/unit/channels/test_delivery_retry_policy.py",
        "tests/contract/channels/test_delivery_repository.py",
        "tests/contract/channels/test_adapter_ownership.py",
        "tests/integration/channels/test_adapter_takeover.py",
        "tests/unit/channels/test_connection_lifecycle.py",
        "tests/unit/channels/test_adapter_readiness_payload.py",
        "tests/integration/channels/test_channel_backend_outages.py"
    ),
    @(
        "Phase 7 trace and audit",
        "-m", "pytest", "-q", "-p", "no:cacheprovider",
        "tests/integration/channels/test_im_trace_propagation.py",
        "tests/contract/channels/test_im_audit_query.py",
        "tests/unit/channels/test_channel_metrics.py",
        "tests/unit/channels/test_channel_redaction.py",
        "tests/contract/channels/test_trace_diagnostic_cli.py"
    ),
    @(
        "Full offline regression",
        "-m", "pytest", "-q", "-p", "no:cacheprovider"
    )
)

Push-Location $repoRoot
try {
    foreach ($gate in $gates) {
        $name = $gate[0]
        $arguments = $gate[1..($gate.Length - 1)]
        Write-Host "Running: $name"
        & $PythonPath @arguments
        if ($LASTEXITCODE -ne 0) {
            throw "Validation gate failed: $name"
        }
    }
}
finally {
    Pop-Location
}
