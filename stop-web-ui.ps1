$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$pidPath = Join-Path $PSScriptRoot ".run\web-ui.pid"
if (-not (Test-Path -LiteralPath $pidPath)) {
    Write-Output "Web UI is not running."
    exit 0
}

$webPid = Get-Content -LiteralPath $pidPath -Raw
try {
    taskkill.exe /PID ([int]$webPid.Trim()) /T /F | Out-Null
} finally {
    Remove-Item -LiteralPath $pidPath -Force -ErrorAction SilentlyContinue
}
Write-Output "Web UI stopped."
