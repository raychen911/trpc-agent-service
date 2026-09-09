$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$uv = Get-Command uv -ErrorAction SilentlyContinue
if ($uv) {
    uv run python scripts\web_ui_launcher.py @args
    exit $LASTEXITCODE
}

$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (Test-Path -LiteralPath $venvPython) {
    & $venvPython scripts\web_ui_launcher.py @args
    exit $LASTEXITCODE
}

# Fallback to global Python (matching Linux start.sh behavior)
$globalPython = Get-Command python -ErrorAction SilentlyContinue
if ($globalPython) {
    Write-Warning "Using global Python. Consider running: uv sync --locked --extra dev --python 3.12"
    python scripts\web_ui_launcher.py @args
    exit $LASTEXITCODE
}

Write-Error "uv, virtual environment, or Python was not found. Install Python or run: uv sync --locked --extra dev --python 3.12"
