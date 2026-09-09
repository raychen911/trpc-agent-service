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

Write-Error "uv or the project virtual environment was not found. Run: uv sync --locked --extra dev --python 3.12"
