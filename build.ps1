$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"

if (-not (Test-Path -LiteralPath ".venv\Scripts\python.exe")) {
    python -m venv .venv
}

& ".venv\Scripts\python.exe" -m pip install -e ".[dev]"

