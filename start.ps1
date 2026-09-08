$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"

$python = if (Test-Path -LiteralPath ".venv\Scripts\python.exe") {
    ".venv\Scripts\python.exe"
} else {
    "python"
}

& $python -m trpc_service serve @args

