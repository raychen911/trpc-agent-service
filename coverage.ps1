$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"

& ".venv\Scripts\python.exe" -m pytest --cov=trpc_service --cov-report=term-missing

