$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"

& ".venv\Scripts\python.exe" -m ruff check --fix .
& ".venv\Scripts\python.exe" -m ruff format .

