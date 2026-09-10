$ErrorActionPreference = 'Stop'
$serviceRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
Set-Location $serviceRoot
$python = Join-Path $serviceRoot '.venv\Scripts\python.exe'
& $python -m pytest -q tests/unit/governance tests/contract/governance tests/integration/governance tests/sdk_validation tests/e2e/governance -p no:cacheprovider
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $python -m pytest -q -p no:cacheprovider
exit $LASTEXITCODE
