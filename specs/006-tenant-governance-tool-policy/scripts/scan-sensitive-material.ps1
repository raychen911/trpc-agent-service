$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$patterns = @('LARK_APP_SECRET\s*=\s*["''](?!<)[^"'']{8,}', 'WECOM_BOT_SECRET\s*=\s*["''](?!<)[^"'']{8,}', 'response_code=[A-Za-z0-9_-]{16,}', 'api[_-]?key\s*[:=]\s*(?!<)[^\s,;]{8,}')
$files = Get-ChildItem -LiteralPath $root -Recurse -File -ErrorAction SilentlyContinue | Where-Object { $_.FullName -notmatch '\\.git\\|\\.venv\\|__pycache__|\\.pytest_cache\\' }
$hits = @()
foreach ($file in $files) {
  $text = Get-Content -LiteralPath $file.FullName -Raw -ErrorAction SilentlyContinue
  foreach ($pattern in $patterns) { if ($text -match $pattern) { $hits += $file.FullName; break } }
}
if ($hits.Count -gt 0) { $hits | Sort-Object -Unique; exit 1 }
Write-Output 'sensitive_scan_hits=0'
