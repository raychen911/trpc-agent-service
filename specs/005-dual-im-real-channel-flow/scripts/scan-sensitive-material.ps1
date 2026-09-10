[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$patterns = @(
    'AKIA[0-9A-Z]{16}',
    '-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
    '(?i)Authorization\s*:\s*Bearer\s+[A-Za-z0-9._~+/=-]{20,}',
    '(?i)(?:LARK_APP_SECRET|WECOM_BOT_SECRET|TRPC_DEMO_(?:ALPHA|BETA)_SECRET)\s*[:=]\s*["''][^<\s][^"'']{15,}["'']',
    '(?i)(?:postgres(?:ql)?|redis)://[^:\s/@]+:[^@\s]+@',
    '(?i)wss://[^\s<>"'']+(?:token|ticket|secret|access_key)='
)

function Test-SensitiveText {
    param([string]$Text)
    foreach ($pattern in $patterns) {
        if ([regex]::IsMatch($Text, $pattern)) {
            return $true
        }
    }
    return $false
}

Push-Location $repoRoot
try {
    $workspaceHits = @()
    $files = Get-ChildItem -Path trpc_service, tests, specs, deploy -Recurse -File -ErrorAction SilentlyContinue |
        Where-Object {
            $_.FullName -notmatch '\\(\.git|\.venv|\.uv-cache|\.pytest_cache)\\' -and
            $_.Name -ne "uv.lock"
        }
    foreach ($file in $files) {
        $text = Get-Content $file.FullName -Raw -ErrorAction SilentlyContinue
        if ($null -ne $text -and (Test-SensitiveText $text)) {
            $workspaceHits += $file.FullName
        }
    }

    $diffLines = git diff --unified=0 -- . ':(exclude)uv.lock'
    $diffAdded = ($diffLines | Where-Object {
        $_.StartsWith("+") -and -not $_.StartsWith("+++")
    }) -join "`n"
    $history = (git log -p --all -- . ':(exclude)uv.lock' | Out-String)

    $result = [ordered]@{
        workspace_sensitive_files = @($workspaceHits | Sort-Object -Unique).Count
        diff_added_sensitive_matches = if (Test-SensitiveText $diffAdded) { 1 } else { 0 }
        history_sensitive_matches = if (Test-SensitiveText $history) { 1 } else { 0 }
    }
    $result | ConvertTo-Json -Compress

    if (
        $result.workspace_sensitive_files -ne 0 -or
        $result.diff_added_sensitive_matches -ne 0 -or
        $result.history_sensitive_matches -ne 0
    ) {
        $workspaceHits | Sort-Object -Unique | ForEach-Object {
            Write-Error ("Sensitive pattern detected in file: " + $_)
        }
        exit 1
    }
}
finally {
    Pop-Location
}
