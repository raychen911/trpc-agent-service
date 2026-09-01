$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
py -3.12 scripts\web_ui_launcher.py @args
