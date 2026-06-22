$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot

if (-not $env:QMT_USERDATA_PATH) {
    $qmtDirName = "$([char]0x56FD)$([char]0x91D1)QMT$([char]0x4EA4)$([char]0x6613)$([char]0x7AEF)$([char]0x6A21)$([char]0x62DF)"
    $env:QMT_USERDATA_PATH = "D:\$qmtDirName\userdata_mini"
}
if (-not $env:QMT_BRIDGE_TOKEN) {
    $env:QMT_BRIDGE_TOKEN = "your-bridge-token"
}
if (-not $env:QMT_BRIDGE_HOST) {
    $env:QMT_BRIDGE_HOST = "0.0.0.0"
}
if (-not $env:QMT_BRIDGE_PORT) {
    $env:QMT_BRIDGE_PORT = "8710"
}
if (-not $env:QMT_BRIDGE_ROLE) {
    $env:QMT_BRIDGE_ROLE = "paper"
}
if (-not $env:QMT_BRIDGE_ALLOW_TRADING) {
    $env:QMT_BRIDGE_ALLOW_TRADING = "1"
}
if (-not $env:QMT_BRIDGE_ACCOUNT_KEY) {
    $env:QMT_BRIDGE_ACCOUNT_KEY = "paper_sim"
}

Write-Host "=========================================="
Write-Host "QMT Bridge Server starting..."
Write-Host "Project Dir: $PWD"
Write-Host "QMT_USERDATA_PATH=$env:QMT_USERDATA_PATH"
Write-Host "QMT_BRIDGE_HOST=$env:QMT_BRIDGE_HOST"
Write-Host "QMT_BRIDGE_PORT=$env:QMT_BRIDGE_PORT"
Write-Host "QMT_BRIDGE_ROLE=$env:QMT_BRIDGE_ROLE"
Write-Host "QMT_BRIDGE_ALLOW_TRADING=$env:QMT_BRIDGE_ALLOW_TRADING"
Write-Host "QMT_BRIDGE_ACCOUNT_KEY=$env:QMT_BRIDGE_ACCOUNT_KEY"
Write-Host "=========================================="

if (-not (Test-Path -Path $env:QMT_USERDATA_PATH)) {
    Write-Host "[ERROR] QMT userdata path not found: $env:QMT_USERDATA_PATH" -ForegroundColor Red
    Write-Host "Please check QMT_USERDATA_PATH in start_qmt_bridge.ps1"
    Read-Host "Press Enter to exit"
    exit 1
}

$pythonCommand = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCommand) {
    $pythonCommand = Get-Command py -ErrorAction SilentlyContinue
}
if (-not $pythonCommand) {
    Write-Host "[ERROR] Python not found. Please install Python or add it to PATH." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

try {
    if ($pythonCommand.Name -eq "py.exe" -or $pythonCommand.Name -eq "py") {
        py -3 scripts\qmt_bridge_server.py
    } else {
        python scripts\qmt_bridge_server.py
    }
} catch {
    Write-Host "[ERROR] QMT Bridge failed to start." -ForegroundColor Red
    Write-Host $_
    Write-Host "Checklist:"
    Write-Host "1. QMT mini client is running and logged in."
    Write-Host "2. xtquant is installed in this Python environment."
    Write-Host "3. fastapi and uvicorn are installed."
    Write-Host "4. Windows Firewall allows inbound port $env:QMT_BRIDGE_PORT."
    Read-Host "Press Enter to exit"
    exit 1
}
