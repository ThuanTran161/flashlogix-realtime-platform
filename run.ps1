<#
.SYNOPSIS
    FlashLogix All-in-One Launcher & Control Script.
.USAGE
    .\run.ps1             # Khoi chay TOAN BO he thong (Docker + DB/Redis + FastAPI + AuditConsumer + UI)
    .\run.ps1 start       # Nhu tren (them -NoBrowser neu khong muon tu mo trinh duyet)
    .\run.ps1 stop        # Tat API, AuditConsumer, Streamlit (giu nguyen Docker & du lieu)
    .\run.ps1 down        # Tat app + docker compose down (chi stack FlashLogix)
    .\run.ps1 status      # Xem trang thai Docker + cac service FlashLogix
    .\run.ps1 test        # Chay Ruff lint + toan bo tests Pytest
    .\run.ps1 bench       # Chay Benchmark E2E (Locust 300 VUs + Kafka + Spark + Gold ETL)
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "stop", "down", "status", "test", "bench", "help")]
    [string]$Action = "start",
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$RootDir = $PSScriptRoot
$PythonExe = Join-Path $RootDir ".venv\Scripts\python.exe"
$RuntimeDir = Join-Path $RootDir ".runtime"
$LogDir = Join-Path $RuntimeDir "logs"
$PidFile = Join-Path $RuntimeDir "pids.json"

function Write-Step([string]$Msg) {
    Write-Host "[FlashLogix] $Msg" -ForegroundColor Cyan
}

function Write-Ok([string]$Msg) {
    Write-Host "[OK] $Msg" -ForegroundColor Green
}

function Write-Warn([string]$Msg) {
    Write-Host "[WARN] $Msg" -ForegroundColor Yellow
}

function Ensure-EnvAndPython {
    if (!(Test-Path $PythonExe)) {
        throw "Khong tim thay Python virtualenv tai '$PythonExe'. Hay tao .venv va cai requirements.txt truoc."
    }
    $envPath = Join-Path $RootDir ".env"
    $envExample = Join-Path $RootDir ".env.example"
    if (!(Test-Path $envPath) -and (Test-Path $envExample)) {
        Copy-Item $envExample $envPath
        Write-Step "Da tao .env tu .env.example"
    }
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

function Stop-TrackedProcesses {
    if (Test-Path $PidFile) {
        try {
            $data = Get-Content $PidFile -Raw | ConvertFrom-Json
            foreach ($prop in $data.PSObject.Properties) {
                $procId = [int]$prop.Value
                if ($procId -gt 0 -and (Get-Process -Id $procId -ErrorAction SilentlyContinue)) {
                    Write-Step "Dang dung $($prop.Name) (PID=$procId)..."
                    & cmd /c "taskkill /F /T /PID $procId >nul 2>&1"
                }
            }
        } catch {
            Write-Warn "Khong doc duoc ${PidFile}: $_"
        }
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    }

    # Don dep them cac tien trinh FlashLogix thu cong con sot lai (tranh xung dot port 8000 / 8501)
    $orphanProcs = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -and (
            $_.CommandLine -match "uvicorn\s+app\.main:app" -or
            $_.CommandLine -match "streaming\.audit_consumer" -or
            $_.CommandLine -match "streamlit\s+run\s+dashboard[\\/]streamlit_app\.py"
        )
    }
    foreach ($p in $orphanProcs) {
        $targetPid = [int]$p.ProcessId
        if ($targetPid -gt 0 -and (Get-Process -Id $targetPid -ErrorAction SilentlyContinue)) {
            Write-Step "Dang don dep tien trinh FlashLogix cu (PID=$targetPid)..."
            & cmd /c "taskkill /F /T /PID $targetPid >nul 2>&1"
        }
    }
}

function Wait-DockerService([string]$ContainerName, [int]$TimeoutSec = 45) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $status = (& docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $ContainerName 2>$null)
        if ($LASTEXITCODE -eq 0 -and ($status -eq "healthy" -or $status -eq "running")) {
            return
        }
        Start-Sleep -Seconds 1
    }
    throw "Container '$ContainerName' chua san sang sau ${TimeoutSec}s."
}

function Wait-HttpReady([string]$Url, [int]$TimeoutSec = 35) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -TimeoutSec 2 -UseBasicParsing
            if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 400) {
                return $true
            }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    return $false
}

function Ensure-BaselineData {
    Write-Step "Kiem tra schema PostgreSQL & du lieu khoi tao trong Redis..."
    $seedScript = Join-Path $RuntimeDir "ensure_baseline.py"
    $pyCode = @'
import os
import sys
sys.path.insert(0, os.getcwd())

from sqlalchemy import text
from app.core.database import engine, ensure_oltp_schema_and_constraints
from app.core.storage import ensure_lakehouse_bucket
from app.services.inventory_service import inventory_service

ensure_oltp_schema_and_constraints()
ensure_lakehouse_bucket()
with engine.begin() as conn:
    count = int(conn.execute(text("SELECT COUNT(*) FROM products")).scalar() or 0)
    if count == 0:
        conn.execute(text(
            "INSERT INTO products (name, price, stock, version) "
            "VALUES ('FlashLogix Mechanical Keyboard Pro', 199.99, 100, 1)"
        ))
        inventory_service.initialize_stock(1, 100)
        print("SEEDED_PRODUCT_1=100")
    else:
        rows = conn.execute(text("SELECT id, stock FROM products ORDER BY id")).fetchall()
        for prod_id, stk in rows:
            if inventory_service.get_stock(int(prod_id)) is None:
                inventory_service.initialize_stock(int(prod_id), int(stk))
        print(f"EXISTING_PRODUCTS={len(rows)}")
'@
    Set-Content -Path $seedScript -Value $pyCode -Encoding UTF8
    Push-Location $RootDir
    try {
        & $PythonExe $seedScript
        if ($LASTEXITCODE -ne 0) {
            throw "Khoi tao schema/seed that bai."
        }
    } finally {
        Pop-Location
        Remove-Item $seedScript -Force -ErrorAction SilentlyContinue
    }
}

function Start-AllInOne {
    Ensure-EnvAndPython
    Stop-TrackedProcesses

    Write-Step "1/5 Khoi dong ha tang Docker FlashLogix (co lap khoi cac project khac)..."
    Push-Location $RootDir
    try {
        & docker compose up -d | Out-Null
    } finally {
        Pop-Location
    }
    Wait-DockerService "flashlogix_postgres" 45
    Wait-DockerService "flashlogix_redis" 30
    Wait-DockerService "flashlogix_kafka" 45
    Wait-DockerService "flashlogix_minio" 30
    Write-Ok "Docker FlashLogix san sang (Postgres:5433, Redis:6380, Kafka:9094, MinIO:9000)."

    Write-Step "2/5 Dong bo schema & ton kho ban dau..."
    Ensure-BaselineData

    Write-Step "3/5 Khoi dong FastAPI Backend (4 workers, port 8000)..."
    $apiOut = Join-Path $LogDir "api.stdout.log"
    $apiErr = Join-Path $LogDir "api.stderr.log"
    $apiProc = Start-Process -FilePath $PythonExe `
        -ArgumentList "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--backlog", "4096" `
        -WorkingDirectory $RootDir `
        -RedirectStandardOutput $apiOut `
        -RedirectStandardError $apiErr `
        -WindowStyle Hidden `
        -PassThru

    if (!(Wait-HttpReady "http://127.0.0.1:8000/health" 35)) {
        throw "FastAPI khong phan hoi /health. Kiem tra log tai '$apiErr'."
    }
    Write-Ok "FastAPI API san sang: http://localhost:8000 (Swagger: http://localhost:8000/docs)"

    Write-Step "4/5 Khoi dong Kafka Audit & Fraud Consumer (nen)..."
    $consOut = Join-Path $LogDir "audit_consumer.stdout.log"
    $consErr = Join-Path $LogDir "audit_consumer.stderr.log"
    $consProc = Start-Process -FilePath $PythonExe `
        -ArgumentList "-m", "streaming.audit_consumer" `
        -WorkingDirectory $RootDir `
        -RedirectStandardOutput $consOut `
        -RedirectStandardError $consErr `
        -WindowStyle Hidden `
        -PassThru
    Write-Ok "AuditConsumer dang chay (PID=$($consProc.Id))."

    Write-Step "5/5 Khoi dong Streamlit Command Center UI (port 8501)..."
    $uiOut = Join-Path $LogDir "dashboard.stdout.log"
    $uiErr = Join-Path $LogDir "dashboard.stderr.log"
    $uiProc = Start-Process -FilePath $PythonExe `
        -ArgumentList "-m", "streamlit", "run", "dashboard/streamlit_app.py", "--server.port", "8501", "--server.headless", "true" `
        -WorkingDirectory $RootDir `
        -RedirectStandardOutput $uiOut `
        -RedirectStandardError $uiErr `
        -WindowStyle Hidden `
        -PassThru

    if (!(Wait-HttpReady "http://127.0.0.1:8501" 35)) {
        throw "Streamlit UI khong phan hoi port 8501. Kiem tra log tai '$uiErr'."
    }

    @{
        api            = $apiProc.Id
        audit_consumer = $consProc.Id
        dashboard      = $uiProc.Id
    } | ConvertTo-Json | Set-Content -Path $PidFile -Encoding UTF8

    Write-Ok "Streamlit Command Center san sang: http://localhost:8501"
    Write-Host ""
    Write-Host "================ FLASHLOGIX PLATFORM READY ================" -ForegroundColor Green
    Write-Host "  Command Center UI : http://localhost:8501"
    Write-Host "  API Swagger Docs  : http://localhost:8000/docs"
    Write-Host "  Telemetry Metrics : http://localhost:8000/metrics"
    Write-Host "  MinIO Console     : http://localhost:9001  (minioadmin / minioadminpassword)"
    Write-Host "  Logs thu muc      : $LogDir"
    Write-Host "  Dung he thong     : .\run.ps1 stop   (hoac .\run.ps1 down)"
    Write-Host "===========================================================" -ForegroundColor Green

    if (-not $NoBrowser) {
        Start-Process "http://localhost:8501"
    }
}

function Show-Status {
    Write-Step "Trang thai Docker FlashLogix:"
    & docker ps --filter "name=flashlogix_" --format "table {{.Names}}`t{{.Status}}`t{{.Ports}}"
    Write-Host ""
    Write-Step "Trang thai tien trinh ung dung:"
    if (Test-Path $PidFile) {
        $data = Get-Content $PidFile -Raw | ConvertFrom-Json
        foreach ($prop in $data.PSObject.Properties) {
            $proc = Get-Process -Id ([int]$prop.Value) -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Ok "$($prop.Name): RUNNING (PID=$($prop.Value))"
            } else {
                Write-Warn "$($prop.Name): STOPPED (stale PID=$($prop.Value))"
            }
        }
    } else {
        Write-Warn "Chua co tien trinh nao duoc khoi dong qua .\run.ps1"
    }
}

switch ($Action) {
    "start" {
        Start-AllInOne
    }
    "stop" {
        Stop-TrackedProcesses
        Write-Ok "Da dung FastAPI, AuditConsumer va Streamlit."
    }
    "down" {
        Stop-TrackedProcesses
        Push-Location $RootDir
        try {
            & docker compose down
        } finally {
            Pop-Location
        }
        Write-Ok "Da tat app va docker compose FlashLogix (du lieu volume van duoc giu)."
    }
    "status" {
        Show-Status
    }
    "test" {
        Ensure-EnvAndPython
        Push-Location $RootDir
        try {
            & $PythonExe -m ruff check app streaming lakehouse scripts dashboard tests
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            & $PythonExe -m pytest tests/ -v
            exit $LASTEXITCODE
        } finally {
            Pop-Location
        }
    }
    "bench" {
        Ensure-EnvAndPython
        Push-Location $RootDir
        try {
            & $PythonExe scripts/benchmark_compare.py --reset-clean --verify-lakehouse
            exit $LASTEXITCODE
        } finally {
            Pop-Location
        }
    }
    "help" {
        Get-Help $PSCommandPath -Detailed
    }
}
