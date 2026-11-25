# Demo end-to-end en Windows (PowerShell). Ejecutar desde la raíz del repo:
#   powershell -ExecutionPolicy Bypass -File .\scripts\demo.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Step($text) { Write-Host "`n=== $text ===" -ForegroundColor Cyan }

Step "1/6 Construyendo y levantando el entorno (la primera vez tarda varios minutos)"
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { throw "docker compose up falló" }

Step "2/6 Esperando a la API de negocio"
$ok = $false
for ($i = 0; $i -lt 60; $i++) {
    try { Invoke-RestMethod http://localhost:8081/health | Out-Null; $ok = $true; break } catch { Start-Sleep 5 }
}
if (-not $ok) { throw "La API no respondió. Revisa: docker compose logs audit-api" }

Step "3/6 Generando 500 transferencias extra y dejando correr el pipeline 90 s"
Invoke-RestMethod -Method Post "http://localhost:8081/api/simulate?count=500" | Format-List
Start-Sleep 90
Write-Host "Estado del outbox en SQL Server:"
Invoke-RestMethod http://localhost:8081/api/outbox/stats | Format-List

Step "4/6 dbt build (silver + gold + tests)"
docker compose run --rm dbt build

Step "5/6 Mantenimiento Iceberg (compactación de small files, expiración de snapshots)"
docker compose run --rm spark-maintenance

Step "6/6 Verificación"
docker compose run --rm spark-sql -f /opt/jobs/verify.sql

Write-Host "`nListo. Archivos S3 (SeaweedFS): http://localhost:8888/buckets/warehouse/  |  Spark UI: http://localhost:4040" -ForegroundColor Green
