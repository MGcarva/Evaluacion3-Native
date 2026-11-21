<#
  Despliegue completo en AWS en un solo paso (lo llama desplegar-aws.bat).
    1) Lee aws\credenciales.txt (bloque [default] copiado de "AWS Details" del laboratorio)
    2) 00-verificar -> 01-red-seguridad -> 02-instancia -> 03-despliegue
    3) Opcional (-ConPruebas): 04-evidencias (broker caido, reinicio, generador, tests) y trae evidencias
  Uso: powershell -ExecutionPolicy Bypass -File aws\desplegar-todo.ps1 [-ConPruebas]
  Las credenciales solo se cargan en esta terminal; nunca se imprimen ni se copian a la EC2.
#>
param([switch]$ConPruebas)
$ErrorActionPreference = "Stop"
$archivo = Join-Path $PSScriptRoot "credenciales.txt"

if (-not (Test-Path $archivo)) { throw "No existe $archivo. Copia credenciales.ejemplo.txt como credenciales.txt y pega tus credenciales." }
$valores = @{}
foreach ($linea in Get-Content $archivo) {
    if ($linea -match '^\s*(aws_access_key_id|aws_secret_access_key|aws_session_token|region)\s*=\s*(\S+)\s*$') {
        $valores[$Matches[1].ToLower()] = $Matches[2]
    }
}
foreach ($k in @("aws_access_key_id", "aws_secret_access_key", "aws_session_token")) {
    if (-not $valores[$k] -or $valores[$k] -match '^PEGAR') { throw "Falta $k en $archivo (pega el bloque [default] completo del laboratorio)" }
}
$env:AWS_ACCESS_KEY_ID = $valores["aws_access_key_id"]
$env:AWS_SECRET_ACCESS_KEY = $valores["aws_secret_access_key"]
$env:AWS_SESSION_TOKEN = $valores["aws_session_token"]
$env:AWS_DEFAULT_REGION = if ($valores["region"]) { $valores["region"] } else { "us-east-1" }
Write-Host "Credenciales cargadas desde credenciales.txt (region $env:AWS_DEFAULT_REGION)" -ForegroundColor Green

$pasos = @(
    @{ s = "00-verificar.ps1"; a = @("-NuevasCredenciales") },
    @{ s = "01-red-seguridad.ps1"; a = @() },
    @{ s = "02-instancia.ps1"; a = @() },
    @{ s = "03-despliegue.ps1"; a = @() }
)
if ($ConPruebas) { $pasos += @{ s = "04-evidencias.ps1"; a = @() } }

$ini = Get-Date
foreach ($p in $pasos) {
    Write-Host ""
    Write-Host "############ $($p.s) ############" -ForegroundColor Magenta
    $a = $p.a
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot $p.s) @a
    if ($LASTEXITCODE -ne 0) {
        throw "$($p.s) fallo (codigo $LASTEXITCODE). Corrige y vuelve a ejecutar desplegar-aws.bat: los pasos son idempotentes y retoman donde quedaron."
    }
}
$e = Get-Content (Join-Path $PSScriptRoot "estado.json") -Raw | ConvertFrom-Json
Write-Host ""
Write-Host "LISTO en $([math]::Round(((Get-Date) - $ini).TotalMinutes)) min" -ForegroundColor Green
Write-Host "  Grafana:    http://$($e.ipPublica):3000"
Write-Host "  Prometheus: http://$($e.ipPublica):9090/targets"
Write-Host "  SSH:        ssh -i `"$env:USERPROFILE\.ssh\ventas-streaming-key.pem`" ubuntu@$($e.ipPublica)"
Write-Host "  Pruebas:    pruebas-aws.bat [broker|reinicio|generador|base]"
Write-Host "  Al terminar: destruir-aws.bat" -ForegroundColor Yellow
