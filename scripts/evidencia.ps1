<#
  Ejecuta un comando y guarda comando + fecha + salida en evidencias\<Nombre>.txt
  Uso:
    powershell -ExecutionPolicy Bypass -File scripts\evidencia.ps1 10-isr-antes "powershell -ExecutionPolicy Bypass -File scripts\kafka.ps1 isr"
  Variable opcional: $env:EVIDENCIA_ENTORNO = "local" | "aws"
  Las claves de AWS que aparezcan en la salida se reemplazan por <REDACTADO>.
#>
param(
    [Parameter(Mandatory = $true, Position = 0)][string]$Nombre,
    [Parameter(Mandatory = $true, Position = 1)][string]$Comando
)

$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$dir = Join-Path $root "evidencias"
New-Item -ItemType Directory -Force $dir | Out-Null
$archivo = Join-Path $dir "$Nombre.txt"
$entorno = if ($env:EVIDENCIA_ENTORNO) { $env:EVIDENCIA_ENTORNO } else { "local" }
$fecha = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

$salida = cmd /c "$Comando 2>&1" | Out-String
# Nunca guardar secretos
$salida = $salida -replace '(AKIA|ASIA)[A-Z0-9]{16}', '<REDACTADO>'
$salida = $salida -replace '(?i)(aws_secret_access_key|aws_session_token|password|token)(\s*[=:]\s*)\S+', '$1$2<REDACTADO>'

$texto = "# Evidencia $Nombre`r`n# Fecha (UTC): $fecha`r`n# Entorno: $entorno`r`n`r`n`$ $Comando`r`n`r`n$salida"
[System.IO.File]::WriteAllText($archivo, $texto, (New-Object System.Text.UTF8Encoding $false))
Write-Host $salida
Write-Host "Evidencia guardada en $archivo" -ForegroundColor Green
