# Carga aws\credenciales.txt en la terminal ACTUAL (para usar los scripts 00..99 por separado).
# Uso (con punto al inicio):  . .\aws\cargar-credenciales.ps1
$archivo = Join-Path $PSScriptRoot "credenciales.txt"
if (-not (Test-Path $archivo)) { throw "No existe $archivo. Copia aws\credenciales.ejemplo.txt como aws\credenciales.txt y pega el bloque [default] del laboratorio." }
foreach ($linea in Get-Content $archivo) {
    if ($linea -match '^\s*aws_access_key_id\s*=\s*(\S+)') { $env:AWS_ACCESS_KEY_ID = $Matches[1] }
    if ($linea -match '^\s*aws_secret_access_key\s*=\s*(\S+)') { $env:AWS_SECRET_ACCESS_KEY = $Matches[1] }
    if ($linea -match '^\s*aws_session_token\s*=\s*(\S+)') { $env:AWS_SESSION_TOKEN = $Matches[1] }
}
if (-not $env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION = "us-east-1" }
Write-Host "Credenciales cargadas en esta terminal (region $env:AWS_DEFAULT_REGION)" -ForegroundColor Green
