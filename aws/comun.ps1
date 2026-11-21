# Funciones comunes de los scripts de AWS (se cargan con: . "$PSScriptRoot\comun.ps1")
# REGLA: nunca imprimir ni guardar credenciales. Solo se usan las variables de entorno de la terminal.

# "Continue": en Windows PowerShell 5.1, con "Stop" cualquier stderr de un ejecutable nativo (aws, ssh)
# se vuelve excepcion. Los errores se controlan revisando $LASTEXITCODE y con throw explicitos.
$ErrorActionPreference = "Continue"
$Global:AwsDir = $PSScriptRoot
$Global:RepoDir = Split-Path $PSScriptRoot -Parent
$Global:EstadoPath = Join-Path $PSScriptRoot "estado.json"     # sin secretos; ignorado por git
$Global:Nombre = "ventas-streaming"
$Global:KeyName = "ventas-streaming-key"
$Global:PemPath = Join-Path $env:USERPROFILE ".ssh\ventas-streaming-key.pem"   # fuera del repositorio
$Global:Tags = "{Key=Proyecto,Value=dsy1107-ep5}"

function Write-Paso($t) { Write-Host ""; Write-Host "==> $t" -ForegroundColor Cyan }
function Write-Ok($t) { Write-Host "    OK  $t" -ForegroundColor Green }
function Write-Aviso($t) { Write-Host "    !!  $t" -ForegroundColor Yellow }

function Assert-Credenciales {
    foreach ($v in @("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")) {
        if (-not (Get-Item "env:$v" -ErrorAction SilentlyContinue)) {
            throw "Falta la variable de entorno $v en esta terminal. Pegala en la terminal (no en archivos ni en el chat):`n" +
                  '  $env:AWS_ACCESS_KEY_ID="..."; $env:AWS_SECRET_ACCESS_KEY="..."; $env:AWS_SESSION_TOKEN="..."; $env:AWS_DEFAULT_REGION="us-east-1"'
        }
    }
    if (-not $env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION = "us-east-1" }
    # Ignorar ~/.aws/credentials y config: solo cuentan las variables de esta terminal
    $env:AWS_SHARED_CREDENTIALS_FILE = Join-Path $AwsDir ".no-usar-credentials"
    $env:AWS_CONFIG_FILE = Join-Path $AwsDir ".no-usar-config"
    $env:AWS_PAGER = ""
}

function Invoke-Aws {
    # Ejecuta aws CLI y devuelve el JSON parseado (o $null). Lanza error si falla.
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$AwsArgs)
    $salida = & aws @AwsArgs --output json
    if ($LASTEXITCODE -ne 0) { throw "aws $($AwsArgs[0]) $($AwsArgs[1]) fallo (codigo $LASTEXITCODE)" }
    if ($salida) { return ($salida -join "`n" | ConvertFrom-Json) }
    return $null
}

function Test-Aws {
    # Igual que Invoke-Aws pero devuelve $null en vez de fallar (para comprobar permisos)
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$AwsArgs)
    $salida = & aws @AwsArgs --output json 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    if ($salida) { return ($salida -join "`n" | ConvertFrom-Json) }
    return $true
}

function Get-Estado {
    if (Test-Path $EstadoPath) {
        $o = Get-Content $EstadoPath -Raw | ConvertFrom-Json
        $h = @{}
        foreach ($p in $o.PSObject.Properties) { $h[$p.Name] = $p.Value }
        return $h
    }
    return @{}
}

function Set-Estado([hashtable]$cambios) {
    $e = Get-Estado
    foreach ($k in $cambios.Keys) { $e[$k] = $cambios[$k] }
    $json = $e | ConvertTo-Json -Depth 5
    [System.IO.File]::WriteAllText($EstadoPath, $json, (New-Object System.Text.UTF8Encoding $false))
}

function Show-TiempoRestante {
    $e = Get-Estado
    if (-not $e.credencialesInicio) { return }
    $ini = [datetime]::Parse($e.credencialesInicio).ToUniversalTime()
    $restante = [math]::Round(180 - ((Get-Date).ToUniversalTime() - $ini).TotalMinutes)
    if ($restante -le 30) {
        Write-Aviso "Quedan ~$restante min de credenciales (inicio $($e.credencialesInicio)). Guarda evidencias o ejecuta 99-destruir.ps1."
    } else {
        Write-Host "    Credenciales: quedan ~$restante min (de 180)" -ForegroundColor DarkGray
    }
}

function Get-MiIpPublica {
    return (Invoke-RestMethod -Uri "https://checkip.amazonaws.com" -TimeoutSec 15 -ErrorAction Stop).ToString().Trim()
}

function Invoke-Remoto([string]$script, [switch]$Silencioso) {
    # Ejecuta un script bash en la EC2 (usuario ubuntu). El script viaja por stdin a un archivo
    # temporal (evita los problemas de comillas de PowerShell 5.1 al pasar argumentos a ssh)
    # y se ejecuta con stdin vacio (para que docker compose run no consuma el script).
    $e = Get-Estado
    if (-not $e.ipPublica) { throw "No hay IP en estado.json: ejecuta 02-instancia.ps1" }
    # PowerShell 5.1 antepone un BOM al stdin de los ejecutables nativos: se envia sin BOM y ademas se
    # elimina en el servidor por si acaso (sed), junto con los \r de Windows (tr).
    $Global:OutputEncoding = New-Object System.Text.UTF8Encoding $false
    $remoto = "tr -d '\r' | sed '1s/^\xEF\xBB\xBF//' > /tmp/ventas-remoto.sh && bash /tmp/ventas-remoto.sh < /dev/null"
    $sshArgs = @("-i", $PemPath, "-o", "StrictHostKeyChecking=accept-new", "-o", "ServerAliveInterval=30",
                 "-o", "ConnectTimeout=15", "-o", "LogLevel=ERROR", "ubuntu@$($e.ipPublica)", $remoto)
    if ($Silencioso) { $script | & ssh @sshArgs 2>$null | Out-Null } else { $script | & ssh @sshArgs | Out-Host }
    return $LASTEXITCODE
}
