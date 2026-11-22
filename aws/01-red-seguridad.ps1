<#
  Paso 1: security group y key pair SSH. Idempotente.
  Uso: powershell -ExecutionPolicy Bypass -File aws\01-red-seguridad.ps1 [-AbrirATodos]
  Puertos: 22 SSH, 3000 Grafana, 9090 Prometheus, 8080 Kafka UI, 9092-9093 Kafka (clientes externos).
  Por defecto todo se abre SOLO a tu IP (/32).
  -AbrirATodos: 22, 3000 y 9090 quedan abiertos a 0.0.0.0/0 (para presentar desde cualquier red, p. ej.
  la wifi de la sede). SSH sigue exigiendo la llave .pem y Grafana es solo lectura (admin con clave
  aleatoria). Kafka (9092-9093) y Kafka UI (8080) NUNCA se abren a todos: no tienen autenticacion.
  Destruir con destruir-aws.bat al terminar.
#>
param([switch]$AbrirATodos)
. "$PSScriptRoot\comun.ps1"
Assert-Credenciales
Show-TiempoRestante

Write-Paso "VPC por defecto"
$vpc = Invoke-Aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query "Vpcs[0].VpcId"
if (-not $vpc) { throw "No hay VPC por defecto en $env:AWS_DEFAULT_REGION" }
Write-Ok $vpc

Write-Paso "Security group $Nombre-sg"
$sg = Invoke-Aws ec2 describe-security-groups --filters "Name=group-name,Values=$Nombre-sg" "Name=vpc-id,Values=$vpc" --query "SecurityGroups[0].GroupId"
if (-not $sg) {
    $sg = (Invoke-Aws ec2 create-security-group --group-name "$Nombre-sg" --description "DSY1107 EP5 Kafka - solo IP del estudiante" --vpc-id $vpc `
            --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=$Nombre-sg},$Tags]").GroupId
    Write-Ok "Creado $sg"
} else { Write-Ok "Ya existia $sg" }

$ip = Get-MiIpPublica
$cidr = "$ip/32"
$reglas = Invoke-Aws ec2 describe-security-groups --group-ids $sg --query "SecurityGroups[0].IpPermissions"
foreach ($p in @(@(22, 22), @(3000, 3000), @(9090, 9090), @(8080, 8080), @(9092, 9093))) {
    # Kafka y Kafka UI siempre solo a tu IP; SSH, Grafana y Prometheus a todos si se pidio -AbrirATodos
    $destino = if ($AbrirATodos -and $p[0] -in @(22, 3000, 9090)) { "0.0.0.0/0" } else { $cidr }
    $existe = $reglas | Where-Object { $_.FromPort -eq $p[0] -and $_.ToPort -eq $p[1] -and ($_.IpRanges.CidrIp -contains $destino) }
    if ($existe) { Write-Ok "Puerto $($p[0])-$($p[1]) ya abierto a $destino"; continue }
    $desc = if ($destino -eq "0.0.0.0/0") { "demo-temporal" } else { "estudiante" }
    $null = Invoke-Aws ec2 authorize-security-group-ingress --group-id $sg --ip-permissions "IpProtocol=tcp,FromPort=$($p[0]),ToPort=$($p[1]),IpRanges=[{CidrIp=$destino,Description=$desc}]"
    if ($destino -eq "0.0.0.0/0") { Write-Aviso "Puerto $($p[0]) abierto a TODO internet (temporal para la demo)" }
    else { Write-Ok "Puerto $($p[0])-$($p[1]) abierto solo a $cidr" }
}
$kafkaAbierto = $reglas | Where-Object { $_.FromPort -in @(8080, 9092) -and ($_.IpRanges.CidrIp -contains "0.0.0.0/0") }
if ($kafkaAbierto) { Write-Aviso "Kafka/Kafka UI esta abierto a 0.0.0.0/0 en ${sg}: cierralo en la consola" }

Write-Paso "Key pair $KeyName (llave privada fuera del repositorio: $PemPath)"
$kp = Test-Aws ec2 describe-key-pairs --key-names $KeyName --query "KeyPairs[0].KeyName"
if ($kp -and -not (Test-Path $PemPath)) {
    Write-Aviso "El key pair existe en AWS pero no tienes el .pem local: se recrea"
    $null = Invoke-Aws ec2 delete-key-pair --key-name $KeyName
    $kp = $null
}
if (-not $kp) {
    New-Item -ItemType Directory -Force (Split-Path $PemPath) | Out-Null
    if (Test-Path $PemPath) {
        # el .pem anterior queda en solo lectura: restaurar permisos para poder reemplazarlo
        & icacls $PemPath /reset | Out-Null
        Remove-Item $PemPath -Force
    }
    $material = Invoke-Aws ec2 create-key-pair --key-name $KeyName --key-type rsa --query "KeyMaterial" `
                  --tag-specifications "ResourceType=key-pair,Tags=[{Key=Name,Value=$KeyName},$Tags]"
    [System.IO.File]::WriteAllText($PemPath, ($material -replace "`r", ""), (New-Object System.Text.UTF8Encoding $false))
    # OpenSSH en Windows exige que solo tu usuario pueda leer la llave
    if (-not (Test-Path $PemPath) -or (Get-Item $PemPath).Length -lt 1000) { throw "No se pudo escribir $PemPath" }
    & icacls $PemPath /inheritance:r /grant:r "$($env:USERNAME):(R)" | Out-Null
    Write-Ok "Llave creada en $PemPath (solo lectura para $env:USERNAME)"
} else { Write-Ok "Ya existia y tienes el .pem local" }

Set-Estado @{ vpcId = $vpc; sgId = $sg; miIp = $ip; keyName = $KeyName }
Write-Host ""
Write-Host "Siguiente: powershell -ExecutionPolicy Bypass -File aws\02-instancia.ps1" -ForegroundColor Green
