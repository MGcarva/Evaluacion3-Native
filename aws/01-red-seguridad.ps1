<#
  Paso 1: security group restrictivo (solo tu IP /32) y key pair SSH. Idempotente.
  Uso: powershell -ExecutionPolicy Bypass -File aws\01-red-seguridad.ps1
  Puertos: 22 SSH, 3000 Grafana, 9090 Prometheus, 8080 Kafka UI, 9092-9093 Kafka (clientes externos).
  Kafka no tiene autenticacion ni TLS: NUNCA se abre a 0.0.0.0/0.
#>
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
    $existe = $reglas | Where-Object { $_.FromPort -eq $p[0] -and $_.ToPort -eq $p[1] -and ($_.IpRanges.CidrIp -contains $cidr) }
    if ($existe) { Write-Ok "Puerto $($p[0])-$($p[1]) ya abierto a $cidr"; continue }
    $null = Invoke-Aws ec2 authorize-security-group-ingress --group-id $sg --ip-permissions "IpProtocol=tcp,FromPort=$($p[0]),ToPort=$($p[1]),IpRanges=[{CidrIp=$cidr,Description=estudiante}]"
    Write-Ok "Puerto $($p[0])-$($p[1]) abierto solo a $cidr"
}
$abiertas = $reglas | Where-Object { $_.IpRanges.CidrIp -contains "0.0.0.0/0" }
if ($abiertas) { Write-Aviso "Hay reglas abiertas a 0.0.0.0/0 en ${sg}: revisalas en la consola" }

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
