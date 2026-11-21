<#
  Paso 2: una EC2 Ubuntu 24.04 con Docker + Compose (instalados por user-data). Idempotente/reanudable.
  Uso: powershell -ExecutionPolicy Bypass -File aws\02-instancia.ps1 [-Tipo t3.large] [-DiscoGB 30]
  La cuenta educativa solo permite t3.medium (4 GB): user-data agrega 2 GB de swap y 03-despliegue baja
  el heap de los brokers a 384 MB. Con t3.large (si se permite) no hace falta.
#>
param([string]$Tipo = "t3.medium", [int]$DiscoGB = 30)
. "$PSScriptRoot\comun.ps1"
Assert-Credenciales
Show-TiempoRestante
$e = Get-Estado
if (-not $e.sgId) { throw "Falta el security group: ejecuta 01-red-seguridad.ps1" }

Write-Paso "Instancia existente?"
$inst = Invoke-Aws ec2 describe-instances --filters "Name=tag:Name,Values=$Nombre" "Name=instance-state-name,Values=pending,running,stopping,stopped" `
          --query "Reservations[].Instances[0].InstanceId"
$id = @($inst)[0]
if ($id) {
    Write-Ok "Ya existe $id"
    $estado = Invoke-Aws ec2 describe-instances --instance-ids $id --query "Reservations[0].Instances[0].State.Name"
    if ($estado -eq "stopped") { $null = Invoke-Aws ec2 start-instances --instance-ids $id; Write-Ok "Iniciada" }
} else {
    $ami = $e.ami
    if (-not $ami) {
        $ami = Invoke-Aws ec2 describe-images --owners 099720109477 --filters "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" "Name=state,Values=available" --query "sort_by(Images,&CreationDate)[-1].ImageId"
    }
    Write-Paso "Lanzando $Tipo con $ami (disco gp3 $DiscoGB GB)"
    $tmp = Join-Path $env:TEMP "ventas-aws"
    New-Item -ItemType Directory -Force $tmp | Out-Null
    $userData = @'
#!/bin/bash
set -eux
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl git python3
curl -fsSL https://get.docker.com | sh
usermod -aG docker ubuntu
systemctl enable --now docker
# Kafka/RocksDB: limites recomendados
sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" > /etc/sysctl.d/99-kafka.conf
# 2 GB de swap: t3.medium (4 GB) es el maximo permitido en la cuenta educativa
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo "/swapfile none swap sw 0 0" >> /etc/fstab
touch /home/ubuntu/.docker-listo
'@
    [System.IO.File]::WriteAllText("$tmp\user-data.sh", ($userData -replace "`r", ""), (New-Object System.Text.UTF8Encoding $false))
    $bdm = "[{""DeviceName"":""/dev/sda1"",""Ebs"":{""VolumeSize"":$DiscoGB,""VolumeType"":""gp3"",""DeleteOnTermination"":true}}]"
    [System.IO.File]::WriteAllText("$tmp\bdm.json", $bdm, (New-Object System.Text.UTF8Encoding $false))

    $r = Invoke-Aws ec2 run-instances --image-id $ami --instance-type $Tipo --count 1 `
           --key-name $KeyName --security-group-ids $e.sgId `
           --block-device-mappings "file://$tmp\bdm.json" --user-data "file://$tmp\user-data.sh" `
           --metadata-options "HttpTokens=required,HttpEndpoint=enabled" `
           --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$Nombre},$Tags]" "ResourceType=volume,Tags=[{Key=Name,Value=$Nombre},$Tags]"
    $id = $r.Instances[0].InstanceId
    Write-Ok "Lanzada $id"
}

Write-Paso "Esperando estado running..."
& aws ec2 wait instance-running --instance-ids $id
$ip = Invoke-Aws ec2 describe-instances --instance-ids $id --query "Reservations[0].Instances[0].PublicIpAddress"
$tipoReal = Invoke-Aws ec2 describe-instances --instance-ids $id --query "Reservations[0].Instances[0].InstanceType"
Set-Estado @{ instanceId = $id; ipPublica = $ip; tipo = $tipoReal }
Write-Ok "$id ($tipoReal) en $ip"

Write-Paso "Esperando SSH y la instalacion de Docker (2-4 min)..."
$ini = Get-Date
do {
    Start-Sleep -Seconds 10
    $rc = Invoke-Remoto "test -f /home/ubuntu/.docker-listo && docker compose version" -Silencioso
    Write-Host "    ... $([math]::Round(((Get-Date) - $ini).TotalSeconds)) s" -ForegroundColor DarkGray
} until ($rc -eq 0 -or ((Get-Date) - $ini).TotalMinutes -gt 8)
if ($rc -ne 0) { throw "Docker no quedo listo en 8 min. Revisa: ssh -i $PemPath ubuntu@$ip 'sudo tail -50 /var/log/cloud-init-output.log'" }
Write-Ok "Docker listo"
Write-Host ""
Write-Host "Siguiente: powershell -ExecutionPolicy Bypass -File aws\03-despliegue.ps1" -ForegroundColor Green
