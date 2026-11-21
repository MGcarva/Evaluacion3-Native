<#
  Paso 0: verifica credenciales temporales y descubre que permite la cuenta (sin crear nada).
  Uso:  powershell -ExecutionPolicy Bypass -File aws\00-verificar.ps1 [-NuevasCredenciales]
  -NuevasCredenciales: registra la hora de inicio de una nueva ventana de 3 h.
#>
param([switch]$NuevasCredenciales)
. "$PSScriptRoot\comun.ps1"

Write-Paso "Credenciales de esta terminal"
Assert-Credenciales
$id = Invoke-Aws sts get-caller-identity
# No se imprime el ARN completo (puede contener datos personales): solo el tipo de identidad
$tipo = ($id.Arn -split ":")[5] -replace "/.*", ""
Write-Ok "Identidad valida (cuenta ****$($id.Account.Substring($id.Account.Length - 4)), tipo $tipo), region $env:AWS_DEFAULT_REGION"

$e = Get-Estado
if ($NuevasCredenciales -or -not $e.credencialesInicio) {
    Set-Estado @{ credencialesInicio = (Get-Date).ToUniversalTime().ToString("o"); region = $env:AWS_DEFAULT_REGION }
    Write-Ok "Inicio de la ventana de credenciales registrado: $((Get-Date).ToString('HH:mm'))"
}
Show-TiempoRestante

Write-Paso "Permisos y recursos disponibles"
$vpc = Test-Aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" --query "Vpcs[0].VpcId"
if ($vpc) { Write-Ok "VPC por defecto: $vpc" } else { Write-Aviso "No hay VPC por defecto o no se puede leer (ec2:DescribeVpcs)" }

foreach ($tipoInst in @("t3.large", "t3.medium")) {
    $ofertas = Test-Aws ec2 describe-instance-type-offerings --filters "Name=instance-type,Values=$tipoInst" --query "InstanceTypeOfferings[].Location"
    if ($ofertas) { Write-Ok "$tipoInst disponible en la region" } else { Write-Aviso "$tipoInst no listado en la region" }
}

$ami = Test-Aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query "Parameter.Value"
if (-not $ami) {
    $ami = Test-Aws ec2 describe-images --owners 099720109477 --filters "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" "Name=state,Values=available" --query "sort_by(Images,&CreationDate)[-1].ImageId"
}
if ($ami) { Write-Ok "AMI Ubuntu 24.04: $ami"; Set-Estado @{ ami = $ami } } else { Write-Aviso "No se encontro AMI de Ubuntu 24.04" }

# Prueba de permisos sin crear nada (DryRun => "DryRunOperation" significa permitido)
$dry = (& aws ec2 run-instances --dry-run --image-id $ami --instance-type t3.large --count 1 2>&1 | Out-String)
if ($dry -match "DryRunOperation") { Write-Ok "Permiso para lanzar t3.large (dry-run)" }
elseif ($dry -match "UnauthorizedOperation|not authorized") { Write-Aviso "Sin permiso para t3.large: se usara t3.medium (02-instancia.ps1 -Tipo t3.medium)" }
else { Write-Aviso "Dry-run t3.large: $(($dry -split "`n")[0])" }

$kp = Test-Aws ec2 describe-key-pairs --query "KeyPairs[].KeyName"
if ($null -ne $kp) { Write-Ok "Se pueden listar key pairs (ec2:DescribeKeyPairs)" } else { Write-Aviso "No se pueden listar key pairs" }

$perfiles = Test-Aws iam list-instance-profiles --query "InstanceProfiles[].InstanceProfileName"
if ($perfiles) { Write-Ok "Perfiles de instancia visibles: $($perfiles -join ', ')" } else { Write-Aviso "IAM no visible (normal en cuentas educativas; no se necesita)" }

Write-Paso "Tu IP publica (se usara para abrir puertos SOLO a ella, /32)"
$ip = Get-MiIpPublica
Write-Ok "$ip/32"
Set-Estado @{ miIp = $ip }
Write-Host ""
Write-Host "Siguiente: powershell -ExecutionPolicy Bypass -File aws\01-red-seguridad.ps1" -ForegroundColor Green
