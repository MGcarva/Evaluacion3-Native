<#
  Paso 99: elimina TODO lo creado (instancia + disco, security group, key pair) y verifica que no quede nada.
  Uso: powershell -ExecutionPolicy Bypass -File aws\99-destruir.ps1
  Las evidencias ya traidas a evidencias\ se conservan.
#>
. "$PSScriptRoot\comun.ps1"
Assert-Credenciales
$e = Get-Estado

Write-Paso "Terminando instancias con tag Name=$Nombre"
$ids = @(Invoke-Aws ec2 describe-instances --filters "Name=tag:Name,Values=$Nombre" "Name=instance-state-name,Values=pending,running,stopping,stopped" --query "Reservations[].Instances[].InstanceId")
if ($ids.Count -gt 0 -and $ids[0]) {
    $null = Invoke-Aws ec2 terminate-instances --instance-ids @ids
    Write-Ok "Terminando: $($ids -join ', ') (el disco se borra con la instancia)"
    & aws ec2 wait instance-terminated --instance-ids @ids
    Write-Ok "Instancias terminadas"
} else { Write-Ok "No hay instancias activas" }

Write-Paso "Security group"
if ($e.sgId) {
    $intentos = 0
    do {
        $ok = Test-Aws ec2 delete-security-group --group-id $e.sgId
        if (-not $ok) { Start-Sleep -Seconds 10; $intentos++ }
    } until ($ok -or $intentos -ge 6)
    if ($ok) { Write-Ok "Eliminado $($e.sgId)" } else { Write-Aviso "No se pudo eliminar $($e.sgId) (puede que ya no exista)" }
}

Write-Paso "Key pair"
$null = Test-Aws ec2 delete-key-pair --key-name $KeyName
if (Test-Path $PemPath) { & icacls $PemPath /reset | Out-Null; Remove-Item $PemPath -Force; Write-Ok "Key pair y $PemPath eliminados" } else { Write-Ok "Key pair eliminado" }

Write-Paso "Verificacion final (debe quedar vacio)"
$quedan = Invoke-Aws ec2 describe-instances --filters "Name=tag:Proyecto,Values=dsy1107-ep5" "Name=instance-state-name,Values=pending,running,stopping,stopped" --query "Reservations[].Instances[].InstanceId"
$vols = Invoke-Aws ec2 describe-volumes --filters "Name=tag:Proyecto,Values=dsy1107-ep5" --query "Volumes[].VolumeId"
$sgs = Invoke-Aws ec2 describe-security-groups --filters "Name=group-name,Values=$Nombre-sg" --query "SecurityGroups[].GroupId"
$kps = Test-Aws ec2 describe-key-pairs --key-names $KeyName --query "KeyPairs[].KeyName"
Write-Host "    instancias: $(@($quedan).Count)  volumenes: $(@($vols).Count)  security groups: $(@($sgs).Count)  key pairs: $(if ($kps) {1} else {0})"
if (-not $quedan -and -not $vols -and -not $sgs -and -not $kps) { Write-Ok "No quedan recursos del proyecto" }

$fecha = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$txt = "# Evidencia 15 - Teardown AWS`r`n# Fecha (UTC): $fecha`r`n`r`ninstancias: $(@($quedan).Count)`r`nvolumenes: $(@($vols).Count)`r`nsecurity groups: $(@($sgs).Count)`r`nkey pairs: $(if ($kps) {1} else {0})`r`n"
[System.IO.File]::WriteAllText((Join-Path $RepoDir "evidencias\15-teardown-aws.txt"), $txt, (New-Object System.Text.UTF8Encoding $false))
Remove-Item $EstadoPath -ErrorAction SilentlyContinue
Write-Ok "evidencias\15-teardown-aws.txt guardado; estado.json eliminado"
