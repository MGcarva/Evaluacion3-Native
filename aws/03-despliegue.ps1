<#
  Paso 3: copia el repositorio a la EC2 y levanta EL MISMO docker-compose.yaml que en local.
  Uso: powershell -ExecutionPolicy Bypass -File aws\03-despliegue.ps1
  - Se copia el codigo (sin .env, sin target/, sin evidencias, sin estado de AWS).
  - En la EC2 se crea .env a partir de .env.example con PUBLIC_HOST=<IP publica>. El .env no se versiona.
#>
. "$PSScriptRoot\comun.ps1"
Assert-Credenciales
Show-TiempoRestante
$e = Get-Estado
if (-not $e.ipPublica) { throw "Falta la instancia: ejecuta 02-instancia.ps1" }

Write-Paso "Empaquetando el repositorio"
$tgz = Join-Path $env:TEMP "ventas-streaming.tgz"
Push-Location $RepoDir
& tar -czf $tgz --exclude=.env --exclude=./app/target --exclude=./evidencias --exclude=./aws/estado.json `
      --exclude=./.git --exclude=./.github --exclude=./.vscode --exclude="*.pem" .
Pop-Location
Write-Ok "$tgz ($([math]::Round((Get-Item $tgz).Length / 1KB)) KB)"

Write-Paso "Copiando a ubuntu@$($e.ipPublica)"
& scp -i $PemPath -o StrictHostKeyChecking=accept-new $tgz "ubuntu@$($e.ipPublica):/home/ubuntu/ventas-streaming.tgz"
if ($LASTEXITCODE -ne 0) { throw "scp fallo" }

Write-Paso "Desplegando (docker compose up -d --build) - la primera vez tarda 4-6 min"
$remoto = @"
set -e
mkdir -p ~/ventas-streaming && cd ~/ventas-streaming
tar -xzf ~/ventas-streaming.tgz
find scripts -name '*.sh' -exec sed -i 's/\r$//' {} + && chmod +x scripts/*.sh
cp .env.example .env
sed -i 's/^PUBLIC_HOST=.*/PUBLIC_HOST=$($e.ipPublica)/' .env
echo 'KAFKA_HEAP_OPTS=-Xms256m -Xmx384m' >> .env
docker compose up -d --build
echo '--- estado ---'
docker compose ps -a --format 'table {{.Name}}\t{{.Status}}'
"@
$rc = Invoke-Remoto ($remoto -replace "`r", "")
if ($rc -ne 0) { throw "El despliegue remoto fallo (codigo $rc). Si un broker no arranca por NodeExists, espera 40 s y vuelve a ejecutar este script." }

Write-Paso "Esperando que Streams quede RUNNING..."
$ini = Get-Date
do {
    Start-Sleep -Seconds 10
    $rc = Invoke-Remoto "docker logs ventas-streams 2>&1 | grep -q 'REBALANCING to RUNNING'" -Silencioso
} until ($rc -eq 0 -or ((Get-Date) - $ini).TotalMinutes -gt 5)
$null = Invoke-Remoto "docker logs ventas-streams 2>&1 | grep 'Kafka Streams config' | tail -1"
Write-Ok "Despliegue listo"
Write-Host ""
Write-Host "Grafana:    http://$($e.ipPublica):3000   (lectura anonima; admin con GRAFANA_ADMIN_PASSWORD)" -ForegroundColor Green
Write-Host "Prometheus: http://$($e.ipPublica):9090" -ForegroundColor Green
Write-Host "Siguiente: powershell -ExecutionPolicy Bypass -File aws\04-evidencias.ps1" -ForegroundColor Green
