<#
  Paso 4: ejecuta los escenarios EN LA EC2 y trae las evidencias (sufijo -aws) a evidencias\.
  Uso: powershell -ExecutionPolicy Bypass -File aws\04-evidencias.ps1 [-Escenario todo|base|generador|broker|reinicio|traer]
  Orden por defecto (lo que mas tarda primero): broker -> reinicio -> generador -> base -> traer.
  Cada escenario se puede repetir por separado si vencen las credenciales (la EC2 sigue corriendo).
#>
param([ValidateSet("todo", "base", "generador", "broker", "reinicio", "traer")][string]$Escenario = "todo")
. "$PSScriptRoot\comun.ps1"
Assert-Credenciales
Show-TiempoRestante
$e = Get-Estado
if (-not $e.ipPublica) { throw "Falta la instancia: ejecuta 02-instancia.ps1 y 03-despliegue.ps1" }

$inicio = "cd ~/ventas-streaming && export EVIDENCIA_ENTORNO=aws`n"

# Los scripts remotos van en here-strings literales (@' '@): sin problemas de comillas.
$escenarios = [ordered]@{
    broker    = @{ titulo = "Broker 2 detenido (min ISR 1 y 2), ~4 min"; script = @'
./scripts/prueba-broker-caido.sh | tail -25
'@ }
    reinicio  = @{ titulo = "Reinicio de Kafka Streams (con y sin estado local), ~9 min"; script = @'
./scripts/prueba-reinicio-streams.sh | tail -20
'@ }
    generador = @{ titulo = "Generador de casos (invalidos, duplicados, tardios), ~4 min"; script = @'
./scripts/evidencia.sh 05-generador-aws '
docker compose run --rm generator todos 2>&1 | grep -E "^(==|--|  \[)"
sleep 10
echo; echo "## DLQ"; ./scripts/kafka.sh dlq 30 | grep -E "invalid|inv-" | tail -6
echo; echo "## Agregados de prueba"; ./scripts/kafka.sh agregadas 500 | grep "Prueba"
echo; echo "## Logs"; ./scripts/kafka.sh logs | grep -E "duplicado|Skipping|rechazado" | tail -10' | tail -30
'@ }
    base      = @{ titulo = "Estado, ISR, configuracion, consumer groups, tests y recursos"; script = @'
./scripts/evidencia.sh 02-estado-aws './scripts/kafka.sh estado' >/dev/null
./scripts/evidencia.sh 03-isr-aws './scripts/kafka.sh isr' >/dev/null
./scripts/evidencia.sh 06-config-aws './scripts/kafka.sh config' >/dev/null
./scripts/evidencia.sh 07-grupos-aws './scripts/kafka.sh grupos' >/dev/null
./scripts/evidencia.sh 08-tests-aws 'docker compose run --rm tests 2>&1 | grep -E "Tests run:|BUILD"' | tail -4
./scripts/evidencia.sh 11-recursos-aws 'free -m; df -h /; docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"' | tail -15
'@ }
}

foreach ($nombre in $escenarios.Keys) {
    if ($Escenario -ne "todo" -and $Escenario -ne $nombre) { continue }
    Write-Paso $escenarios[$nombre].titulo
    $rc = Invoke-Remoto ($inicio + $escenarios[$nombre].script)
    if ($rc -ne 0) { Write-Aviso "$nombre termino con codigo $rc" } else { Write-Ok $nombre }
    Show-TiempoRestante
}

Write-Paso "Trayendo evidencias *-aws a evidencias\"
$destino = Join-Path $RepoDir "evidencias"
New-Item -ItemType Directory -Force $destino | Out-Null
& scp -i $PemPath -o LogLevel=ERROR "ubuntu@$($e.ipPublica):ventas-streaming/evidencias/*-aws.txt" $destino
Get-ChildItem $destino -Filter "*-aws.txt" | ForEach-Object { Write-Ok $_.Name }

Write-Host ""
Write-Host "Capturas manuales sugeridas (guardar en evidencias\ con estos nombres):" -ForegroundColor Green
Write-Host "  12-grafana-aws.png      http://$($e.ipPublica):3000  (dashboard 'Ventas Streaming - Kafka', ultimos 30 min)"
Write-Host "  13-prometheus-aws.png   http://$($e.ipPublica):9090/targets  (todos UP)"
Write-Host "  14-consola-ec2-aws.png  Consola AWS > EC2 > Instancias (instancia $($e.instanceId) running)"
Write-Host "Al terminar: powershell -ExecutionPolicy Bypass -File aws\99-destruir.ps1" -ForegroundColor Yellow
