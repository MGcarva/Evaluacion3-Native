<#
  Operacion e inspeccion del cluster (Windows PowerShell 5.1+).
  Uso:  powershell -ExecutionPolicy Bypass -File scripts\kafka.ps1 <accion> [argumento]
  Ejecuta "scripts\kafka.ps1 ayuda" para ver las acciones.
#>
param(
    [Parameter(Position = 0)][string]$Accion = "ayuda",
    [Parameter(Position = 1)][string]$Arg = ""
)

Set-Location (Split-Path $PSScriptRoot -Parent)
$BS = "kafka-1:29092,kafka-2:29092"
$TOPICOS = @("ventas-raw", "ventas-agregadas", "ventas-raw-dlq")
$REGEX = ($TOPICOS -join "|")
$GRUPO = "ventas-aggregator"

# Ejecuta las herramientas en un broker que este corriendo (sirve con un broker caido)
$K = "ventas-kafka-1"
if ((docker inspect -f "{{.State.Running}}" ventas-kafka-1 2>$null) -ne "true") { $K = "ventas-kafka-2" }

function Titulo($t) { Write-Host ""; Write-Host "=== $t ===" -ForegroundColor Cyan }
function Leer($topico, $max) {
    $n = if ($max) { $max } else { "50" }
    docker exec $K kafka-console-consumer --bootstrap-server $BS --topic $topico --from-beginning `
        --isolation-level read_committed --property print.key=true --property print.partition=true `
        --property print.offset=true --max-messages $n --timeout-ms 10000
}

switch ($Accion) {
    "estado" {
        Titulo "Servicios"; docker compose ps -a
        Titulo "Brokers registrados en ZooKeeper"
        docker exec ventas-zookeeper zookeeper-shell localhost:2181 ls /brokers/ids 2>$null | Select-Object -Last 1
    }
    "topicos" {
        Titulo "Topicos de negocio (particiones, lider, replicas, ISR)"
        docker exec $K kafka-topics --bootstrap-server $BS --describe --topic $REGEX
    }
    "topicos-todos" {
        Titulo "Todos los topicos (incluye internos de Streams: changelog y repartition)"
        docker exec $K kafka-topics --bootstrap-server $BS --list
    }
    "isr" {
        Titulo "Descripcion de topicos (ISR)"
        docker exec $K kafka-topics --bootstrap-server $BS --describe --topic $REGEX
        Titulo "Particiones sub-replicadas (ISR < RF) - vacio = sano"
        docker exec $K kafka-topics --bootstrap-server $BS --describe --under-replicated-partitions
        Titulo "Particiones bajo min.insync.replicas - vacio = se puede producir con acks=all"
        docker exec $K kafka-topics --bootstrap-server $BS --describe --under-min-isr-partitions
    }
    "config" {
        # Formato: clave=valor [origen]  (DYNAMIC_TOPIC_CONFIG = fijado en el topico,
        # STATIC_BROKER_CONFIG = docker-compose, DEFAULT_CONFIG = valor por defecto de Kafka)
        $formato = { $_ -replace '^\s*(\S+=\S*) sensitive=\S+ synonyms=\{(\w+):.*$', '  $1  [$2]' }
        foreach ($t in $TOPICOS) {
            Titulo "Configuracion relevante de $t"
            docker exec $K kafka-configs --bootstrap-server $BS --entity-type topics --entity-name $t --describe --all |
                Where-Object { $_ -match '^\s*(retention\.ms|min\.insync\.replicas|cleanup\.policy|unclean\.leader\.election\.enable|segment\.bytes)=' } |
                ForEach-Object $formato
        }
        Titulo "Broker 1: replicacion por defecto"
        docker exec $K kafka-configs --bootstrap-server $BS --entity-type brokers --entity-name 1 --describe --all |
            Where-Object { $_ -match '^\s*(default\.replication\.factor|min\.insync\.replicas|offsets\.topic\.replication\.factor|transaction\.state\.log\.(replication\.factor|min\.isr))=' } |
            ForEach-Object $formato
    }
    "grupos" {
        Titulo "Consumer groups"
        docker exec $K kafka-consumer-groups --bootstrap-server $BS --list
        Titulo "Offsets y lag de $GRUPO (LAG = mensajes pendientes)"
        docker exec $K kafka-consumer-groups --bootstrap-server $BS --describe --group $GRUPO
        Titulo "Miembros de $GRUPO"
        docker exec $K kafka-consumer-groups --bootstrap-server $BS --describe --group $GRUPO --members
    }
    "raw" { Titulo "ventas-raw"; Leer "ventas-raw" $Arg }
    "agregadas" { Titulo "ventas-agregadas"; Leer "ventas-agregadas" $Arg }
    "dlq" { Titulo "ventas-raw-dlq"; Leer "ventas-raw-dlq" $Arg }
    "logs" {
        Titulo "Eventos relevantes del log de ventas-streams"
        cmd /c "docker logs ventas-streams 2>&1" |
            Select-String "Kafka Streams config|evento rechazado|duplicado ignorado|Skipping record|to RUNNING|Restoration|Finished restoring|ERROR"
    }
    "min-isr" {
        if ($Arg -notin @("1", "2")) { Write-Host "Uso: kafka.ps1 min-isr 1|2"; exit 1 }
        foreach ($t in $TOPICOS) {
            docker exec $K kafka-configs --bootstrap-server $BS --entity-type topics --entity-name $t `
                --alter --add-config "min.insync.replicas=$Arg"
        }
        Titulo "min.insync.replicas ahora"
        foreach ($t in $TOPICOS) {
            docker exec $K kafka-configs --bootstrap-server $BS --entity-type topics --entity-name $t --describe |
                Select-String "min.insync"
        }
    }
    "crear-topicos" { docker compose run --rm kafka-init }
    default {
        Write-Host @"
Acciones:
  estado          servicios de compose y brokers registrados en ZooKeeper
  topicos         describe los 3 topicos (particiones, lider, replicas, ISR)
  topicos-todos   lista todos los topicos, incluidos los internos de Streams
  isr             ISR + particiones sub-replicadas + bajo min ISR
  config          retencion, min.insync.replicas, cleanup.policy de cada topico y del broker
  grupos          consumer groups, offsets, lag y miembros
  raw [N]         lee N mensajes de ventas-raw (por defecto 50)
  agregadas [N]   lee ventas-agregadas
  dlq [N]         lee ventas-raw-dlq
  logs            rechazos, duplicados, descartes por tardio, restauracion, estado
  min-isr 1|2     cambia min.insync.replicas de los 3 topicos (prueba de broker caido)
  crear-topicos   vuelve a ejecutar kafka-init (idempotente)
"@
    }
}
