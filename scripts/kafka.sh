#!/usr/bin/env bash
# Operacion e inspeccion del cluster (Linux / macOS / Git Bash / EC2).
# Uso: ./scripts/kafka.sh <accion> [argumento]     (./scripts/kafka.sh ayuda)
set -u
cd "$(dirname "$0")/.."
BS=kafka-1:29092,kafka-2:29092
TOPICOS="ventas-raw ventas-agregadas ventas-raw-dlq"
REGEX="ventas-raw|ventas-agregadas|ventas-raw-dlq"
GRUPO=ventas-aggregator
ACCION=${1:-ayuda}
ARG=${2:-}
export MSYS_NO_PATHCONV=1

# Ejecuta las herramientas en un broker que este corriendo (sirve con un broker caido)
K=ventas-kafka-1
[ "$(docker inspect -f '{{.State.Running}}' ventas-kafka-1 2>/dev/null)" = "true" ] || K=ventas-kafka-2

titulo() { printf '\n=== %s ===\n' "$1"; }
leer() {
  docker exec "$K" kafka-console-consumer --bootstrap-server $BS --topic "$1" --from-beginning \
    --isolation-level read_committed --property print.key=true --property print.partition=true \
    --property print.offset=true --max-messages "${2:-50}" --timeout-ms 10000
}

case "$ACCION" in
  estado)
    titulo "Servicios"; docker compose ps -a
    titulo "Brokers registrados en ZooKeeper"
    docker exec ventas-zookeeper zookeeper-shell localhost:2181 ls /brokers/ids 2>/dev/null | tail -1 ;;
  topicos)
    titulo "Topicos de negocio (particiones, lider, replicas, ISR)"
    docker exec "$K" kafka-topics --bootstrap-server $BS --describe --topic "$REGEX" ;;
  topicos-todos)
    titulo "Todos los topicos (incluye internos de Streams)"
    docker exec "$K" kafka-topics --bootstrap-server $BS --list ;;
  isr)
    titulo "Descripcion de topicos (ISR)"
    docker exec "$K" kafka-topics --bootstrap-server $BS --describe --topic "$REGEX"
    titulo "Particiones sub-replicadas (ISR < RF) - vacio = sano"
    docker exec "$K" kafka-topics --bootstrap-server $BS --describe --under-replicated-partitions
    titulo "Particiones bajo min.insync.replicas - vacio = se puede producir con acks=all"
    docker exec "$K" kafka-topics --bootstrap-server $BS --describe --under-min-isr-partitions ;;
  config)
    # Formato: clave=valor [origen] (DYNAMIC_TOPIC_CONFIG = topico, STATIC_BROKER_CONFIG = compose, DEFAULT_CONFIG = Kafka)
    fmt() { sed -E 's/^[[:space:]]*([^ ]+=[^ ]*) sensitive=[^ ]+ synonyms=\{([A-Z_]+):.*$/  \1  [\2]/'; }
    for t in $TOPICOS; do
      titulo "Configuracion relevante de $t"
      docker exec "$K" kafka-configs --bootstrap-server $BS --entity-type topics --entity-name "$t" --describe --all \
        | grep -E "^[[:space:]]*(retention\.ms|min\.insync\.replicas|cleanup\.policy|unclean\.leader\.election\.enable|segment\.bytes)=" | fmt
    done
    titulo "Broker 1: replicacion por defecto"
    docker exec "$K" kafka-configs --bootstrap-server $BS --entity-type brokers --entity-name 1 --describe --all \
      | grep -E "^[[:space:]]*(default\.replication\.factor|min\.insync\.replicas|offsets\.topic\.replication\.factor|transaction\.state\.log\.(replication\.factor|min\.isr))=" | fmt ;;
  grupos)
    titulo "Consumer groups"; docker exec "$K" kafka-consumer-groups --bootstrap-server $BS --list
    titulo "Offsets y lag de $GRUPO"; docker exec "$K" kafka-consumer-groups --bootstrap-server $BS --describe --group $GRUPO
    titulo "Miembros de $GRUPO"; docker exec "$K" kafka-consumer-groups --bootstrap-server $BS --describe --group $GRUPO --members ;;
  raw) titulo "ventas-raw"; leer ventas-raw "$ARG" ;;
  agregadas) titulo "ventas-agregadas"; leer ventas-agregadas "$ARG" ;;
  dlq) titulo "ventas-raw-dlq"; leer ventas-raw-dlq "$ARG" ;;
  logs)
    titulo "Eventos relevantes del log de ventas-streams"
    docker logs ventas-streams 2>&1 | grep -E "Kafka Streams config|evento rechazado|duplicado ignorado|Skipping record|to RUNNING|Restoration|Finished restoring|ERROR" ;;
  min-isr)
    case "$ARG" in 1|2) ;; *) echo "Uso: kafka.sh min-isr 1|2"; exit 1 ;; esac
    for t in $TOPICOS; do
      docker exec "$K" kafka-configs --bootstrap-server $BS --entity-type topics --entity-name "$t" --alter --add-config "min.insync.replicas=$ARG"
    done
    titulo "min.insync.replicas ahora"
    for t in $TOPICOS; do
      docker exec "$K" kafka-configs --bootstrap-server $BS --entity-type topics --entity-name "$t" --describe | grep min.insync
    done ;;
  crear-topicos) docker compose run --rm kafka-init ;;
  *)
    cat <<'EOF'
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
EOF
    ;;
esac
