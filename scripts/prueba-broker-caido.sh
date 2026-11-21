#!/usr/bin/env bash
# =============================================================================
# Escenario "Broker detenido" (requisito 9, indicador 6). Reproducible en local y AWS.
#   Windows:  bash scripts/prueba-broker-caido.sh          (Git Bash)
#   Linux:    ./scripts/prueba-broker-caido.sh
# Guarda todo en evidencias/09-broker-caido[-aws].txt
#
# Fases:
#   0) ANTES: ISR completo, carga de ventas en curso (productor acks=all, idempotente)
#   1) Se detiene kafka-2 con min.insync.replicas=1 -> ISR degradado, se sigue produciendo
#   2) Con kafka-2 caido se sube min.insync.replicas=2 -> la produccion con acks=all se bloquea
#   3) Se vuelve a min.insync.replicas=1 -> la produccion se reanuda (reintentos, sin perdida)
#   4) Se reinicia kafka-2 -> se mide cuanto tarda el ISR en recuperarse; eleccion de lider preferido
#   5) Conteo: ventas confirmadas por el productor vs mensajes nuevos en ventas-raw
# =============================================================================
set -u
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
SUFIJO=""; [ "${EVIDENCIA_ENTORNO:-local}" = "aws" ] && SUFIJO="-aws"
OUT="evidencias/09-broker-caido${SUFIJO}.txt"
# Las herramientas se conectan solo a kafka-1 (nunca se detiene) para evitar avisos de DNS de kafka-2
BS=kafka-1:29092
K=ventas-kafka-1
CARGA=ventas-carga
TOPICOS="ventas-raw ventas-agregadas ventas-raw-dlq"
mkdir -p evidencias

t() { date -u +%H:%M:%S; }
fase() { echo; echo "################ [$(t)] $* ################"; }
paso() { echo; echo "### [$(t)] $*"; }
kx() { docker exec "$K" "$@"; }
isr() { kx kafka-topics --bootstrap-server $BS --describe --topic 'ventas-raw|ventas-agregadas|ventas-raw-dlq' | grep Partition: ; }
sub() { echo "-- sub-replicadas:"; kx kafka-topics --bootstrap-server $BS --describe --under-replicated-partitions | grep -c Partition: ; }
bajo_min() { echo "-- bajo min ISR:"; kx kafka-topics --bootstrap-server $BS --describe --under-min-isr-partitions | grep -c Partition: ; }
brokers() { echo "-- brokers en ZooKeeper: $(docker exec ventas-zookeeper zookeeper-shell localhost:2181 ls /brokers/ids 2>/dev/null | tail -1)"; }
offsets() { kx kafka-get-offsets --bootstrap-server $BS --topic ventas-raw | awk -F: '{s+=$3} END{print s}'; }
enviados() { docker logs "$CARGA" 2>/dev/null | grep -c '"eventId"'; }
min_isr() {
  for tp in $TOPICOS; do
    kx kafka-configs --bootstrap-server $BS --entity-type topics --entity-name "$tp" --alter --add-config "min.insync.replicas=$1" >/dev/null
  done
  echo "-- min.insync.replicas=$1 aplicado a: $TOPICOS"
}
lag() { kx kafka-consumer-groups --bootstrap-server $BS --describe --group ventas-aggregator 2>/dev/null | awk 'NR>1 && $2=="ventas-raw" {l+=$6} END{print "-- lag total de ventas-aggregator en ventas-raw: " l}'; }
streams_ok() {
  local h
  h=$(curl -s -G "localhost:${PROMETHEUS_PORT:-9090}/api/v1/query" \
        --data-urlencode 'query=sum(kafka_streams_alive_stream_threads)' 2>/dev/null \
      | sed -nE 's/.*"value":\[[^,]*,"([^"]*)"\].*/\1/p')
  echo "-- ventas-streams: $(docker inspect -f '{{.State.Status}}' ventas-streams), hilos de procesamiento vivos (Prometheus): ${h:-sin dato}"
}

{
echo "# Evidencia 09 - Prueba de broker detenido (requisito 9 / indicador 6)"
echo "# Fecha (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)  Entorno: ${EVIDENCIA_ENTORNO:-local}"
echo "# Comando: bash scripts/prueba-broker-caido.sh"

fase "FASE 0 - ANTES (cluster sano)"
min_isr 1
brokers; isr; sub; bajo_min
docker rm -f "$CARGA" >/dev/null 2>&1
O_INI=$(offsets)
docker compose run -d --name "$CARGA" -e PRODUCE_INTERVAL=0.2 producer >/dev/null 2>&1
sleep 10
E0=$(enviados)
paso "Carga en curso: productor con acks=all, idempotente, ~5 ventas/s. Offsets ventas-raw al inicio=$O_INI, enviadas=$E0"
lag; streams_ok

fase "FASE 1 - Se detiene kafka-2 (docker compose stop kafka-2) con min.insync.replicas=1"
docker compose stop kafka-2 2>&1 | tail -1
sleep 20
brokers; isr; sub; bajo_min
E1=$(enviados)
paso "Produccion durante la caida (min ISR=1): $((E1 - E0)) ventas confirmadas en ~20 s -> SE SIGUE PRODUCIENDO"
lag; streams_ok

fase "FASE 2 - Con kafka-2 caido: min.insync.replicas=2 (prioriza durabilidad)"
min_isr 2
sleep 5
E2a=$(enviados)
sleep 25
E2b=$(enviados)
bajo_min
paso "Produccion con min ISR=2 y 1 solo broker: $((E2b - E2a)) ventas confirmadas en 25 s -> BLOQUEADA (NotEnoughReplicas, el productor reintenta)"
echo "-- advertencias del productor:"
docker logs "$CARGA" 2>&1 | grep -E "NOT_ENOUGH_REPLICAS|NotEnoughReplicas" | tail -2 | cut -c1-220

fase "FASE 3 - Se vuelve a min.insync.replicas=1 (kafka-2 sigue caido)"
min_isr 1
sleep 15
E3=$(enviados)
paso "Produccion reanudada: $((E3 - E2b)) ventas confirmadas en 15 s (los envios bloqueados se completan por reintento)"
streams_ok

fase "FASE 4 - Se reinicia kafka-2 (docker compose start kafka-2)"
T_START=$(date +%s)
docker compose start kafka-2 2>&1 | tail -1
until [ "$(docker inspect -f '{{.State.Health.Status}}' ventas-kafka-2)" = "healthy" ] || [ $(( $(date +%s) - T_START )) -gt 180 ]; do sleep 2; done
T_HEALTHY=$(date +%s)
paso "kafka-2 healthy en $((T_HEALTHY - T_START)) s"
until [ "$(kx kafka-topics --bootstrap-server $BS --describe --under-replicated-partitions | grep -c Partition:)" = "0" ] || [ $(( $(date +%s) - T_HEALTHY )) -gt 180 ]; do sleep 2; done
paso "ISR completo de nuevo $(( $(date +%s) - T_HEALTHY )) s despues de healthy (replica 2 se puso al dia copiando lo producido durante la caida)"
brokers; isr; sub; bajo_min
paso "Lideres: tras el reinicio todos quedan en el broker 1. Eleccion de lider preferido para repartirlos:"
kx kafka-leader-election --bootstrap-server $BS --election-type preferred --all-topic-partitions 2>&1 | tail -2
sleep 3
isr

fase "FASE 5 - Fin de la carga y conteo (sin perdida silenciosa)"
docker stop -t 20 "$CARGA" >/dev/null
sleep 2
O_FIN=$(offsets); E_FIN=$(enviados)
echo "-- resumen del productor: $(docker logs "$CARGA" 2>&1 | grep -E "producer detenido" | tail -1)"
echo "-- ventas confirmadas (acks=all) por el productor: $E_FIN"
echo "-- mensajes nuevos en ventas-raw (offset final $O_FIN - offset inicial $O_INI): $((O_FIN - O_INI))"
if [ "$E_FIN" -eq "$((O_FIN - O_INI))" ]; then
  echo "-- RESULTADO: coinciden -> ninguna venta confirmada se perdio y no hubo duplicados por reintentos (idempotencia)"
else
  echo "-- RESULTADO: NO coinciden (revisar)"
fi
lag; streams_ok
echo "-- errores en ventas-streams durante la prueba:"
docker logs --since 10m ventas-streams 2>&1 | grep -c -E " ERROR "
docker rm -f "$CARGA" >/dev/null 2>&1
echo; echo "# Fin $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} 2>&1 | tee "$OUT"
