#!/usr/bin/env bash
# =============================================================================
# Escenario "Reinicio del consumidor" (Kafka Streams). Reproducible en local y AWS.
#   Windows: bash scripts/prueba-reinicio-streams.sh     Linux: ./scripts/prueba-reinicio-streams.sh
# Guarda todo en evidencias/10-reinicio-streams[-aws].txt
#
#   A) Reinicio con estado local (volumen streams-state): continua desde los offsets confirmados
#   B) Reinicio SIN estado local (se borra el volumen): el estado se reconstruye desde los changelog
#   C) Evento viejo enviado justo despues del reinicio: se verifica si se descarta como tardio
#   En A y B se reenvia una venta ya vista: debe seguir detectandose como duplicado.
#   Al final: verificacion de consistencia de todas las ventanas publicadas durante la prueba.
# =============================================================================
set -u
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
SUFIJO=""; [ "${EVIDENCIA_ENTORNO:-local}" = "aws" ] && SUFIJO="-aws"
OUT="evidencias/10-reinicio-streams${SUFIJO}.txt"
BS=kafka-1:29092
K=ventas-kafka-1
CARGA=ventas-carga-reinicio
RUN=$(date -u +%H%M%S)
TIENDA="Prueba Reinicio $RUN"
mkdir -p evidencias

t() { date -u +%H:%M:%S; }
fase() { echo; echo "################ [$(t)] $* ################"; }
paso() { echo; echo "### [$(t)] $*"; }
iso() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ; }
lag() {
  docker exec "$K" kafka-consumer-groups --bootstrap-server $BS --describe --group ventas-aggregator 2>/dev/null \
    | awk 'NR>1 && $2=="ventas-raw" {l+=$6; o=o" p"$3"="$4} END{print "-- ventas-raw: lag total=" l "  offsets confirmados:" o}'
}
venta() {  # venta <eventId> <epoch> <total>
  echo "cliente-reinicio|{\"eventId\":\"$1\",\"eventTime\":\"$(iso "$2")\",\"store\":\"$TIENDA\",\"storeFormat\":\"tienda\",\"city\":\"Santiago\",\"channel\":\"presencial\",\"category\":\"ropa\",\"product\":\"Polera\",\"quantity\":1,\"unitPriceClp\":$3,\"totalClp\":$3,\"customerId\":\"cliente-reinicio\",\"paymentMethod\":\"debito\"}"
}
enviar() {
  docker exec -i "$K" kafka-console-producer --bootstrap-server $BS --topic ventas-raw \
    --property parse.key=true --property key.separator='|' --producer-property acks=all
}
esperar_running() {
  local ini; ini=$(date +%s)
  until docker logs --since "$1" ventas-streams 2>&1 | grep -q "stream-client .*REBALANCING to RUNNING" \
        || [ $(( $(date +%s) - ini )) -gt 180 ]; do sleep 2; done
  echo "-- ventas-streams en RUNNING tras $(( $(date +%s) - ini )) s"
}
restauracion() {
  docker logs --since "$1" ventas-streams 2>&1 \
    | grep -E "Finished restoring changelog|Restoration in progress" \
    | sed -E 's/.*(Finished restoring changelog [^ ]+ to store [^ ]+ with a total number of [0-9]+ records).*/  \1/; s/.*(Restoration in progress for [0-9]+ partitions).*/  \1/' \
    | sort | uniq -c
}
duplicados_de() { docker logs --since "$2" ventas-streams 2>&1 | grep "duplicado ignorado eventId=$1" | sed -E 's/.*(duplicado ignorado.*)/  \1/'; }

{
echo "# Evidencia 10 - Reinicio del consumidor Kafka Streams (indicador 6)"
echo "# Fecha (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)  Entorno: ${EVIDENCIA_ENTORNO:-local}"
echo "# Comando: bash scripts/prueba-reinicio-streams.sh"
INICIO=$(date -u +%Y-%m-%dT%H:%M:00Z)

fase "PREPARACION - carga continua + una venta marcada (para probar la deduplicacion tras cada reinicio)"
docker rm -f "$CARGA" >/dev/null 2>&1
docker compose run -d --name "$CARGA" -e PRODUCE_INTERVAL=0.3 producer >/dev/null 2>&1
MARCA="reinicio-$RUN"
venta "$MARCA" "$(date +%s)" 1000 | enviar
sleep 10
lag

fase "A) REINICIO CON ESTADO LOCAL (docker compose stop/start streams, volumen conservado)"
docker compose stop streams 2>&1 | tail -1
paso "Streams detenido 20 s: el productor sigue escribiendo y el lag crece"
sleep 20
lag
DESDE_A=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker compose start streams 2>&1 | tail -1
esperar_running "$DESDE_A"
paso "Restauracion (con checkpoint local solo se lee lo que falta del changelog):"
restauracion "$DESDE_A"
sleep 10
paso "Tras el reinicio el lag vuelve a ~0 (continua desde los offsets confirmados, sin saltarse mensajes)"
lag
paso "Reenvio de la venta marcada $MARCA -> debe ser duplicado (el estado de deduplicacion sobrevivio)"
venta "$MARCA" "$(date +%s)" 1000 | enviar
sleep 5
duplicados_de "$MARCA" "$DESDE_A"

fase "B) REINICIO SIN ESTADO LOCAL (se elimina el contenedor y el volumen streams-state)"
VOL=$(docker volume ls -q | grep -E 'streams-state$' | head -1)
docker compose rm -sf streams 2>&1 | tail -1
docker volume rm "$VOL" >/dev/null && echo "-- volumen eliminado: $VOL"
sleep 10
lag
DESDE_B=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker compose up -d streams 2>&1 | tail -1
esperar_running "$DESDE_B"
paso "Restauracion completa desde los topicos changelog (registros por store):"
restauracion "$DESDE_B"
sleep 10
lag
paso "Reenvio de la venta marcada $MARCA -> debe seguir siendo duplicado (estado reconstruido del changelog)"
venta "$MARCA" "$(date +%s)" 1000 | enviar
sleep 5
duplicados_de "$MARCA" "$DESDE_B"

fase "C) EVENTO VIEJO JUSTO DESPUES DEL REINICIO (tiempo de stream tras reiniciar)"
VIEJO=$(( $(date +%s) - 300 ))
paso "Se envia una venta con eventTime de hace 5 min ($(iso $VIEJO)); la gracia es 30 s"
DESDE_C=$(date -u +%Y-%m-%dT%H:%M:%SZ)
venta "viejo-$RUN" "$VIEJO" 7777 | enviar
sleep 5
if docker logs --since "$DESDE_C" ventas-streams 2>&1 | grep -q "Skipping record for expired window"; then
  echo "-- RESULTADO: descartado como tardio (el tiempo de stream se conserva tras el reinicio)"
  docker logs --since "$DESDE_C" ventas-streams 2>&1 | grep "Skipping record for expired window" | tail -1 | sed -E 's/.*(Skipping.*)/  \1/'
else
  echo "-- RESULTADO: NO descartado (el tiempo de stream de la tarea se reinicio con la app)"
fi

fase "CIERRE - se detiene la carga y se verifica la consistencia de las ventanas publicadas"
docker stop -t 20 "$CARGA" >/dev/null
echo "-- $(docker logs "$CARGA" 2>&1 | grep 'producer detenido' | tail -1)"
docker rm -f "$CARGA" >/dev/null 2>&1
CFG=$(docker logs ventas-streams 2>&1 | grep "Kafka Streams config" | tail -1)
VENTANA=$(echo "$CFG" | sed -nE 's/.*ventana=([0-9]+)s.*/\1/p'); GRACIA=$(echo "$CFG" | sed -nE 's/.*gracia=([0-9]+)s.*/\1/p')
ESPERA=$(( ${VENTANA:-60} + ${GRACIA:-120} + 5 ))
paso "Esperando $ESPERA s (ventana ${VENTANA}s + gracia ${GRACIA}s + 5) para cerrar las ultimas ventanas; luego un empujador..."
sleep "$ESPERA"
docker compose run --rm -e PRODUCER_MAX_EVENTS=20 -e PRODUCE_INTERVAL=0.05 producer >/dev/null 2>&1
sleep 10
lag
paso "Verificacion: cada ventana publicada desde $INICIO vs. ventas crudas (validas, sin duplicados, por eventTime)"
{
  docker exec "$K" kafka-console-consumer --bootstrap-server $BS --topic ventas-raw --from-beginning --timeout-ms 15000 2>/dev/null | sed 's/^/RAW\t/'
  docker exec "$K" kafka-console-consumer --bootstrap-server $BS --topic ventas-agregadas --from-beginning --isolation-level read_committed --timeout-ms 15000 2>/dev/null | sed 's/^/AGG\t/'
} | docker run --rm -i python:3.12-alpine python -c "$(cat scripts/verificar_consistencia.py)" "$INICIO"
echo "-- errores en ventas-streams desde el ultimo arranque: $(docker logs --since "$DESDE_B" ventas-streams 2>&1 | grep -c ' ERROR ')"
echo; echo "# Fin $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} 2>&1 | tee "$OUT"
