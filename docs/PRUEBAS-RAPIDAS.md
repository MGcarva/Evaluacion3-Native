# Pruebas rápidas · comandos por escenario

Comandos para ejecutar **dentro de la EC2** (o en local con Git Bash). Cada bloque es independiente. Mira Grafana (`http://<IP>:3000`, rango *Last 15 minutes*) mientras los ejecutas: los cambios aparecen en 10–20 s.

## 0. Preparación (una vez por terminal)

```bash
ssh -o ServerAliveInterval=15 -i ~/.ssh/ventas-streaming-key.pem ubuntu@<IP>   # desde tu PC (en PowerShell: "$env:USERPROFILE\.ssh\...")
cd ~/ventas-streaming
alias k=./scripts/kafka.sh
BS=kafka-1:29092
```

---

## 1. Estado y salud

```bash
docker compose ps                    # todos Up/healthy (kafka-init y jmx-agent: Exited 0 es normal)
k estado                             # servicios + brokers registrados en ZooKeeper ([1, 2])
k isr                                # líder, réplicas e ISR por partición; sub-replicadas; bajo min ISR
k config                             # retención, min.insync.replicas, RF (con su origen)
k grupos                             # offsets y LAG del consumer group ventas-aggregator
docker stats --no-stream             # CPU y memoria de cada contenedor
free -m; df -h /                     # RAM y disco de la EC2
```

## 2. Producir ventas

```bash
docker compose run --rm -e PRODUCER_MAX_EVENTS=100 -e PRODUCE_INTERVAL=0.2 producer   # 100 ventas (~20 s)
docker compose run --rm -e PRODUCER_MAX_EVENTS=500 -e PRODUCE_INTERVAL=0.02 producer  # ráfaga de 500 (pico en Grafana)
```
**Carga continua en segundo plano** (útil para ver que la producción sigue durante otras pruebas):
```bash
docker compose run -d --name carga -e PRODUCE_INTERVAL=0.2 producer   # iniciar (~5 ventas/s)
docker logs carga 2>/dev/null | grep -c eventId                       # cuántas lleva confirmadas
docker stop carga && docker rm carga                                  # detener
```
**Contar mensajes en un tópico:**
```bash
docker exec ventas-kafka-1 kafka-get-offsets --bootstrap-server $BS --topic ventas-raw | awk -F: '{s+=$3} END{print s}'
```

## 3. Broker caído y recuperación (la prueba en vivo)

```bash
k isr                                  # ANTES: Isr 1,2
docker compose stop kafka-2            # cae el broker 2
k isr                                  # DURANTE: todos los líderes en 1, Isr 1, sub-replicadas
sleep 30
docker compose start kafka-2           # vuelve el broker 2
sleep 25; k isr                        # DESPUÉS: Isr 1,2 y 0 sub-replicadas
```
Grafana: *Brokers activos* 2→1→2, *Particiones sub-replicadas* sube y vuelve a 0, *ISR por partición* 2→1→2, *Targets* `kafka-2` en rojo.

**Repartir los líderes de nuevo** (tras la recuperación quedan todos en el broker 1; Kafka lo hace solo en ~5 min):
```bash
docker exec ventas-kafka-1 kafka-leader-election --bootstrap-server $BS --election-type preferred --all-topic-partitions
k topicos                              # líderes repartidos otra vez entre 1 y 2
```

## 4. min.insync.replicas = 2: durabilidad frente a disponibilidad

```bash
docker compose stop kafka-2
k min-isr 2                            # exigir 2 copias confirmadas
k isr                                  # aparecen particiones "bajo min ISR"
docker compose run --rm -e PRODUCER_MAX_EVENTS=3 producer   # se BLOQUEA (NotEnoughReplicas) → Ctrl+C
k min-isr 1                            # volver a disponibilidad
docker compose run --rm -e PRODUCER_MAX_EVENTS=3 producer   # ahora sí produce
docker compose start kafka-2
```
> ⚠️ Siempre terminar con `k min-isr 1`.

## 5. Cae el broker 1 (el otro broker y posible controller)

```bash
docker exec ventas-zookeeper zookeeper-shell localhost:2181 get /controller 2>/dev/null | tail -1   # quién es el controller
docker compose stop kafka-1
docker exec ventas-kafka-2 kafka-topics --bootstrap-server kafka-2:29092 --describe --topic ventas-raw   # líderes pasan al broker 2
docker compose run --rm -e PRODUCER_MAX_EVENTS=20 producer      # se sigue produciendo
docker compose start kafka-1
```
Muestra que la tolerancia funciona en ambos sentidos y que el rol de controller se traspasa. (Mientras kafka-1 está caído, usar `ventas-kafka-2` en los comandos: `k` lo detecta solo.)

## 6. Broker caído con carga continua (sin pérdida)

```bash
docker compose run -d --name carga -e PRODUCE_INTERVAL=0.2 producer
docker compose stop kafka-2; sleep 30; docker compose start kafka-2; sleep 25
docker stop carga                      # el productor informa "producer detenido tras N eventos"
docker logs carga | tail -1; docker rm carga
```
Comparar N con el aumento de mensajes en `ventas-raw` (sección 2): deben coincidir.
Versión automática y medida: `./scripts/prueba-broker-caido.sh` (~4 min, guarda `evidencias/09-broker-caido-aws.txt`).

## 7. Reinicio del consumidor (Kafka Streams)

```bash
docker compose stop streams
docker compose run --rm -e PRODUCER_MAX_EVENTS=50 -e PRODUCE_INTERVAL=0.05 producer
k grupos                               # LAG ≈ 50 (pendientes)
docker compose start streams; sleep 15
k logs | grep -E "restoring|RUNNING"   # restauración desde changelog
k grupos                               # LAG 0: continuó desde el último offset
```
Grafana: *Lag por consumer group* sube y baja; *Hilos de Streams vivos* desaparece y vuelve a 1.

## 8. Reinicio SIN estado local (reconstrucción completa desde changelog)

```bash
docker compose rm -sf streams
docker volume rm ventas-streaming_streams-state
docker compose up -d streams; sleep 25
k logs | grep "Finished restoring"     # cuántos registros restauró por store
k grupos
```
Versión automática (con y sin estado, verificación 100 % de ventanas): `./scripts/prueba-reinicio-streams.sh` (~9 min).

## 9. Casos de datos con el generador

```bash
docker compose run --rm generator invalidos    # 6 inválidos → DLQ (instantáneo)
docker compose run --rm generator duplicados   # misma venta ×3 (~3,5 min, espera el cierre de ventana)
docker compose run --rm generator tardios      # dentro y fuera de gracia (~4 min)
docker compose run --rm generator todos        # los tres
```
Ver resultados:
```bash
k dlq 10                               # original, clave, tópico, motivo, correlationId
k agregadas 500 | grep Prueba          # Prueba Duplicados: sales_count=1 · Prueba Tardios: ventanas
k logs | grep -E "rechazado|duplicado|Skipping"
```

## 10. Enviar mensajes a mano

```bash
P(){ docker exec -i ventas-kafka-1 kafka-console-producer --bootstrap-server $BS --topic ventas-raw --property parse.key=true --property key.separator='|'; }
T=$(date -u +%Y-%m-%dT%H:%M:%SZ)
```
**JSON roto** → DLQ con `JSON_INVALIDO`:
```bash
echo 'cliente-x|{"eventId":"roto-1","store":' | P
```
**Total incorrecto** → DLQ con `TOTAL_INCONSISTENTE`:
```bash
echo "cliente-x|{\"eventId\":\"mal-1\",\"eventTime\":\"$T\",\"store\":\"Demo\",\"quantity\":2,\"unitPriceClp\":1000,\"totalClp\":5000}" | P
```
**Venta válida enviada 3 veces** → 2 `duplicado ignorado`:
```bash
V="{\"eventId\":\"demo-dup-$RANDOM\",\"eventTime\":\"$T\",\"store\":\"Demo\",\"quantity\":1,\"unitPriceClp\":1000,\"totalClp\":1000}"
for c in A B C; do echo "cliente-$c|$V" | P; done
k logs | grep duplicado | tail -2
```
**Venta de hace 10 minutos** (fuera de gracia) → `Skipping record`, no va a la DLQ:
```bash
V10=$(date -u -d '-10 min' +%Y-%m-%dT%H:%M:%SZ)
echo "cliente-x|{\"eventId\":\"viejo-$RANDOM\",\"eventTime\":\"$V10\",\"store\":\"Demo\",\"quantity\":1,\"unitPriceClp\":1000,\"totalClp\":1000}" | P
sleep 3; k logs | grep Skipping | tail -1
```

## 11. Leer tópicos

```bash
k raw 10                               # entrada (clave, partición, offset)
k agregadas 20                         # resultados por tienda y ventana
k dlq 10                               # rechazados
docker exec ventas-kafka-1 kafka-console-consumer --bootstrap-server $BS --topic ventas-raw --partition 0 --offset 0 --max-messages 5 --property print.key=true
```
Comprobar el orden por cliente: todas las líneas con la misma clave tienen la misma `Partition`.

## 12. Métricas desde Prometheus (sin Grafana)

```bash
q(){ curl -s -G localhost:9090/api/v1/query --data-urlencode "query=$1" | grep -o '"value":\[[^]]*\]'; }
q 'kafka_brokers'                                        # brokers activos
q 'sum(kafka_topic_partition_under_replicated_partition)'  # sub-replicadas
q 'sum(clamp_min(kafka_consumergroup_lag,0))'            # lag total
q 'sum(kafka_streams_task_dropped_records_total)'        # tardíos descartados
q 'sum(kafka_streams_dedup_duplicados_ignorados_total)'  # duplicados ignorados
q 'up'                                                   # fuentes de métricas arriba (1) o caídas (0)
```

## 13. Pruebas automatizadas

```bash
docker compose run --rm tests | grep -E "Tests run:|BUILD"   # 23 pruebas, BUILD SUCCESS (~2 min)
```

## 14. ZooKeeper caído (punto único de falla) · ⚠️ con cuidado

```bash
docker compose stop zookeeper
docker compose run --rm -e PRODUCER_MAX_EVENTS=5 producer   # sigue produciendo: los brokers atienden con los metadatos que ya tienen
docker compose start zookeeper; sleep 15; k estado
```
Mientras ZooKeeper está caído **no** se pueden elegir líderes nuevos: no detener un broker al mismo tiempo. Si un broker no vuelve con `NodeExists`, esperar ~40 s y `docker compose up -d`.

## 15. Volver a la normalidad

```bash
k min-isr 1
docker compose up -d                  # levanta lo que esté detenido
docker exec ventas-kafka-1 kafka-leader-election --bootstrap-server $BS --election-type preferred --all-topic-partitions
k isr                                 # Isr 1,2 y 0 sub-replicadas
```
