# Explicación de las pruebas y del dashboard de Grafana

Guía para explicar en la presentación **qué demuestra cada prueba**, **qué se ve** y **qué decir**, y cómo cada prueba se refleja en Grafana. Complementa el guion [DEMO-15MIN.md](DEMO-15MIN.md).

---

# Parte 1 · Las pruebas

## 1. Configuración y tópicos · Indicador 1 (15 %)

**Comandos:** `docker compose ps` · `docker logs ventas-streams 2>&1 | grep "Kafka Streams config"` · `k config` · `k isr`

**Qué demuestra:** que todo el sistema se levanta solo y con una configuración explícita, centralizada y justificada.

**Qué se ve y cómo leerlo:**
- `docker compose ps`: todos los servicios `Up` (los brokers `healthy`). `kafka-init` y `jmx-agent` aparecen `Exited (0)`: **es correcto**, son tareas que se ejecutan una vez (crear tópicos, copiar el agente de métricas) y terminan bien.
- Línea `Kafka Streams config …`: la aplicación imprime al arrancar la configuración **que realmente está usando** (ventana 60 s, gracia 120 s, retención de deduplicación 10 min, RF 2). Sirve para comprobar qué versión corre.
- `k config`: cada valor con su origen. `[DYNAMIC_TOPIC_CONFIG]` = fijado en el tópico al crearlo; `[STATIC_BROKER_CONFIG]` = viene del docker-compose; `[DEFAULT_CONFIG]` = valor por defecto de Kafka. `retention.ms=604800000` son 7 días; `2592000000` son 30 días (DLQ).
- `k isr`: cada tópico con 3 particiones; `Replicas: 1,2` (una copia en cada broker) e `Isr: 1,2` (las dos copias al día). Los líderes están repartidos entre los brokers 1 y 2.

**Qué decir:**
> "Es el mismo docker-compose en local y en AWS; solo cambia el `.env` (la IP pública). La configuración está centralizada en `.env` y en la clase `AppConfig`, y los tópicos los crea `kafka-init` automáticamente: no hay pasos manuales. Usamos RF=2 porque es el máximo con 2 brokers (cada copia debe ir en un broker distinto) y `min.insync.replicas=1` para priorizar disponibilidad. La DLQ retiene 30 días porque investigar y reprocesar errores toma más tiempo."

---

## 2. Mensajes inválidos y DLQ · Indicador 4 (15 %)

**Comandos:** el generador (`docker compose run --rm generator todos` o `invalidos`) envía 6 eventos defectuosos; luego `k dlq 6` · `k logs | grep rechazado` · `docker compose ps streams`

**Qué envía el generador:** JSON cortado, valor vacío, venta sin `store`, `eventTime` ilegible ("ayer a las 10"), cantidad 0 y total incorrecto (2 × 1000 declarado como 5000). Todos con fecha en el pasado para no adelantar el reloj de las ventanas.

**Qué se ve y cómo leerlo** (cada mensaje de la DLQ):
- `originalValue`: el mensaje **exactamente** como llegó, incluso el JSON roto.
- `key`: la clave Kafka original (la DLQ usa la misma clave).
- `sourceTopic`: de dónde vino (`ventas-raw`).
- `reason`: el motivo preciso (`JSON_INVALIDO`, `CAMPO_OBLIGATORIO_AUSENTE: store`, `TOTAL_INCONSISTENTE: esperado=2000 recibido=5000`…).
- `correlationId`: el mismo identificador aparece en la línea `evento rechazado correlationId=…` del log → se puede cruzar log y DLQ.
- `docker compose ps streams` sigue `Up`: el error **no detuvo** el consumidor.

**Qué decir:**
> "Se distinguen errores de sintaxis (JSON inválido) y de negocio (total que no cuadra). Ninguno detiene la aplicación ni desaparece: va a la DLQ con el valor original, la clave, el tópico y la causa, para auditar y reprocesar. El `correlationId` une el log con el mensaje de la DLQ."

---

## 3. Broker caído · Indicador 6 (10 %)

**Comandos:** `docker compose stop kafka-2` → `k isr` → `k min-isr 2` → `docker compose run --rm -e PRODUCER_MAX_EVENTS=3 producer` (se bloquea, Ctrl+C) → `k min-isr 1` → `docker compose start kafka-2` → `sleep 20` → `k isr`

**Qué demuestra:** la tolerancia a la caída de un broker y la diferencia entre priorizar disponibilidad o durabilidad.

**Qué se ve y cómo leerlo:**
| Momento | `k isr` muestra | Significado |
|---|---|---|
| Antes | `Isr: 1,2`, líderes en 1 y 2 | Sano: 2 copias al día |
| Broker 2 detenido | `Isr: 1`, todos los líderes en 1; lista de sub-replicadas | Las particiones que lideraba el broker 2 pasaron al 1 (*failover*). Siguen funcionando con una sola copia |
| `min-isr 2` + producir | El productor **no avanza** | `acks=all` exige 2 copias confirmadas y solo hay 1 → `NotEnoughReplicas`. El productor reintenta (no pierde la venta) hasta que vuelva el broker o bajemos a 1 |
| `min-isr 1` | El productor se destraba | Con 1 copia mínima se acepta |
| Broker 2 reiniciado | `Isr: 1,2` de nuevo | El broker 2 copió lo que se produjo mientras estaba caído y volvió a estar "en sincronía" |

**Qué decir:**
> "Con RF=2 cada partición tiene una copia en cada broker. Al caer el broker 2, el broker 1 asume los líderes y el sistema sigue. Con `min.insync.replicas=1` priorizamos disponibilidad: se sigue vendiendo con un broker. Con 2 priorizamos durabilidad: ninguna venta queda con una sola copia, pero la producción se detiene. Al volver el broker, el ISR se recupera solo. En la prueba automatizada, las ventas confirmadas coinciden exactamente con los mensajes en el tópico: no se perdió nada. Límite: ZooKeeper es un solo nodo y los dos brokers están en la misma máquina."

---

## 4. Reinicio del consumidor · Indicador 6 (10 %)

**Comandos:** `docker compose stop streams` → producir 30 ventas → `k grupos` → `docker compose start streams` → `sleep 15` → `k logs | grep -E "restoring|RUNNING"` → `k grupos`

**Qué demuestra:** que la aplicación retoma donde quedó, sin perder ni duplicar, y reconstruye su estado.

**Qué se ve y cómo leerlo:**
- `k grupos` con Streams detenido: la columna **LAG** de `ventas-raw` sube a ~30. *Lag* = mensajes que llegaron y nadie ha procesado. `CURRENT-OFFSET` es el último confirmado por la app; `LOG-END-OFFSET`, el último escrito.
- Logs al reiniciar: `Finished restoring changelog … to store …` → el estado (ventanas abiertas, `eventId` vistos, buffer de suppress) se carga desde los **tópicos changelog** de Kafka; luego `REBALANCING to RUNNING`.
- `k grupos` después: LAG vuelve a 0 → procesó todo lo pendiente desde el último offset confirmado.

**Qué decir:**
> "Kafka guarda en qué offset quedó la aplicación; al volver continúa desde ahí, sin saltarse ni repetir mensajes (exactly_once_v2). El estado no vive solo en el disco local: cada cambio se respalda en un tópico changelog, así que incluso borrando el volumen se reconstruye. Probamos que tras reiniciar sigue detectando duplicados vistos antes, y que todas las ventanas publicadas coinciden con las ventas crudas. Con gracia de 30 s perdimos 22 ventas al reiniciar; por eso usamos 120 s."

---

## 5. Duplicados · Indicador 5 (15 %)

**Comandos:** (del generador) `k agregadas 500 | grep "Prueba Duplicados"` · `k logs | grep duplicado`

**Qué envía el generador:** la **misma venta** (mismo `eventId`) 3 veces, con claves distintas.

**Qué se ve y cómo leerlo:**
- En `ventas-raw` hay 3 mensajes (3 offsets distintos): para Kafka son 3 registros válidos.
- Log: 2 líneas `duplicado ignorado eventId=dup-… origen=…-repartition-N@offset` (la primera copia se cuenta, las otras 2 se ignoran; `origen` indica partición y offset).
- Agregado `Prueba Duplicados …`: `sales_count=1`, `total_clp=3000` (no 3 ni 9000).

**Qué decir:**
> "`exactly_once_v2` garantiza que **un mismo registro** no se procese dos veces aunque la app falle, y la idempotencia del productor evita duplicados por reintentos de red. Pero si la caja envía la venta dos veces, son **dos registros distintos** para Kafka. Eso lo resuelve nuestro deduplicador: cambiamos la clave a `eventId` para que todas las copias lleguen a la misma tarea, y un state store recuerda los `eventId` durante 10 minutos (más que ventana + gracia), respaldado por changelog."

---

## 6. Eventos tardíos · Indicador 3 (15 %)

**Comandos:** (del generador) `k agregadas 500 | grep "Prueba Tardios"` · `k logs | grep Skipping`

**Qué envía el generador** (para una tienda de prueba, con hora T0):
1. Venta normal en T0 → avanza el reloj.
2. Venta de T0 − 15 s (llega **después**, fuera de orden) → su ventana aún está dentro de la gracia.
3. Venta de hace ~4 min → su ventana ya cerró (fin + 120 s quedó atrás).

**Qué se ve y cómo leerlo:**
- Dos ventanas publicadas: la anterior con 2000 (la venta fuera de orden **sí se sumó**) y la de T0 con 1000.
- Log `Skipping record for expired window … expiration=… streamTime=…`: la venta vieja se descartó; el log muestra hasta cuándo aceptaba esa ventana y la hora del reloj.
- **No** aparece en la DLQ.

**Qué decir:**
> "Agrupamos por `eventTime`, la hora en que ocurrió la venta, no por cuándo llegó. Una ventana de 60 s acepta atrasos durante 120 s de gracia; después se cierra y `suppress` publica un único resultado final. Un tardío no es un dato inválido, por eso no va a la DLQ: se descarta y se detecta en el log y en la métrica de Grafana."

---

## 7. Pruebas automatizadas · Indicador 7 (15 %)

**Comando:** `docker compose run --rm tests` → `Tests run: 23, Failures: 0` · `BUILD SUCCESS`

**Qué demuestra:** que la lógica está verificada sin necesidad de un cluster, y que se puede repetir con un comando.

| Grupo | Qué verifica |
|---|---|
| `SaleValidatorTest` (11) | Cada motivo de rechazo y una venta válida |
| DLQ (2) | Contenido completo del mensaje de DLQ; el flujo continúa con los válidos |
| Duplicados (3) | Mismo `eventId` ×3 cuenta 1; `eventId` distintos sí cuentan; purga por retención |
| Agregación (2) | Conteo, monto, ticket promedio, inicio/fin de ventana; usa `eventTime` |
| Tardíos (2) | `suppress` espera fin + gracia; tardío dentro se suma, fuera se descarta, no va a DLQ |
| Configuración (3) | Valores por defecto y sobrescritura por entorno |

**Qué decir:**
> "Usamos `TopologyTestDriver`, que ejecuta la topología real en memoria y simula el tiempo: así probamos ventanas y gracia en segundos, sin esperar minutos. Corren dentro de Docker, sin instalar Java."

---

# Parte 2 · Grafana: qué muestra y cómo interactúa con las pruebas

## Cómo llegan los datos

```
Brokers / ZooKeeper / Streams ──(agente JMX, puerto 7071)──┐
Kafka (API) ──(kafka-exporter: ISR, offsets, lag)──────────┼──► Prometheus (lee cada 10 s) ──► Grafana (refresca cada 10 s)
```
- El **agente JMX** corre dentro de cada proceso Java y publica sus métricas internas (réplicas, controller, memoria, registros descartados, duplicados).
- **kafka-exporter** consulta Kafka desde afuera (brokers vivos, ISR por partición, offsets, lag de consumer groups).
- **Prometheus** guarda todo como series de tiempo; **Grafana** lo grafica. Por eso un cambio tarda **10–20 s** en verse.

Dashboard: *Ventas Streaming - Kafka* (se abre al entrar). Arriba a la derecha: rango (*Last 30 minutes*) y refresco (10 s).

## Fila "Cluster Kafka"

| Panel | Qué mide (fuente) | Normal | Durante las pruebas |
|---|---|---|---|
| **Brokers activos** | Brokers que ve el cluster (kafka-exporter) | **2** (verde) | Broker caído → **1** (amarillo) |
| **Particiones sub-replicadas** | Particiones con menos copias al día que el RF | **0** | Broker caído → sube (≈24 contando tópicos internos), rojo. Vuelve a 0 al reiniciar el broker |
| **Particiones bajo min ISR** | Particiones donde no se cumple `min.insync.replicas` (JMX) | **0** | Broker caído **y** `min-isr 2` → sube (9): ahí la producción se bloquea. Con `min-isr 1` vuelve a 0 aunque el broker siga caído |
| **Controller activo** | Brokers que actúan como controlador (JMX) | **1** | Debe seguir en 1 (si cae el controlador, el otro asume) |
| **Targets arriba (up)** | Fuentes que Prometheus puede leer | Todas verdes | Broker caído → el cuadro de `kafka-2` en rojo. Streams detenido → el de `kafka-streams` en rojo |
| **ISR por partición** | Copias en sincronía de cada partición de negocio (kafka-exporter) | Línea en **2** | Broker caído → baja a **1**; reiniciado → vuelve a **2**. Es el gráfico "antes / durante / después" |
| **Líderes y sub-replicadas por broker** | Líderes de cada broker y sus particiones sub-replicadas (JMX) | Líderes repartidos | Broker caído → el broker 1 tiene todos los líderes y muchas sub-replicadas. Tras reiniciar, los líderes quedan en el broker 1 hasta el rebalanceo automático (≈5 min) |

## Fila "Flujo de ventas"

| Panel | Qué mide | Durante las pruebas |
|---|---|---|
| **Mensajes por segundo por tópico** | Velocidad de escritura en `ventas-raw`, `ventas-agregadas`, `ventas-raw-dlq` | Sube cuando corre el productor o el generador. `ventas-agregadas` tiene picos pequeños cada minuto (un resultado por tienda y ventana cerrada) |
| **Lag por consumer group** | Mensajes pendientes del grupo `ventas-aggregator` por tópico | **Reinicio de Streams**: sube mientras la app está detenida (~30) y cae a 0 al volver. Es la evidencia visual de "continúa desde el offset" |
| **Mensajes en la DLQ (total)** | Total de mensajes guardados en `ventas-raw-dlq` | Sube en 6 cada vez que corre el generador (los 6 inválidos). Es **acumulado** y no se reinicia |
| **Tardíos descartados** | Métrica `dropped-records` de Kafka Streams | Sube en 1 con el tardío fuera de gracia del generador |
| **Duplicados ignorados** | Métrica propia del deduplicador | Sube en 2 por cada corrida del generador (3 copias → 2 ignoradas) |
| **Hilos de Streams vivos** | Hilos de procesamiento de la app | **1** normal; Streams detenido → sin dato; vuelve a 1 al reiniciar |
| **Eventos en la DLQ, descartes y duplicados (por minuto)** | Los tres contadores anteriores como incrementos por minuto | Picos en el minuto en que corrió el generador: sirve para mostrar *cuándo* pasó cada caso |
| **Procesamiento de Kafka Streams** | Registros/s procesados y registros/s restaurados del changelog | Cae a 0 con Streams detenido; al reiniciar hay un pico para ponerse al día con el lag |

> **Importante:** *Tardíos descartados* y *Duplicados ignorados* son contadores **en memoria de la aplicación**: después de reiniciar Streams vuelven a 0 (Prometheus conserva el historial en el gráfico por minuto). *Mensajes en la DLQ* no se reinicia porque se lee del tópico.

## Fila "JVM"

| Panel | Qué mide | Cómo leerlo |
|---|---|---|
| **Heap usado por servicio** | Memoria Java de brokers, ZooKeeper y Streams | Forma de "sierra": sube y baja con el recolector de basura (normal). En AWS (t3.medium, 4 GB) los brokers quedan bajo ~224 MiB con el límite de 384 MB. Un broker caído deja un hueco en su línea |

## Guion corto para mostrar Grafana

1. **Al inicio:** "Todo verde: 2 brokers, 0 sub-replicadas, 1 controller, todas las fuentes arriba, ISR en 2."
2. **Broker caído:** "Brokers activos baja a 1, aparecen particiones sub-replicadas, el ISR baja a 1 y en Targets `kafka-2` está caído. Con `min-isr 2` aparecen particiones bajo min ISR: ahí se bloquea la producción." Al reiniciar: "todo vuelve a verde y el ISR a 2."
3. **Reinicio de Streams:** "El lag sube mientras la app está detenida y baja a 0 al volver; los hilos vivos vuelven a 1."
4. **Generador:** "La DLQ sube en 6, los duplicados en 2 y los tardíos descartados en 1: cada caso de prueba se ve reflejado."
5. **Cierre:** "Con esto el equipo de operación detecta caídas, pérdida de réplicas, atrasos y datos problemáticos sin entrar a los logs."
