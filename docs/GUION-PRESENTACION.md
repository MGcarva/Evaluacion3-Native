# Guion de estudio · Presentación (5 láminas)

Para estudiar y presentar [presentacion.html](presentacion.html). Por cada lámina: **objetivo**, **qué decir** (guion hablado), **qué hacer** en la lámina, **conceptos clave** y **preguntas probables** con su respuesta.

Tiempo sugerido: ~1 min portada, ~2,5 min por cada lámina siguiente (≈ 11 min) y el resto para la demo o preguntas.

---

## Lámina 1 · Portada

**Objetivo:** presentar el problema y la solución en una frase.

**Qué decir:**
> "Nuestro proyecto es un pipeline de ventas en tiempo real con Kafka Streams. El objetivo no es solo procesar ventas, sino que el sistema sea **resiliente**: que los datos malos no lo detengan, que una venta repetida no se cuente dos veces, que las ventas atrasadas se traten con una regla clara y que siga funcionando si se cae un broker o se reinicia la aplicación. Todo se levanta con Docker Compose, se monitorea con Prometheus y Grafana, y lo demostramos en local y en AWS."

**Qué hacer:** dejar que la terminal animada muestre `docker compose up` y los 23 tests; señalar los chips de tecnologías.

**Conceptos clave:**
- **Kafka**: plataforma de mensajería distribuida; los mensajes se guardan en **tópicos** divididos en **particiones**.
- **Kafka Streams**: librería Java para procesar los mensajes de Kafka en tiempo real (filtrar, agrupar, sumar) con estado.
- **Resiliencia**: capacidad de seguir funcionando y recuperarse ante errores y fallas.

**Pregunta probable:** *"¿Por qué Kafka y no una base de datos?"*
→ "Porque las ventas llegan como un flujo continuo: Kafka las guarda en orden, las replica entre brokers y permite procesarlas en tiempo real y re-procesarlas si hace falta."

---

## Lámina 2 · Arquitectura

**Objetivo:** explicar el recorrido de una venta y justificar las decisiones de diseño.

**Qué decir** (siguiendo el diagrama de izquierda a derecha):
> "Los **productores** envían cada venta a `ventas-raw` con clave `customerId`. La aplicación Kafka Streams hace tres pasos: **1. Validar** el JSON y las reglas de negocio; si es inválido va a la **DLQ**. **2. Deduplicar** por `eventId`. **3. Agrupar** por tienda en ventanas de 60 segundos según la hora real de la venta. El resultado sale a `ventas-agregadas`. Todo se monitorea con Prometheus y Grafana."

**Qué hacer:** hacer **clic** en *Validar*, *Dedup* y *Ventana* para mostrar el detalle; señalar los tres cuadros de abajo.

**Conceptos clave:**
| Concepto | Explicación simple |
|---|---|
| **Clave del mensaje** (`customerId`) | Decide la partición: todas las ventas de un cliente van a la misma → se mantienen **en orden** |
| **Partición** | Subdivisión de un tópico; permite paralelismo (3 particiones = hasta 3 tareas en paralelo) |
| **RF = 2** (factor de replicación) | Cada partición tiene 2 copias, una en cada broker. RF = 3 es imposible: solo hay 2 brokers |
| **min.insync.replicas = 1** | Cuántas copias deben confirmar una escritura. Con 1 se sigue vendiendo si cae un broker (disponibilidad) |
| **Re-key + repartition** | Cambiar la clave reenvía los mensajes por un tópico interno para que los iguales queden juntos (por `eventId`, luego por `store`) |
| **exactly_once_v2** | Lectura, cambio de estado y escritura en una transacción: un registro nunca tiene efecto doble |
| **Changelog** | Tópico interno donde Streams respalda su estado (dedup, ventanas); permite reconstruirlo tras un reinicio |

**Preguntas probables:**
- *"¿Por qué la clave es `customerId` y no `store`?"* → "Para garantizar el orden de las ventas de cada cliente y repartir bien la carga (200 clientes en 3 particiones). Para agregar por tienda, la app cambia la clave a `store` internamente."
- *"¿Qué pasa si quieren RF 3?"* → "Hace falta un tercer broker; con dos, cada copia debe estar en un broker distinto."
- *"¿Dónde está la configuración?"* → "Centralizada en `.env` y la clase `AppConfig`; los tópicos los crea `kafka-init` automáticamente. No hay pasos manuales."

---

## Lámina 3 · Calidad de datos (3 pestañas)

**Objetivo:** demostrar que ningún dato se pierde sin rastro y que se distinguen tres casos: inválido, duplicado y tardío.

### Pestaña "Validación y DLQ"
**Qué decir:**
> "Validamos sintaxis y reglas de negocio: por ejemplo, que el total sea cantidad por precio. Si algo falla, el consumidor **no se detiene**: el mensaje va a la **DLQ** con el valor original, la clave, el tópico de origen, el motivo y un `correlationId` que también aparece en el log, para poder cruzarlos."

**Qué hacer:** clic en **"Enviar JSON roto"** y **"Enviar total incorrecto"**; señalar la línea `WARN` del log y el mensaje de la DLQ con el mismo `correlationId`; mostrar que el estado sigue `RUNNING`.

**Conceptos:** **DLQ** (*Dead Letter Queue*): tópico donde se guardan los mensajes que no se pudieron procesar, para auditarlos y reprocesarlos. Guardamos el **original** porque sin él no se puede corregir ni reenviar la venta.

### Pestaña "Duplicados"
**Qué decir:**
> "Aquí está la diferencia clave: `exactly_once_v2` garantiza que **un mismo registro** no se procese dos veces aunque la app falle. Pero si la caja envía **la misma venta dos veces**, Kafka guarda **dos registros distintos**, con offsets distintos. Eso lo resuelve nuestro deduplicador: recuerda cada `eventId` durante 10 minutos en un state store."

**Qué hacer:** clic en **"Enviar la misma venta ×3"** → 3 registros en `ventas-raw`, 2 duplicados ignorados, `sales_count = 1`.

**Conceptos:**
- **Idempotencia del productor**: evita duplicar un envío por reintentos de red (mismo envío).
- **EOS (exactly_once_v2)**: evita efectos dobles al reprocesar tras un fallo (mismo registro).
- **Deduplicación de negocio**: detecta dos registros distintos que representan la misma venta (mismo `eventId`).
- **¿Por qué 10 min?** Debe ser mayor que ventana + gracia (3 min). Una copia que llegue después cae en una ventana ya cerrada y se descarta igual. Y la memoria queda acotada.

### Pestaña "Eventos tardíos"
**Qué decir:**
> "Agrupamos por la hora en que **ocurrió** la venta (`eventTime`), no por cuándo llegó. La ventana de 10:00 a 10:01 acepta ventas atrasadas hasta las 10:03 (120 s de gracia). Si llega después, Kafka Streams la descarta y lo registra en el log y en una métrica. Un tardío **no va a la DLQ** porque no es un dato inválido."

**Qué hacer:** clic en **"10:02:30"** (la venta vuelve a su ventana, se suma) y luego en **"10:04:00"** (se descarta, `Skipping record`). Señalar la línea rosa: es el **reloj del stream**.

**Conceptos:**
- **Ventana tumbling**: bloques fijos que no se solapan (10:00–10:01, 10:01–10:02…).
- **Gracia**: tiempo extra que una ventana espera ventas atrasadas.
- **Reloj del stream (stream time)**: la mayor hora de evento vista hasta ahora; avanza con las ventas, no con el reloj del computador.
- **suppress**: publica **un solo resultado final** por ventana cuando termina la gracia (por eso el agregado aparece unos 3 min después).
- **¿Por qué 120 s y no 30?** "Probamos con 30 s y, al reiniciar la app, se perdieron 22 ventas como tardías: al procesar el atraso, una partición adelantó el reloj antes que las otras. Con 120 s la misma prueba dio 29/29 ventanas correctas."

---

## Lámina 4 · Resiliencia

**Objetivo:** demostrar la caída de un broker, el efecto de `min.insync.replicas` y la recuperación del consumidor.

### Simulador del cluster (izquierda)
**Qué decir:**
> "Cada partición tiene una copia en cada broker y uno de ellos es el **líder** (la estrella). Si detengo `kafka-2`, el broker 1 asume todos los líderes y el **ISR** baja a 1: las particiones siguen funcionando con una sola copia. Con `min.insync.replicas = 1` se sigue produciendo. Si lo subo a 2, el productor se **bloquea** con `NotEnoughReplicas`: priorizamos durabilidad sobre disponibilidad. Al reiniciar el broker, se pone al día y el ISR vuelve a 1,2."

**Qué hacer:** clic en **"Detener kafka-2"** (líderes ★ pasan a kafka-1, ISR = 1, productor sigue) → clic en **"min.insync.replicas = 1"** para pasar a 2 (productor **BLOQUEADO**) → volver a 1 → **"Reiniciar kafka-2"**.

**Conceptos:**
- **Líder**: la réplica que recibe las escrituras y lecturas de una partición.
- **ISR** (*In-Sync Replicas*): réplicas al día con el líder.
- **Partición sub-replicada**: tiene menos copias al día que su RF.
- **acks=all**: el productor espera la confirmación de todas las réplicas en sincronía (mínimo `min.insync.replicas`).

### Reinicio del consumidor (derecha)
**Qué decir:**
> "Si detengo la aplicación, las ventas siguen llegando y el **lag** crece: son mensajes pendientes. Al reiniciar, Streams restaura su estado desde los **changelog** y continúa desde el **último offset confirmado**, sin saltarse ni repetir mensajes. El lag vuelve a 0."

**Qué hacer:** clic en **"Detener Streams"** (sube el LAG) → **"Reiniciar Streams"** (mensajes de restauración, LAG baja a 0).

**Conceptos:**
- **Offset**: posición de un mensaje en su partición; el consumidor guarda el último que procesó.
- **Lag**: diferencia entre el último mensaje escrito y el último procesado.
- **Restauración**: reconstrucción del estado local desde el changelog.

### Datos medidos (para citar)
| Dato | Significado |
|---|---|
| 179 / 20 s | Ventas confirmadas con kafka-2 caído y min ISR 1 → el sistema sigue |
| 0 / 25 s | Con min ISR 2 → producción bloqueada |
| 609 = 609 | Ventas confirmadas = mensajes en el tópico → sin pérdida |
| 27 / 27 | Ventanas correctas tras reiniciar con y sin estado local |
| 22 → 0 | Ventas perdidas con gracia 30 s vs 120 s |

**Preguntas probables:**
- *"¿Qué pasa si cae ZooKeeper?"* → "Es un punto único de falla: no habría elección de líderes ni cambios de metadatos. Lo documentamos como límite; la solución es un ensamble de 3 ZooKeeper o KRaft."
- *"¿Esto tolera la caída del servidor completo?"* → "No: los dos brokers están en la misma máquina. Tolera la caída de un proceso broker; para tolerar la del servidor habría que repartirlos en máquinas distintas."
- *"¿Se perdió alguna venta?"* → "No: en la prueba con carga continua, las 609 ventas confirmadas están en el tópico, sin duplicados."

---

## Lámina 5 · Resultados

**Objetivo:** cerrar con evidencia medible, observabilidad y despliegue reproducible.

**Qué decir:**
> "Todo está respaldado por evidencia: 23 pruebas automatizadas pasan, los 6 tipos de inválidos llegan a la DLQ con su causa, no se perdió ninguna venta con un broker caído y las ventanas coinciden exactamente tras reiniciar. En Grafana se ve la caída y la recuperación del broker, y los contadores de DLQ, tardíos y duplicados. En AWS se despliega todo con un clic usando el mismo docker-compose, y al terminar se destruye sin dejar recursos. Y documentamos los límites del diseño."

**Qué hacer:** esperar la animación de los KPIs; hacer **clic** en *Broker caído* y *Recuperado* para ampliarlas y comparar (1 → 2 brokers, 24 → 0 sub-replicadas); luego *DLQ · tardíos · duplicados*.

**Conceptos:**
- **TopologyTestDriver**: ejecuta la topología real en memoria y simula el tiempo; prueba ventanas y gracia en segundos sin cluster.
- **Observabilidad**: JMX exporter (métricas internas de brokers, ZooKeeper y Streams) + kafka-exporter (ISR, offsets, lag) → Prometheus → Grafana.
- **Reproducible**: compila y prueba dentro de Docker (sin Java local), tópicos automáticos, configuración en `.env`, sin secretos en el repositorio.

**Límites conocidos (decirlos antes de que pregunten):** ZooKeeper de un nodo; dos brokers en una máquina; RF 3 imposible; Kafka sin TLS ni autenticación; una caída del consumidor mayor a la gracia puede descartar ventas (se detecta en Grafana).

**Pregunta probable:** *"¿Qué mejorarían?"*
→ "Un tercer broker para RF 3 y min ISR 2 sin perder disponibilidad, KRaft o 3 ZooKeeper, TLS/SASL en Kafka, y alertas en Grafana para lag y particiones sub-replicadas."

---

## Repaso rápido (memorizar)

| Pregunta | Respuesta corta |
|---|---|
| ¿Por qué RF 2? | Máximo con 2 brokers; cada copia en un broker distinto |
| ¿Por qué min ISR 1? | Disponibilidad: se sigue produciendo con un broker caído |
| ¿Para qué la clave `customerId`? | Orden por cliente (misma partición) y buena distribución |
| ¿Qué guarda la DLQ? | Original, clave, tópico fuente, motivo, correlationId |
| ¿EOS evita duplicados de negocio? | No: evita reprocesar el mismo registro; dos envíos son registros distintos |
| ¿Cómo deduplicamos? | Re-key por eventId + state store 10 min con changelog |
| ¿Ventana y gracia? | 60 s tumbling por eventTime + 120 s de gracia |
| ¿Un tardío va a la DLQ? | No: se descarta y se detecta en log y métrica |
| ¿Por qué el agregado tarda 3 min? | suppress publica un solo resultado al cerrar ventana + gracia |
| ¿Cómo se recupera Streams? | Offsets confirmados + restauración desde changelog |
