# Informe y manual de ejecución — EP5 DSY1107

**Streaming resiliente de ventas: integración, validación y recuperación con Kafka Streams**
Autor: Gonzalo · Fecha: 2026-11-21 · Repositorio: <https://github.com/MGcarva/Evaluacion3-Native>

> Manual completo de instalación y operación: [README](../README.md). Evidencias: [`evidencias/INDEX.md`](../evidencias/INDEX.md).

## 1. Resumen

Se construyó un pipeline Kafka Streams que valida ventas, aísla los errores en una DLQ con su causa, elimina duplicados de negocio por `eventId` y agrega por tienda en ventanas de 60 s según el tiempo del evento con 120 s de gracia. El sistema corre con Docker Compose (2 brokers, ZooKeeper, aplicación, Prometheus, Grafana), se crea y configura sin pasos manuales, tiene 23 pruebas automatizadas y fue sometido a la caída de un broker y al reinicio del consumidor, con evidencia de que no hay pérdida silenciosa.

## 2. Arquitectura

Ver diagrama en el [README §1](../README.md#1-arquitectura). Flujo: `ProducerApp`/`TestEventGenerator` → `ventas-raw` (clave `customerId`) → extractor de `eventTime` → validación (inválidos → `ventas-raw-dlq`) → re-key `eventId` → deduplicación (store + changelog) → re-key `store` → ventana tumbling + gracia + `suppress` → `ventas-agregadas` (clave `store`).

## 3. Decisiones justificadas

| Decisión | Valor | Justificación |
|---|---|---|
| Clave de entrada | `customerId` | Orden por cliente y distribución uniforme |
| Particiones / RF | 3 / 2 | Paralelismo; RF máximo con 2 brokers (RF=3 imposible) |
| `min.insync.replicas` | 1 | Disponibilidad ante la caída de un broker; con 2 la producción se bloquea (demostrado) |
| Retención | 7 d / 7 d / 30 d (DLQ) | La DLQ requiere más tiempo de análisis y reproceso |
| Ventana / gracia | 60 s / 120 s | La gracia cubre desfase del productor **y** reordenamiento tras una caída del consumidor (hallazgo §5.5) |
| Deduplicación | 10 min, store persistente | ≥ ventana + gracia; memoria acotada; sobrevive reinicios |
| Semántica | EOS v2 + productor idempotente `acks=all` | Sin efectos dobles por reintentos o fallos; los duplicados de negocio los elimina el deduplicador |

**EOS vs. duplicados de negocio.** `exactly_once_v2` hace atómicas la lectura, el cambio de estado y la escritura de cada registro: si la aplicación falla, la transacción se aborta y el registro se reprocesa sin efectos dobles. La idempotencia del productor evita que un reintento de red duplique un envío. Pero si el origen envía dos veces la misma venta, Kafka almacena dos registros distintos (offsets 27, 28 y 29 en la evidencia 03), y ambos mecanismos los procesarían "exactamente una vez" cada uno. Solo la deduplicación por `eventId` con estado identifica que son la misma venta.

## 4. Matriz de escenarios

| Escenario | Comando | Resultado esperado | Resultado obtenido (local) | Evidencia | AWS |
|---|---|---|---|---|---|
| Mensaje inválido | `docker compose run --rm generator invalidos` | App activa; DLQ con original, clave, tópico, causa; log correlacionable | 6/6 motivos correctos en DLQ; `correlationId` igual en log y DLQ; app `RUNNING` | 01, 05 | ✅ 05-aws |
| Duplicado | `docker compose run --rm generator duplicados` | Conteo y monto no se duplican | 3 registros (offsets 27-29) → `sales_count=1`, `total_clp=3000`; 2 logs `duplicado ignorado` | 03, 05 | ✅ 05-aws |
| Tardío | `docker compose run --rm generator tardios` | Dentro de gracia se suma; fuera se descarta y se detecta; no va a DLQ | Dentro: ventana 15:59 = 2000; fuera: `Skipping record for expired window`, `dropped-records` +1, 0 en DLQ | 04, 05 | ✅ 05-aws |
| Broker detenido | `bash scripts/prueba-broker-caido.sh` | ISR degradado y recuperado; explicación de min ISR y RF | ISR 1,2→1→1,2; min ISR 1: 156 ventas en 20 s; min ISR 2: 0 en 25 s; ISR completo 2 s después de healthy; 475 confirmadas = 475 en tópico | 09 | ✅ 609 = 609; min ISR 2 bloquea (09-aws) |
| Reinicio consumidor | `bash scripts/prueba-reinicio-streams.sh` | Continuidad desde offsets y changelog; sin pérdida silenciosa | Lag 85→1 tras reiniciar; restauración completa desde changelog sin volumen; duplicado detectado tras ambos reinicios; 29/29 ventanas = ventas crudas (con gracia 30 s: 22 descartadas, ver §5.5) | 10, 10a | ✅ 27/27 (10-aws) |
| Pruebas | `docker compose run --rm tests` | Todas pasan | 23/23 | 08 | ✅ 23/23 (08-aws) |

## 5. Hallazgos durante las pruebas

1. **Arranque de brokers con `NodeExists`.** Al recrear los brokers, ZooKeeper aún tenía la sesión anterior; se resuelve esperando ~40 s. Incluido en el runbook.
2. **Lag `-1` en kafka-exporter** para tópicos de repartición (Streams purga sus registros tras confirmarlos). El panel usa `clamp_min(…, 0)`.
3. **Transacciones abortadas con `min.insync.replicas=2`.** Con un broker caído, Streams no puede escribir en `ventas-agregadas`; el coordinador aborta la transacción por timeout (`InvalidProducerEpochException` → `TaskMigratedException`) y la aplicación se recupera sola (REBALANCING → RUNNING) sin salidas parciales.
4. **Tiempo de stream tras reiniciar.** La hipótesis inicial era que el reloj se reiniciaba con la app; la prueba mostró lo contrario: un evento de hace 5 min enviado justo después del reinicio se descartó como tardío.
5. **Gracia insuficiente tras una caída del consumidor (decisión de diseño).** Con gracia 30 s, una detención de ~35 s produjo 22 ventas descartadas como tardías: al procesar el atraso, una partición adelantó el reloj de la tarea de agregación antes que las otras. Coincide exactamente con `dropped-records-total = 22` en Prometheus. Se subió la gracia a 120 s y se repitió la prueba (evidencia 10).

## 6. Límites conocidos

ZooKeeper de un nodo (punto único de falla); dos brokers en una máquina no toleran la caída del nodo físico; RF=3 imposible; `suppress` retiene la última ventana; caídas del consumidor mayores a la gracia pueden descartar ventas (detectable); eventos válidos con fecha futura adelantan el reloj; Kafka sin autenticación ni TLS. Detalle en el [README §10](../README.md#10-límites-conocidos).

## 7. Recuperación

Runbook en el [README §11](../README.md#11-recuperación-runbook).

## 8. Despliegue en AWS

- **Infraestructura**: una EC2 Ubuntu 24.04 en `us-east-1`, creada con los scripts `aws/00…04` (o `desplegar-aws.bat`). Security group solo a la IP del estudiante (/32), key pair fuera del repo, mismo `docker-compose.yaml` con `.env` propio (`PUBLIC_HOST` = IP pública).
- **Límite de la cuenta**: solo se permite `t3.medium` (4 GB); `t3.large`, `t2.large` y `m5.large` dan `UnauthorizedOperation`. Se agregaron 2 GB de swap y heap de brokers de 384 MB. Consumo observado: 2,2 GB de RAM, swap sin uso (evidencia 11-aws).
- **Resultados**: los cinco escenarios y las 23 pruebas se repitieron en AWS con los mismos resultados que en local (columna AWS de la matriz; capturas 12a-12e, 13-aws, 14).
- **Hallazgos de la operación**: (a) al vencer la sesión del laboratorio, AWS elimina los recursos a mitad de una prueba (`ssh` código 255); los scripts idempotentes permitieron recrear todo con credenciales nuevas. (b) Windows PowerShell 5.1 antepone un BOM al stdin de `ssh`; se elimina antes de ejecutar los scripts remotos. (c) Un `.pem` en solo lectura impedía reemplazar la llave; `01-red-seguridad.ps1` ahora restaura permisos y verifica la escritura.
- **Teardown**: `destruir-aws.bat` elimina la instancia, el disco, el security group y el key pair, y deja la verificación en `evidencias/15-teardown-aws.txt`.
