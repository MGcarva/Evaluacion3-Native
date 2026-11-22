# Guion de demostración (15 minutos)

Idea: lo lento (generador ≈4 min y tests ≈2 min) se lanza **al inicio** en otras terminales; mientras corre se presenta lo instantáneo.

## Minuto 0 · Preparación (3 terminales PowerShell en la carpeta del proyecto)

**Terminal 2** (casos de prueba, ~4 min):
```powershell
docker compose run --rm generator todos
```

**Terminal 3** (pruebas automatizadas, ~2 min):
```powershell
docker compose run --rm tests
```

**Terminal 1** (presentar):
```powershell
Set-ExecutionPolicy -Scope Process Bypass -Force
Set-Alias k "$PWD\scripts\kafka.ps1"
```

## 1. Configuración y tópicos (min 1–2) · Indicador 1

```powershell
docker logs ventas-streams 2>&1 | findstr "Kafka Streams config"
k config
k isr
```
Mostrar: RF=2, `min.insync.replicas=1`, retención 7 d / 30 d (DLQ), ISR `1,2`.
Decir: *"Toda la configuración está en `.env` + `AppConfig` y los tópicos los crea `kafka-init`: nada manual. RF=2 es el máximo con 2 brokers."*

## 2. Mensajes inválidos y DLQ (min 2–4) · Indicador 4

```powershell
k dlq 6
k logs | findstr "rechazado"
docker compose ps streams
```
Mostrar: `originalValue`, `key`, `sourceTopic`, `reason` y el **mismo `correlationId`** en el log; streams sigue `Up`.
Decir: *"El error no detiene el consumidor ni desaparece: queda el original para auditar y reprocesar."*

## 3. Broker caído (min 4–8) · Indicador 6

```powershell
docker compose stop kafka-2
k isr
```
Grafana (<http://localhost:3000>): *Brokers activos* 1, *Particiones sub-replicadas* > 0.

```powershell
k min-isr 2
docker compose run --rm -e PRODUCER_MAX_EVENTS=3 producer
```
Se queda **bloqueado** (`NotEnoughReplicas`): explicar y **Ctrl+C**.

```powershell
k min-isr 1
docker compose start kafka-2
```
Esperar ~20 s:
```powershell
k isr
```
ISR de vuelta a `1,2`.
Decir: *"Con min ISR 1 priorizamos disponibilidad; con 2, durabilidad: la producción se detiene si falta una réplica."*

> ⚠️ No olvidar `k min-isr 1`.

## 4. Reinicio del consumidor (min 8–10) · Indicador 6

```powershell
docker compose stop streams
docker compose run --rm -e PRODUCER_MAX_EVENTS=30 -e PRODUCE_INTERVAL=0.1 producer
k grupos
```
LAG ≈ 30.

```powershell
docker compose start streams
```
Esperar ~15 s:
```powershell
k logs | findstr "restoring RUNNING"
k grupos
```
LAG 0; estado restaurado desde changelog.
Decir: *"Continúa desde el último offset confirmado; el estado (ventanas y deduplicación) se reconstruye desde los tópicos changelog."*

## 5. Duplicados y tardíos (min 10–12) · Indicadores 5 y 3

```powershell
k agregadas 500 | findstr "Prueba"
k logs | findstr "duplicado Skipping"
```
Mostrar: `Prueba Duplicados` con `sales_count=1` (enviada 3 veces); `Prueba Tardios` con la venta dentro de la gracia sumada; log `Skipping record` del tardío fuera de gracia (no está en la DLQ).
Decir: *"EOS evita reprocesar el mismo registro; dos envíos de la misma venta son registros distintos para Kafka: eso lo resuelve la deduplicación por `eventId`."*

## 6. Pruebas y cierre (min 12–14) · Indicador 7

Terminal 3: **`Tests run: 23, Failures: 0`** · **`BUILD SUCCESS`**.
Cerrar con Grafana: *Mensajes en la DLQ*, *Tardíos descartados*, *Duplicados ignorados*.

## Plan B (si algo se demora)

```powershell
notepad evidencias\09-broker-caido.txt      # prueba completa del broker
notepad evidencias\10-reinicio-streams.txt  # 29/29 ventanas correctas
notepad evidencias\08-tests.txt             # 23 pruebas OK
```
