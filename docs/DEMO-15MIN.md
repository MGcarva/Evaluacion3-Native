# Guion de demostración en AWS (15 minutos)

Idea: lo lento (generador ≈4 min y tests ≈2 min) se lanza **al inicio** en otras terminales; mientras corre se presenta lo instantáneo.

## Antes de tu turno (≈15 min antes)

1. AWS Academy: *Start Lab* → *AWS Details* → *AWS CLI: Show* → copiar el bloque `[default]` en `aws\credenciales.txt`.
2. Doble clic en **`desplegar-aws.bat`** (~10 min). Al final muestra la **IP**: anótala (también queda en `aws\estado.json`).
3. Abrir en el navegador y dejar listas las pestañas:
   - Grafana: `http://<IP>:3000` (dashboard *Ventas Streaming - Kafka*)
   - Prometheus: `http://<IP>:9090/targets`
4. SSH, Grafana y Prometheus quedan abiertos a cualquier red (temporal para la demo). Si aun así no cargan, la red de la sede puede bloquear esos puertos: usar el **celular como hotspot**.

## Minuto 0 · Abrir 3 terminales y entrar a la EC2

En **cada** terminal (PowerShell):
```powershell
ssh -i "$env:USERPROFILE\.ssh\ventas-streaming-key.pem" ubuntu@<IP>
```
Ya dentro de la EC2:
```bash
cd ~/ventas-streaming
alias k=./scripts/kafka.sh
```

**Terminal 2** (casos de prueba, ~4 min):
```bash
docker compose run --rm generator todos
```

**Terminal 3** (pruebas automatizadas, ~2-3 min):
```bash
docker compose run --rm tests
```

**Terminal 1**: presentar con los pasos siguientes.

## 1. Configuración y tópicos (min 1–2) · Indicador 1

```bash
docker compose ps
docker logs ventas-streams 2>&1 | grep "Kafka Streams config"
k config
k isr
```
Mostrar: todo `Up/healthy` en la EC2, RF=2, `min.insync.replicas=1`, retención 7 d / 30 d (DLQ), ISR `1,2`.
Decir: *"Es el mismo docker-compose que en local; solo cambia el `.env` (`PUBLIC_HOST` = IP pública). Toda la configuración está en `.env` + `AppConfig` y los tópicos los crea `kafka-init`: nada manual. RF=2 es el máximo con 2 brokers."*

## 2. Mensajes inválidos y DLQ (min 2–4) · Indicador 4

```bash
k dlq 6
k logs | grep rechazado
docker compose ps streams
```
Mostrar: `originalValue`, `key`, `sourceTopic`, `reason` y el **mismo `correlationId`** en el log; streams sigue `Up`.
Decir: *"El error no detiene el consumidor ni desaparece: queda el original para auditar y reprocesar."*

## 3. Broker caído (min 4–8) · Indicador 6

```bash
docker compose stop kafka-2
k isr
```
Grafana (`http://<IP>:3000`): *Brokers activos* 1, *Particiones sub-replicadas* > 0. Prometheus: `kafka-2` DOWN.

```bash
k min-isr 2
docker compose run --rm -e PRODUCER_MAX_EVENTS=3 producer
```
Se queda **bloqueado** (`NotEnoughReplicas`): explicar y **Ctrl+C**.

```bash
k min-isr 1
docker compose start kafka-2
sleep 20
k isr
```
ISR de vuelta a `1,2`; en Grafana *Brokers activos* vuelve a 2.
Decir: *"Con min ISR 1 priorizamos disponibilidad; con 2, durabilidad: la producción se detiene si falta una réplica."*

> ⚠️ No olvidar `k min-isr 1`.

## 4. Reinicio del consumidor (min 8–10) · Indicador 6

```bash
docker compose stop streams
docker compose run --rm -e PRODUCER_MAX_EVENTS=30 -e PRODUCE_INTERVAL=0.1 producer
k grupos
```
LAG ≈ 30 (en Grafana: panel *Lag por consumer group*).

```bash
docker compose start streams
sleep 15
k logs | grep -E "restoring|RUNNING"
k grupos
```
LAG 0; estado restaurado desde changelog.
Decir: *"Continúa desde el último offset confirmado; el estado (ventanas y deduplicación) se reconstruye desde los tópicos changelog."*

## 5. Duplicados y tardíos (min 10–12) · Indicadores 5 y 3

```bash
k agregadas 500 | grep Prueba
k logs | grep -E "duplicado|Skipping"
```
Mostrar: `Prueba Duplicados` con `sales_count=1` (enviada 3 veces); `Prueba Tardios` con la venta dentro de la gracia sumada; log `Skipping record` del tardío fuera de gracia (no está en la DLQ). En Grafana: *Mensajes en la DLQ*, *Tardíos descartados*, *Duplicados ignorados*.
Decir: *"EOS evita reprocesar el mismo registro; dos envíos de la misma venta son registros distintos para Kafka: eso lo resuelve la deduplicación por `eventId`."*

## 6. Pruebas y cierre (min 12–14) · Indicador 7

Terminal 3: **`Tests run: 23, Failures: 0`** · **`BUILD SUCCESS`**.
Cerrar en Grafana mostrando el dashboard completo (caída y recuperación del broker visibles en el gráfico de ISR).

## Plan B (si algo se demora)

Dentro de la EC2:
```bash
cat evidencias/09-broker-caido-aws.txt      # si ya corriste pruebas-aws.bat
```
O en tu PC (evidencias de ensayos anteriores):
```powershell
notepad evidencias\09-broker-caido-aws.txt
notepad evidencias\10-reinicio-streams-aws.txt
notepad evidencias\08-tests-aws.txt
```
Capturas: `evidencias\12a-grafana-aws-broker2-caido.png`, `12b-grafana-aws-recuperado.png`.

## Al terminar

`destruir-aws.bat` (borra instancia, disco, security group y llave).

---

## Anexo: misma demo en local (respaldo si AWS falla)

PowerShell en la carpeta del proyecto: `Set-ExecutionPolicy -Scope Process Bypass -Force; Set-Alias k "$PWD\scripts\kafka.ps1"`, luego los mismos pasos cambiando `grep` por `findstr` (`k logs | findstr "rechazado"`), `sleep 20` por `Start-Sleep 20` y Grafana por `http://localhost:3000`.
