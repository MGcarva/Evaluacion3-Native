# Comandos para la evaluación (chuleta)

Todos se ejecutan en **PowerShell**, desde la carpeta del proyecto. Los `.bat` también se pueden abrir con doble clic.

## A. Levantar y verificar

| Qué | Comando |
|---|---|
| Levantar todo en local | `levantar-local.bat`  (o `docker compose up -d --build`) |
| Ver servicios | `docker compose ps -a` |
| Versión y configuración que corre | `docker logs ventas-streams 2>&1 \| findstr "Kafka Streams config"` |
| Brokers vivos + servicios | `powershell -ExecutionPolicy Bypass -File scripts\kafka.ps1 estado` |
| Grafana / Prometheus | <http://localhost:3000> · <http://localhost:9090/targets> |
| Detener (sin borrar datos) | `docker compose down` |

> Para abreviar: `Set-Alias k "$PWD\scripts\kafka.ps1"` y luego `k isr`, `k grupos`, etc. (si PowerShell bloquea scripts: `Set-ExecutionPolicy -Scope Process Bypass`).

## B. Tópicos, ISR, configuración y consumer groups

| Qué | Comando |
|---|---|
| Particiones, líder, réplicas, ISR | `scripts\kafka.ps1 topicos` |
| ISR + sub-replicadas + bajo min ISR | `scripts\kafka.ps1 isr` |
| Retención, min.insync.replicas, RF (con origen) | `scripts\kafka.ps1 config` |
| Consumer groups, offsets y **lag** | `scripts\kafka.ps1 grupos` |
| Todos los tópicos (incluye changelog/repartition) | `scripts\kafka.ps1 topicos-todos` |
| Volver a crear tópicos (idempotente) | `scripts\kafka.ps1 crear-topicos` |

## C. Producir y leer

| Qué | Comando |
|---|---|
| Ventas válidas continuas (Ctrl+C para parar) | `docker compose run --rm producer` |
| 50 ventas y termina | `docker compose run --rm -e PRODUCER_MAX_EVENTS=50 producer` |
| Leer entrada / agregados / DLQ | `scripts\kafka.ps1 raw 20` · `scripts\kafka.ps1 agregadas` · `scripts\kafka.ps1 dlq` |
| Logs relevantes (rechazos, duplicados, tardíos, restauración) | `scripts\kafka.ps1 logs` |

> Un agregado aparece **3 min** después de abrir su ventana (60 s + 120 s de gracia) y solo si llega un evento posterior.

## D. Escenarios de la rúbrica

| Escenario | Comando | Qué mostrar |
|---|---|---|
| **Mensaje inválido** (6 tipos) | `docker compose run --rm generator invalidos` → `scripts\kafka.ps1 dlq` → `scripts\kafka.ps1 logs` | DLQ con `originalValue`, `key`, `sourceTopic`, `reason`; el mismo `correlationId` en el log; `docker compose ps` → streams sigue `Up` |
| **Duplicado** | `docker compose run --rm generator duplicados` (≈3,5 min) → `scripts\kafka.ps1 agregadas` | `Prueba Duplicados …` con `sales_count=1`; 2 logs `duplicado ignorado`; panel *Duplicados ignorados* |
| **Tardío** dentro/fuera de gracia | `docker compose run --rm generator tardios` (≈4 min) → `scripts\kafka.ps1 agregadas` → `scripts\kafka.ps1 logs` | Ventana anterior con 2000 (dentro de gracia); log `Skipping record for expired window`; panel *Tardíos descartados* +1; **no** está en la DLQ |
| Todo lo anterior junto | `docker compose run --rm generator todos` | — |
| **Broker detenido** (automático) | `bash scripts/prueba-broker-caido.sh` (≈4 min, Git Bash) | ISR 1,2 → 1 → 1,2; min ISR 1 produce, min ISR 2 bloquea; confirmadas = nuevas en `ventas-raw` → `evidencias\09-broker-caido.txt` |
| **Broker detenido** (manual, paso a paso) | `scripts\kafka.ps1 isr` → `docker compose stop kafka-2` → `scripts\kafka.ps1 isr` → `scripts\kafka.ps1 min-isr 2` → `docker compose run --rm -e PRODUCER_MAX_EVENTS=3 producer` (queda bloqueado: Ctrl+C) → `scripts\kafka.ps1 min-isr 1` → `docker compose start kafka-2` → `scripts\kafka.ps1 isr` | Grafana: *Brokers activos* 1→2, *Particiones sub-replicadas* |
| **Reinicio del consumidor** (automático) | `bash scripts/prueba-reinicio-streams.sh` (≈7 min) | Lag sube y vuelve a 0; `Finished restoring changelog`; duplicado detectado tras reiniciar; `RESULTADO: OK` de consistencia |
| **Reinicio** (manual) | `scripts\kafka.ps1 grupos` → `docker compose stop streams` → producir → `scripts\kafka.ps1 grupos` (lag sube) → `docker compose start streams` → `scripts\kafka.ps1 logs` → `scripts\kafka.ps1 grupos` (lag 0) | Sin volumen: `docker compose rm -sf streams; docker volume rm ventas-streaming_streams-state; docker compose up -d streams` |
| **Pruebas automatizadas** | `docker compose run --rm tests` | `Tests run: 23, Failures: 0` · `python scripts\reporte_tests.py` |

Guardar cualquier salida como evidencia: `scripts\evidencia.ps1 20-mi-prueba "powershell -ExecutionPolicy Bypass -File scripts\kafka.ps1 isr"`

## E. AWS en un paso

1. En AWS Academy: *Start Lab* → *AWS Details* → *AWS CLI: Show* → copiar el bloque `[default]`.
2. Pegarlo en `aws\credenciales.txt` (si no existe, `desplegar-aws.bat` lo crea y abre el Bloc de notas). **Nunca se sube a git.**
3. Ejecutar:

| Qué | Comando |
|---|---|
| Desplegar todo (~10 min) | `desplegar-aws.bat` |
| Desplegar + todos los escenarios + evidencias (~35 min) | `desplegar-aws.bat pruebas` |
| Un escenario en la EC2 | `pruebas-aws.bat broker` · `reinicio` · `generador` · `base` · `traer` |
| Entrar a la EC2 | `ssh -i "$env:USERPROFILE\.ssh\ventas-streaming-key.pem" ubuntu@<IP>` y luego `cd ventas-streaming; ./scripts/kafka.sh isr` |
| Borrar todo en AWS | `destruir-aws.bat` |

- La IP aparece al final del despliegue (y en `aws\estado.json`). Grafana: `http://<IP>:3000`.
- Los puertos solo se abren a **tu IP**: si cambias de red, vuelve a ejecutar `desplegar-aws.bat` (actualiza el security group).
- Si el laboratorio expira, los recursos se borran: con credenciales nuevas, `desplegar-aws.bat` recrea todo.
