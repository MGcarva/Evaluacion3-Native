import json, sys

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
pid = 0


def add(kind, title, exprs, x, y, w, h, desc="", unit=None, thresholds=None, legend=None, extra=None):
    global pid
    pid += 1
    targets = []
    for i, e in enumerate(exprs):
        expr, leg = e if isinstance(e, tuple) else (e, legend or "")
        targets.append({"datasource": DS, "expr": expr, "legendFormat": leg, "refId": chr(65 + i)})
    defaults = {"color": {"mode": "palette-classic" if kind == "timeseries" else "thresholds"}}
    if unit:
        defaults["unit"] = unit
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
    p = {
        "id": pid, "type": kind, "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {},
    }
    if kind == "stat":
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background", "graphMode": "area", "textMode": "value_and_name"}
    if kind == "timeseries":
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi"}}
        defaults["custom"] = {"lineWidth": 2, "fillOpacity": 10, "showPoints": "never"}
    if extra:
        p.update(extra)
    panels.append(p)


def row(title, y):
    global pid
    pid += 1
    panels.append({"id": pid, "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})


GREEN, RED, YELLOW = "green", "red", "yellow"

row("Cluster Kafka", 0)
add("stat", "Brokers activos", ["kafka_brokers"], 0, 1, 4, 4,
    "Brokers que reporta el cluster (kafka-exporter). Esperado: 2.",
    thresholds=[{"color": RED, "value": None}, {"color": YELLOW, "value": 1}, {"color": GREEN, "value": 2}])
add("stat", "Particiones sub-replicadas", ["sum(kafka_topic_partition_under_replicated_partition)"], 4, 1, 4, 4,
    "Particiones cuyo ISR es menor que el RF. >0 cuando un broker esta caido.",
    thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 1}])
add("stat", "Particiones bajo min ISR", ["sum(kafka_server_replicamanager_underminisrpartitioncount)"], 8, 1, 4, 4,
    "Con min.insync.replicas=2 y un broker caido, la produccion con acks=all falla.",
    thresholds=[{"color": GREEN, "value": None}, {"color": RED, "value": 1}])
add("stat", "Controller activo", ["sum(kafka_controller_kafkacontroller_activecontrollercount)"], 12, 1, 4, 4,
    "Debe ser exactamente 1.",
    thresholds=[{"color": RED, "value": None}, {"color": GREEN, "value": 1}, {"color": RED, "value": 2}])
add("stat", "Targets arriba (up)", [("up", "{{job}} {{instance}}")], 16, 1, 8, 4,
    "1 = Prometheus puede leer las metricas del servicio (brokers, ZooKeeper, Streams, kafka-exporter).",
    thresholds=[{"color": RED, "value": None}, {"color": GREEN, "value": 1}],
    extra={"options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                       "colorMode": "background", "graphMode": "none", "textMode": "name",
                       "orientation": "vertical"}})

add("timeseries", "ISR por particion (topicos de negocio)",
    [("kafka_topic_partition_in_sync_replica{topic=~\"ventas-raw|ventas-agregadas|ventas-raw-dlq\"}",
      "{{topic}}-{{partition}}")], 0, 5, 12, 8,
    "Cantidad de replicas en sincronia. Normal = 2; baja a 1 al detener un broker y vuelve a 2 al recuperarlo.")
add("timeseries", "Lideres y particiones sub-replicadas por broker",
    [("kafka_server_replicamanager_leadercount", "lideres {{instance}}"),
     ("kafka_server_replicamanager_underreplicatedpartitions", "sub-replicadas {{instance}}")], 12, 5, 12, 8,
    "JMX de cada broker. Al caer un broker, el otro asume todos los lideres.")

row("Flujo de ventas", 13)
add("timeseries", "Mensajes por segundo por topico",
    [("sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~\"ventas-raw|ventas-agregadas|ventas-raw-dlq\"}[1m]))",
      "{{topic}}")], 0, 14, 12, 8, "Tasa de escritura (offset final) en cada topico.", unit="ops")
add("timeseries", "Lag por consumer group",
    [("sum by (consumergroup, topic) (clamp_min(kafka_consumergroup_lag{topic=~\"ventas-raw|ventas-aggregator.*\"}, 0))",
      "{{consumergroup}} / {{topic}}")], 12, 14, 12, 8,
    "Mensajes pendientes de procesar. Sube si la app se detiene y baja al reanudar.")
add("stat", "Mensajes en la DLQ (total)",
    ["sum(kafka_topic_partition_current_offset{topic=\"ventas-raw-dlq\"}) - sum(kafka_topic_partition_oldest_offset{topic=\"ventas-raw-dlq\"})"],
    0, 22, 6, 5, "Eventos invalidos rechazados y conservados en ventas-raw-dlq.",
    thresholds=[{"color": GREEN, "value": None}, {"color": YELLOW, "value": 1}])
add("stat", "Tardios descartados (dropped-records)",
    ["sum(kafka_streams_task_dropped_records_total)"], 6, 22, 6, 5,
    "Metrica dropped-records de Kafka Streams: eventos fuera de la gracia (y otros descartes). Se reinicia con la app.",
    thresholds=[{"color": GREEN, "value": None}, {"color": YELLOW, "value": 1}])
add("stat", "Duplicados ignorados",
    ["sum(kafka_streams_dedup_duplicados_ignorados_total)"], 12, 22, 6, 5,
    "Metrica propia del DeduplicadorPorEventId. Se reinicia con la app.",
    thresholds=[{"color": GREEN, "value": None}, {"color": YELLOW, "value": 1}])
add("stat", "Hilos de Streams vivos",
    ["sum(kafka_streams_alive_stream_threads)"], 18, 22, 6, 5,
    "0 o sin dato = la app ventas-aggregator no esta corriendo.",
    thresholds=[{"color": RED, "value": None}, {"color": GREEN, "value": 1}])
add("timeseries", "Eventos en la DLQ, descartes y duplicados (por minuto)",
    [("sum(increase(kafka_topic_partition_current_offset{topic=\"ventas-raw-dlq\"}[1m]))", "DLQ"),
     ("sum(increase(kafka_streams_task_dropped_records_total[1m]))", "tardios descartados"),
     ("sum(increase(kafka_streams_dedup_duplicados_ignorados_total[1m]))", "duplicados ignorados")],
    0, 27, 12, 8, "Permite correlacionar cada escenario de prueba con su efecto.")
add("timeseries", "Procesamiento de Kafka Streams",
    [("sum(kafka_streams_thread_process_rate)", "registros/s procesados"),
     ("sum(rate(kafka_streams_restore_records_consumed_total[1m]))", "registros/s restaurados (changelog)")],
    12, 27, 12, 8, "La restauracion aparece tras un reinicio de la app sin volumen de estado.", unit="ops")

row("JVM", 35)
add("timeseries", "Heap usado por servicio",
    [("sum by (job, instance) (jvm_memory_used_bytes{area=\"heap\"})", "{{job}} {{instance}}")],
    0, 36, 24, 7, "Memoria heap de brokers, ZooKeeper y Streams.", unit="bytes")

dash = {
    "uid": "ventas-streaming", "title": "Ventas Streaming - Kafka", "tags": ["kafka", "dsy1107"],
    "timezone": "browser", "schemaVersion": 39, "version": 1, "refresh": "10s",
    "time": {"from": "now-30m", "to": "now"}, "panels": panels, "templating": {"list": []},
    "annotations": {"list": []}, "editable": True,
}
with open(sys.argv[1], "w", encoding="utf-8", newline="\n") as f:
    json.dump(dash, f, indent=2, ensure_ascii=False)
print(len(panels), "paneles")
