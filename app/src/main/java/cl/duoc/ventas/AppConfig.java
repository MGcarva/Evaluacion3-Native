package cl.duoc.ventas;

import java.util.Map;

/**
 * Configuracion unica de la aplicacion (indicador 1 de la rubrica).
 *
 * Todas las clases (StreamsApp, ProducerApp y el generador de pruebas) leen
 * su configuracion desde aqui. Cada valor sale de una variable de entorno
 * (definida en docker-compose.yaml / .env) y, si no existe, usa un valor por
 * defecto documentado en el README.
 */
public record AppConfig(
        // --- Conexion ---
        String bootstrap,            // KAFKA_BOOTSTRAP: brokers por la red interna de Docker
        String applicationId,        // GROUP_ID: application.id de Streams = consumer group
        // --- Topicos ---
        String rawTopic,             // RAW_TOPIC: entrada de ventas crudas
        String aggTopic,             // AGG_TOPIC: salida de agregados por tienda y ventana
        String dlqTopic,             // DLQ_TOPIC: eventos rechazados con su causa
        // --- Tiempo de evento ---
        long windowSeconds,          // WINDOW_SECONDS: tamano de la ventana tumbling
        long graceSeconds,           // WINDOW_GRACE_SECONDS: tolerancia a eventos tardios
        long dedupRetentionMinutes,  // DEDUP_RETENTION_MINUTES: cuanto se recuerda un eventId
        // --- Kafka Streams ---
        int replicationFactor,       // STREAMS_REPLICATION_FACTOR: RF de changelog/repartition internos
        long commitIntervalMs,       // COMMIT_INTERVAL_MS: cada cuanto se confirma la transaccion
        String stateDir,             // STREAMS_STATE_DIR: RocksDB local (montado en un volumen)
        // --- Productor de ventas ---
        double produceIntervalSeconds, // PRODUCE_INTERVAL: segundos entre ventas
        long producerMaxEvents,        // PRODUCER_MAX_EVENTS: 0 = infinito
        int customerPool) {            // CUSTOMER_POOL: cantidad de clientes distintos (claves)

    /** Lee la configuracion de las variables de entorno del proceso. */
    public static AppConfig fromEnv() {
        return fromEnv(System.getenv());
    }

    /** Version testeable: recibe el "entorno" como mapa. */
    public static AppConfig fromEnv(Map<String, String> env) {
        return new AppConfig(
                get(env, "KAFKA_BOOTSTRAP", "localhost:9092"),
                get(env, "GROUP_ID", "ventas-aggregator"),
                get(env, "RAW_TOPIC", "ventas-raw"),
                get(env, "AGG_TOPIC", "ventas-agregadas"),
                get(env, "DLQ_TOPIC", "ventas-raw-dlq"),
                Long.parseLong(get(env, "WINDOW_SECONDS", "60")),
                Long.parseLong(get(env, "WINDOW_GRACE_SECONDS", "120")),
                Long.parseLong(get(env, "DEDUP_RETENTION_MINUTES", "10")),
                Integer.parseInt(get(env, "STREAMS_REPLICATION_FACTOR", "2")),
                Long.parseLong(get(env, "COMMIT_INTERVAL_MS", "1000")),
                get(env, "STREAMS_STATE_DIR", "/tmp/kafka-streams"),
                Double.parseDouble(get(env, "PRODUCE_INTERVAL", "1.0")),
                Long.parseLong(get(env, "PRODUCER_MAX_EVENTS", "0")),
                Integer.parseInt(get(env, "CUSTOMER_POOL", "200")));
    }

    /** Valores por defecto, sin mirar el entorno (util para tests). */
    public static AppConfig defaults() {
        return fromEnv(Map.of());
    }

    /** Linea de arranque: permite verificar que version y valores estan corriendo. */
    public String resumen() {
        return String.format(
                "config bootstrap=%s appId=%s raw=%s agg=%s dlq=%s ventana=%ds gracia=%ds "
                        + "dedupRetencion=%dmin rf=%d commitMs=%d stateDir=%s",
                bootstrap, applicationId, rawTopic, aggTopic, dlqTopic, windowSeconds, graceSeconds,
                dedupRetentionMinutes, replicationFactor, commitIntervalMs, stateDir);
    }

    private static String get(Map<String, String> env, String key, String fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : value.trim();
    }
}
