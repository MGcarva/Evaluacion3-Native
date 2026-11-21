package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public final class StreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsApp.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String STATE_STORE = "ventas-por-tienda";

    public static void main(String[] args) {
        AppConfig cfg = AppConfig.fromEnv();

        var streams = new KafkaStreams(buildTopology(cfg), streamsProperties(cfg));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("Kafka Streams " + cfg.resumen());
        streams.start();
    }

    /** Configuracion del cliente Kafka Streams, derivada de AppConfig. */
    public static Properties streamsProperties(AppConfig cfg) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, cfg.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // EOS v2: lectura + estado + escritura en una sola transaccion (ver README, deduplicacion)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // RF de los topicos internos (changelog y repartition); 2 = maximo con 2 brokers
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, cfg.replicationFactor());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, cfg.commitIntervalMs());
        props.put(StreamsConfig.STATE_DIR_CONFIG, cfg.stateDir());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public static Topology buildTopology(AppConfig cfg) {
        String rawTopic = cfg.rawTopic();
        String aggTopic = cfg.aggTopic();
        String dlqTopic = cfg.dlqTopic();
        long windowSeconds = cfg.windowSeconds();
        long graceSeconds = cfg.graceSeconds();
        StreamsBuilder builder = new StreamsBuilder();

        // NUEVO: validar cada evento y separarlo en validos / rechazados
        Map<String, KStream<String, SaleValidator.Resultado>> ramas = builder
                .stream(rawTopic, Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new SaleTimestampExtractor()))
                .mapValues(SaleValidator::validar)
                .split(Named.as("validacion-"))
                .branch((key, r) -> r.esValida(), Branched.as("ok"))
                .defaultBranch(Branched.as("rechazados"));

        // NUEVO: los rechazados van a la DLQ y el flujo sigue
        ramas.get("validacion-rechazados")
                .map((key, r) -> KeyValue.pair(key, dlqJson(rawTopic, key, r)))
                .to(dlqTopic, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, Sale> sales = ramas.get("validacion-ok")
                .mapValues(SaleValidator.Resultado::sale);

        // Deduplicacion por eventId: re-key + repartition para que todas las copias de una
        // venta lleguen a la misma tarea, y un state store (con changelog) que las recuerda.
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DeduplicadorPorEventId.STORE),
                Serdes.String(), Serdes.Long()));
        Duration dedupRetencion = Duration.ofMinutes(cfg.dedupRetentionMinutes());

        KStream<String, Sale> unicas = sales
                .selectKey((key, sale) -> sale.eventId(), Named.as("rekey-por-eventid"))
                .repartition(Repartitioned.<String, Sale>as("por-eventid")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<>(Sale.class)))
                .process(() -> new DeduplicadorPorEventId(dedupRetencion),
                        Named.as("deduplicar"), DeduplicadorPorEventId.STORE);

        TimeWindows windows = TimeWindows.ofSizeAndGrace(
                Duration.ofSeconds(windowSeconds),
                Duration.ofSeconds(graceSeconds));

        KTable<Windowed<String>, TicketAggregate> aggregate = unicas
                .selectKey((key, sale) -> sale.store(), Named.as("rekey-por-tienda"))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Sale.class)))
                .windowedBy(windows)
                .aggregate(
                        TicketAggregate::new,
                        (store, sale, totals) -> totals.add(sale),
                        Materialized.<String, TicketAggregate, WindowStore<Bytes, byte[]>>as(STATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TicketAggregate.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        .withName("suppress-ventanas"));

        aggregate.toStream()
                .map((windowedKey, totals) -> KeyValue.pair(
                        windowedKey.key(), resultJson(windowedKey, totals)))
                .to(aggTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    // NUEVO: arma el mensaje de la DLQ y deja un log con el mismo correlationId
    private static String dlqJson(String sourceTopic, String key, SaleValidator.Resultado r) {
        String correlationId = UUID.randomUUID().toString();
        LOG.warn("evento rechazado correlationId={} topico={} clave={} motivo={}",
                correlationId, sourceTopic, key, r.motivo());
        try {
            return MAPPER.writeValueAsString(new DlqMessage(
                    correlationId, sourceTopic, key, r.original(), r.motivo(), Instant.now().toString()));
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo serializar el mensaje DLQ", e);
        }
    }

    private static String resultJson(Windowed<String> windowedKey, TicketAggregate totals) {
        long average = totals.count == 0 ? 0 : Math.round((double) totals.totalClp / totals.count);
        var result = new WindowResult(
                windowedKey.window().startTime().toString(),
                windowedKey.window().endTime().toString(),
                windowedKey.key(),
                totals.count,
                totals.totalClp,
                average);
        try {
            return MAPPER.writeValueAsString(result);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo serializar el agregado", e);
        }
    }
}