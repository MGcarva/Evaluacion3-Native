package cl.duoc.ventas;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Deduplicacion de eventos de negocio por eventId (requisito 5).
 *
 * Recibe ventas ya re-keyeadas por eventId (todas las copias de una venta llegan a
 * la misma tarea). Guarda en un state store eventId -> eventTime (ms). Si el eventId
 * ya existe, la copia se ignora y queda un log; si no, se registra y se reenvia.
 *
 * El store es persistente y tiene changelog: tras un reinicio se restaura y no se
 * "olvidan" los eventId vistos. Una puntuacion periodica (por stream time) borra los
 * eventId mas viejos que la retencion, para que el estado no crezca sin limite.
 */
public final class DeduplicadorPorEventId implements Processor<String, Sale, String, Sale> {

    public static final String STORE = "dedup-eventid";
    private static final Logger LOG = LoggerFactory.getLogger(DeduplicadorPorEventId.class);

    private final long retencionMs;
    private ProcessorContext<String, Sale> context;
    private KeyValueStore<String, Long> vistos;
    private Sensor duplicados;

    public DeduplicadorPorEventId(Duration retencion) {
        this.retencionMs = retencion.toMillis();
    }

    @Override
    public void init(ProcessorContext<String, Sale> context) {
        this.context = context;
        this.vistos = context.getStateStore(STORE);
        // Metrica JMX kafka.streams:type=stream-dedup-metrics -> duplicados-ignorados-total/-rate
        this.duplicados = context.metrics().addRateTotalSensor(
                "dedup", "eventid", "duplicados-ignorados", Sensor.RecordingLevel.INFO);
        // Purga cada minuto de "tiempo de stream" (avanza con el eventTime de los registros)
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::purgar);
    }

    @Override
    public void process(Record<String, Sale> record) {
        String eventId = record.key();
        Long primeraVez = vistos.get(eventId);
        if (primeraVez != null) {
            String origen = context.recordMetadata()
                    .map(DeduplicadorPorEventId::describir)
                    .orElse("desconocido");
            LOG.info("duplicado ignorado eventId={} store={} eventTime={} primerEventTime={} origen={}",
                    eventId, record.value().store(), record.timestamp(), primeraVez, origen);
            duplicados.record();
            return;
        }
        vistos.put(eventId, record.timestamp());
        context.forward(record);
    }

    private void purgar(long streamTime) {
        long limite = streamTime - retencionMs;
        List<String> expirados = new ArrayList<>();
        try (KeyValueIterator<String, Long> it = vistos.all()) {
            while (it.hasNext()) {
                KeyValue<String, Long> kv = it.next();
                if (kv.value < limite) {
                    expirados.add(kv.key);
                }
            }
        }
        expirados.forEach(vistos::delete);
        if (!expirados.isEmpty()) {
            LOG.debug("dedup: purgados {} eventId anteriores a {}", expirados.size(), limite);
        }
    }

    private static String describir(RecordMetadata m) {
        return m.topic() + "-" + m.partition() + "@" + m.offset();
    }
}
