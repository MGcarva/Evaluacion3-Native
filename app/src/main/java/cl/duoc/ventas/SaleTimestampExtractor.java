package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public final class SaleTimestampExtractor implements TimestampExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        try {
            JsonNode node = MAPPER.readTree((String) record.value());
            return Instant.parse(node.get("eventTime").asText()).toEpochMilli();
        } catch (Exception e) {
            // No lanzar ni devolver negativo: Streams descartaría el registro en silencio
            // y nunca llegaría a la validación/DLQ que construiremos después.
            return partitionTime >= 0 ? partitionTime : record.timestamp();
        }
    }
}