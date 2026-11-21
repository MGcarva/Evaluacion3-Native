package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pruebas de la topologia completa con TopologyTestDriver (sin brokers):
 * DLQ (ind. 4), duplicados (ind. 5), agregacion y tiempo de evento (ind. 3).
 *
 * Configuracion usada: ventana 60 s, gracia 30 s (fijadas en setUp), retencion de
 * deduplicacion 10 min (valor por defecto).
 */
class StreamsTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** Base de tiempo: inicio exacto de un minuto (ventana [T0, T0+60s)). */
    private static final Instant T0 = Instant.parse("2026-11-21T10:00:00Z");

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> raw;
    private TestOutputTopic<String, String> agregadas;
    private TestOutputTopic<String, String> dlq;
    private AppConfig cfg;

    @BeforeEach
    void setUp() {
        // Las pruebas fijan ventana 60 s y gracia 30 s para que los tiempos simulados sean cortos;
        // la logica es la misma con la gracia de produccion (120 s).
        cfg = AppConfig.fromEnv(Map.of(
                "STREAMS_STATE_DIR", stateDir.toString(),
                "WINDOW_SECONDS", "60",
                "WINDOW_GRACE_SECONDS", "30"));
        driver = new TopologyTestDriver(StreamsApp.buildTopology(cfg), StreamsApp.streamsProperties(cfg));
        raw = driver.createInputTopic(cfg.rawTopic(), new StringSerializer(), new StringSerializer());
        agregadas = driver.createOutputTopic(cfg.aggTopic(), new StringDeserializer(), new StringDeserializer());
        dlq = driver.createOutputTopic(cfg.dlqTopic(), new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    // ------------------------------------------------------------------ utilidades

    private static String venta(String eventId, Instant eventTime, String store, int qty, int price) {
        return "{\"eventId\":\"" + eventId + "\",\"eventTime\":\"" + eventTime + "\",\"store\":\"" + store
                + "\",\"storeFormat\":\"tienda\",\"city\":\"Santiago\",\"channel\":\"presencial\","
                + "\"category\":\"ropa\",\"product\":\"Polera\",\"quantity\":" + qty
                + ",\"unitPriceClp\":" + price + ",\"totalClp\":" + (qty * price)
                + ",\"customerId\":\"cliente-1\",\"paymentMethod\":\"debito\"}";
    }

    private void enviar(String key, String eventId, Instant eventTime, String store, int qty, int price) {
        raw.pipeInput(key, venta(eventId, eventTime, store, qty, price), eventTime);
    }

    /** Evento posterior de la misma tienda que avanza el reloj para que suppress publique. */
    private void empujar(String store, Instant cuando) {
        enviar("empuje", "empuje-" + cuando.toEpochMilli(), cuando, store, 1, 1);
    }

    private static JsonNode json(String s) {
        try {
            return MAPPER.readTree(s);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private List<JsonNode> agregadosDe(String store) {
        return agregadas.readValuesToList().stream().map(StreamsTopologyTest::json)
                .filter(n -> n.get("store").asText().equals(store)).toList();
    }

    private double metricaDescartados() {
        return driver.metrics().values().stream()
                .filter(m -> m.metricName().name().equals("dropped-records-total"))
                .mapToDouble(m -> ((Number) m.metricValue()).doubleValue()).sum();
    }

    // ------------------------------------------------------------------ DLQ

    @Nested
    @DisplayName("Validacion y DLQ")
    class Dlq {

        @Test
        @DisplayName("JSON malformado va a la DLQ con original, clave, topico fuente y motivo")
        void malformadoADlq() {
            String roto = "{\"eventId\":\"x\",\"store\":";
            raw.pipeInput("clave-rota", roto, T0);

            TestRecord<String, String> r = dlq.readRecord();
            JsonNode m = json(r.value());
            assertEquals("clave-rota", r.key(), "la clave Kafka de la DLQ es la original");
            assertEquals("clave-rota", m.get("key").asText());
            assertEquals(roto, m.get("originalValue").asText(), "el valor original se conserva byte a byte");
            assertEquals(cfg.rawTopic(), m.get("sourceTopic").asText());
            assertTrue(m.get("reason").asText().startsWith("JSON_INVALIDO"));
            assertFalse(m.get("correlationId").asText().isBlank(), "correlationId para cruzar con el log");
            assertFalse(m.get("detectedAt").asText().isBlank());
            assertTrue(dlq.isEmpty());
        }

        @Test
        @DisplayName("Error de negocio (total incorrecto) va a la DLQ y el flujo sigue con los validos")
        void errorDeNegocioYFlujoContinua() {
            String malo = venta("m1", T0, "Tienda A", 2, 1000).replace("\"totalClp\":2000", "\"totalClp\":5000");
            raw.pipeInput("c1", malo, T0);
            enviar("c2", "ok-1", T0.plusSeconds(5), "Tienda A", 1, 1000);
            empujar("Tienda A", T0.plusSeconds(120));

            JsonNode m = json(dlq.readValue());
            assertEquals("TOTAL_INCONSISTENTE: esperado=2000 recibido=5000", m.get("reason").asText());
            assertTrue(dlq.isEmpty(), "solo el invalido va a la DLQ");

            List<JsonNode> out = agregadosDe("Tienda A");
            assertEquals(1, out.size());
            assertEquals(1, out.get(0).get("sales_count").asLong(), "el invalido no se cuenta, el valido si");
            assertEquals(1000, out.get(0).get("total_clp").asLong());
        }
    }

    // ------------------------------------------------------------------ duplicados

    @Nested
    @DisplayName("Deduplicacion por eventId")
    class Duplicados {

        @Test
        @DisplayName("El mismo eventId enviado 3 veces (claves distintas) se cuenta una sola vez")
        void duplicadoSeCuentaUnaVez() {
            enviar("cliente-A", "dup-1", T0.plusSeconds(10), "Tienda D", 2, 1500);
            enviar("cliente-B", "dup-1", T0.plusSeconds(10), "Tienda D", 2, 1500);
            enviar("cliente-C", "dup-1", T0.plusSeconds(10), "Tienda D", 2, 1500);
            empujar("Tienda D", T0.plusSeconds(120));

            List<JsonNode> out = agregadosDe("Tienda D");
            assertEquals(1, out.size());
            assertEquals(1, out.get(0).get("sales_count").asLong());
            assertEquals(3000, out.get(0).get("total_clp").asLong());
            assertTrue(dlq.isEmpty(), "un duplicado no es un dato invalido");
        }

        @Test
        @DisplayName("Ventas distintas con los mismos datos pero distinto eventId SI se cuentan")
        void eventIdDistintoSeCuenta() {
            enviar("c", "a-1", T0.plusSeconds(10), "Tienda E", 1, 1000);
            enviar("c", "a-2", T0.plusSeconds(10), "Tienda E", 1, 1000);
            empujar("Tienda E", T0.plusSeconds(120));

            assertEquals(2, agregadosDe("Tienda E").get(0).get("sales_count").asLong());
        }

        @Test
        @DisplayName("Pasada la retencion (10 min) el eventId se olvida: el estado no crece sin limite")
        void retencionPurgaEstado() {
            enviar("c", "r-1", T0.plusSeconds(10), "Tienda R", 1, 1000);
            // avanzar el reloj 12 minutos (dispara la purga por stream time)
            empujar("Tienda R", T0.plusSeconds(12 * 60));
            // misma venta reenviada con un eventTime nuevo: ya no esta en el store
            enviar("c", "r-1", T0.plusSeconds(12 * 60 + 5), "Tienda R", 1, 1000);
            empujar("Tienda R", T0.plusSeconds(15 * 60));

            long total = agregadosDe("Tienda R").stream()
                    .filter(n -> n.get("window_start").asText().equals(T0.plusSeconds(12 * 60).toString()))
                    .mapToLong(n -> n.get("sales_count").asLong()).sum();
            assertEquals(2, total, "ventana 10:12 = empujador + reenvio aceptado tras la retencion");
        }
    }

    // ------------------------------------------------------------------ agregacion

    @Nested
    @DisplayName("Agregacion por tienda y ventana")
    class Agregacion {

        @Test
        @DisplayName("Conteo, monto, ticket promedio e inicio/fin de ventana por tienda")
        void agregaPorTiendaYVentana() {
            enviar("c1", "v1", T0.plusSeconds(1), "Tienda A", 1, 1000);   // 1000
            enviar("c2", "v2", T0.plusSeconds(20), "Tienda A", 2, 1500);  // 3000
            enviar("c3", "v3", T0.plusSeconds(59), "Tienda A", 1, 2000);  // 2000
            enviar("c4", "v4", T0.plusSeconds(30), "Tienda B", 1, 9990);
            enviar("c5", "v5", T0.plusSeconds(61), "Tienda A", 1, 500);   // siguiente ventana
            empujar("Tienda A", T0.plusSeconds(200));
            empujar("Tienda B", T0.plusSeconds(200));

            List<JsonNode> out = agregadas.readValuesToList().stream().map(StreamsTopologyTest::json).toList();
            JsonNode a = out.stream().filter(n -> n.get("store").asText().equals("Tienda A")
                    && n.get("window_start").asText().equals("2026-11-21T10:00:00Z")).findFirst().orElseThrow();
            assertEquals("2026-11-21T10:01:00Z", a.get("window_end").asText());
            assertEquals(3, a.get("sales_count").asLong());
            assertEquals(6000, a.get("total_clp").asLong());
            assertEquals(2000, a.get("avg_ticket_clp").asLong());

            JsonNode b = out.stream().filter(n -> n.get("store").asText().equals("Tienda B")
                    && n.get("window_start").asText().equals("2026-11-21T10:00:00Z")).findFirst().orElseThrow();
            assertEquals(1, b.get("sales_count").asLong());
            assertEquals(9990, b.get("total_clp").asLong());

            JsonNode a2 = out.stream().filter(n -> n.get("store").asText().equals("Tienda A")
                    && n.get("window_start").asText().equals("2026-11-21T10:01:00Z")).findFirst().orElseThrow();
            assertEquals(1, a2.get("sales_count").asLong(), "la venta del segundo 61 va a la ventana siguiente");
        }

        @Test
        @DisplayName("La ventana usa eventTime, no la hora de llegada")
        void usaEventTime() {
            // Llega con timestamp Kafka 'ahora', pero su eventTime es T0+10s
            raw.pipeInput("c", venta("et-1", T0.plusSeconds(10), "Tienda T", 1, 700), Instant.parse("2030-01-01T00:00:00Z"));
            empujar("Tienda T", T0.plusSeconds(120));
            JsonNode n = agregadosDe("Tienda T").get(0);
            assertEquals("2026-11-21T10:00:00Z", n.get("window_start").asText());
        }
    }

    // ------------------------------------------------------------------ tiempo de evento

    @Nested
    @DisplayName("Eventos tardios y periodo de gracia")
    class Tardios {

        @Test
        @DisplayName("La ventana no se publica hasta que el reloj pasa fin + gracia (suppress)")
        void suppressEsperaLaGracia() {
            enviar("c", "s1", T0.plusSeconds(10), "Tienda S", 1, 1000);
            empujar("Tienda S", T0.plusSeconds(60 + 29));   // fin=60s, gracia=30s: aun abierta
            assertTrue(agregadosDe("Tienda S").isEmpty(), "no debe publicarse antes de cerrar la gracia");
            empujar("Tienda S", T0.plusSeconds(60 + 30));   // ahora cierra
            assertEquals(1, agregadosDe("Tienda S").size());
        }

        @Test
        @DisplayName("Tardio DENTRO de la gracia se incorpora; FUERA de la gracia se descarta (no va a la DLQ)")
        void tardioDentroYFueraDeGracia() {
            Instant ahora = T0.plusSeconds(60 + 10);                    // reloj en 10:01:10
            enviar("c", "n1", ahora, "Tienda G", 1, 1000);              // ventana 10:01
            enviar("c", "dentro", T0.plusSeconds(55), "Tienda G", 1, 2000);  // 10:00:55 -> ventana 10:00 (cierra 10:01:30)
            double antes = metricaDescartados();
            enviar("c", "fuera", ahora.minusSeconds(180), "Tienda G", 1, 4000); // 09:58:10 -> ventana expirada
            assertEquals(antes + 1, metricaDescartados(), "dropped-records-total debe aumentar en 1");

            empujar("Tienda G", T0.plusSeconds(300));
            List<JsonNode> out = agregadosDe("Tienda G");
            JsonNode v1000 = out.stream().filter(n -> n.get("window_start").asText().equals("2026-11-21T10:00:00Z"))
                    .findFirst().orElseThrow();
            assertEquals(2000, v1000.get("total_clp").asLong(), "el tardio dentro de la gracia se sumo");
            assertFalse(out.stream().anyMatch(n -> n.get("window_start").asText().equals("2026-11-21T09:58:00Z")),
                    "el tardio fuera de la gracia no genera ventana");
            assertTrue(dlq.isEmpty(), "un tardio NO es un JSON invalido: no va a la DLQ");
        }
    }
}
