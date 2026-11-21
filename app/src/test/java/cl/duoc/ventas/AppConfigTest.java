package cl.duoc.ventas;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Configuracion centralizada (indicador 1). */
class AppConfigTest {

    @Test
    @DisplayName("Valores por defecto documentados")
    void defaults() {
        AppConfig c = AppConfig.defaults();
        assertEquals("ventas-raw", c.rawTopic());
        assertEquals("ventas-agregadas", c.aggTopic());
        assertEquals("ventas-raw-dlq", c.dlqTopic());
        assertEquals(60, c.windowSeconds());
        assertEquals(120, c.graceSeconds());
        assertEquals(10, c.dedupRetentionMinutes());
        assertEquals(2, c.replicationFactor());
    }

    @Test
    @DisplayName("Las variables de entorno sobreescriben los valores por defecto")
    void overrides() {
        AppConfig c = AppConfig.fromEnv(Map.of("WINDOW_SECONDS", "300", "WINDOW_GRACE_SECONDS", " 45 ",
                "DLQ_TOPIC", "otra-dlq", "STREAMS_REPLICATION_FACTOR", "1"));
        assertEquals(300, c.windowSeconds());
        assertEquals(45, c.graceSeconds());
        assertEquals("otra-dlq", c.dlqTopic());
        assertEquals(1, c.replicationFactor());
        assertEquals("1", StreamsApp.streamsProperties(c).get("replication.factor").toString());
    }

    @Test
    @DisplayName("La retencion de deduplicacion cubre al menos ventana + gracia")
    void retencionCubreVentanaYGracia() {
        AppConfig c = AppConfig.defaults();
        assertEquals(true, c.dedupRetentionMinutes() * 60 >= c.windowSeconds() + c.graceSeconds());
    }
}
