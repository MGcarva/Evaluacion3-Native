package cl.duoc.ventas;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Validacion de sintaxis y reglas de negocio (indicador 4). */
class SaleValidatorTest {

    private static String venta(String eventId, String eventTime, String store, int qty, int price, int total) {
        return "{\"eventId\":" + q(eventId) + ",\"eventTime\":" + q(eventTime) + ",\"store\":" + q(store)
                + ",\"storeFormat\":\"tienda\",\"city\":\"Santiago\",\"channel\":\"presencial\","
                + "\"category\":\"ropa\",\"product\":\"Polera\",\"quantity\":" + qty
                + ",\"unitPriceClp\":" + price + ",\"totalClp\":" + total
                + ",\"customerId\":\"cliente-1\",\"paymentMethod\":\"debito\"}";
    }

    private static String q(String s) {
        return s == null ? "null" : "\"" + s + "\"";
    }

    @Test
    @DisplayName("Una venta correcta es valida y conserva el original")
    void ventaValida() {
        String json = venta("e1", "2026-11-21T10:00:00Z", "Tienda A", 2, 1500, 3000);
        var r = SaleValidator.validar(json);
        assertTrue(r.esValida());
        assertNull(r.motivo());
        assertEquals("Tienda A", r.sale().store());
        assertEquals(json, r.original());
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource(delimiter = '|', value = {
            "vacio            | VALOR_VACIO",
            "malformado       | JSON_INVALIDO",
            "tipo-incorrecto  | JSON_INVALIDO",
            "sin-eventId      | CAMPO_OBLIGATORIO_AUSENTE: eventId",
            "sin-eventTime    | CAMPO_OBLIGATORIO_AUSENTE: eventTime",
            "eventTime-malo   | EVENTTIME_INVALIDO",
            "sin-store        | CAMPO_OBLIGATORIO_AUSENTE: store",
            "cantidad-cero    | CANTIDAD_INVALIDA",
            "precio-negativo  | PRECIO_INVALIDO",
            "total-incorrecto | TOTAL_INCONSISTENTE: esperado=2000 recibido=5000",
    })
    @DisplayName("Cada error de entrada tiene un motivo especifico")
    void rechazos(String caso, String motivoEsperado) {
        String t = "2026-11-21T10:00:00Z";
        String json = switch (caso) {
            case "vacio" -> "";
            case "malformado" -> "{\"eventId\":\"x\",\"store\":";
            case "tipo-incorrecto" -> venta("e", t, "A", 1, 1, 1).replace("\"quantity\":1", "\"quantity\":\"dos\"");
            case "sin-eventId" -> venta(null, t, "A", 1, 1000, 1000);
            case "sin-eventTime" -> venta("e", null, "A", 1, 1000, 1000);
            case "eventTime-malo" -> venta("e", "ayer", "A", 1, 1000, 1000);
            case "sin-store" -> venta("e", t, null, 1, 1000, 1000);
            case "cantidad-cero" -> venta("e", t, "A", 0, 1000, 0);
            case "precio-negativo" -> venta("e", t, "A", 1, -5, -5);
            case "total-incorrecto" -> venta("e", t, "A", 2, 1000, 5000);
            default -> throw new IllegalArgumentException(caso);
        };
        var r = SaleValidator.validar(json);
        assertNotNull(r.motivo(), "debia rechazarse");
        assertTrue(r.motivo().startsWith(motivoEsperado), () -> "motivo real: " + r.motivo());
        assertEquals(json, r.original(), "el original debe conservarse sin cambios");
        assertNull(r.sale());
    }
}
