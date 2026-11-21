package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;

public final class SaleValidator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record Resultado(Sale sale, String original, String motivo) {
        public boolean esValida() {
            return motivo == null;
        }
    }

    private SaleValidator() {
    }

    public static Resultado validar(String json) {
        if (json == null || json.isBlank()) {
            return rechazo(json, "VALOR_VACIO");
        }
        Sale sale;
        try {
            sale = MAPPER.readValue(json, Sale.class);
        } catch (Exception e) {
            return rechazo(json, "JSON_INVALIDO: " + e.getClass().getSimpleName());
        }
        if (sale == null) {
            return rechazo(json, "JSON_INVALIDO: valor null");
        }
        if (vacio(sale.eventId())) {
            return rechazo(json, "CAMPO_OBLIGATORIO_AUSENTE: eventId");
        }
        if (vacio(sale.eventTime())) {
            return rechazo(json, "CAMPO_OBLIGATORIO_AUSENTE: eventTime");
        }
        try {
            Instant.parse(sale.eventTime());
        } catch (Exception e) {
            return rechazo(json, "EVENTTIME_INVALIDO: " + sale.eventTime());
        }
        if (vacio(sale.store())) {
            return rechazo(json, "CAMPO_OBLIGATORIO_AUSENTE: store");
        }
        if (sale.quantity() <= 0) {
            return rechazo(json, "CANTIDAD_INVALIDA: " + sale.quantity());
        }
        if (sale.unitPriceClp() <= 0) {
            return rechazo(json, "PRECIO_INVALIDO: " + sale.unitPriceClp());
        }
        long esperado = (long) sale.quantity() * sale.unitPriceClp();
        if (esperado != sale.totalClp()) {
            return rechazo(json, "TOTAL_INCONSISTENTE: esperado=" + esperado + " recibido=" + sale.totalClp());
        }
        return new Resultado(sale, json, null);
    }

    private static Resultado rechazo(String json, String motivo) {
        return new Resultado(null, json, motivo);
    }

    private static boolean vacio(String valor) {
        return valor == null || valor.isBlank();
    }
}