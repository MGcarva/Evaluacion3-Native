package cl.duoc.ventas;

public record DlqMessage(
        String correlationId,
        String sourceTopic,
        String key,
        String originalValue,
        String reason,
        String detectedAt) {
}