package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

/**
 * Generador reproducible de casos de prueba (requisito 6).
 *
 * Uso (dentro de Docker):  docker compose run --rm generator [escenario]
 *   invalidos   JSON malformado, valor vacio, campo ausente, eventTime invalido,
 *               cantidad invalida, total != cantidad x precio      -> DLQ
 *   duplicados  la misma venta (mismo eventId) 3 veces con claves distintas -> cuenta 1
 *   tardios     normal (T0), fuera de orden dentro de la gracia (T0-15s) y
 *               fuera de la gracia (T0 - (ventana + gracia + 60 s)) -> 1 se incorpora, 1 se descarta
 *   todos       los tres anteriores (por defecto)
 *
 * Despues de enviar, espera a que cierren las ventanas (ventana + gracia + 5 s) y envia
 * un evento "empujador" por tienda de prueba para avanzar el reloj del stream y que
 * suppress publique el resultado. Desactivar con GENERATOR_WAIT=false.
 *
 * Los eventos invalidos usan un eventTime en el pasado: asi nunca adelantan el reloj.
 */
public final class TestEventGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter HHMMSS =
            DateTimeFormatter.ofPattern("HHmmss").withZone(ZoneOffset.UTC);

    private final AppConfig cfg;
    private final KafkaProducer<String, String> producer;
    private final String run;

    private TestEventGenerator(AppConfig cfg, KafkaProducer<String, String> producer) {
        this.cfg = cfg;
        this.producer = producer;
        this.run = HHMMSS.format(Instant.now());
    }

    public static void main(String[] args) throws Exception {
        // Solo WARN+ de la libreria Kafka: la salida queda legible (los casos y su resultado esperado)
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "warn");
        String escenario = args.length > 0 ? args[0].toLowerCase() : "todos";
        boolean esperar = !"false".equalsIgnoreCase(System.getenv("GENERATOR_WAIT"));
        AppConfig cfg = AppConfig.fromEnv();

        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (var producer = new KafkaProducer<String, String>(props)) {
            var gen = new TestEventGenerator(cfg, producer);
            System.out.printf("== Generador de casos: escenario=%s run=%s topico=%s ventana=%ds gracia=%ds%n",
                    escenario, gen.run, cfg.rawTopic(), cfg.windowSeconds(), cfg.graceSeconds());
            List<String> tiendas = switch (escenario) {
                case "invalidos" -> { gen.invalidos(); yield List.of(); }
                case "duplicados" -> List.of(gen.duplicados());
                case "tardios" -> List.of(gen.tardios());
                case "todos" -> {
                    gen.invalidos();
                    String d = gen.duplicados();
                    String t = gen.tardios();
                    yield List.of(d, t);
                }
                default -> throw new IllegalArgumentException(
                        "Escenario desconocido: " + escenario + " (invalidos|duplicados|tardios|todos)");
            };
            if (esperar && !tiendas.isEmpty()) {
                gen.empujar(tiendas);
            }
            System.out.println("== Fin. Verificar con: scripts/kafka.ps1 dlq | agregadas | logs");
        }
    }

    // ------------------------------------------------------------------ escenarios

    private void invalidos() throws Exception {
        titulo("INVALIDOS -> deben ir a " + cfg.dlqTopic() + " con su motivo; la app sigue activa");
        String pasado = iso(Instant.now().minus(1, ChronoUnit.HOURS));
        String base = "inv-" + run;

        enviar("invalido-malformado", base + "-json",
                "{\"eventId\":\"" + base + "-json\",\"eventTime\":\"" + pasado + "\",\"store\":",
                "JSON_INVALIDO");
        enviar("invalido-vacio", base + "-vacio", "", "VALOR_VACIO");

        Sale sinStore = venta(base + "-sin-store", pasado, null, 1, 1000, 1000);
        enviar("invalido-sin-store", base + "-sin-store", json(sinStore), "CAMPO_OBLIGATORIO_AUSENTE: store");

        Sale fechaMala = venta(base + "-fecha", "ayer a las 10", "Tienda X", 1, 1000, 1000);
        enviar("invalido-eventtime", base + "-fecha", json(fechaMala), "EVENTTIME_INVALIDO");

        Sale cantidadCero = venta(base + "-cantidad", pasado, "Tienda X", 0, 1000, 0);
        enviar("invalido-cantidad", base + "-cantidad", json(cantidadCero), "CANTIDAD_INVALIDA");

        Sale totalMalo = venta(base + "-total", pasado, "Tienda X", 2, 1000, 5000);
        enviar("invalido-total", base + "-total", json(totalMalo), "TOTAL_INCONSISTENTE: esperado=2000 recibido=5000");
    }

    private String duplicados() throws Exception {
        String tienda = "Prueba Duplicados " + run;
        titulo("DUPLICADOS -> misma venta 3 veces; '" + tienda + "' debe contar 1 venta de 3000");
        Sale s = venta("dup-" + run, iso(Instant.now()), tienda, 2, 1500, 3000);
        String valor = json(s);
        enviar("duplicado-original", "cliente-A", valor, "se cuenta");
        enviar("duplicado-copia-1", "cliente-B", valor, "log 'duplicado ignorado'");
        enviar("duplicado-copia-2", "cliente-C", valor, "log 'duplicado ignorado'");
        return tienda;
    }

    private String tardios() throws Exception {
        String tienda = "Prueba Tardios " + run;
        // Alinear T0 entre los segundos 5 y 15 del minuto: asi T0-15s cae en la ventana anterior
        // y esa ventana todavia esta dentro de la gracia (cierra en el segundo 30).
        while (true) {
            int s = Instant.now().atZone(ZoneOffset.UTC).getSecond();
            if (s >= 5 && s <= 15) {
                break;
            }
            Thread.sleep(500);
        }
        Instant t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        titulo("TARDIOS -> tienda '" + tienda + "' T0=" + iso(t0));
        enviar("tardio-normal", "cliente-tardio",
                json(venta("tar-normal-" + run, iso(t0), tienda, 1, 1000, 1000)),
                "ventana " + ventana(t0) + " (avanza el reloj a T0)");
        Instant dentro = t0.minusSeconds(15);
        enviar("tardio-dentro-gracia", "cliente-tardio",
                json(venta("tar-dentro-" + run, iso(dentro), tienda, 1, 2000, 2000)),
                "fuera de orden pero DENTRO de la gracia -> se suma a ventana " + ventana(dentro));
        // Su ventana termina a mas tardar en T0 - (gracia + 60 s): con reloj en T0 ya expiro
        Instant fuera = t0.minusSeconds(cfg.windowSeconds() + cfg.graceSeconds() + 60);
        enviar("tardio-fuera-gracia", "cliente-tardio",
                json(venta("tar-fuera-" + run, iso(fuera), tienda, 1, 4000, 4000)),
                "FUERA de la gracia -> descartado (log 'Skipping record for expired window', NO va a DLQ)");
        return tienda;
    }

    private void empujar(List<String> tiendas) throws Exception {
        long espera = cfg.windowSeconds() + cfg.graceSeconds() + 5;
        titulo("Esperando " + espera + " s para que cierren las ventanas (ventana + gracia + 5 s)...");
        Thread.sleep(espera * 1000);
        for (String tienda : tiendas) {
            enviar("empujador", "cliente-empujador",
                    json(venta("push-" + tienda.replace(' ', '-') + "-" + System.currentTimeMillis(),
                            iso(Instant.now()), tienda, 1, 500, 500)),
                    "avanza el reloj de '" + tienda + "' para que suppress publique");
        }
    }

    // ------------------------------------------------------------------ utilidades

    private void enviar(String caso, String clave, String valor, String esperado) throws Exception {
        RecordMetadata md = producer.send(new ProducerRecord<>(cfg.rawTopic(), clave, valor)).get();
        System.out.printf("  [%-22s] clave=%-22s -> %s-%d@%d | esperado: %s%n",
                caso, clave, md.topic(), md.partition(), md.offset(), esperado);
    }

    private static void titulo(String texto) {
        System.out.println();
        System.out.println("-- " + texto);
    }

    private static Sale venta(String eventId, String eventTime, String store,
                              int cantidad, int precio, int total) {
        return new Sale(eventId, eventTime, store, "tienda", "Santiago", "presencial", "ropa",
                "Polera", cantidad, precio, total, "cliente-prueba", "debito");
    }

    private static String json(Sale s) throws Exception {
        return MAPPER.writeValueAsString(s);
    }

    private static String iso(Instant t) {
        return t.truncatedTo(ChronoUnit.SECONDS).toString();
    }

    private String ventana(Instant t) {
        long size = cfg.windowSeconds() * 1000;
        long start = t.toEpochMilli() - (t.toEpochMilli() % size);
        return Instant.ofEpochMilli(start) + "-" + Instant.ofEpochMilli(start + size);
    }
}
