package kz.qazmarka.h2k.endpoint;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfig.Keys;
import kz.qazmarka.h2k.kafka.serializer.RowKeySliceSerializer;

/**
 * Построитель настроек Kafka Producer. Вынесен для уменьшения размеров эндпоинта.
 */
final class ProducerPropsFactory {
    /**
     * Формирует {@link Properties} для {@link org.apache.kafka.clients.producer.KafkaProducer}:
     *  - базовые обязательные параметры (bootstrap, сериализаторы);
     *  - безопасные дефолты (acks=all, enable.idempotence=true, retries и т.д.);
     *  - pass-through: только валидные {@code h2k.producer.*} прокидываются как «родные» ключи продьюсера, если не заданы выше;
     *  - {@code client.id}: по умолчанию префикс + hostname + короткий случайный суффикс, фолбэк — UUID.
     *
     * @param cfg       HBase-конфигурация
     * @param bootstrap список брокеров Kafka (host:port[,host2:port2])
     * @return заполненный набор свойств для конструктора продьюсера
     */
    private static final String BYTE_ARRAY_SERIALIZER = ByteArraySerializer.class.getName();
    private static final String ROW_KEY_SERIALIZER = RowKeySliceSerializer.class.getName();
    private static final String FORCED_COMPRESSION = "lz4";
    private static final Set<String> H2K_INTERNAL_KEYS = new HashSet<>(
            Arrays.asList(
                    "await.every",
                    "await.timeout.ms",
                    "batch.counters.enabled",
                    "batch.debug.on.failure",
                    ProducerConfig.COMPRESSION_TYPE_CONFIG
            )
    );

    private ProducerPropsFactory() {
    }

    static Properties build(Configuration cfg, String bootstrap) {
        Properties props = createBaseProperties(bootstrap);
        applyDefaultProducerSettings(props, cfg);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, computeClientId(cfg));
        applyPassThroughSettings(props, cfg);
        enforceIdempotentOrdering(props);
        return props;
    }

    private static Properties createBaseProperties(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ROW_KEY_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER);
        return props;
    }

    private static void applyDefaultProducerSettings(Properties props, Configuration cfg) {
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, cfg.get("h2k.producer.enable.idempotence", "true"));
        props.put(ProducerConfig.ACKS_CONFIG, cfg.get("h2k.producer.acks", "all"));
        props.put(ProducerConfig.RETRIES_CONFIG, cfg.get("h2k.producer.retries", String.valueOf(Integer.MAX_VALUE)));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, cfg.get("h2k.producer.delivery.timeout.ms", "180000"));
        props.put(ProducerConfig.LINGER_MS_CONFIG, cfg.get("h2k.producer.linger.ms", "50"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, cfg.get("h2k.producer.batch.size", "65536"));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, FORCED_COMPRESSION);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                cfg.get("h2k.producer.max.in.flight", "1"));
    }

    private static String computeClientId(Configuration cfg) {
        String defaultClientId = buildDefaultClientId();
        String configuredClientId = cfg.get("h2k.producer.client.id", defaultClientId);
        if (!configuredClientId.equals(defaultClientId)) {
            return configuredClientId;
        }
        String randomSuffix = UUID.randomUUID().toString().substring(0, 8);
        return configuredClientId + '-' + randomSuffix;
    }

    private static String buildDefaultClientId() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            if (host == null || host.isEmpty()) {
                return H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + UUID.randomUUID();
            }
            return H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + host;
        } catch (UnknownHostException ignore) {
            return H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + UUID.randomUUID();
        }
    }

    private static void applyPassThroughSettings(Properties props, Configuration cfg) {
        Set<String> kafkaKeys = ProducerConfig.configNames();
        String prefix = Keys.PRODUCER_PREFIX;
        int prefixLen = prefix.length();
        for (Map.Entry<String, String> entry : cfg) {
            String key = entry.getKey();
            if (key.startsWith(prefix) && !"h2k.producer.max.in.flight".equals(key)) {
                String realKey = key.substring(prefixLen);
                if (!H2K_INTERNAL_KEYS.contains(realKey) && kafkaKeys.contains(realKey)) {
                    props.putIfAbsent(realKey, entry.getValue());
                }
            }
        }
    }

    /**
     * Жестко фиксирует связку идемпотентности и порядка: acks=all, бесконечные retry и max.in.flight<=5.
     * Это удерживает порядок при Kafka 2.3.1+ даже при пользовательских overrides.
     */
    private static void enforceIdempotentOrdering(Properties props) {
        String idempotence = String.valueOf(props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        if ("false".equalsIgnoreCase(idempotence)) {
            return;
        }

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

        int maxInFlight = parseIntOrDefault(
                props.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                5);
        if (maxInFlight > 5) {
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        } else {
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(maxInFlight));
        }
    }

    private static int parseIntOrDefault(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (Exception ignore) {
            return defaultValue;
        }
    }
}
