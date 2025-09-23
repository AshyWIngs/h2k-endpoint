package kz.qazmarka.h2k.kafka;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;

/**
 * Публичная обёртка над ensure-логикой Kafka-топиков.
 * Делегирует работу {@link TopicEnsureService}, сохраняя прежний API.
 */
public final class TopicEnsurer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TopicEnsurer.class);

    private final TopicEnsureService service;

    private TopicEnsurer(TopicEnsureService service) {
        this.service = service;
    }

    /**
     * Создаёт {@link TopicEnsurer}, если ensureTopics включён и задан bootstrap.
     * Возвращает {@code null}, если ensure отключён конфигурацией.
     */
    public static TopicEnsurer createIfEnabled(H2kConfig cfg) {
        if (!cfg.isEnsureTopics()) return null;
        final String bootstrap = cfg.getBootstrap();
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            LOG.warn("TopicEnsurer: не задан bootstrap Kafka — ensureTopics будет отключён");
            return null;
        }
        Properties props = cfg.kafkaAdminProps();
        String baseId = props.getProperty(AdminClientConfig.CLIENT_ID_CONFIG, "h2k-admin");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, uniqueClientId(baseId));
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) cfg.getAdminTimeoutMs());
        AdminClient adminClient = AdminClient.create(props);
        KafkaTopicAdmin admin = new KafkaTopicAdminClient(adminClient);
        TopicEnsureConfig config = TopicEnsureConfig.from(cfg);
        TopicEnsureState state = new TopicEnsureState();
        TopicEnsureService service = new TopicEnsureService(admin, config, state);
        return new TopicEnsurer(service);
    }

    public void ensureTopic(String topic) { service.ensureTopic(topic); }

    public boolean ensureTopicOk(String topic) { return service.ensureTopicOk(topic); }

    public void ensureTopics(Collection<String> topics) { service.ensureTopics(topics); }

    public Map<String, Long> getMetrics() { return service.getMetrics(); }

    @Override
    public void close() { service.close(); }

    @Override
    public String toString() { return service.toString(); }

    /**
     * Вспомогательный метод генерации уникального client.id для AdminClient.
     * Используется для избежания конфликтов MBean AppInfo.
     */
    private static String uniqueClientId(String base) {
        String host = "host";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignore) { /* no-op */ }
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        int at = pid.indexOf('@');
        if (at > 0) pid = pid.substring(0, at);
        long ts = System.currentTimeMillis();
        String prefix = (base == null || base.trim().isEmpty()) ? "h2k-admin" : base.trim();
        return prefix + "-" + host + "-" + pid + "-" + ts;
    }
}
