package kz.qazmarka.h2k.kafka.ensure;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.admin.NewTopic;

/**
 * Формирует параметры создаваемых топиков и выполняет batch create с учётом timeouts/backoff.
 */
final class TopicConfigPlanner {

    private final KafkaTopicAdmin admin;
    private final TopicParams params;
    private final TopicEnsureState state;
    private final TopicBackoffManager backoffManager;

    TopicConfigPlanner(KafkaTopicAdmin admin,
                       TopicParams params,
                       TopicEnsureState state,
                       TopicBackoffManager backoffManager) {
        this.admin = admin;
        this.params = params;
        this.state = state;
        this.backoffManager = backoffManager;
    }

    /** Собирает {@link NewTopic} для каждого имени, используя параметры из конфигурации. */
    List<NewTopic> planTopics(List<String> names) {
        List<NewTopic> newTopics = new ArrayList<>(names.size());
        for (String t : names) {
            newTopics.add(params.newTopic(t));
        }
        return newTopics;
    }

    /** Отмечает успешное создание темы, обновляя метрики и кеш. */
    void recordSuccess(String topic) {
        state.createOk.increment();
        state.ensured.add(topic);
        backoffManager.markSuccess(topic);
    }

    /** Отмечает гонку (TopicExists) как успешный исход и снимает backoff. */
    void recordRace(String topic) {
        state.createRace.increment();
        state.ensured.add(topic);
        backoffManager.markSuccess(topic);
    }

    /** Регистрирует неуспех и планирует повторную попытку. */
    void recordFailure(String topic, Throwable cause) {
        state.createFail.increment();
        backoffManager.scheduleRetry(topic);
        if (TopicEnsureService.LOG.isDebugEnabled()) {
            TopicEnsureService.LOG.debug("Не удалось создать Kafka-топик '{}': {}", topic, cause.toString());
        }
    }

    KafkaTopicAdmin admin() {
        return admin;
    }

    /** Создаёт тему через AdminClient и фиксирует метрики/логи. */
    void createTopic(String topic, long timeoutMs)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        NewTopic nt = params.newTopic(topic);
        if (TopicEnsureService.LOG.isDebugEnabled() && !params.configs().isEmpty()) {
            TopicEnsureService.LOG.debug("Применяю конфиги для Kafka-топика '{}': {}", topic, summarizeConfigs());
        }
        admin.createTopic(nt, timeoutMs);
        recordSuccess(topic);
        logTopicCreated(topic);
    }

    /** Пишет INFO о созданной теме и DEBUG о конфигурации при наличии. */
    void logTopicCreated(String topic) {
        TopicEnsureService.LOG.info("Создал Kafka-топик '{}': partitions={}, replication={}",
                topic, params.partitions(), params.replication());
        if (TopicEnsureService.LOG.isDebugEnabled() && !params.configs().isEmpty()) {
            TopicEnsureService.LOG.debug("Конфиги Kafka-топика '{}': {}", topic, summarizeConfigs());
        }
    }

    /** Возвращает краткое текстовое представление ключевых конфигов темы. */
    String summarizeConfigs() {
        if (params.configs().isEmpty()) {
            return "без явных конфигов";
        }
        String retention = params.configs().get(TopicEnsureService.CFG_RETENTION_MS);
        String cleanup   = params.configs().get(TopicEnsureService.CFG_CLEANUP_POLICY);
        String comp      = params.configs().get(TopicEnsureService.CFG_COMPRESSION_TYPE);
        String minIsr    = params.configs().get(TopicEnsureService.CFG_MIN_INSYNC_REPLICAS);
        StringBuilder sb = new StringBuilder(128);
        boolean first = true;
        first = append(sb, TopicEnsureService.CFG_RETENTION_MS, retention, first);
        first = append(sb, TopicEnsureService.CFG_CLEANUP_POLICY, cleanup, first);
        first = append(sb, TopicEnsureService.CFG_COMPRESSION_TYPE, comp, first);
        first = append(sb, TopicEnsureService.CFG_MIN_INSYNC_REPLICAS, minIsr, first);
        int others = params.configs().size() - countNonNull(retention, cleanup, comp, minIsr);
        if (others > 0) {
            if (!first) sb.append(", ");
            sb.append("+").append(others).append(" др.");
        }
        return sb.toString();
    }

    private static boolean append(StringBuilder sb, String key, String value, boolean first) {
        if (value == null) return first;
        if (!first) sb.append(", ");
        sb.append(key).append("=").append(value);
        return false;
    }

    private static int countNonNull(Object... vals) {
        int n = 0;
        if (vals != null) {
            for (Object v : vals) if (v != null) n++;
        }
        return n;
    }
}
