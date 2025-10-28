package kz.qazmarka.h2k.kafka.ensure;

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;

/**
 * Общий набор зависимостей, используемых вспомогательными сервисами ensure-процесса.
 */
final class TopicEnsureContext {

    private final TopicBackoffManager backoffManager;
    private final long adminTimeoutMs;
    private final Map<String, String> topicConfigs;
    private final Consumer<String> markEnsured;
    private final Logger log;
    private final EnsureMetricsSink metrics;

    TopicEnsureContext(TopicBackoffManager backoffManager,
                       long adminTimeoutMs,
                       Map<String, String> topicConfigs,
                       Consumer<String> markEnsured,
                       EnsureMetricsSink metrics,
                       Logger log) {
        this.backoffManager = backoffManager;
        this.adminTimeoutMs = adminTimeoutMs;
        this.topicConfigs = topicConfigs;
        this.markEnsured = markEnsured;
        this.metrics = metrics;
        this.log = log;
    }

    long adminTimeoutMs() {
        return adminTimeoutMs;
    }

    Map<String, String> topicConfigs() {
        return topicConfigs;
    }

    void markEnsured(String topic) {
        markEnsured.accept(topic);
    }

    long scheduleRetry(String topic) {
        return backoffManager.scheduleRetry(topic);
    }

    EnsureMetricsSink metrics() {
        return metrics;
    }

    Logger log() {
        return log;
    }
}
