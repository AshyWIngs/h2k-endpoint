package kz.qazmarka.h2k.kafka.ensure;

import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;

/**
 * Общий набор зависимостей, используемых вспомогательными сервисами ensure-процесса.
 */
final class TopicEnsureContext {

    private final TopicEnsureState state;
    private final TopicBackoffManager backoffManager;
    private final long adminTimeoutMs;
    private final Map<String, String> topicConfigs;
    private final Consumer<String> markEnsured;
    private final Logger log;

    TopicEnsureContext(TopicEnsureState state,
                       TopicBackoffManager backoffManager,
                       long adminTimeoutMs,
                       Map<String, String> topicConfigs,
                       Consumer<String> markEnsured,
                       Logger log) {
        this.state = state;
        this.backoffManager = backoffManager;
        this.adminTimeoutMs = adminTimeoutMs;
        this.topicConfigs = topicConfigs;
        this.markEnsured = markEnsured;
        this.log = log;
    }

    TopicEnsureState state() {
        return state;
    }

    TopicBackoffManager backoffManager() {
        return backoffManager;
    }

    long adminTimeoutMs() {
        return adminTimeoutMs;
    }

    Map<String, String> topicConfigs() {
        return topicConfigs;
    }

    Consumer<String> markEnsured() {
        return markEnsured;
    }

    Logger log() {
        return log;
    }
}
