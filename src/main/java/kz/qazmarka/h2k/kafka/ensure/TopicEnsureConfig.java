package kz.qazmarka.h2k.kafka.ensure;

import java.util.Collections;
import java.util.Map;
import java.util.function.UnaryOperator;

import kz.qazmarka.h2k.config.H2kConfig;

/**
 * Иммутабельный набор параметров ensure-цикла: целевые партиции/репликация, набор конфигов,
 * таймауты и политики апгрейда. Формируется один раз из {@link H2kConfig} и далее передаётся
 * во внутренние компоненты {@link TopicEnsureService}.
 */
final class TopicEnsureConfig {
    final int topicNameMaxLen;
    final UnaryOperator<String> topicSanitizer;
    final int topicPartitions;
    final short topicReplication;
    final Map<String, String> topicConfigs;
    final boolean ensureIncreasePartitions;
    final boolean ensureDiffConfigs;
    final long adminTimeoutMs;
    final long unknownBackoffMs;

    private TopicEnsureConfig(Builder b) {
        this.topicNameMaxLen = b.topicNameMaxLen;
        this.topicSanitizer = b.topicSanitizer;
        this.topicPartitions = b.topicPartitions;
        this.topicReplication = b.topicReplication;
        this.topicConfigs = b.topicConfigs;
        this.ensureIncreasePartitions = b.ensureIncreasePartitions;
        this.ensureDiffConfigs = b.ensureDiffConfigs;
        this.adminTimeoutMs = b.adminTimeoutMs;
        this.unknownBackoffMs = b.unknownBackoffMs;
    }

    /**
     * Билдер защищён от внешнего использования; служит для пошаговой сборки конфига внутри пакета.
     */
    static final class Builder {
        int topicNameMaxLen;
        UnaryOperator<String> topicSanitizer = UnaryOperator.identity();
        int topicPartitions;
        short topicReplication;
        Map<String, String> topicConfigs = Collections.emptyMap();
        boolean ensureIncreasePartitions;
        boolean ensureDiffConfigs;
        long adminTimeoutMs;
        long unknownBackoffMs;

        Builder topicNameMaxLen(int v) { this.topicNameMaxLen = v; return this; }
        Builder topicSanitizer(UnaryOperator<String> v) { this.topicSanitizer = v; return this; }
        Builder topicPartitions(int v) { this.topicPartitions = v; return this; }
        Builder topicReplication(short v) { this.topicReplication = v; return this; }
        Builder topicConfigs(Map<String, String> v) { this.topicConfigs = v; return this; }
        Builder ensureIncreasePartitions(boolean v) { this.ensureIncreasePartitions = v; return this; }
        Builder ensureDiffConfigs(boolean v) { this.ensureDiffConfigs = v; return this; }
        Builder adminTimeoutMs(long v) { this.adminTimeoutMs = v; return this; }
        Builder unknownBackoffMs(long v) { this.unknownBackoffMs = v; return this; }
        TopicEnsureConfig build() { return new TopicEnsureConfig(this); }
    }

    static Builder builder() { return new Builder(); }

    static TopicEnsureConfig from(H2kConfig cfg) {
        Map<String, String> topicConfigs = cfg.getTopicConfigs();
        if (topicConfigs == null) {
            topicConfigs = Collections.emptyMap();
        }
        return builder()
                .topicNameMaxLen(cfg.getTopicMaxLength())
                .topicSanitizer(cfg::sanitizeTopic)
                .topicPartitions(cfg.getTopicPartitions())
                .topicReplication(cfg.getTopicReplication())
                .topicConfigs(Collections.unmodifiableMap(topicConfigs))
                .ensureIncreasePartitions(cfg.isEnsureIncreasePartitions())
                .ensureDiffConfigs(cfg.isEnsureDiffConfigs())
                .adminTimeoutMs(cfg.getAdminTimeoutMs())
                .unknownBackoffMs(cfg.getUnknownBackoffMs())
                .build();
    }
}
