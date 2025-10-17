package kz.qazmarka.h2k.kafka.ensure.config;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import kz.qazmarka.h2k.config.EnsureSettings;
import kz.qazmarka.h2k.config.TopicNamingSettings;

/**
 * Иммутабельный набор параметров ensure-цикла: целевые партиции/репликация, набор конфигов,
 * таймауты и политики апгрейда. Формируется один раз из плоских DTO конфигурации и далее передаётся
 * во внутренние компоненты {@link kz.qazmarka.h2k.kafka.ensure.TopicEnsureService}.
 */
public final class TopicEnsureConfig {
    private final int topicNameMaxLen;
    private final UnaryOperator<String> topicSanitizer;
    private final int topicPartitions;
    private final short topicReplication;
    private final Map<String, String> topicConfigs;
    private final boolean ensureIncreasePartitions;
    private final boolean ensureDiffConfigs;
    private final long adminTimeoutMs;
    private final long unknownBackoffMs;

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
    public static final class Builder {
        private int topicNameMaxLen;
        private UnaryOperator<String> topicSanitizer = UnaryOperator.identity();
        private int topicPartitions;
        private short topicReplication;
        private Map<String, String> topicConfigs = Collections.emptyMap();
        private boolean ensureIncreasePartitions;
        private boolean ensureDiffConfigs;
        private long adminTimeoutMs;
        private long unknownBackoffMs;

        public Builder topicNameMaxLen(int v) { this.topicNameMaxLen = v; return this; }
        public Builder topicSanitizer(UnaryOperator<String> v) { this.topicSanitizer = v; return this; }
        public Builder topicPartitions(int v) { this.topicPartitions = v; return this; }
        public Builder topicReplication(short v) { this.topicReplication = v; return this; }
        public Builder topicConfigs(Map<String, String> v) { this.topicConfigs = v; return this; }
        public Builder ensureIncreasePartitions(boolean v) { this.ensureIncreasePartitions = v; return this; }
        public Builder ensureDiffConfigs(boolean v) { this.ensureDiffConfigs = v; return this; }
        public Builder adminTimeoutMs(long v) { this.adminTimeoutMs = v; return this; }
        public Builder unknownBackoffMs(long v) { this.unknownBackoffMs = v; return this; }
        public TopicEnsureConfig build() { return new TopicEnsureConfig(this); }
    }

    public static Builder builder() { return new Builder(); }

    public static TopicEnsureConfig from(EnsureSettings ensureSettings, TopicNamingSettings topicSettings) {
        Objects.requireNonNull(ensureSettings, "EnsureSettings не может быть null");
        Objects.requireNonNull(topicSettings, "TopicNamingSettings не может быть null");
        EnsureSettings.TopicSpec topicSpec = ensureSettings.getTopicSpec();
        EnsureSettings.AdminSpec adminSpec = ensureSettings.getAdminSpec();
        Map<String, String> topicConfigs = topicSettings.getTopicConfigs();

        return builder()
                .topicNameMaxLen(topicSettings.getMaxLength())
                .topicSanitizer(topicSettings::sanitize)
                .topicPartitions(topicSpec.getPartitions())
                .topicReplication(topicSpec.getReplication())
                .topicConfigs(topicConfigs)
                .ensureIncreasePartitions(ensureSettings.isAllowIncreasePartitions())
                .ensureDiffConfigs(ensureSettings.isAllowDiffConfigs())
                .adminTimeoutMs(adminSpec.getTimeoutMs())
                .unknownBackoffMs(adminSpec.getUnknownBackoffMs())
                .build();
    }

    public int topicNameMaxLen() { return topicNameMaxLen; }

    public UnaryOperator<String> topicSanitizer() { return topicSanitizer; }

    public int topicPartitions() { return topicPartitions; }

    public short topicReplication() { return topicReplication; }

    public Map<String, String> topicConfigs() { return topicConfigs; }

    public boolean ensureIncreasePartitions() { return ensureIncreasePartitions; }

    public boolean ensureDiffConfigs() { return ensureDiffConfigs; }

    public long adminTimeoutMs() { return adminTimeoutMs; }

    public long unknownBackoffMs() { return unknownBackoffMs; }
}
