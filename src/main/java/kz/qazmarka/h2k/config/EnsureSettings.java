package kz.qazmarka.h2k.config;

import java.util.Objects;

/**
 * DTO с настройками ensure-топиков: флаги включения, целевые параметры топика и конфигурация AdminClient.
 */
public final class EnsureSettings {

    private final boolean ensureTopics;
    private final boolean allowIncreasePartitions;
    private final boolean allowDiffConfigs;
    private final TopicSpec topicSpec;
    private final AdminSpec adminSpec;

    EnsureSettings(boolean ensureTopics,
                   boolean allowIncreasePartitions,
                   boolean allowDiffConfigs,
                   TopicSpec topicSpec,
                   AdminSpec adminSpec) {
        this.ensureTopics = ensureTopics;
        this.allowIncreasePartitions = allowIncreasePartitions;
        this.allowDiffConfigs = allowDiffConfigs;
        this.topicSpec = Objects.requireNonNull(topicSpec, "TopicSpec не может быть null");
        this.adminSpec = Objects.requireNonNull(adminSpec, "AdminSpec не может быть null");
    }

    public boolean isEnsureTopics() {
        return ensureTopics;
    }

    public boolean isAllowIncreasePartitions() {
        return allowIncreasePartitions;
    }

    public boolean isAllowDiffConfigs() {
        return allowDiffConfigs;
    }

    public TopicSpec getTopicSpec() {
        return topicSpec;
    }

    public AdminSpec getAdminSpec() {
        return adminSpec;
    }

    /**
     * Параметры создаваемого Kafka-топика: число партиций и фактор репликации.
     */
    public static final class TopicSpec {
        private final int partitions;
        private final short replication;

        public TopicSpec(int partitions, short replication) {
            this.partitions = partitions < 1 ? 1 : partitions;
            short normalizedReplication = replication < 1 ? (short) 1 : replication;
            this.replication = normalizedReplication;
        }

        public int getPartitions() {
            return partitions;
        }

        public short getReplication() {
            return replication;
        }
    }

    /**
     * Настройки Kafka AdminClient, применяемого для ensure.
     */
    public static final class AdminSpec {
        private final long timeoutMs;
        private final String clientId;
        private final long unknownBackoffMs;

        public AdminSpec(long timeoutMs, String clientId, long unknownBackoffMs) {
            this.timeoutMs = timeoutMs < 1 ? 1 : timeoutMs;
            this.clientId = Objects.requireNonNull(clientId, "clientId не может быть null");
            this.unknownBackoffMs = unknownBackoffMs < 1 ? 1 : unknownBackoffMs;
        }

        public long getTimeoutMs() {
            return timeoutMs;
        }

        public String getClientId() {
            return clientId;
        }

        public long getUnknownBackoffMs() {
            return unknownBackoffMs;
        }
    }
}
