package kz.qazmarka.h2k.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;

/**
 * Отдельный билдер для сборки итогового {@link H2kConfig} без жёсткой связности.
 * Используется загрузчиком конфигурации и тестами для декларативной настройки секций.
 */
public final class H2kConfigBuilder {
    private final String bootstrap;
    private String topicPattern = H2kConfig.PLACEHOLDER_TABLE;
    private int topicMaxLength = H2kConfig.DEFAULT_TOPIC_MAX_LENGTH;
    private String avroSchemaDir = H2kConfig.DEFAULT_AVRO_SCHEMA_DIR;
    private List<String> avroSchemaRegistryUrls = Collections.emptyList();
    private Map<String, String> avroSrAuth = Collections.emptyMap();
    private Map<String, String> avroProps = Collections.emptyMap();
    private int avroMaxPendingRetries = H2kConfig.DEFAULT_MAX_PENDING_RETRIES;
    private boolean ensureTopics = H2kConfig.DEFAULT_ENSURE_TOPICS;
    private boolean ensureIncreasePartitions = H2kConfig.DEFAULT_ENSURE_INCREASE_PARTITIONS;
    private boolean ensureDiffConfigs = H2kConfig.DEFAULT_ENSURE_DIFF_CONFIGS;
    private int topicPartitions = H2kConfig.DEFAULT_TOPIC_PARTITIONS;
    private short topicReplication = H2kConfig.DEFAULT_TOPIC_REPLICATION;
    private long adminTimeoutMs = H2kConfig.DEFAULT_ADMIN_TIMEOUT_MS;
    private String adminClientId = H2kConfig.DEFAULT_ADMIN_CLIENT_ID;
    private long unknownBackoffMs = H2kConfig.DEFAULT_UNKNOWN_BACKOFF_MS;
    private int awaitEvery = H2kConfig.DEFAULT_AWAIT_EVERY;
    private int awaitTimeoutMs = H2kConfig.DEFAULT_AWAIT_TIMEOUT_MS;
    private Map<String, String> topicConfigs = Collections.emptyMap();
    private PhoenixTableMetadataProvider tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
    private boolean observersEnabled = H2kConfig.DEFAULT_OBSERVERS_ENABLED;
    private boolean jmxEnabled = H2kConfig.DEFAULT_JMX_ENABLED;

    public H2kConfigBuilder(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public H2kConfigBuilder tableMetadataProvider(PhoenixTableMetadataProvider provider) {
        this.tableMetadataProvider = (provider == null) ? PhoenixTableMetadataProvider.NOOP : provider;
        return this;
    }

    public H2kConfigBuilder observersEnabled(boolean enabled) {
        this.observersEnabled = enabled;
        return this;
    }

    public H2kConfigBuilder jmxEnabled(boolean enabled) {
        this.jmxEnabled = enabled;
        return this;
    }

    public TopicOptions topic() {
        return new TopicOptions();
    }

    public AvroOptions avro() {
        return new AvroOptions();
    }

    public EnsureOptions ensure() {
        return new EnsureOptions();
    }

    public ProducerOptions producer() {
        return new ProducerOptions();
    }

    public H2kConfig build() {
        List<String> urls = (avroSchemaRegistryUrls == null) ? Collections.<String>emptyList() : avroSchemaRegistryUrls;
        Map<String, String> auth = (avroSrAuth == null) ? Collections.<String, String>emptyMap() : avroSrAuth;
        Map<String, String> props = (avroProps == null) ? Collections.<String, String>emptyMap() : avroProps;
        Map<String, String> configs = (topicConfigs == null) ? Collections.<String, String>emptyMap() : topicConfigs;

        List<String> immutableUrls = Collections.unmodifiableList(new java.util.ArrayList<>(urls));
        Map<String, String> immutableAuth = Collections.unmodifiableMap(new HashMap<>(auth));
        Map<String, String> immutableProps = Collections.unmodifiableMap(new HashMap<>(props));
        Map<String, String> immutableConfigs = Collections.unmodifiableMap(new HashMap<>(configs));

        TopicNamingSettings topicSettings = new TopicNamingSettings(
                topicPattern,
                topicMaxLength,
                immutableConfigs);
        AvroSettings avroSettings = new AvroSettings(
                avroSchemaDir,
                immutableUrls,
                immutableAuth,
                immutableProps,
                avroMaxPendingRetries);
        EnsureSettings.TopicSpec topicSpec = new EnsureSettings.TopicSpec(
                topicPartitions,
                topicReplication);
        EnsureSettings.AdminSpec adminSpec = new EnsureSettings.AdminSpec(
                adminTimeoutMs,
                adminClientId,
                unknownBackoffMs);
        EnsureSettings ensureSettings = new EnsureSettings(
                ensureTopics,
                ensureIncreasePartitions,
                ensureDiffConfigs,
                topicSpec,
                adminSpec);
        ProducerAwaitSettings producerAwaitSettings = new ProducerAwaitSettings(
                awaitEvery,
                awaitTimeoutMs);

        MonitoringSettings monitoring = new MonitoringSettings(observersEnabled, jmxEnabled);
        H2kConfig.Sections sections = new H2kConfig.Sections(
                topicSettings,
                avroSettings,
                ensureSettings,
                producerAwaitSettings,
                tableMetadataProvider,
                monitoring);
        return new H2kConfig(bootstrap, sections);
    }

    public final class TopicOptions {
        public TopicOptions pattern(String pattern) {
            topicPattern = pattern;
            return this;
        }

        public TopicOptions maxLength(int maxLength) {
            topicMaxLength = maxLength;
            return this;
        }

        public TopicOptions configs(Map<String, String> configs) {
            topicConfigs = configs;
            return this;
        }

        public H2kConfigBuilder done() {
            return H2kConfigBuilder.this;
        }
    }

    public final class AvroOptions {
        public AvroOptions schemaDir(String dir) {
            avroSchemaDir = (dir == null || dir.trim().isEmpty()) ? H2kConfig.DEFAULT_AVRO_SCHEMA_DIR : dir.trim();
            return this;
        }

        public AvroOptions schemaRegistryUrls(List<String> urls) {
            avroSchemaRegistryUrls = urls;
            return this;
        }

        public AvroOptions schemaRegistryAuth(Map<String, String> auth) {
            avroSrAuth = auth;
            return this;
        }

        public AvroOptions properties(Map<String, String> props) {
            avroProps = props;
            return this;
        }

        public AvroOptions maxPendingRetries(int maxRetries) {
            avroMaxPendingRetries = maxRetries;
            return this;
        }

        public H2kConfigBuilder done() {
            return H2kConfigBuilder.this;
        }
    }

    public final class EnsureOptions {
        public EnsureOptions enabled(boolean enabled) {
            ensureTopics = enabled;
            return this;
        }

        public EnsureOptions allowIncreasePartitions(boolean allow) {
            ensureIncreasePartitions = allow;
            return this;
        }

        public EnsureOptions allowDiffConfigs(boolean allow) {
            ensureDiffConfigs = allow;
            return this;
        }

        public EnsureOptions partitions(int partitions) {
            topicPartitions = partitions;
            return this;
        }

        public EnsureOptions replication(short replication) {
            topicReplication = replication;
            return this;
        }

        public EnsureOptions adminTimeoutMs(long timeoutMs) {
            adminTimeoutMs = timeoutMs;
            return this;
        }

        public EnsureOptions adminClientId(String clientId) {
            adminClientId = clientId;
            return this;
        }

        public EnsureOptions unknownBackoffMs(long backoffMs) {
            unknownBackoffMs = backoffMs;
            return this;
        }

        public H2kConfigBuilder done() {
            return H2kConfigBuilder.this;
        }
    }

    public final class ProducerOptions {
        public ProducerOptions awaitEvery(int value) {
            awaitEvery = value;
            return this;
        }

        public ProducerOptions awaitTimeoutMs(int value) {
            awaitTimeoutMs = value;
            return this;
        }

        public H2kConfigBuilder done() {
            return H2kConfigBuilder.this;
        }
    }
}
