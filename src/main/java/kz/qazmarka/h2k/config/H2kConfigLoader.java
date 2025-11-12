package kz.qazmarka.h2k.config;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.util.Parsers;

/**
 * Загружает {@link H2kConfig} из HBase {@link Configuration}, инкапсулируя логику парсинга.
 */
final class H2kConfigLoader {
    private static final Logger LOG = LoggerFactory.getLogger(H2kConfigLoader.class);

    /** Загружает конфигурацию h2k без табличного провайдера (используется NOOP). */
    H2kConfig load(Configuration cfg, String bootstrap) {
        return load(cfg, bootstrap, PhoenixTableMetadataProvider.NOOP);
    }

    /**
     * Формирует основной {@link H2kConfig}, объединяя секции {@code h2k.*} из конфигурации.
     *
     * @param cfg              исходная конфигурация HBase/endpoint
     * @param bootstrap        обязательный список брокеров (host:port)
     * @param metadataProvider поставщик табличных метаданных Avro (позволяет брать соль/ёмкость из .avsc)
     * @return иммутабельная конфигурация, готовая к передаче в рабочие компоненты
     * @throws IllegalArgumentException если bootstrap не задан
     */
    H2kConfig load(Configuration cfg, String bootstrap, PhoenixTableMetadataProvider metadataProvider) {
        String sanitizedBootstrap = sanitizeBootstrap(bootstrap);
        ConfigSections sections = ConfigSections.collect(cfg);
        return assembleConfig(metadataProvider, sanitizedBootstrap, sections);
    }

    private static String sanitizeBootstrap(String bootstrap) {
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            throw new IllegalArgumentException("Отсутствует обязательный параметр bootstrap.servers: h2k.kafka.bootstrap.servers пустой или не задан");
        }
        return bootstrap.trim();
    }

    private H2kConfig assembleConfig(PhoenixTableMetadataProvider metadataProvider,
                                     String sanitizedBootstrap,
                                     ConfigSections sections) {
        H2kConfigBuilder builder = new H2kConfigBuilder(sanitizedBootstrap);
        builder.tableMetadataProvider(metadataProvider);

        applyTopicSection(builder, sections.topic, sections.topicConfigs);
        applyAvroSection(builder, sections.avro);
        applyEnsureSection(builder, sections.ensure);
        applyProducerSection(builder, sections.batch);
        builder.observersEnabled(sections.monitoring.isObserversEnabled());
        builder.jmxEnabled(sections.monitoring.isJmxEnabled());
        return builder.build();
    }

    private static void applyTopicSection(H2kConfigBuilder builder,
                                          TopicSection topic,
                                          Map<String, String> topicConfigs) {
        builder.topic()
                .pattern(topic.topicPattern)
                .maxLength(topic.topicMaxLength)
                .configs(topicConfigs)
                .done();
    }

    private static void applyAvroSection(H2kConfigBuilder builder, AvroSection avro) {
        builder.avro()
                .schemaDir(avro.schemaDir)
                .schemaRegistryUrls(avro.schemaRegistryUrls)
                .schemaRegistryAuth(avro.auth)
                .properties(avro.props)
                .done();
    }

    private static void applyEnsureSection(H2kConfigBuilder builder, EnsureSection ensure) {
        builder.ensure()
                .enabled(ensure.ensureTopics)
                .allowIncreasePartitions(ensure.ensureIncreasePartitions)
                .allowDiffConfigs(ensure.ensureDiffConfigs)
                .partitions(ensure.topicPartitions)
                .replication(ensure.topicReplication)
                .adminTimeoutMs(ensure.adminTimeoutMs)
                .adminClientId(ensure.adminClientId)
                .unknownBackoffMs(ensure.unknownBackoffMs)
                .done();
    }

    private static void applyProducerSection(H2kConfigBuilder builder, ProducerBatchSection batch) {
        builder.producer()
                .awaitEvery(batch.awaitEvery)
                .awaitTimeoutMs(batch.awaitTimeoutMs)
                .done();
    }

    private static final class ConfigSections {
        final TopicSection topic;
        final AvroSection avro;
        final EnsureSection ensure;
        final ProducerBatchSection batch;
        final Map<String, String> topicConfigs;
        final MonitoringSettings monitoring;

        private ConfigSections(TopicSection topic,
                               AvroSection avro,
                               EnsureSection ensure,
                               ProducerBatchSection batch,
                               Map<String, String> topicConfigs,
                               MonitoringSettings monitoring) {
            this.topic = topic;
            this.avro = avro;
            this.ensure = ensure;
            this.batch = batch;
            this.topicConfigs = topicConfigs;
            this.monitoring = monitoring;
        }

        static ConfigSections collect(Configuration cfg) {
            TopicSection topic = TopicSection.from(cfg);
            AvroSection avro = AvroSection.from(cfg);
            EnsureSection ensure = EnsureSection.from(cfg);
            ProducerBatchSection batch = ProducerBatchSection.from(cfg);
            Map<String, String> topicConfigs = Parsers.readWithPrefix(cfg, H2kConfig.Keys.TOPIC_CONFIG_PREFIX);
            if (topicConfigs.remove("compression.type") != null) {
                LOG.warn("Игнорируем h2k.topic.config.compression.type — endpoint всегда использует lz4");
            }
            boolean observersEnabled = Parsers.readBoolean(
                    cfg,
                    H2kConfig.Keys.OBSERVERS_ENABLED,
                    H2kConfig.DEFAULT_OBSERVERS_ENABLED);
            boolean jmxEnabled = Parsers.readBoolean(
                    cfg,
                    H2kConfig.Keys.JMX_ENABLED,
                    H2kConfig.DEFAULT_JMX_ENABLED);
            MonitoringSettings monitoring = new MonitoringSettings(observersEnabled, jmxEnabled);
            return new ConfigSections(topic, avro, ensure, batch, topicConfigs, monitoring);
        }
    }
}
