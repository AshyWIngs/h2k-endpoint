package kz.qazmarka.h2k.config;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import kz.qazmarka.h2k.util.Parsers;

/**
 * Загружает {@link H2kConfig} из HBase {@link Configuration}, инкапсулируя логику парсинга.
 */
final class H2kConfigLoader {

    H2kConfig load(Configuration cfg, String bootstrap) {
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            throw new IllegalArgumentException("Отсутствует обязательный параметр bootstrap.servers: h2k.kafka.bootstrap.servers пустой или не задан");
        }
        bootstrap = bootstrap.trim();

        TopicSection topic = TopicSection.from(cfg);
        PayloadSection payload = PayloadSection.from(cfg);
        AvroSection avro = AvroSection.from(cfg);
        EnsureSection ensure = EnsureSection.from(cfg);
        ProducerBatchSection batch = ProducerBatchSection.from(cfg);
        TableMapSection tables = TableMapSection.from(cfg);
        Map<String, String> topicConfigs = Parsers.readWithPrefix(cfg, H2kConfig.Keys.TOPIC_CONFIG_PREFIX);

        return new H2kConfig.Builder(bootstrap)
                .topicPattern(topic.topicPattern)
                .topicMaxLength(topic.topicMaxLength)
                .cfNames(topic.cfNames)
                .cfBytes(topic.cfBytes)
                .includeRowKey(payload.includeRowKey)
                .rowkeyEncoding(payload.rowkeyEncoding)
                .rowkeyBase64(payload.rowkeyBase64)
                .includeMeta(payload.includeMeta)
                .includeMetaWal(payload.includeMetaWal)
                .jsonSerializeNulls(payload.jsonSerializeNulls)
                .payloadFormat(payload.payloadFormat)
                .serializerFactoryClass(payload.serializerFactoryClass)
                .avroMode(avro.mode)
                .avroSchemaDir(avro.schemaDir)
                .avroSchemaRegistryUrls(avro.schemaRegistryUrls)
                .avroSrAuth(avro.auth)
                .avroProps(avro.props)
                .ensureTopics(ensure.ensureTopics)
                .ensureIncreasePartitions(ensure.ensureIncreasePartitions)
                .ensureDiffConfigs(ensure.ensureDiffConfigs)
                .topicPartitions(ensure.topicPartitions)
                .topicReplication(ensure.topicReplication)
                .adminTimeoutMs(ensure.adminTimeoutMs)
                .adminClientId(ensure.adminClientId)
                .unknownBackoffMs(ensure.unknownBackoffMs)
                .awaitEvery(batch.awaitEvery)
                .awaitTimeoutMs(batch.awaitTimeoutMs)
                .producerBatchCountersEnabled(batch.countersEnabled)
                .producerBatchDebugOnFailure(batch.debugOnFailure)
                .topicConfigs(topicConfigs)
                .saltBytesByTable(tables.saltMap)
                .capacityHintByTable(tables.capacityHints)
                .build();
    }
}
