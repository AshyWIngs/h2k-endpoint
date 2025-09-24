package kz.qazmarka.h2k.kafka.ensure;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;

/**
 * Параметры создаваемого Kafka-топика: partitions/replication/configs.
 * Выделены в отдельный класс, чтобы исключить дублирование при одиночном и batch-создании.
 */
final class TopicParams {
    private final int partitions;
    private final short replication;
    private final Map<String, String> configs;

    private TopicParams(int partitions, short replication, Map<String, String> configs) {
        this.partitions = partitions;
        this.replication = replication;
        this.configs = configs;
    }

    static TopicParams from(TopicEnsureConfig config) {
        return new TopicParams(config.topicPartitions, config.topicReplication, config.topicConfigs);
    }

    /** Строит {@link NewTopic} с заданными partitions/replication/configs. */
    NewTopic newTopic(String name) {
        NewTopic nt = new NewTopic(name, partitions, replication);
        if (!configs.isEmpty()) {
            nt.configs(configs);
        }
        return nt;
    }

    int partitions() {
        return partitions;
    }

    short replication() {
        return replication;
    }

    Map<String, String> configs() {
        return configs;
    }
}
