package kz.qazmarka.h2k.config;

import kz.qazmarka.h2k.util.Parsers;

import org.apache.hadoop.conf.Configuration;

/**
 * Настройки, связанные с именованием Kafka-топиков и набором CF, подлежащих репликации.
 */
public final class TopicSection {
    final String topicPattern;
    final int topicMaxLength;
    final String[] cfNames;
    final byte[][] cfBytes;

    private TopicSection(String topicPattern,
                         int topicMaxLength,
                         String[] cfNames,
                         byte[][] cfBytes) {
        this.topicPattern = topicPattern;
        this.topicMaxLength = topicMaxLength;
        this.cfNames = cfNames;
        this.cfBytes = cfBytes;
    }

    /**
     * Парсит шаблон имён, ограничение длины и список CF из конфигурации.
     */
    static TopicSection from(Configuration cfg) {
        String topicPattern = Parsers.readTopicPattern(cfg, H2kConfig.K_TOPIC_PATTERN, H2kConfig.PLACEHOLDER_TABLE);
        int topicMaxLength = Parsers.readIntMin(cfg, H2kConfig.K_TOPIC_MAX_LENGTH, H2kConfig.DEFAULT_TOPIC_MAX_LENGTH, 1);
        String[] cfNames = Parsers.readCfNames(cfg, H2kConfig.K_CF_LIST, H2kConfig.DEFAULT_CF_NAME);
        byte[][] cfBytes = Parsers.toUtf8Bytes(cfNames);
        return new TopicSection(topicPattern, topicMaxLength, cfNames, cfBytes);
    }
}
