package kz.qazmarka.h2k.config;

import kz.qazmarka.h2k.util.Parsers;

import org.apache.hadoop.conf.Configuration;

/**
 * Настройки, связанные с именованием Kafka-топиков.
 */
public final class TopicSection {
    final String topicPattern;
    final int topicMaxLength;
    
    private TopicSection(String topicPattern,
                         int topicMaxLength) {
        this.topicPattern = topicPattern;
        this.topicMaxLength = topicMaxLength;
    }

    /**
     * Парсит шаблон имён и ограничение длины из конфигурации.
     *
     * @param cfg конфигурация Hadoop с ключами {@code h2k.topic.*}
     * @return секция настроек топиков (иммутабельная)
     */
    static TopicSection from(Configuration cfg) {
        String topicPattern = Parsers.readTopicPattern(cfg, H2kConfig.K_TOPIC_PATTERN, H2kConfig.PLACEHOLDER_TABLE);
        int topicMaxLength = Parsers.readIntMin(cfg, H2kConfig.K_TOPIC_MAX_LENGTH, H2kConfig.DEFAULT_TOPIC_MAX_LENGTH, 1);
        return new TopicSection(topicPattern, topicMaxLength);
    }
}
