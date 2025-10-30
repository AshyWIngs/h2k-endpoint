package kz.qazmarka.h2k.config;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.util.Parsers;

/**
 * Конфигурация подсистемы ensureTopics: целевые партиции/репликация, таймауты, client.id и политика backoff.
 * Значения валидируются и приводятся к минимально допустимым, чтобы избежать некорректных настроек в рантайме.
 */
public final class EnsureSection {
    private static final Logger LOG = LoggerFactory.getLogger(EnsureSection.class);
    private static final String ERR_MIN_FMT = "Некорректное значение {}={}, устанавливаю минимум: {}";

    final boolean ensureTopics;
    final boolean ensureIncreasePartitions;
    final boolean ensureDiffConfigs;
    final int topicPartitions;
    final short topicReplication;
    final long adminTimeoutMs;
    final String adminClientId;
    final long unknownBackoffMs;

    private static final class Params {
        boolean ensureTopics;
        boolean ensureIncreasePartitions;
        boolean ensureDiffConfigs;
        int topicPartitions;
        short topicReplication;
        long adminTimeoutMs;
        String adminClientId;
        long unknownBackoffMs;
    }

    private EnsureSection(Params p) {
        this.ensureTopics = p.ensureTopics;
        this.ensureIncreasePartitions = p.ensureIncreasePartitions;
        this.ensureDiffConfigs = p.ensureDiffConfigs;
        this.topicPartitions = p.topicPartitions;
        this.topicReplication = p.topicReplication;
        this.adminTimeoutMs = p.adminTimeoutMs;
        this.adminClientId = p.adminClientId;
        this.unknownBackoffMs = p.unknownBackoffMs;
    }

    /**
     * Считывает ключи {@code h2k.ensure.*} и возврашает иммутабельный объект с безопасными значениями.
     *
     * @param cfg конфигурация, содержащая параметры ensure
     * @return нормализованный набор ensure-настроек
     */
    static EnsureSection from(Configuration cfg) {
        Params p = new Params();
        p.ensureTopics = readFlag(cfg, H2kConfig.K_ENSURE_TOPICS, H2kConfig.DEFAULT_ENSURE_TOPICS);
        p.ensureIncreasePartitions = readFlag(cfg, H2kConfig.K_ENSURE_INCREASE_PARTITIONS, H2kConfig.DEFAULT_ENSURE_INCREASE_PARTITIONS);
        p.ensureDiffConfigs = readFlag(cfg, H2kConfig.K_ENSURE_DIFF_CONFIGS, H2kConfig.DEFAULT_ENSURE_DIFF_CONFIGS);
        p.topicPartitions = readTopicPartitions(cfg);
        p.topicReplication = readTopicReplication(cfg);
        AdminSettings admin = readAdminSettings(cfg);
        p.adminTimeoutMs = admin.timeoutMs;
        p.adminClientId = admin.clientId;
        p.unknownBackoffMs = readUnknownBackoff(cfg);

        return new EnsureSection(p);
    }

    private static boolean readFlag(Configuration cfg, String key, boolean defaultValue) {
        return cfg.getBoolean(key, defaultValue);
    }

    private static int readTopicPartitions(Configuration cfg) {
        int partitions = cfg.getInt(H2kConfig.K_TOPIC_PARTITIONS, H2kConfig.DEFAULT_TOPIC_PARTITIONS);
        return ensureMinInt(partitions, 1, H2kConfig.K_TOPIC_PARTITIONS);
    }

    private static short readTopicReplication(Configuration cfg) {
        int replication = cfg.getInt(H2kConfig.K_TOPIC_REPLICATION_FACTOR, H2kConfig.DEFAULT_TOPIC_REPLICATION);
        return (short) ensureMinInt(replication, 1, H2kConfig.K_TOPIC_REPLICATION_FACTOR);
    }

    private static long readUnknownBackoff(Configuration cfg) {
        long backoff = cfg.getLong(H2kConfig.K_ENSURE_UNKNOWN_BACKOFF_MS, H2kConfig.DEFAULT_UNKNOWN_BACKOFF_MS);
        return ensureMinLong(backoff, 1L, H2kConfig.K_ENSURE_UNKNOWN_BACKOFF_MS);
    }

    private static AdminSettings readAdminSettings(Configuration cfg) {
        long timeoutMs = Parsers.readLong(cfg, H2kConfig.K_ADMIN_TIMEOUT_MS, H2kConfig.DEFAULT_ADMIN_TIMEOUT_MS);
        String clientId = Parsers.buildAdminClientId(cfg, H2kConfig.K_ADMIN_CLIENT_ID, H2kConfig.DEFAULT_ADMIN_CLIENT_ID);
        return new AdminSettings(timeoutMs, clientId);
    }

    private static int ensureMinInt(int value, int minimum, String key) {
        if (value < minimum) {
            LOG.warn(ERR_MIN_FMT, key, value, minimum);
            return minimum;
        }
        return value;
    }

    private static long ensureMinLong(long value, long minimum, String key) {
        if (value < minimum) {
            LOG.warn(ERR_MIN_FMT, key, value, minimum);
            return minimum;
        }
        return value;
    }

    private static final class AdminSettings {
        final long timeoutMs;
        final String clientId;

        AdminSettings(long timeoutMs, String clientId) {
            this.timeoutMs = timeoutMs;
            this.clientId = clientId;
        }
    }
}
