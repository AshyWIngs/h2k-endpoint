package kz.qazmarka.h2k.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.util.Parsers;

/**
 * Настройки именования Kafka-топиков: шаблон и ограничения длины, а также карта дополнительных конфигураций.
 * Используется как плоский DTO, чтобы читатели конфигурации не зависели от громоздкого {@link H2kConfig}.
 */
public final class TopicNamingSettings {

    private static final String SAFE_PLACEHOLDER = "topic";

    private final String pattern;
    private final int maxLength;
    private final Map<String, String> topicConfigs;

    TopicNamingSettings(String pattern, int maxLength, Map<String, String> topicConfigs) {
        this.pattern = Objects.requireNonNull(pattern, "Шаблон топика не может быть null");
        this.maxLength = maxLength;
        Map<String, String> cfg = Objects.requireNonNull(topicConfigs, "Конфиги топиков не могут быть null");
        this.topicConfigs = Collections.unmodifiableMap(new HashMap<>(cfg));
    }

    public String getPattern() {
        return pattern;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public Map<String, String> getTopicConfigs() {
        return topicConfigs;
    }

    /**
     * Строит имя Kafka-топика для таблицы, используя шаблон и правила санитизации.
     * Плейсхолдеры {@code ${table}}, {@code ${namespace}}, {@code ${qualifier}} заменяются на значения
     * из {@link TableName}, после чего применяется {@link #sanitize(String)}.
     *
     * @param table имя таблицы HBase/Phoenix
     * @return нормализованное имя Kafka-топика
     */
    public String resolve(TableName table) {
        Objects.requireNonNull(table, "Имя таблицы не может быть null");
        String ns = table.getNamespaceAsString();
        String qualifier = table.getQualifierAsString();
        String tableAtom = H2kConfig.HBASE_DEFAULT_NS.equals(ns)
                ? qualifier
                : (ns + '_' + qualifier);
        String nsAtom = H2kConfig.HBASE_DEFAULT_NS.equals(ns) ? "" : ns;
        String base = pattern
                .replace(H2kConfig.PLACEHOLDER_TABLE, tableAtom)
                .replace(H2kConfig.PLACEHOLDER_NAMESPACE, nsAtom)
                .replace(H2kConfig.PLACEHOLDER_QUALIFIER, qualifier);
        return sanitize(base);
    }

    /**
     * Санитизирует произвольное имя топика по тем же правилам, что и {@link H2kConfig#sanitizeTopic(String)}:
     * удаляет повторные разделители, заменяет недопустимые символы на подчёркивания, защищает от значения "."/".."
     * и обрезает строку по ограничению {@link #getMaxLength()}.
     *
     * @param raw исходное имя топика или шаблон
     * @return безопасное имя, готовое к передаче в Kafka AdminClient
     */
    public String sanitize(String raw) {
        String base = raw == null ? "" : raw;
        String collapsed = Parsers.topicCollapseRepeatedDelimiters(
                Parsers.topicStripLeadingDelimiters(base));
        String sanitized = Parsers.topicSanitizeKafkaChars(collapsed);
        if (".".equals(sanitized) || "..".equals(sanitized) || sanitized.isEmpty()) {
            sanitized = SAFE_PLACEHOLDER;
        }
        return sanitized.length() > maxLength ? sanitized.substring(0, maxLength) : sanitized;
    }
}
