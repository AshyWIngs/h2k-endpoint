package kz.qazmarka.h2k.config;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Юнит‑тесты плоского DTO {@link TopicNamingSettings}: проверяем подстановку плейсхолдеров
 * и правила санитизации имён топиков.
 */
class TopicNamingSettingsTest {

    @Test
    @DisplayName("resolve: ${table} формирует namespace_qualifier")
    void resolveReplacesTablePlaceholder() {
        TopicNamingSettings settings = new TopicNamingSettings(
                "${table}",
                64,
                Collections.emptyMap());

        TableName table = TableName.valueOf("SALES", "RAW_ORDERS");
        String topic = settings.resolve(table);

        assertEquals("SALES_RAW_ORDERS", topic);
    }

    @Test
    @DisplayName("resolve: default namespace не даёт лишних разделителей")
    void resolveDefaultNamespaceHasNoPrefix() {
        TopicNamingSettings settings = new TopicNamingSettings(
                "prefix_${namespace}_${qualifier}",
                64,
                Collections.emptyMap());

        TableName table = TableName.valueOf("default", "T_DOCS");
        String topic = settings.resolve(table);

        assertTrue(topic.startsWith("prefix__T_DOCS") || topic.startsWith("prefix_T_DOCS"));
        assertTrue(topic.endsWith("T_DOCS"));
    }

    @Test
    @DisplayName("sanitize: некорректные символы заменяются на безопасные подчёркивания")
    void sanitizeReplacesInvalidNames() {
    TopicNamingSettings settings = new TopicNamingSettings(
        "ignored",
        3,
        Collections.emptyMap());

        String sanitized = settings.sanitize("../????");
        assertEquals(3, sanitized.length());
        assertTrue(sanitized.matches("_+"));

        final Map<String, String> configsView = settings.getTopicConfigs();
        UnsupportedOperationException error = assertThrows(
                UnsupportedOperationException.class,
                () -> configsView.put("k", "v"));
        assertNotNull(error);
    }
}
