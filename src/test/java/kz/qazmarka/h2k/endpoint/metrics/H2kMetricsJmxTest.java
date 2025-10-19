package kz.qazmarka.h2k.endpoint.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanAttributeInfo;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Проверяет нормализацию и уникальность имён атрибутов JMX-метрик.
 */
class H2kMetricsJmxTest {

    @Test
    @DisplayName("Имена метрик нормализуются и не пересекаются")
    void metricNamesNormalizedAndUnique() throws Exception {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("wal.entries.total", 1L);
        metrics.put("Wal Entries Total", 2L);
        metrics.put("wal_entries_total", 3L);

        H2kMetricsJmx mbean = H2kMetricsJmx.createForTest(() -> metrics);

        MBeanAttributeInfo[] attributes = mbean.getMBeanInfo().getAttributes();
        Set<String> uniqueNames = new HashSet<>();
        for (MBeanAttributeInfo info : attributes) {
            assertTrue(uniqueNames.add(info.getName()), "Имя атрибута дублируется: " + info.getName());
        }

        assertMetricValue(mbean, "wal_entries_total", 1L);
        assertMetricValue(mbean, "wal_entries_total_2", 2L);
        assertMetricValue(mbean, "wal_entries_total_3", 3L);
    }

    @Test
    @DisplayName("Пустые и некорректные имена превращаются в плейсхолдеры metric")
    void blankNamesBecomeMetricPlaceholder() throws Exception {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("", 7L);
        metrics.put("???", 9L);

        H2kMetricsJmx mbean = H2kMetricsJmx.createForTest(() -> metrics);

        assertMetricValue(mbean, "metric", 7L);
        assertMetricValue(mbean, "metric_2", 9L);
    }

    private static void assertMetricValue(H2kMetricsJmx mbean, String attribute, long expected)
            throws Exception {
        Object value = mbean.getAttribute(attribute);
        assertNotNull(value, "Атрибут " + attribute + " должен существовать");
        assertEquals(expected, ((Number) value).longValue(), "Некорректное значение метрики " + attribute);
    }
}
