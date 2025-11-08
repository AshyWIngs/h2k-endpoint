package kz.qazmarka.h2k.endpoint.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.MBeanAttributeInfo;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Проверяет, что для русских ключей метрик формируются корректные JMX‑алиасы
 * (ASCII‑атрибуты), и чтение значений по алиасу возвращает данные по исходному ключу.
 */
class H2kMetricsJmxAliasTest {

    @Test
    @DisplayName("Алиасы: создание.успех → create_ok, ensure.бэкофф.размер → unknown_backoff_size")
    void aliasesForRussianKeys() throws Exception {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("создание.успех", 7L);
        metrics.put("ensure.бэкофф.размер", 9L);
        metrics.put("существует.да", 11L);
        metrics.put("ensure.пропуски.из-за.паузы", 13L);

        H2kMetricsJmx mbean = H2kMetricsJmx.createForTest(() -> metrics);

        // Проверяем, что среди атрибутов есть нужные имена
        Set<String> names = java.util.Arrays.stream(mbean.getMBeanInfo().getAttributes())
                .map(MBeanAttributeInfo::getName)
                .collect(Collectors.toSet());
        assertTrue(names.contains("create_ok"), "Ожидается атрибут create_ok для 'создание.успех'");
        assertTrue(names.contains("unknown_backoff_size"), "Ожидается атрибут unknown_backoff_size для 'ensure.бэкофф.размер'");
        assertTrue(names.contains("exists_true"), "Ожидается атрибут exists_true для 'существует.да'");
        assertTrue(names.contains("ensure_cooldown_skipped"), "Ожидается атрибут ensure_cooldown_skipped для 'ensure.пропуски.из-за.паузы'");

        // Проверяем чтение значений по алиасам
        assertEquals(7L, ((Number) mbean.getAttribute("create_ok")).longValue());
        assertEquals(9L, ((Number) mbean.getAttribute("unknown_backoff_size")).longValue());
        assertEquals(11L, ((Number) mbean.getAttribute("exists_true")).longValue());
        assertEquals(13L, ((Number) mbean.getAttribute("ensure_cooldown_skipped")).longValue());
    }
}
