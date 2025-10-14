package kz.qazmarka.h2k.endpoint.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;

class TopicManagerTest {

    @Test
    @DisplayName("getMetrics() включает зарегистрированные дополнительные счётчики")
    void metricsIncludeRegisteredValues() {
        TopicManager manager = topicManager();
        manager.registerMetric("custom.metric", () -> 42L);
        Map<String, Long> metrics = manager.getMetrics();
        assertEquals(42L, metrics.get("custom.metric"));
        UnsupportedOperationException immutableSnapshot =
                assertThrows(UnsupportedOperationException.class, () -> metrics.put("x", 1L));
        assertEquals(UnsupportedOperationException.class, immutableSnapshot.getClass());
    }

    @Test
    @DisplayName("registerMetric() отклоняет пустое имя")
    void registerMetricRejectsBlankName() {
        TopicManager manager = topicManager();
        IllegalArgumentException invalidName =
                assertThrows(IllegalArgumentException.class, () -> manager.registerMetric("  ", () -> 1L));
        assertEquals(IllegalArgumentException.class, invalidName.getClass());
    }

    @Test
    @DisplayName("resolveTopic кеширует значения по TableName")
    void resolveTopicCachesPerTable() throws Exception {
        TopicManager manager = topicManager();
        TableName table = TableName.valueOf("ns", "tbl");

        String topic1 = manager.resolveTopic(table);
        String topic2 = manager.resolveTopic(table);

        assertEquals(topic1, topic2, "Повторное разрешение должно давать тот же топик");

        ConcurrentMap<?, ?> cache = topicCache(manager);
        assertEquals(1, cache.size(), "Кеш должен содержать одно значение");
        Object cached = cache.get(table.getNameWithNamespaceInclAsString());
        assertEquals(topic1, String.valueOf(cached));
    }

    @Test
    @DisplayName("ensureTopicIfNeeded пропускает пустые имена и отключённый ensure")
    void ensureTopicNoopWhenDisabledOrBlank() {
        TopicManager manager = topicManager();
        manager.ensureTopicIfNeeded(null);
        manager.ensureTopicIfNeeded("");
        assertFalse(manager.ensureEnabled(), "Ensure должен быть отключён для TopicEnsurer.disabled()");
    }

    @Test
    @DisplayName("ensureTopicIfNeeded подавляет исключения TopicEnsurer")
    void ensureTopicSwallowsExceptions() throws Exception {
        TopicEnsurer throwingEnsurer = enabledEnsurerWithNullService();
        TopicManager manager = new TopicManager(h2kConfig(), throwingEnsurer);

        manager.ensureTopicIfNeeded("valid-topic"); // NPE внутри TopicEnsurer.ensureTopic() не должен выплыть
        assertTrue(manager.ensureEnabled(), "Ensure должен считаться включённым для кастомного энсюрера");
    }

    @Test
    @DisplayName("ensureEnabled отражает состояние TopicEnsurer")
    void ensureEnabledReflectsEnsurer() throws Exception {
        TopicManager disabled = topicManager();
        assertFalse(disabled.ensureEnabled(), "Для TopicEnsurer.disabled() ensureEnabled=false");

        TopicManager enabled = new TopicManager(h2kConfig(), enabledEnsurerWithNullService());
        assertTrue(enabled.ensureEnabled(), "Кастомный TopicEnsurer с disabledMode=false должен считаться активным");
    }

    private static TopicManager topicManager() {
        return new TopicManager(h2kConfig(), TopicEnsurer.disabled());
    }

    private static H2kConfig h2kConfig() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.topic.pattern", "${namespace}.${qualifier}");
        return H2kConfig.from(cfg, "mock:9092");
    }

    private static ConcurrentMap<?, ?> topicCache(TopicManager manager) throws Exception {
        java.lang.reflect.Field cacheField = TopicManager.class.getDeclaredField("topicCache");
        cacheField.setAccessible(true);
        Object raw = cacheField.get(manager);
        if (raw instanceof ConcurrentMap<?, ?>) {
            return (ConcurrentMap<?, ?>) raw;
        }
        throw new IllegalStateException("topicCache field имеет неожиданный тип: " + raw);
    }

    private static TopicEnsurer enabledEnsurerWithNullService() throws Exception {
        Class<?> serviceClass = Class.forName("kz.qazmarka.h2k.kafka.ensure.TopicEnsureService");
        java.lang.reflect.Constructor<TopicEnsurer> ctor =
                TopicEnsurer.class.getDeclaredConstructor(serviceClass, boolean.class);
        ctor.setAccessible(true);
        return ctor.newInstance(null, false);
    }
}
