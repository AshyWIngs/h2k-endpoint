package kz.qazmarka.h2k.endpoint.topic;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.TopicNamingSettings;
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
    void resolveTopicCachesPerTable() {
        TopicManager manager = topicManager();
        TableName table = TableName.valueOf("ns", "tbl");

        String topic1 = manager.resolveTopic(table);
        String topic2 = manager.resolveTopic(table);

        assertEquals(topic1, topic2, "Повторное разрешение должно давать тот же топик");

        Map<String, String> cache = manager.topicCacheSnapshotForTest();
        assertEquals(1, cache.size(), "Кеш должен содержать одно значение");
        assertEquals(topic1, cache.get(table.getNameWithNamespaceInclAsString()));
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
    void ensureTopicSwallowsExceptions() {
        TopicEnsurer throwingEnsurer = enabledEnsurerWithNullService();
        TopicManager manager = new TopicManager(topicSettings(), throwingEnsurer);

        manager.ensureTopicIfNeeded("valid-topic"); // NPE внутри TopicEnsurer.ensureTopic() не должен выплыть
        assertTrue(manager.ensureEnabled(), "Ensure должен считаться включённым для кастомного энсюрера");
    }

    @Test
    @DisplayName("ensureEnabled отражает состояние TopicEnsurer")
    void ensureEnabledReflectsEnsurer() {
        TopicManager disabled = topicManager();
        assertFalse(disabled.ensureEnabled(), "Для TopicEnsurer.disabled() ensureEnabled=false");

        TopicManager enabled = new TopicManager(topicSettings(), enabledEnsurerWithNullService());
        assertTrue(enabled.ensureEnabled(), "Кастомный TopicEnsurer с disabledMode=false должен считаться активным");
    }

    @Test
    @DisplayName("ensureTopicIfNeeded ограничивает повторные ensure вызовы успешных тем")
    void ensureTopicHonoursSuccessCooldown() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        TopicEnsurer delegate = TopicEnsurer.testingDelegate(topic -> attempts.incrementAndGet());
        TopicManager manager = new TopicManager(topicSettings(), delegate);

        manager.ensureTopicIfNeeded("hot-topic");
        manager.ensureTopicIfNeeded("hot-topic");
        assertEquals(1, attempts.get(), "повторное ensure должно ожидать cooldown");
        assertEquals(1L, manager.ensureSkippedCount(), "пропущенные ensure должны учитываться в метрике");

        rewindEnsureState(manager, "hot-topic", true);
        manager.ensureTopicIfNeeded("hot-topic");
        assertEquals(2, attempts.get(), "после истечения cooldown ensure должен повториться");
        assertEquals(1L, manager.ensureSkippedCount(), "успешное ensure не увеличивает счётчик пропусков");
    }

    @Test
    @DisplayName("ensureTopicIfNeeded повторяет попытку после сбоя с учётом cooldown")
    void ensureTopicRetriesAfterFailureCooldown() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        TopicEnsurer failing = TopicEnsurer.testingDelegate(topic -> {
            attempts.incrementAndGet();
            throw new RuntimeException("fail");
        });
        TopicManager manager = new TopicManager(topicSettings(), failing);

        manager.ensureTopicIfNeeded("flaky-topic");
        manager.ensureTopicIfNeeded("flaky-topic");
        assertEquals(1, attempts.get(), "повторение сразу после сбоя должно блокироваться");
        assertEquals(1L, manager.ensureSkippedCount(), "сбойное ensure также учитывается в пропусках");

        rewindEnsureState(manager, "flaky-topic", false);
        manager.ensureTopicIfNeeded("flaky-topic");
        assertEquals(2, attempts.get(), "после истечения cooldown повторная попытка должна выполниться");
        assertEquals(1L, manager.ensureSkippedCount(), "повторная попытка не должна увеличивать пропуски");
    }

    private static TopicManager topicManager() {
        return new TopicManager(topicSettings(), TopicEnsurer.disabled());
    }

    private static H2kConfig h2kConfig() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.topic.pattern", "${namespace}.${qualifier}");
        return H2kConfig.from(cfg, "mock:9092");
    }

    private static TopicNamingSettings topicSettings() {
        return h2kConfig().getTopicSettings();
    }

    private static TopicEnsurer enabledEnsurerWithNullService() {
        return TopicEnsurer.testingDelegate(topic -> { /* no-op для теста */ });
    }

    private static void rewindEnsureState(TopicManager manager, String topic, boolean success) throws Exception {
        Field field = TopicManager.class.getDeclaredField("ensureStates");
        field.setAccessible(true);
        Object mapObj = field.get(manager);
        if (!(mapObj instanceof ConcurrentMap)) {
            throw new IllegalStateException("ensureStates не является ConcurrentMap");
        }
        ConcurrentMap<?, ?> states = (ConcurrentMap<?, ?>) mapObj;
        Object state = states.get(topic);
        if (state == null) {
            throw new IllegalStateException("ensure state для '" + topic + "' не найден");
        }
        Field inProgress = state.getClass().getDeclaredField("inProgress");
        inProgress.setAccessible(true);
        inProgress.setBoolean(state, false);

        Field lastSuccess = state.getClass().getDeclaredField("lastSuccess");
        Field lastFailure = state.getClass().getDeclaredField("lastFailure");
        lastSuccess.setAccessible(true);
        lastFailure.setAccessible(true);
        long rewindTs = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2);
        if (success) {
            lastSuccess.setLong(state, rewindTs);
            lastFailure.setLong(state, Long.MIN_VALUE);
        } else {
            lastFailure.setLong(state, rewindTs);
            lastSuccess.setLong(state, Long.MIN_VALUE);
        }
    }
}
