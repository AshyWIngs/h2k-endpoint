package kz.qazmarka.h2k.endpoint.topic;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

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

    private static final String HOT_TOPIC = "hot-topic";
    private static final String FLAKY_TOPIC = "flaky-topic";
    private static final long REWIND_DELTA_MS = TimeUnit.MINUTES.toMillis(2);

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
    void ensureTopicHonoursSuccessCooldown() {
        AtomicInteger attempts = new AtomicInteger();
        TopicEnsurer delegate = TopicEnsurer.testingDelegate(topic -> attempts.incrementAndGet());
        TopicManager manager = new TopicManager(topicSettings(), delegate);

        manager.ensureTopicIfNeeded(HOT_TOPIC);
        awaitAttempts(attempts, 1);
        manager.ensureTopicIfNeeded(HOT_TOPIC);
        assertEquals(1, attempts.get(), "повторное ensure должно ожидать cooldown");
        assertEquals(1L, manager.ensureSkippedCount(), "пропущенные ensure должны учитываться в метрике");

        manager.resetEnsureStateForTest(HOT_TOPIC, true, System.currentTimeMillis() - REWIND_DELTA_MS);
        manager.ensureTopicIfNeeded(HOT_TOPIC);
        awaitAttempts(attempts, 2);
        assertEquals(2, attempts.get(), "после истечения cooldown ensure должен повториться");
        assertEquals(1L, manager.ensureSkippedCount(), "успешное ensure не увеличивает счётчик пропусков");
    }

    @Test
    @DisplayName("ensureTopicIfNeeded повторяет попытку после сбоя с учётом cooldown")
    void ensureTopicRetriesAfterFailureCooldown() {
        AtomicInteger attempts = new AtomicInteger();
        TopicEnsurer failing = TopicEnsurer.testingDelegate(topic -> {
            attempts.incrementAndGet();
            throw new RuntimeException("fail");
        });
        TopicManager manager = new TopicManager(topicSettings(), failing);

        manager.ensureTopicIfNeeded(FLAKY_TOPIC);
        awaitAttempts(attempts, 1);
        manager.ensureTopicIfNeeded(FLAKY_TOPIC);
        assertEquals(1, attempts.get(), "повторение сразу после сбоя должно блокироваться");
        assertEquals(1L, manager.ensureSkippedCount(), "сбойное ensure также учитывается в пропусках");

        manager.resetEnsureStateForTest(FLAKY_TOPIC, false, System.currentTimeMillis() - REWIND_DELTA_MS);
        manager.ensureTopicIfNeeded(FLAKY_TOPIC);
        awaitAttempts(attempts, 2);
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

    private static void awaitAttempts(AtomicInteger attempts, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (attempts.get() < expected && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertEquals(expected, attempts.get(), "ensureTopic не выполнился вовремя");
    }
}
