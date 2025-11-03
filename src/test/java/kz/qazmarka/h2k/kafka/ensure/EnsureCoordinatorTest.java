package kz.qazmarka.h2k.kafka.ensure;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.errors.TopicExistsException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.kafka.ensure.EnsureCoordinatorTestBuilder.DescribeOutcome;
import kz.qazmarka.h2k.kafka.ensure.EnsureCoordinatorTestBuilder.FakeAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Тесты для EnsureCoordinator - координатора создания и проверки топиков Kafka.
 */
class EnsureCoordinatorTest extends BaseEnsureCoordinatorTest {

    @Test
    @DisplayName("ensureTopic(): увеличивает партиции, если текущее число меньше заданного")
    void ensureTopicIncreasesPartitionsWhenBelowTarget() {
        FakeAdmin admin = builder().withPartitions(1).buildAdmin();
        TopicEnsureConfig cfg = builder()
                .withIncreasePartitions(true)
                .withTopicPartitions(3)
                .buildConfig();
        EnsureRuntimeState state = builder().buildState();

        try (EnsureCoordinator svc = builder().buildCoordinator(admin, cfg, state)) {
            svc.ensureTopic("orders");
        }

        assertTrue(admin.increaseCalled, "Ожидалось увеличение партиций до 3");
        assertEquals(3, admin.increaseNewCount, "Новая мощность должна совпадать с конфигом");
    }

    @Test
    @DisplayName("ensureTopic(): приводит конфиги темы diff-only")
    void ensureTopicAppliesConfigDiff() {
        Map<String, String> desired = Collections.singletonMap("retention.ms", "60000");
        FakeAdmin admin = builder()
                .withAdminConfig("retention.ms", "1000")
                .buildAdmin();
        TopicEnsureConfig cfg = builder()
                .withTopicConfigs(desired)
                .withDiffConfigs(true)
                .buildConfig();
        EnsureRuntimeState state = builder().buildState();

        try (EnsureCoordinator svc = builder().buildCoordinator(admin, cfg, state)) {
            svc.ensureTopic("analytics");
        }

        assertTrue(admin.configUpdated, "Ожидалась передача ALTER CONFIG для retention.ms");
        assertEquals("60000", admin.lastConfigValue, "На брокер должен уйти diff с новым значением");
    }

    @Test
    @DisplayName("ensureTopic(): повторный вызов использует кеш подтверждённой темы")
    void ensureTopicUsesCacheAfterSuccess() {
        FakeAdmin admin = builder().buildAdmin();
        EnsureRuntimeState state = builder().buildState();
        TopicEnsureConfig cfg = builder().buildConfig();

        try (EnsureCoordinator svc = builder().buildCoordinator(admin, cfg, state)) {
            svc.ensureTopic("analytics");
            svc.ensureTopic("analytics");
        }

        assertEquals(1, admin.describeRequests, "describeTopics должен вызываться один раз");
        assertEquals(1L, state.ensureHitCache().longValue(), "повторное ensure должно отработать через кеш");
        assertTrue(state.isEnsured("analytics"), "тема должна быть помечена как подтверждённая");
    }

    @Test
    @DisplayName("ensureTopic(): ensureTopics=false пропускает вызов без изменения метрик")
    void ensureTopicSkipsWhenAdminDisabled() {
        EnsureRuntimeState state = builder().buildState();
        TopicEnsureConfig cfg = builder().buildConfig();

        try (EnsureCoordinator svc = builder().buildDisabledCoordinator(cfg, state)) {
            svc.ensureTopic("orders");
        }

        assertEquals(0L, state.ensureInvocations().longValue(), "метрики не должны меняться при ensureTopics=false");
        assertEquals(0, state.ensuredSize(), "кеш ensured остаётся пустым");
    }

    @Test
    @DisplayName("ensureTopic(): отсутствующая тема создаётся и попадает в кеш")
    void ensureTopicCreatesMissingTopic() {
        FakeAdmin admin = builder()
                .whenDescribe("inventory", DescribeOutcome.MISSING)
                .buildAdmin();
        EnsureRuntimeState state = builder().buildState();
        TopicEnsureConfig cfg = builder().buildConfig();

        try (EnsureCoordinator svc = builder().buildCoordinator(admin, cfg, state)) {
            svc.ensureTopic("inventory");
        }

        assertEquals(1, admin.createTopicCalls, "ожидается вызов createTopic");
        assertEquals(1L, state.createOk().longValue(), "создание должно считаться успешным");
        assertEquals(1L, state.existsFalse().longValue(), "describe фиксирует отсутствие темы");
        assertTrue(state.isEnsured("inventory"), "тема попадает в кеш ensured");
        assertEquals(0, state.unknownSize(), "backoff не назначается при успешном создании");
    }

    @Test
    @DisplayName("ensureTopic(): TopicExistsException учитывается как гонка")
    void ensureTopicHandlesCreateRace() {
        FakeAdmin admin = builder()
                .whenDescribe("billing", DescribeOutcome.MISSING)
                .failCreateWith(new ExecutionException(new TopicExistsException("billing already exists")))
                .buildAdmin();
        EnsureRuntimeState state = builder().buildState();
        TopicEnsureConfig cfg = builder().buildConfig();

        try (EnsureCoordinator svc = builder().buildCoordinator(admin, cfg, state)) {
            svc.ensureTopic("billing");
        }

        assertEquals(1, admin.createTopicCalls, "должна быть предпринята попытка createTopic");
        assertEquals(1L, state.createRace().longValue(), "гонка фиксируется метрикой createRace");
        assertEquals(0L, state.createFail().longValue(), "ошибки создания не учитываются");
        assertTrue(state.isEnsured("billing"), "тема помечается как ensured после гонки");
    }

    @Test
    @DisplayName("ensureTopic(): RuntimeException при создании назначает backoff и считает неуспех")
    void ensureTopicRecordsFailuresForRuntimeCreateErrors() {
        FakeAdmin admin = builder()
                .whenDescribe("ledger", DescribeOutcome.MISSING)
                .failCreateWith(new IllegalStateException("Симуляция сетевого сбоя при create"))
                .buildAdmin();
        EnsureRuntimeState state = builder().buildState();
        TopicEnsureConfig cfg = builder().buildConfig();

        try (EnsureCoordinator svc = builder().buildCoordinator(admin, cfg, state)) {
            svc.ensureTopic("ledger");
        }

        assertEquals(1, admin.createTopicCalls, "создание пытались выполнить один раз");
        assertEquals(1L, state.createFail().longValue(), "неуспех должен учитываться");
        assertFalse(state.isEnsured("ledger"), "тема не попадёт в кеш при ошибке");
        assertEquals(1, state.unknownSize(), "backoff-планировщик получает запись");
    }

    @Test
    @DisplayName("ensureTopicOk(): кешированный результат возвращает true без повторного describe")
    void ensureTopicOkUsesCache() {
        FakeAdmin admin = builder().withPartitions(2).buildAdmin();
        EnsureRuntimeState state = builder().buildState();
        TopicEnsureConfig cfg = builder().buildConfig();

        try (EnsureCoordinator svc = builder().buildCoordinator(admin, cfg, state)) {
            assertTrue(svc.ensureTopicOk("audit"), "первая проверка должна вернуть true");
            assertTrue(svc.ensureTopicOk("audit"), "повторная проверка также true, но уже из кеша");
        }

        assertEquals(1, admin.describeRequests, "describeTopics выполняется только в первую проверку");
        assertEquals(1L, state.ensureInvocations().longValue(), "ensureTopic вызывался один раз");
    }
}
