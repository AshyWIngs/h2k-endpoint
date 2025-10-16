package kz.qazmarka.h2k.kafka.ensure;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

class TopicEnsureServiceTest {

    @Test
    @DisplayName("ensureTopic(): увеличивает партиции, если текущее число меньше заданного")
    void ensureTopicIncreasesPartitionsWhenBelowTarget() {
        FakeAdmin admin = new FakeAdmin().withPartitions(1);
        TopicEnsureConfig cfg = TopicEnsureConfig.builder()
                .topicNameMaxLen(249)
                .topicSanitizer(UnaryOperator.identity())
                .topicPartitions(3)
                .topicReplication((short) 1)
                .topicConfigs(Collections.emptyMap())
                .ensureIncreasePartitions(true)
                .ensureDiffConfigs(false)
                .adminTimeoutMs(50L)
                .unknownBackoffMs(10L)
                .build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, new TopicEnsureState())) {
            svc.ensureTopic("orders");
        }

        assertTrue(admin.increaseCalled, "Ожидалось увеличение партиций до 3");
        assertEquals(3, admin.increaseNewCount, "Новая мощность должна совпадать с конфигом");
    }

    @Test
    @DisplayName("ensureTopic(): приводит конфиги темы diff-only")
    void ensureTopicAppliesConfigDiff() {
        Map<String, String> desired = Collections.singletonMap("retention.ms", "60000");
        FakeAdmin admin = new FakeAdmin()
                .withPartitions(3)
                .withConfig("retention.ms", "1000");
        TopicEnsureConfig cfg = TopicEnsureConfig.builder()
                .topicNameMaxLen(249)
                .topicSanitizer(UnaryOperator.identity())
                .topicPartitions(3)
                .topicReplication((short) 1)
                .topicConfigs(desired)
                .ensureIncreasePartitions(false)
                .ensureDiffConfigs(true)
                .adminTimeoutMs(50L)
                .unknownBackoffMs(10L)
                .build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, new TopicEnsureState())) {
            svc.ensureTopic("analytics");
        }

        assertTrue(admin.configUpdated, "Ожидалась передача ALTER CONFIG для retention.ms");
       assertEquals("60000", admin.lastConfigValue, "На брокер должен уйти diff с новым значением");
    }

    @Test
    @DisplayName("ensureTopic(): повторный вызов использует кеш подтверждённой темы")
    void ensureTopicUsesCacheAfterSuccess() {
        FakeAdmin admin = new FakeAdmin().withPartitions(3);
        TopicEnsureState state = new TopicEnsureState();
        TopicEnsureConfig cfg = defaultConfigBuilder().build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, state)) {
            svc.ensureTopic("analytics");
            svc.ensureTopic("analytics");
        }

        assertEquals(1, admin.describeRequests, "describeTopics должен вызываться один раз");
        assertEquals(1L, state.ensureHitCache.longValue(), "повторное ensure должно отработать через кеш");
        assertTrue(state.ensured.contains("analytics"), "тема должна быть помечена как подтверждённая");
    }

    @Test
    @DisplayName("ensureTopic(): ensureTopics=false пропускает вызов без изменения метрик")
    void ensureTopicSkipsWhenAdminDisabled() {
        TopicEnsureState state = new TopicEnsureState();
        TopicEnsureConfig cfg = defaultConfigBuilder().build();

        try (TopicEnsureService svc = new TopicEnsureService(null, cfg, state)) {
            svc.ensureTopic("orders");
        }

        assertEquals(0L, state.ensureInvocations.longValue(), "метрики не должны меняться при ensureTopics=false");
        assertTrue(state.ensured.isEmpty(), "кеш ensured остаётся пустым");
    }

    @Test
    @DisplayName("ensureTopic(): отсутствующая тема создаётся и попадает в кеш")
    void ensureTopicCreatesMissingTopic() {
        FakeAdmin admin = new FakeAdmin()
                .withPartitions(3)
                .whenDescribe("inventory", DescribeOutcome.MISSING);
        TopicEnsureState state = new TopicEnsureState();
        TopicEnsureConfig cfg = defaultConfigBuilder().build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, state)) {
            svc.ensureTopic("inventory");
        }

        assertEquals(1, admin.createTopicCalls, "ожидается вызов createTopic");
        assertEquals(1L, state.createOk.longValue(), "создание должно считаться успешным");
        assertEquals(1L, state.existsFalse.longValue(), "describe фиксирует отсутствие темы");
        assertTrue(state.ensured.contains("inventory"), "тема попадает в кеш ensured");
        assertEquals(0, state.unknownSize(), "backoff не назначается при успешном создании");
    }

    @Test
    @DisplayName("ensureTopic(): TopicExistsException учитывается как гонка")
    void ensureTopicHandlesCreateRace() {
        FakeAdmin admin = new FakeAdmin()
                .withPartitions(3)
                .whenDescribe("billing", DescribeOutcome.MISSING)
                .failCreateWith(new ExecutionException(new TopicExistsException("billing already exists")));
        TopicEnsureState state = new TopicEnsureState();
        TopicEnsureConfig cfg = defaultConfigBuilder().build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, state)) {
            svc.ensureTopic("billing");
        }

        assertEquals(1, admin.createTopicCalls, "должна быть предпринята попытка createTopic");
        assertEquals(1L, state.createRace.longValue(), "гонка фиксируется метрикой createRace");
        assertEquals(0L, state.createFail.longValue(), "ошибки создания не учитываются");
        assertTrue(state.ensured.contains("billing"), "тема помечается как ensured после гонки");
    }

    @Test
    @DisplayName("ensureTopic(): RuntimeException при создании назначает backoff и считает неуспех")
    void ensureTopicRecordsFailuresForRuntimeCreateErrors() {
        FakeAdmin admin = new FakeAdmin()
                .withPartitions(3)
                .whenDescribe("ledger", DescribeOutcome.MISSING)
                .failCreateWith(new IllegalStateException("Симуляция сетевого сбоя при create"));
        TopicEnsureState state = new TopicEnsureState();
        TopicEnsureConfig cfg = defaultConfigBuilder().build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, state)) {
            svc.ensureTopic("ledger");
        }

        assertEquals(1, admin.createTopicCalls, "создание пытались выполнить один раз");
        assertEquals(1L, state.createFail.longValue(), "неуспех должен учитываться");
        assertFalse(state.ensured.contains("ledger"), "тема не попадёт в кеш при ошибке");
        assertEquals(1, state.unknownSize(), "backoff-планировщик получает запись");
    }

    @Test
    @DisplayName("ensureTopicOk(): кешированный результат возвращает true без повторного describe")
    void ensureTopicOkUsesCache() {
        FakeAdmin admin = new FakeAdmin().withPartitions(2);
        TopicEnsureState state = new TopicEnsureState();
        TopicEnsureConfig cfg = defaultConfigBuilder().build();

        try (TopicEnsureService svc = new TopicEnsureService(admin, cfg, state)) {
            assertTrue(svc.ensureTopicOk("audit"), "первая проверка должна вернуть true");
            assertTrue(svc.ensureTopicOk("audit"), "повторная проверка также true, но уже из кеша");
        }

        assertEquals(1, admin.describeRequests, "describeTopics выполняется только в первую проверку");
        assertEquals(1L, state.ensureInvocations.longValue(), "ensureTopic вызывался один раз");
    }

    private static TopicEnsureConfig.Builder defaultConfigBuilder() {
        return TopicEnsureConfig.builder()
                .topicNameMaxLen(249)
                .topicSanitizer(UnaryOperator.identity())
                .topicPartitions(3)
                .topicReplication((short) 1)
                .topicConfigs(Collections.emptyMap())
                .ensureIncreasePartitions(false)
                .ensureDiffConfigs(false)
                .adminTimeoutMs(50L)
                .unknownBackoffMs(10L);
    }

    private static final class FakeAdmin implements KafkaTopicAdmin {
        private int partitions = 1;
        private Config topicConfig = emptyConfig();
        private boolean increaseCalled;
        private int increaseNewCount;
        private boolean configUpdated;
        private String lastConfigValue;
        private final Map<String, ArrayDeque<DescribeOutcome>> describePlan = new LinkedHashMap<>();
        private final DescribeOutcome defaultOutcome = DescribeOutcome.EXISTS;
        private int describeRequests;
        private int createTopicCalls;
        private Throwable plannedCreateFailure;

        FakeAdmin withPartitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        FakeAdmin withConfig(String key, String value) {
            this.topicConfig = new Config(Collections.singletonList(new ConfigEntry(key, value)));
            return this;
        }

        FakeAdmin whenDescribe(String topic, DescribeOutcome... outcomes) {
            ArrayDeque<DescribeOutcome> queue = describePlan.computeIfAbsent(topic, t -> new ArrayDeque<>());
            for (DescribeOutcome outcome : outcomes) {
                queue.addLast(outcome);
            }
            return this;
        }

        FakeAdmin failCreateWith(Throwable failure) {
            this.plannedCreateFailure = failure;
            return this;
        }

        @Override
        public Map<String, KafkaFuture<TopicDescription>> describeTopics(Set<String> names) {
            describeRequests++;
            Map<String, KafkaFuture<TopicDescription>> out = new LinkedHashMap<>();
            for (String n : names) {
                DescribeOutcome outcome = resolveOutcome(n);
                out.put(n, futureFor(n, outcome));
            }
            return out;
        }

        @Override
        public Map<String, KafkaFuture<Void>> createTopics(List<NewTopic> newTopics) {
            return Collections.emptyMap();
        }

        @Override
        public void createTopic(NewTopic topic, long timeoutMs)
                throws InterruptedException, ExecutionException, TimeoutException {
            createTopicCalls++;
            Throwable failure = plannedCreateFailure;
            if (failure != null) {
                plannedCreateFailure = null; // ошибка одноразовая по умолчанию
                if (failure instanceof InterruptedException) {
                    throw (InterruptedException) failure;
                } else if (failure instanceof TimeoutException) {
                    throw (TimeoutException) failure;
                } else if (failure instanceof ExecutionException) {
                    throw (ExecutionException) failure;
                } else if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                } else {
                    throw new ExecutionException(failure);
                }
            }
        }

        @Override
        public void close(Duration timeout) { /* no-op */ }

        @Override
        public void increasePartitions(String topic, int newCount, long timeoutMs) {
            this.increaseCalled = true;
            this.increaseNewCount = newCount;
        }

        @Override
        public Map<ConfigResource, KafkaFuture<Config>> describeConfigs(Collection<ConfigResource> resources) {
            Map<ConfigResource, KafkaFuture<Config>> out = new LinkedHashMap<>(resources.size());
            for (ConfigResource r : resources) {
                out.put(r, KafkaFuture.completedFuture(topicConfig));
            }
            return out;
        }

        @Override
        public void incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops, long timeoutMs)
                throws InterruptedException, ExecutionException, TimeoutException {
            this.configUpdated = !ops.isEmpty();
            if (configUpdated) {
                AlterConfigOp op = ops.values().iterator().next().iterator().next();
                this.lastConfigValue = op.configEntry().value();
            }
        }

        private DescribeOutcome resolveOutcome(String topic) {
            ArrayDeque<DescribeOutcome> queue = describePlan.get(topic);
            if (queue != null && !queue.isEmpty()) {
                DescribeOutcome outcome = queue.peekFirst();
                if (queue.size() > 1) {
                    outcome = queue.removeFirst();
                }
                return outcome;
            }
            return defaultOutcome;
        }

        private KafkaFuture<TopicDescription> futureFor(String topic, DescribeOutcome outcome) {
            KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
            switch (outcome) {
                case EXISTS:
                    future.complete(topicDescription(topic, partitions));
                    break;
                case MISSING:
                    future.completeExceptionally(new UnknownTopicOrPartitionException("no such topic " + topic));
                    break;
                case UNKNOWN:
                    future.completeExceptionally(new IllegalStateException("Симуляция сбоя describe для топика " + topic));
                    break;
                case TIMEOUT:
                    future.completeExceptionally(new TimeoutException("describe timeout " + topic));
                    break;
            }
            return future;
        }

        private static TopicDescription topicDescription(String name, int partitions) {
            List<TopicPartitionInfo> infos = new ArrayList<>(partitions);
            Node leader = new Node(0, "localhost", 9092);
            List<Node> replicas = Collections.singletonList(leader);
            for (int p = 0; p < partitions; p++) {
                infos.add(new TopicPartitionInfo(p, leader, replicas, replicas));
            }
            return new TopicDescription(name, false, infos);
        }

        private static Config emptyConfig() {
            return new Config(Collections.<ConfigEntry>emptyList());
        }

    }

    private enum DescribeOutcome { EXISTS, MISSING, UNKNOWN, TIMEOUT }
}
