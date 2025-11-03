package kz.qazmarka.h2k.kafka.ensure;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.UnaryOperator;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Тесты для {@link TopicEnsureExecutor}: проверяем, что задачи исполняются асинхронно,
 * повторно не ставятся без необходимости и учитывают запланированный backoff.
 */
class TopicEnsureExecutorTest {

    @Test
    @DisplayName("submit(): тема создаётся асинхронно и кешируется")
    void submitExecutesAsync() {
        SimpleAdmin admin = new SimpleAdmin();
        EnsureRuntimeState state = new EnsureRuntimeState();
        try (EnsureCoordinator coordinator = new EnsureCoordinator(admin, config(), state);
             TopicEnsureExecutor executor = new TopicEnsureExecutor(coordinator, "test-ensure")) {
            assertTrue(executor.submit("orders"));
            await(() -> state.isEnsured("orders"));
            assertEquals(1, admin.createCalls.get());
            assertTrue(state.isEnsured("orders"));
        }
    }

    @Test
    @DisplayName("submit(): backoff учитывается при повторной постановке темы")
    void submitHonorsBackoff() {
        SimpleAdmin admin = new SimpleAdmin();
        EnsureRuntimeState state = new EnsureRuntimeState();
        try (EnsureCoordinator coordinator = new EnsureCoordinator(admin, config(), state);
             TopicEnsureExecutor executor = new TopicEnsureExecutor(coordinator, "test-ensure")) {
            long delayNs = TimeUnit.MILLISECONDS.toNanos(80);
            state.scheduleUnknown("payments", System.nanoTime() + delayNs);
            assertTrue(executor.submit("payments"));
            assertNoCreateBefore(admin, TimeUnit.MILLISECONDS.toNanos(50));
            await(() -> state.isEnsured("payments"));
            assertTrue(admin.createCalls.get() >= 1);
        }
    }

    private static void await(Check check) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!check.ok()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Ожидаемое состояние не наступило");
            }
            pause();
        }
    }

    private static void assertNoCreateBefore(SimpleAdmin admin, long durationNs) {
        long deadline = System.nanoTime() + durationNs;
        while (System.nanoTime() < deadline) {
            if (admin.createCalls.get() > 0) {
                throw new AssertionError("Создание топика не должно стартовать до истечения backoff");
            }
            pause();
        }
        assertEquals(0, admin.createCalls.get(), "До истечения backoff create выполняться не должен");
    }

    private static void pause() {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }

    private interface Check {
        boolean ok();
    }

    private static TopicEnsureConfig config() {
    return TopicEnsureConfig.builder()
        .topicNameMaxLen(249)
        .topicSanitizer(UnaryOperator.identity())
        .topicPartitions(3)
        .topicReplication((short) 1)
        .topicConfigs(Collections.<String, String>emptyMap())
        .ensureIncreasePartitions(false)
        .ensureDiffConfigs(false)
        .adminTimeoutMs(100L)
        .unknownBackoffMs(50L)
        .build();
    }

    private static final class SimpleAdmin implements KafkaTopicAdmin {
        final AtomicInteger createCalls = new AtomicInteger();

        @Override
        public Map<String, KafkaFuture<TopicDescription>> describeTopics(Set<String> names) {
            Map<String, KafkaFuture<TopicDescription>> out = new HashMap<>();
            for (String t : names) {
                out.put(t, failedFuture(new UnknownTopicOrPartitionException("missing")));
            }
            return out;
        }

        @Override
        public Map<String, KafkaFuture<Void>> createTopics(List<NewTopic> newTopics) {
            Map<String, KafkaFuture<Void>> out = new HashMap<>();
            for (NewTopic nt : newTopics) {
                out.put(nt.name(), KafkaFuture.completedFuture(null));
                createCalls.incrementAndGet();
            }
            return out;
        }

        @Override
        public void createTopic(NewTopic topic, long timeoutMs)
                throws InterruptedException, ExecutionException, TimeoutException {
            createCalls.incrementAndGet();
        }

        @Override
        public void close(Duration timeout) {
            // no-op
        }

        @Override
        public void increasePartitions(String topic, int newCount, long timeoutMs) {
            // не используется в тестах
        }

        @Override
        public Map<ConfigResource, KafkaFuture<Config>> describeConfigs(Collection<ConfigResource> resources) {
            Map<ConfigResource, KafkaFuture<Config>> out = new HashMap<>();
            for (ConfigResource r : resources) {
                out.put(r, KafkaFuture.completedFuture(new Config(Collections.<ConfigEntry>emptyList())));
            }
            return out;
        }

        @Override
        public void incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops, long timeoutMs) {
            // не используется
        }
    }

    private static <T> KafkaFuture<T> failedFuture(RuntimeException ex) {
        return KafkaFuture.<T>completedFuture(null).thenApply(value -> {
            throw ex;
        });
    }
}
