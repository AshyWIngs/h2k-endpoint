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
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Билдер тестовых данных для создания EnsureCoordinator и связанных компонентов.
 */
class EnsureCoordinatorTestBuilder {

    private static final DescribeOutcome[] EMPTY_PLAN = new DescribeOutcome[0];

    // Настройки FakeAdmin
    private int partitions = 3;
    private final Map<String, String> adminConfigs = new LinkedHashMap<>();
    private final Map<String, ArrayDeque<DescribeOutcome>> describePlan = new LinkedHashMap<>();
    private Throwable createFailure = null;

    // Настройки TopicEnsureConfig
    private final int topicNameMaxLen = 249;
    private final UnaryOperator<String> topicSanitizer = UnaryOperator.identity();
    private int topicPartitions = 3;
    private final short topicReplication = 1;
    private Map<String, String> topicConfigs = Collections.emptyMap();
    private boolean ensureIncreasePartitions = false;
    private boolean ensureDiffConfigs = false;
    private final long adminTimeoutMs = 50L;
    private final long unknownBackoffMs = 10L;

    /**
     * Устанавливает количество партиций для FakeAdmin.
     */
    EnsureCoordinatorTestBuilder withPartitions(int partitions) {
        this.partitions = partitions;
        return this;
    }

    /**
     * Устанавливает конфигурацию для FakeAdmin.
     */
    EnsureCoordinatorTestBuilder withAdminConfig(String key, String value) {
        this.adminConfigs.put(key, value);
        return this;
    }

    /**
     * Устанавливает сценарий describe для конкретного топика.
     */
    EnsureCoordinatorTestBuilder whenDescribe(String topic, DescribeOutcome... outcomes) {
        ArrayDeque<DescribeOutcome> queue = describePlan.computeIfAbsent(topic, t -> new ArrayDeque<>());
        for (DescribeOutcome outcome : outcomes) {
            queue.addLast(outcome);
        }
        return this;
    }

    /**
     * Устанавливает ошибку при создании топика.
     */
    EnsureCoordinatorTestBuilder failCreateWith(Throwable failure) {
        this.createFailure = failure;
        return this;
    }

    /**
     * Устанавливает желаемые конфигурации топика.
     */
    EnsureCoordinatorTestBuilder withTopicConfigs(Map<String, String> configs) {
        this.topicConfigs = configs;
        return this;
    }

    /**
     * Включает увеличение партиций.
     */
    EnsureCoordinatorTestBuilder withIncreasePartitions(boolean enable) {
        this.ensureIncreasePartitions = enable;
        return this;
    }

    /**
     * Включает применение diff конфигураций.
     */
    EnsureCoordinatorTestBuilder withDiffConfigs(boolean enable) {
        this.ensureDiffConfigs = enable;
        return this;
    }

    /**
     * Устанавливает количество партиций для TopicEnsureConfig.
     */
    EnsureCoordinatorTestBuilder withTopicPartitions(int partitions) {
        this.topicPartitions = partitions;
        return this;
    }

    /**
     * Создаёт FakeAdmin с заданными параметрами.
     */
    FakeAdmin buildAdmin() {
        FakeAdmin admin = new FakeAdmin()
                .withPartitions(partitions);
        
        for (Map.Entry<String, String> entry : adminConfigs.entrySet()) {
            admin.withConfig(entry.getKey(), entry.getValue());
        }
        
        for (Map.Entry<String, ArrayDeque<DescribeOutcome>> entry : describePlan.entrySet()) {
            DescribeOutcome[] plan = entry.getValue().toArray(EMPTY_PLAN);
            admin.whenDescribe(entry.getKey(), plan);
        }
        
        if (createFailure != null) {
            admin.failCreateWith(createFailure);
        }
        
        return admin;
    }

    /**
     * Создаёт TopicEnsureConfig с заданными параметрами.
     */
    TopicEnsureConfig buildConfig() {
        return TopicEnsureConfig.builder()
                .topicNameMaxLen(topicNameMaxLen)
                .topicSanitizer(topicSanitizer)
                .topicPartitions(topicPartitions)
                .topicReplication(topicReplication)
                .topicConfigs(topicConfigs)
                .ensureIncreasePartitions(ensureIncreasePartitions)
                .ensureDiffConfigs(ensureDiffConfigs)
                .adminTimeoutMs(adminTimeoutMs)
                .unknownBackoffMs(unknownBackoffMs)
                .build();
    }

    /**
     * Создаёт EnsureRuntimeState.
     */
    EnsureRuntimeState buildState() {
        return new EnsureRuntimeState();
    }

    /**
     * Создаёт EnsureCoordinator с заданными параметрами.
     */
    EnsureCoordinator buildCoordinator(FakeAdmin admin, TopicEnsureConfig config, EnsureRuntimeState state) {
        return new EnsureCoordinator(admin, config, state);
    }

    /**
     * Создаёт EnsureCoordinator с admin=null (disabled режим).
     */
    EnsureCoordinator buildDisabledCoordinator(TopicEnsureConfig config, EnsureRuntimeState state) {
        return new EnsureCoordinator(null, config, state);
    }

    /**
     * Вложенный класс FakeAdmin - mock для KafkaTopicAdmin.
     */
    static final class FakeAdmin implements KafkaTopicAdmin {
        private int partitions = 1;
        private Config topicConfig = emptyConfig();
        boolean increaseCalled;
        int increaseNewCount;
        boolean configUpdated;
        String lastConfigValue;
        private final Map<String, ArrayDeque<DescribeOutcome>> describePlan = new LinkedHashMap<>();
        private final DescribeOutcome defaultOutcome = DescribeOutcome.EXISTS;
        int describeRequests;
        int createTopicCalls;
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
                plannedCreateFailure = null;
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

    /**
     * Enum для сценариев describe топика.
     */
    enum DescribeOutcome {
        EXISTS,    // Топик существует
        MISSING,   // Топик отсутствует
        UNKNOWN,   // Ошибка при describe
        TIMEOUT    // Таймаут при describe
    }
}
