package kz.qazmarka.h2k.kafka.ensure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Вспомогательный класс, выполняющий создание топиков и связанные апгрейды.
 */
final class TopicCreationSupport {

    private final KafkaTopicAdmin admin;
    private final TopicEnsureConfig config;
    private final TopicEnsureContext ctx;

    TopicCreationSupport(KafkaTopicAdmin admin,
                         TopicEnsureConfig config,
                         TopicEnsureContext ctx) {
        this.admin = admin;
        this.config = config;
        this.ctx = ctx;
    }

    void ensureTopicCreated(String topic) {
        try {
            createTopicDirect(topic);
        } catch (InterruptedException ie) {
            onCreateInterrupted(topic, ie);
        } catch (ExecutionException e) {
            onCreateExecException(topic, e);
        } catch (TimeoutException te) {
            onCreateTimeout(topic, te);
        } catch (RuntimeException re) {
            onCreateRuntime(topic, re);
        }
    }

    void ensureUpgrades(String topic) {
        if (admin == null) {
            return;
        }
        if (config.ensureIncreasePartitions()) {
            ensurePartitions(topic);
        }
        if (config.ensureDiffConfigs() && !ctx.topicConfigs().isEmpty()) {
            ensureConfigs(topic);
        }
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Kafka-топик '{}' уже существует — создание не требуется", topic);
        }
    }

    void createMissingTopics(List<String> missing) {
        List<NewTopic> newTopics = planTopics(missing);
        Map<String, KafkaFuture<Void>> futures = admin.createTopics(newTopics);
        for (String topic : missing) {
            processCreateResult(futures, topic);
        }
    }

    private void createTopicDirect(String topic)
            throws InterruptedException, ExecutionException, TimeoutException {
        NewTopic newTopic = buildNewTopic(topic);
        if (ctx.log().isDebugEnabled() && hasExplicitConfigs()) {
            ctx.log().debug("Применяю конфиги для Kafka-топика '{}': {}", topic, summarizeConfigs());
        }
        admin.createTopic(newTopic, ctx.adminTimeoutMs());
        recordCreateSuccess(topic);
        logTopicCreated(topic);
    }

    private List<NewTopic> planTopics(List<String> names) {
        List<NewTopic> newTopics = new ArrayList<>(names.size());
        for (String name : names) {
            newTopics.add(buildNewTopic(name));
        }
        return newTopics;
    }

    private NewTopic buildNewTopic(String name) {
        NewTopic nt = new NewTopic(name, config.topicPartitions(), config.topicReplication());
        if (hasExplicitConfigs()) {
            nt.configs(ctx.topicConfigs());
        }
        return nt;
    }

    private void recordCreateSuccess(String topic) {
        ctx.metrics().recordCreateSuccess();
        ctx.markEnsured(topic);
    }

    private void recordCreateRace(String topic) {
        ctx.metrics().recordCreateRace();
        ctx.markEnsured(topic);
    }

    private void recordCreateFailure(String topic, Throwable cause) {
        ctx.metrics().recordCreateFailure();
        ctx.scheduleRetry(topic);
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Не удалось создать Kafka-топик '{}': {}", topic, safeCause(cause));
        }
    }

    private void logTopicCreated(String topic) {
        ctx.log().info("Создал Kafka-топик '{}': partitions={}, replication={}",
                topic, config.topicPartitions(), config.topicReplication());
        if (ctx.log().isDebugEnabled() && hasExplicitConfigs()) {
            ctx.log().debug("Конфиги Kafka-топика '{}': {}", topic, summarizeConfigs());
        }
    }

    private boolean hasExplicitConfigs() {
        return ctx.topicConfigs() != null && !ctx.topicConfigs().isEmpty();
    }

    private String summarizeConfigs() {
        if (!hasExplicitConfigs()) {
            return "без явных конфигов";
        }
        Map<String, String> configs = ctx.topicConfigs();
        String retention = configs.get(EnsureCoordinator.CFG_RETENTION_MS);
        String cleanup   = configs.get(EnsureCoordinator.CFG_CLEANUP_POLICY);
        String minIsr    = configs.get(EnsureCoordinator.CFG_MIN_INSYNC_REPLICAS);
        StringBuilder sb = new StringBuilder(128);
        boolean first = true;
        first = append(sb, EnsureCoordinator.CFG_RETENTION_MS, retention, first);
        first = append(sb, EnsureCoordinator.CFG_CLEANUP_POLICY, cleanup, first);
        first = append(sb, EnsureCoordinator.CFG_MIN_INSYNC_REPLICAS, minIsr, first);
        int others = configs.size() - countNonNull(retention, cleanup, minIsr);
        if (others > 0) {
            if (!first) sb.append(", ");
            sb.append("+").append(others).append(" др.");
        }
        return sb.toString();
    }

    private void processCreateResult(Map<String, KafkaFuture<Void>> futures, String topic) {
        try {
            futures.get(topic).get(ctx.adminTimeoutMs(), TimeUnit.MILLISECONDS);
            recordCreateSuccess(topic);
            logTopicCreated(topic);
        } catch (InterruptedException ie) {
            onCreateInterrupted(topic, ie);
        } catch (TimeoutException te) {
            onCreateTimeout(topic, te);
        } catch (ExecutionException ee) {
            handleCreateExecution(topic, ee);
        }
    }

    private void handleCreateExecution(String topic, ExecutionException ee) {
        Throwable cause = ee.getCause();
        if (cause instanceof TopicExistsException) {
            recordCreateRace(topic);
            if (ctx.log().isDebugEnabled()) {
                ctx.log().debug("Kafka-топик '{}' уже существует (создан параллельно)", topic);
            }
            return;
        }
        recordCreateFailure(topic, cause == null ? ee : cause);
        ctx.log().warn("Не удалось создать Kafka-топик '{}': {}: {}",
                topic,
                (cause == null ? ee.getClass().getSimpleName() : cause.getClass().getSimpleName()),
                (cause == null ? ee.getMessage() : cause.getMessage()));
    }

    private void onCreateInterrupted(String topic, InterruptedException ie) {
        Thread.currentThread().interrupt();
        recordCreateFailure(topic, ie);
        ctx.log().warn("Создание Kafka-топика '{}' было прервано", topic);
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Трассировка прерывания при создании темы '{}'", topic, ie);
        }
    }

    private void onCreateExecException(String topic, ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof TopicExistsException) {
            recordCreateRace(topic);
            if (ctx.log().isDebugEnabled()) {
                ctx.log().debug("Kafka-топик '{}' уже существует (создан параллельно)", topic);
            }
            return;
        }
        recordCreateFailure(topic, cause == null ? e : cause);
        ctx.log().warn("Не удалось создать Kafka-топик '{}': {}: {}",
                topic,
                (cause == null ? e.getClass().getSimpleName() : cause.getClass().getSimpleName()),
                (cause == null ? e.getMessage() : cause.getMessage()));
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Трассировка ошибки создания темы '{}'", topic, e);
        }
    }

    private void onCreateTimeout(String topic, TimeoutException te) {
        recordCreateFailure(topic, te);
        ctx.log().warn("Не удалось создать Kafka-топик '{}': таймаут {} мс", topic, ctx.adminTimeoutMs());
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Трассировка таймаута создания темы '{}'", topic, te);
        }
    }

    private void onCreateRuntime(String topic, RuntimeException re) {
        recordCreateFailure(topic, re);
        ctx.log().warn("Не удалось создать Kafka-топик '{}' (runtime): {}", topic, re.getMessage());
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Трассировка runtime при создании темы '{}'", topic, re);
        }
    }

    private void ensurePartitions(String topic) {
        if (!config.ensureIncreasePartitions()) {
            return;
        }
        try {
            int current = currentPartitionCount(topic);
            handlePartitionDifference(topic, current);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logPartitionInterrupted(topic, ie);
        } catch (TimeoutException | ExecutionException e) {
            logPartitionFailure(topic, e);
        } catch (RuntimeException e) {
            logPartitionRuntime(topic, e);
        }
    }

    private void handlePartitionDifference(String topic, int currentPartitions) throws InterruptedException, ExecutionException, TimeoutException {
        int desired = config.topicPartitions();
        if (currentPartitions < desired) {
            ctx.log().info("Увеличиваю партиции Kafka-топика '{}' {}→{}", topic, currentPartitions, desired);
            admin.increasePartitions(topic, desired, ctx.adminTimeoutMs());
            return;
        }
        if (currentPartitions > desired) {
            ctx.log().warn("Текущее число партиций Kafka-топика '{}' ({}) больше заданного ({}); уменьшение не поддерживается — оставляю как есть",
                    topic, currentPartitions, desired);
        }
    }

    private void logPartitionInterrupted(String topic, InterruptedException ie) {
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Проверка/увеличение партиций прерваны для '{}'", topic, ie);
        } else {
            ctx.log().warn("Проверка/увеличение партиций прерваны для '{}'", topic);
        }
    }

    private void logPartitionFailure(String topic, Exception e) {
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Не удалось проверить/увеличить партиции Kafka-топика '{}'", topic, e);
        } else {
            ctx.log().warn("Не удалось проверить/увеличить партиции Kafka-топика '{}': {}", topic, e.getMessage());
        }
    }

    private void logPartitionRuntime(String topic, RuntimeException e) {
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Не удалось проверить/увеличить партиции Kafka-топика '{}' (runtime)", topic, e);
        } else {
            ctx.log().warn("Не удалось проверить/увеличить партиции Kafka-топика '{}' (runtime): {}", topic, e.getMessage());
        }
    }

    private int currentPartitionCount(String topic)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, KafkaFuture<TopicDescription>> descriptions =
                admin.describeTopics(Collections.singleton(topic));
        TopicDescription desc = descriptions.get(topic).get(ctx.adminTimeoutMs(), TimeUnit.MILLISECONDS);
        return desc.partitions().size();
    }

    private void ensureConfigs(String topic) {
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config current = fetchCurrentTopicConfig(resource);
            List<AlterConfigOp> ops = diffConfigOps(current, ctx.topicConfigs());
            if (!ops.isEmpty()) {
                applyConfigChanges(topic, resource, ops);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logConfigInterrupted(topic, ie);
        } catch (TimeoutException te) {
            logConfigTimeout(topic, te);
        } catch (ExecutionException e) {
            logConfigExecution(topic, e);
        } catch (RuntimeException e) {
            logConfigRuntime(topic, e);
        }
    }

    private Config fetchCurrentTopicConfig(ConfigResource cr)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<ConfigResource, KafkaFuture<Config>> vals =
                admin.describeConfigs(Collections.singleton(cr));
        return vals.get(cr).get(ctx.adminTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private List<AlterConfigOp> diffConfigOps(Config current, Map<String, String> desired) {
        List<AlterConfigOp> ops = new ArrayList<>();
        if (desired == null || desired.isEmpty()) {
            return ops;
        }
        for (Map.Entry<String, String> entry : desired.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            ConfigEntry ce = current.get(key);
            String currentValue = (ce == null) ? null : ce.value();
            if (!Objects.equals(currentValue, value)) {
                ops.add(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET));
            }
        }
        return ops;
    }

    private void applyConfigChanges(String topic,
                                    ConfigResource cr,
                                    List<AlterConfigOp> ops)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<ConfigResource, Collection<AlterConfigOp>> request = new LinkedHashMap<>(1);
        request.put(cr, ops);
        ctx.log().info("Привожу конфиги Kafka-топика '{}' ({} ключ(а/ей))", topic, ops.size());
        admin.incrementalAlterConfigs(request, ctx.adminTimeoutMs());
    }

    private void logConfigInterrupted(String topic, InterruptedException ie) {
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Приведение конфигов прервано для Kafka-топика '{}'", topic, ie);
        } else {
            ctx.log().warn("Приведение конфигов прервано для Kafka-топика '{}'", topic);
        }
    }

    private void logConfigTimeout(String topic, TimeoutException te) {
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Не удалось привести конфиги Kafka-топика '{}' из-за таймаута {}", topic, ctx.adminTimeoutMs(), te);
        } else {
            ctx.log().warn("Не удалось привести конфиги Kafka-топика '{}': таймаут {} мс", topic, ctx.adminTimeoutMs());
        }
    }

    private void logConfigExecution(String topic, ExecutionException e) {
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Не удалось привести конфиги Kafka-топика '{}'", topic, e);
        } else {
            ctx.log().warn("Не удалось привести конфиги Kafka-топика '{}': {}", topic, e.getMessage());
        }
    }

    private void logConfigRuntime(String topic, RuntimeException e) {
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Не удалось привести конфиги Kafka-топика '{}' (runtime)", topic, e);
        } else {
            ctx.log().warn("Не удалось привести конфиги Kafka-топика '{}' (runtime): {}", topic, e.getMessage());
        }
    }

    private static String safeCause(Throwable cause) {
        if (cause == null) {
            return "неизвестная причина";
        }
        String type = cause.getClass().getSimpleName();
        String msg = cause.getMessage();
        if (msg == null || msg.isEmpty()) {
            return type;
        }
        return type + ": " + msg;
    }

    private static boolean append(StringBuilder sb, String key, String value, boolean first) {
        if (value == null) {
            return first;
        }
        if (!first) {
            sb.append(", ");
        }
        sb.append(key).append("=").append(value);
        return false;
    }

    private static int countNonNull(Object... vals) {
        int n = 0;
        if (vals != null) {
            for (Object v : vals) {
                if (v != null) {
                    n++;
                }
            }
        }
        return n;
    }
}
