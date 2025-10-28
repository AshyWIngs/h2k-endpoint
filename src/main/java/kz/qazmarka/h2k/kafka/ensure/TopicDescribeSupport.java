package kz.qazmarka.h2k.kafka.ensure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;

/**
 * Инкапсулирует describe-логику ensure-процесса, обновляя состояние и backoff.
 */
final class TopicDescribeSupport {

    private final KafkaTopicAdmin admin;
    private final TopicEnsureContext ctx;

    TopicDescribeSupport(KafkaTopicAdmin admin, TopicEnsureContext ctx) {
        this.admin = admin;
        this.ctx = ctx;
    }

    TopicExistence describeSingle(String topic) {
        Map<String, KafkaFuture<TopicDescription>> result =
                admin.describeTopics(Collections.singleton(topic));
        return analyzeDescribeFuture(topic, result.get(topic), true, true);
    }

    List<String> describeMissing(Set<String> topics) {
        List<String> missing = new ArrayList<>(topics.size());
        Map<String, KafkaFuture<TopicDescription>> futures = admin.describeTopics(topics);
        for (String t : topics) {
            TopicExistence ex = analyzeDescribeFuture(t, futures.get(t), true, false);
            if (ex == TopicExistence.FALSE) {
                missing.add(t);
            }
        }
        return missing;
    }

    void onExistsUnknown(String topic, long delayMs) {
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Не удалось надёжно определить существование Kafka-топика '{}'; повторю попытку после ~{} мс",
                    topic, delayMs);
        }
    }

    private TopicExistence analyzeDescribeFuture(String topic,
                                                 KafkaFuture<TopicDescription> future,
                                                 boolean updateState,
                                                 boolean singleDescribeCall) {
        if (future == null) {
            return handleDescribeRuntime(topic,
                    new IllegalStateException("describeTopics returned null future"),
                    updateState);
        }
        try {
            future.get(ctx.adminTimeoutMs(), TimeUnit.MILLISECONDS);
            markExistsTrue(topic, updateState);
            return TopicExistence.TRUE;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return handleDescribeInterrupted(topic, ex, updateState);
        } catch (TimeoutException ex) {
            return handleDescribeTimeout(topic, ex, updateState, singleDescribeCall);
        } catch (ExecutionException ex) {
            return handleDescribeExecution(topic, ex, updateState);
        } catch (RuntimeException ex) {
            return handleDescribeRuntime(topic, ex, updateState);
        }
    }

    private TopicExistence handleDescribeInterrupted(String topic,
                                                     InterruptedException ex,
                                                     boolean updateState) {
        long delayMs = scheduleDescribeUnknown(topic, updateState);
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Проверка Kafka-топика '{}' прервана", topic, ex);
        }
        onExistsUnknown(topic, delayMs);
        return TopicExistence.UNKNOWN;
    }

    private TopicExistence handleDescribeTimeout(String topic,
                                                 TimeoutException ex,
                                                 boolean updateState,
                                                 boolean singleDescribeCall) {
        long delayMs = scheduleDescribeUnknown(topic, updateState);
        if (ctx.log().isDebugEnabled()) {
            if (singleDescribeCall) {
                ctx.log().debug("Проверка Kafka-топика '{}' превысила таймаут {} мс (single)", topic, ctx.adminTimeoutMs(), ex);
            } else {
                ctx.log().debug("Проверка Kafka-топика '{}' превысила таймаут {} мс", topic, ctx.adminTimeoutMs(), ex);
            }
        }
        onExistsUnknown(topic, delayMs);
        return TopicExistence.UNKNOWN;
    }

    private TopicExistence handleDescribeExecution(String topic,
                                                   ExecutionException ex,
                                                   boolean updateState) {
        Throwable cause = ex.getCause();
        if (cause instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
            ctx.metrics().recordExistsFalse();
            return TopicExistence.FALSE;
        }
        long delayMs = scheduleDescribeUnknown(topic, updateState);
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Ошибка при проверке Kafka-топика '{}'", topic, ex);
        }
        onExistsUnknown(topic, delayMs);
        return TopicExistence.UNKNOWN;
    }

    private TopicExistence handleDescribeRuntime(String topic,
                                                 RuntimeException ex,
                                                 boolean updateState) {
        long delayMs = scheduleDescribeUnknown(topic, updateState);
        if (ctx.log().isDebugEnabled()) {
            ctx.log().debug("Не удалось проверить Kafka-топик '{}' (runtime)", topic, ex);
        }
        onExistsUnknown(topic, delayMs);
        return TopicExistence.UNKNOWN;
    }

    private void markExistsTrue(String topic, boolean updateState) {
        if (updateState) {
            ctx.metrics().recordExistsTrue();
        }
        ctx.markEnsured(topic);
    }

    private long scheduleDescribeUnknown(String topic, boolean updateState) {
        if (updateState) {
            ctx.metrics().recordExistsUnknown();
        }
        return ctx.scheduleRetry(topic);
    }
}
