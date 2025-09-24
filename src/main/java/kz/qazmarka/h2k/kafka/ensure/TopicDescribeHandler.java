package kz.qazmarka.h2k.kafka.ensure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Инкапсулирует работу с {@code describeTopics}: преобразует ответы в удобное перечисление,
 * обновляет счётчики и планирует повторные попытки через {@link TopicBackoffManager}.
 */
final class TopicDescribeHandler {

    private final KafkaTopicAdmin admin;
    private final TopicEnsureState state;
    private final TopicBackoffManager backoffManager;
    private final long adminTimeoutMs;

    TopicDescribeHandler(KafkaTopicAdmin admin,
                         TopicEnsureState state,
                         TopicBackoffManager backoffManager,
                         long adminTimeoutMs) {
        this.admin = admin;
        this.state = state;
        this.backoffManager = backoffManager;
        this.adminTimeoutMs = adminTimeoutMs;
    }

    /** Выполняет describe одной темы, возвращая её статус (TRUE/FALSE/UNKNOWN). */
    TopicExistence describeSingle(String topic) {
        try {
            Map<String, KafkaFuture<TopicDescription>> result =
                    admin.describeTopics(java.util.Collections.singleton(topic));
            result.get(topic).get(adminTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            return TopicExistence.TRUE;
        } catch (InterruptedException | java.util.concurrent.TimeoutException ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            backoffManager.scheduleRetry(topic);
            return TopicExistence.UNKNOWN;
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                state.existsFalse.increment();
                return TopicExistence.FALSE;
            }
            backoffManager.scheduleRetry(topic);
            return TopicExistence.UNKNOWN;
        } catch (RuntimeException re) {
            backoffManager.scheduleRetry(topic);
            return TopicExistence.UNKNOWN;
        }
    }

    /** Describe для набора тем; возвращает список отсутствующих. */
    ArrayList<String> describeBatch(java.util.Set<String> topics) {
        ArrayList<String> missing = new ArrayList<>(topics.size());
        Map<String, KafkaFuture<TopicDescription>> fmap = admin.describeTopics(topics);
        for (String t : topics) {
            classifyDescribeTopic(fmap, t, missing);
        }
        return missing;
    }

    private void classifyDescribeTopic(Map<String, KafkaFuture<TopicDescription>> fmap,
                                       String topic,
                                       List<String> missing) {
        KafkaFuture<TopicDescription> future = fmap.get(topic);
        try {
            future.get(adminTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            state.existsTrue.increment();
            state.ensured.add(topic);
            backoffManager.markSuccess(topic);
        } catch (InterruptedException ex) {
            handleInterrupted(topic, ex);
        } catch (java.util.concurrent.TimeoutException ex) {
            handleTimeout(topic, ex);
        } catch (java.util.concurrent.ExecutionException ex) {
            handleExecution(topic, ex, missing);
        } catch (RuntimeException ex) {
            handleRuntime(topic, ex);
        }
    }

    enum TopicExistence { TRUE, FALSE, UNKNOWN }

    private void handleInterrupted(String topic, InterruptedException ex) {
        Thread.currentThread().interrupt();
        state.existsUnknown.increment();
        backoffManager.scheduleRetry(topic);
        if (TopicEnsureService.LOG.isDebugEnabled()) {
            TopicEnsureService.LOG.debug("Проверка Kafka-топика '{}' прервана", topic, ex);
        }
    }

    private void handleTimeout(String topic, java.util.concurrent.TimeoutException ex) {
        state.existsUnknown.increment();
        backoffManager.scheduleRetry(topic);
        if (TopicEnsureService.LOG.isDebugEnabled()) {
            TopicEnsureService.LOG.debug("Проверка Kafka-топика '{}' превысила таймаут {} мс", topic, adminTimeoutMs, ex);
        }
    }

    private void handleExecution(String topic,
                                 java.util.concurrent.ExecutionException ex,
                                 List<String> missing) {
        Throwable cause = ex.getCause();
        if (cause instanceof UnknownTopicOrPartitionException) {
            state.existsFalse.increment();
            missing.add(topic);
            return;
        }
        state.existsUnknown.increment();
        backoffManager.scheduleRetry(topic);
        if (TopicEnsureService.LOG.isDebugEnabled()) {
            TopicEnsureService.LOG.debug("Ошибка при проверке Kafka-топика '{}'", topic, ex);
        }
    }

    private void handleRuntime(String topic, RuntimeException ex) {
        state.existsUnknown.increment();
        backoffManager.scheduleRetry(topic);
        if (TopicEnsureService.LOG.isDebugEnabled()) {
            TopicEnsureService.LOG.debug("Не удалось проверить Kafka-топик '{}' (runtime)", topic, ex);
        }
    }
}
