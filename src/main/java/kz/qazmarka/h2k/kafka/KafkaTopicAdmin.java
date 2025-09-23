package kz.qazmarka.h2k.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

/**
 * Минимальный контракт для работы с Kafka AdminClient в ensure-процессе.
 *
 * Интерфейс преднамеренно лишён зависимостей от конкретной реализации AdminClient, что упрощает unit-тесты.
 */
interface KafkaTopicAdmin {

    Map<String, KafkaFuture<TopicDescription>> describeTopics(Set<String> names);

    Map<String, KafkaFuture<Void>> createTopics(List<NewTopic> newTopics);

    void createTopic(NewTopic topic, long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException;

    void close(Duration timeout);

    void increasePartitions(String topic, int newCount, long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException;

    Map<ConfigResource, KafkaFuture<Config>> describeConfigs(Collection<ConfigResource> resources);

    void incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops,
                                 long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException;
}
