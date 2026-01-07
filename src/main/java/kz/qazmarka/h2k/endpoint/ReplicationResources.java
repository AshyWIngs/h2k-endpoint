package kz.qazmarka.h2k.endpoint;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;

import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.util.RowKeySlice;
import org.apache.kafka.clients.producer.Producer;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/**
 * Инкапсулирует жизненный цикл горячих компонентов (PayloadBuilder, WalEntryProcessor, TopicManager).
 * Позволяет создавать/закрывать их атомарно и облегчает тестирование {@link KafkaReplicationEndpoint}.
 */
final class ReplicationResources implements AutoCloseable {

    private final PayloadBuilder payloadBuilder;
    private final TopicManager topicManager;
    private final WalEntryProcessor walEntryProcessor;

    private ReplicationResources(PayloadBuilder payloadBuilder,
                                 TopicManager topicManager,
                                 WalEntryProcessor walEntryProcessor) {
        this.payloadBuilder = payloadBuilder;
        this.topicManager = topicManager;
        this.walEntryProcessor = walEntryProcessor;
    }

    static ReplicationResources create(H2kConfig config,
                                       Decoder decoder,
                                       Producer<RowKeySlice, byte[]> producer) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(decoder, "decoder");
        Objects.requireNonNull(producer, "producer");

        try (ResourceGuard guard = new ResourceGuard()) {
            PayloadBuilder payload = guard.register(new PayloadBuilder(decoder, config));
            TopicEnsurer topicEnsurer = guard.register(TopicEnsurer.createIfEnabled(
                    config.getEnsureSettings(),
                    config.getTopicSettings(),
                    config.getBootstrap(),
                    null));
            TopicManager manager = new TopicManager(config.getTopicSettings(), topicEnsurer);
            guard.register((AutoCloseable) manager::closeQuietly);
            WalEntryProcessor processor = guard.register(new WalEntryProcessor(payload, manager, producer, config));
            guard.releaseAll();
            return new ReplicationResources(payload, manager, processor);
        }
    }

    PayloadBuilder payloadBuilder() {
        return payloadBuilder;
    }

    TopicManager topicManager() {
        return topicManager;
    }

    WalEntryProcessor walEntryProcessor() {
        return walEntryProcessor;
    }

    @Override
    public void close() {
        closeQuietly(walEntryProcessor);
        topicManager.closeQuietly();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignore) { // NOPMD - вызывающая сторона решает, где фиксировать ошибки остановки
        }
    }

    private static final class ResourceGuard implements AutoCloseable {
        private final Deque<AutoCloseable> stack = new ArrayDeque<>();

        <T extends AutoCloseable> T register(T resource) {
            if (resource != null) {
                stack.push(resource);
            }
            return resource;
        }

        void releaseAll() {
            stack.clear();
        }

        @Override
        public void close() {
            while (!stack.isEmpty()) {
                closeQuietly(stack.pop());
            }
        }
    }
}
