package kz.qazmarka.h2k.endpoint.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.TopicEnsurer;

/**
 * Управляет разрешением имён топиков и их ensure-циклом.
 */
public final class TopicManager {

    private static final Logger LOG = LoggerFactory.getLogger(TopicManager.class);

    private final H2kConfig cfg;
    private final TopicEnsurer topicEnsurer;
    private final Map<TableName, String> topicCache = new HashMap<>(8);
    private final Set<String> ensuredTopics = new HashSet<>(8);

    public TopicManager(H2kConfig cfg, TopicEnsurer topicEnsurer) {
        this.cfg = cfg;
        this.topicEnsurer = topicEnsurer;
    }

    public String resolveTopic(TableName table) {
        return topicCache.computeIfAbsent(table, cfg::topicFor);
    }

    public void ensureTopicIfNeeded(String topic) {
        if (topicEnsurer == null || topic == null || topic.isEmpty()) {
            return;
        }
        if (!ensuredTopics.add(topic)) {
            return;
        }
        try {
            topicEnsurer.ensureTopic(topic);
        } catch (Exception e) {
            LOG.warn("Проверка/создание топика '{}' не выполнена: {}. Репликацию не прерываю", topic, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки ensureTopic()", e);
            }
            ensuredTopics.remove(topic);
        }
    }

    public boolean ensureEnabled() {
        return topicEnsurer != null;
    }

    public void closeQuietly() {
        if (topicEnsurer == null) {
            return;
        }
        try {
            topicEnsurer.close();
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ошибка при закрытии TopicEnsurer (игнорируется при остановке)", e);
            }
        }
    }
}
