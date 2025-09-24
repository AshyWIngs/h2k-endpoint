package kz.qazmarka.h2k.endpoint.internal;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;

/**
 * Отвечает за разрешение имён Kafka-топиков и ленивые ensure-вызовы.
 * <p>
 * Использует {@link H2kConfig#topicFor(org.apache.hadoop.hbase.TableName)} для кеширования имён,
 * а также {@link TopicEnsurer} в режиме NOOP/active — вызывающий код не проверяет конфигурацию.
 */
public final class TopicManager {

    private static final Logger LOG = LoggerFactory.getLogger(TopicManager.class);

    private final H2kConfig cfg;
    private final TopicEnsurer topicEnsurer;
    /**
     * Кеш разрешённых имён топиков. Потокобезопасный {@link ConcurrentMap}, чтобы несколько потоков
     * ReplicationEndpoint не гонялись за одним и тем же TableName.
     */
    private final ConcurrentMap<TableName, String> topicCache = new ConcurrentHashMap<>(8);
    public TopicManager(H2kConfig cfg, TopicEnsurer topicEnsurer) {
        this.cfg = Objects.requireNonNull(cfg, "конфигурация h2k");
        this.topicEnsurer = topicEnsurer == null ? TopicEnsurer.disabled() : topicEnsurer;
    }

    /**
     * Возвращает имя Kafka-топика для таблицы, используя кеш и шаблон из {@link H2kConfig}.
     */
    public String resolveTopic(TableName table) {
        return topicCache.computeIfAbsent(table, cfg::topicFor);
    }

    /**
     * Ленивая ensure-проверка: при необходимости вызывает {@link TopicEnsurer#ensureTopic(String)}
     * после валидации имени. В режиме NOOP вызов безопасно игнорируется.
     */
    public void ensureTopicIfNeeded(String topic) {
        if (topic == null || topic.isEmpty()) {
            return;
        }
        try {
            topicEnsurer.ensureTopic(topic);
        } catch (Exception e) {
            LOG.warn("Проверка/создание топика '{}' не выполнена: {}. Репликацию не прерываю", topic, e.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки ensureTopic()", e);
            }
        }
    }

    /**
     * @return {@code true}, если ensure действительно активен (не NOOP).
     */
    public boolean ensureEnabled() {
        return topicEnsurer.isEnabled();
    }

    /**
     * Закрывает обёрнутый {@link TopicEnsurer}, проглатывая исключения на случай остановки.
     */
    public void closeQuietly() {
        try {
            topicEnsurer.close();
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ошибка при закрытии TopicEnsurer (игнорируется при остановке)", e);
            }
        }
    }
}
