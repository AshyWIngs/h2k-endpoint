package kz.qazmarka.h2k.endpoint.topic;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.TopicNamingSettings;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;

/**
 * Отвечает за разрешение имён Kafka-топиков и ленивые ensure-вызовы.
 * Использует {@link TopicNamingSettings#resolve(TableName)} для кеширования с учётом шаблона,
 * а также {@link TopicEnsurer} в режиме NOOP/active — вызывающий код не проверяет конфигурацию.
 */
public final class TopicManager {

    private static final Logger LOG = LoggerFactory.getLogger(TopicManager.class);

    private final TopicNamingSettings topicSettings;
    private final TopicEnsurer topicEnsurer;
    /**
     * Кеш разрешённых имён топиков. Потокобезопасный {@link ConcurrentMap}, чтобы несколько потоков
     * ReplicationEndpoint не гонялись за одним и тем же TableName.
     */
    private final ConcurrentMap<String, String> topicCache = new ConcurrentHashMap<>(8);
    private final ConcurrentMap<String, LongSupplier> extraMetrics = new ConcurrentHashMap<>(8);
    private final ConcurrentMap<String, EnsureState> ensureStates = new ConcurrentHashMap<>(8);
    private final LongAdder ensureSkipped = new LongAdder();

    private static final long ENSURE_SUCCESS_COOLDOWN_MS = TimeUnit.MINUTES.toMillis(1);
    private static final long ENSURE_FAILURE_RETRY_MS = TimeUnit.SECONDS.toMillis(5);

    public TopicManager(TopicNamingSettings topicSettings, TopicEnsurer topicEnsurer) {
        this.topicSettings = Objects.requireNonNull(topicSettings, "настройки топиков h2k");
        this.topicEnsurer = topicEnsurer == null ? TopicEnsurer.disabled() : topicEnsurer;
    }

    /**
     * Возвращает имя Kafka-топика для таблицы, используя кеш и шаблон из {@link H2kConfig}.
     * Значение кешируется по строковому представлению имени таблицы, что исключает удержание
     * временных экземпляров {@link TableName}, которые создаёт HBase на горячем пути.
     */
    public String resolveTopic(TableName table) {
        String cacheKey = table.getNameWithNamespaceInclAsString();
        String cached = topicCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        String resolved = topicSettings.resolve(table);
        String race = topicCache.putIfAbsent(cacheKey, resolved);
        return race != null ? race : resolved;
    }

    /**
     * Ленивая ensure-проверка: при необходимости вызывает {@link TopicEnsurer#ensureTopic(String)}
     * после валидации имени. В режиме NOOP вызов безопасно игнорируется.
     */
    public void ensureTopicIfNeeded(String topic) {
        if (topic == null || topic.isEmpty()) {
            return;
        }
        if (!topicEnsurer.isEnabled()) {
            return;
        }
        long now = System.currentTimeMillis();
        EnsureState state = ensureStates.computeIfAbsent(topic, key -> new EnsureState());
        if (!state.tryAcquire(now)) {
            ensureSkipped.increment();
            return;
        }
        boolean success = false;
        try {
            topicEnsurer.ensureTopic(topic);
            success = true;
        } catch (RuntimeException e) {
            LOG.warn("Проверка/создание топика '{}' не выполнена: {}. Репликацию не прерываю", topic, e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки ensureTopic()", e);
            }
        } finally {
            state.finish(success, System.currentTimeMillis());
        }
    }

    /**
     * @return {@code true}, если ensure действительно активен (не NOOP).
     */
    public boolean ensureEnabled() {
        return topicEnsurer.isEnabled();
    }

    /** Снимок кеша топиков для модульных тестов (неизменяемое представление). */
    Map<String, String> topicCacheSnapshotForTest() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(topicCache));
    }

    /**
     * Регистрирует дополнительную числовую метрику, которую необходимо включить в снимок {@link #getMetrics()}.
     * Повторная регистрация с тем же именем перезаписывает предыдущий supplier.
     *
     * @param name     уникальное имя метрики (не пустое)
     * @param supplier поставщик значения; вызывается при формировании снимка
     */
    public void registerMetric(String name, LongSupplier supplier) {
        Objects.requireNonNull(name, "metric name");
        Objects.requireNonNull(supplier, "metric supplier");
        String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("metric name must not be blank");
        }
        extraMetrics.put(trimmed, supplier);
    }

    /**
     * Возвращает снимок метрик ensure и зарегистрированных дополнительных счётчиков.
     * Ключи ensure.* сохраняются как есть, дополнительные метрики добавляются без предиктов.
     * Возвращаемая карта неизменяема и безопасна для экспорта в JMX.
     */
    public Map<String, Long> getMetrics() {
        Map<String, Long> ensureMetrics = topicEnsurer.getMetrics();
        if (extraMetrics.isEmpty()) {
            if (ensureSkipped.sum() == 0) {
                return ensureMetrics;
            }
            Map<String, Long> snapshot = new LinkedHashMap<>(ensureMetrics.size() + 1);
            snapshot.putAll(ensureMetrics);
            snapshot.put("ensure.cooldown.skipped", ensureSkipped.sum());
            return Collections.unmodifiableMap(snapshot);
        }
        int extra = extraMetrics.size();
        if (ensureSkipped.sum() > 0) {
            extra++;
        }
        Map<String, Long> snapshot = new LinkedHashMap<>(ensureMetrics.size() + extra);
        snapshot.putAll(ensureMetrics);
        for (Map.Entry<String, LongSupplier> e : extraMetrics.entrySet()) {
            try {
                snapshot.put(e.getKey(), e.getValue().getAsLong());
            } catch (RuntimeException ex) {
                LOG.warn("Метрика '{}' недоступна: {}", e.getKey(), ex.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Трассировка ошибки метрики '{}'", e.getKey(), ex);
                }
            }
        }
        if (ensureSkipped.sum() > 0) {
            snapshot.put("ensure.cooldown.skipped", ensureSkipped.sum());
        }
        return Collections.unmodifiableMap(snapshot);
    }

    public long ensureSkippedCount() {
        return ensureSkipped.sum();
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

    /**
     * Сохраняет информацию о последних ensure-вызовах и предотвращает частые повторы.
     * Использует синхронизацию на уровне экземпляра; нагрузка невелика, поскольку ensure вызывается редко.
     */
    private static final class EnsureState {
        private static final long NO_TIME = Long.MIN_VALUE;

        private long lastSuccess = NO_TIME;
        private long lastFailure = NO_TIME;
        private boolean inProgress;

        synchronized boolean tryAcquire(long now) {
            if (inProgress) {
                return false;
            }
            if (lastSuccess != NO_TIME && (now - lastSuccess) < ENSURE_SUCCESS_COOLDOWN_MS) {
                return false;
            }
            if (lastFailure != NO_TIME && (now - lastFailure) < ENSURE_FAILURE_RETRY_MS) {
                return false;
            }
            inProgress = true;
            return true;
        }

        synchronized void finish(boolean success, long timestamp) {
            if (success) {
                lastSuccess = timestamp;
                lastFailure = NO_TIME;
            } else {
                lastFailure = timestamp;
            }
            inProgress = false;
        }
    }
}
