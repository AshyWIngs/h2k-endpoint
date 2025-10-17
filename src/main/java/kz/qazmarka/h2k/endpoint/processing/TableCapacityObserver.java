package kz.qazmarka.h2k.endpoint.processing;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.TableMetadataView;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.config.TableValueSource;

/**
 * Наблюдает фактическое количество колонок на строку и рекомендует обновления h2k.capacity.hints.
 * Максимальные значения фиксируются на потоке-репликации без дополнительных аллокаций.
 */
final class TableCapacityObserver {

    private static final AtomicReference<Logger> LOG = new AtomicReference<>(LoggerFactory.getLogger(TableCapacityObserver.class));

    private static final long MIN_ROWS_BEFORE_RECOMMEND = 200L;

    private final ConcurrentHashMap<TableName, Stats> statsByTable = new ConcurrentHashMap<>();
    private final TableMetadataView metadata;
    private final boolean enabled;

    private TableCapacityObserver(TableMetadataView metadata, boolean enabled) {
        this.metadata = metadata;
        this.enabled = enabled;
    }

    static TableCapacityObserver create(TableMetadataView metadata) {
        return new TableCapacityObserver(Objects.requireNonNull(metadata, "метаданные таблиц"), true);
    }

    static TableCapacityObserver disabled() {
        return new TableCapacityObserver(null, false);
    }

    /**
     * Минимальный хук для модульных тестов: сигнализирует, активен ли наблюдатель.
     */
    boolean isEnabledForTest() {
        return enabled;
    }

    /**
     * Фиксирует наблюдение по таблице: обновляет максимум фактических полей в одной строке
     * и накапливает количество строк, по которым статистика уже собрана. Срабатывает на горячем пути
     * репликации, поэтому не аллоцирует временные объекты.
     *
     * @param table        имя таблицы HBase
     * @param fieldsPerRow максимум полей, замеченных в одной строке текущей партии
     * @param rowsMeasured сколько строк обработано в партии (используется как "вес" наблюдения)
     */
    void observe(TableName table, int fieldsPerRow, long rowsMeasured) {
        if (!enabled) {
            return;
        }
        if (table == null || fieldsPerRow <= 0 || rowsMeasured <= 0L) {
            return;
        }
        Stats stats = statsByTable.computeIfAbsent(table, t -> new Stats());
        stats.rowsObserved.add(rowsMeasured);

        long candidate = fieldsPerRow;
        long prev;
        do {
            prev = stats.maxFields.get();
            if (candidate <= prev) {
                break;
            }
        } while (!stats.maxFields.compareAndSet(prev, candidate));

        maybeRecommend(table, stats);
    }

    /**
     * @return агрегированный максимум по всем таблицам — удобно для тестов и диагностики.
     */
    long totalObservedMax() {
        if (!enabled) {
            return 0L;
        }
        long sum = 0L;
        for (Stats stats : statsByTable.values()) {
            sum += stats.maxFields.get();
        }
        return sum;
    }

    /**
     * Создаёт моментальный снимок накопленных метрик без блокировок.
     * Возвращается новая map, поэтому дальнейшие изменения не влияют на snapshot.
     */
    Map<TableName, StatsSnapshot> snapshot() {
        if (!enabled) {
            return java.util.Collections.emptyMap();
        }
        Map<TableName, StatsSnapshot> copy = new ConcurrentHashMap<>();
        statsByTable.forEach((table, stats) -> copy.put(table,
                new StatsSnapshot(stats.maxFields.get(), stats.rowsObserved.sum(), stats.lastWarnedRecommendation.get())));
        return copy;
    }

    /**
     * Проверяет, достаточно ли данных для рекомендации, и логирует предупреждение,
     * если фактический максимум полей превышает текущую подсказку из конфигурации.
     */
    private void maybeRecommend(TableName table, Stats stats) {
        if (!enabled) {
            return;
        }
        long rows = stats.rowsObserved.sum();
        long maxFields = stats.maxFields.get();
        boolean hasSufficientData = rows >= MIN_ROWS_BEFORE_RECOMMEND && maxFields > 0L;
        if (!hasSufficientData) {
            return;
        }
        int configuredHint = metadata.getCapacityHintFor(table);
        long lastWarned = stats.lastWarnedRecommendation.get();
        boolean needsCapacityUpdate = configuredHint <= 0 || maxFields > configuredHint;
        boolean newerRecommendation = maxFields > lastWarned;
        if (!(needsCapacityUpdate && newerRecommendation)) {
            return;
        }
        if (!stats.lastWarnedRecommendation.compareAndSet(lastWarned, maxFields)) {
            return;
        }
        Logger logger = LOG.get();
        if (!logger.isWarnEnabled()) {
            return;
        }
        TableOptionsSnapshot snapshot = metadata.describeTableOptions(table);
        int currentHint = (snapshot == null) ? configuredHint : snapshot.capacityHint();
        TableValueSource source = (snapshot == null) ? TableValueSource.DEFAULT : snapshot.capacitySource();
        logger.warn("Таблица {}: замечено {} полей при {} строках (источник подсказки: {}). Рекомендуется обновить h2k.capacityHint в Avro-схеме не ниже {} (сейчас {}).",
                table,
                maxFields,
                rows,
                label(source),
                maxFields,
                currentHint);
    }

    static AutoCloseable withLoggerForTest(Logger testLogger) {
        Logger previous = LOG.getAndSet(testLogger);
        return () -> LOG.set(previous);
    }

    /**
     * Возвращает человекочитаемую метку источника для логирования.
     */
    private static String label(TableValueSource source) {
        return source == null ? "" : source.label();
    }

    /**
     * Снимок накопленных статистик по таблице: максимум полей и объём выборки.
     */
    static final class StatsSnapshot {
        private final long maxFields;
        private final long rowsObserved;
        private final long lastRecommendation;

        StatsSnapshot(long maxFields, long rowsObserved, long lastRecommendation) {
            this.maxFields = maxFields;
            this.rowsObserved = rowsObserved;
            this.lastRecommendation = lastRecommendation;
        }

        /**
         * @return фиксированный максимум полей в одной строке.
         */
        long maxFields() {
            return maxFields;
        }

        /**
         * @return сколько строк учтено при расчёте статистики.
         */
        long rowsObserved() {
            return rowsObserved;
        }

        /**
         * @return последнее рекомендованное значение (0 — рекомендация ещё не выдавалась).
         */
        long lastRecommendation() {
            return lastRecommendation;
        }
    }

    /**
     * Накопитель статистики на горячем пути: максимум полей, количество строк и последний порог оповещения.
     */
    private static final class Stats {
        final AtomicLong maxFields = new AtomicLong();
        final LongAdder rowsObserved = new LongAdder();
        final AtomicLong lastWarnedRecommendation = new AtomicLong();
    }
}
