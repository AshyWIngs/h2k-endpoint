package kz.qazmarka.h2k.endpoint.processing;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.config.TableValueSource;

/**
 * Фиксирует использование соли Phoenix (h2k.saltBytes) на горячем пути.
 * Считывает длину rowkey и оценивает эффективность соли, логируя предупреждения при аномалиях.
 */
/**
 * Потокобезопасный сборщик статистики по длине rowkey vs saltBytes.
 *
 * Конкурентность и производительность:
 * - На горячем пути используются lock-free примитивы (LongAdder/Atomic*),
 *   чтобы не блокировать обработку WAL.
 * - lastInfoLogged — AtomicLong для редкого INFO-логирования пакетами по LOG_THRESHOLD строк.
 * - Локирование отсутствует, кроме редкого обновления метаданных соли (synchronized внутри refreshSaltMetadata()).
 */
final class SaltUsageObserver {

    private static final AtomicReference<Logger> LOG = new AtomicReference<>(LoggerFactory.getLogger(SaltUsageObserver.class));
    private static final long LOG_THRESHOLD = 200L;

    private final ConcurrentHashMap<TableName, SaltStats> statsByTable = new ConcurrentHashMap<>();
    private final boolean enabled;

    private SaltUsageObserver(boolean enabled) {
        this.enabled = enabled;
    }

    static SaltUsageObserver create() {
        return new SaltUsageObserver(true);
    }

    static SaltUsageObserver disabled() {
        return new SaltUsageObserver(false);
    }

    /**
     * Тестовый индикатор: сообщает, включён ли сбор статистики.
     */
    boolean isEnabledForTest() {
        return enabled;
    }

    /**
     * @return сколько таблиц накопили статистику; используется в модульных тестах.
     */
    int observedTablesCountForTest() {
        if (!enabled) {
            return 0;
        }
        return statsByTable.size();
    }

    /** Позволяет временно подменить логгер для модульных тестов. */
    static AutoCloseable withLoggerForTest(Logger testLogger) {
        if (testLogger == null) {
            throw new IllegalArgumentException("testLogger must not be null");
        }
        Logger previous = LOG.getAndSet(testLogger);
        return () -> LOG.set(previous);
    }

    void observeRow(TableName table,
                    TableOptionsSnapshot tableOpts,
                    int rowKeyLength) {
        if (!enabled) {
            return;
        }
        if (table == null || tableOpts == null) {
            return;
        }
        int saltBytes = tableOpts.saltBytes();
        if (saltBytes <= 0) {
            return;
        }
        if (rowKeyLength <= 0) {
            return;
        }
        SaltStats stats = statsByTable.computeIfAbsent(table, t -> new SaltStats());
        stats.capture(table, saltBytes, tableOpts.saltSource(), rowKeyLength);
    }

    private static final class SaltStats {
        private volatile int saltBytes;
        private volatile TableValueSource saltSource;
        private final LongAdder rows = new LongAdder();
        private final LongAdder pkBytesTotal = new LongAdder();
        private final AtomicInteger minPk = new AtomicInteger(Integer.MAX_VALUE);
        private final AtomicInteger maxPk = new AtomicInteger();
        private final LongAdder shortRows = new LongAdder();
        private final AtomicBoolean warnIssued = new AtomicBoolean();
        private final AtomicLong lastInfoLogged = new AtomicLong();

        void capture(TableName table,
                     int newSaltBytes,
                     TableValueSource newSaltSource,
                     int rowKeyLength) {
            refreshSaltMetadata(newSaltBytes, newSaltSource);

            int pkLength = rowKeyLength - saltBytes;
            if (pkLength < 0) {
                pkLength = 0;
            }

            rows.increment();
            pkBytesTotal.add(pkLength);
            updateMin(pkLength);
            updateMax(pkLength);

            Logger logger = LOG.get();

            if (rowKeyLength <= saltBytes) {
                shortRows.increment();
                if (logger.isWarnEnabled() && warnIssued.compareAndSet(false, true)) {
                    logger.warn("Соль Phoenix таблицы {}: обнаружены строки длиной {} байт (saltBytes={}). Проверьте конфигурацию Avro.",
                            table,
                            rowKeyLength,
                            saltBytes);
                }
            }

            long total = rows.sum();
            long previous = lastInfoLogged.get();
            long delta = total - previous;
            boolean reachedThreshold = total >= LOG_THRESHOLD;
            boolean farEnoughFromLast = delta >= LOG_THRESHOLD;
            if (logger.isInfoEnabled()
                    && reachedThreshold
                    && farEnoughFromLast
                    && lastInfoLogged.compareAndSet(previous, total)) {
                long pkSum = pkBytesTotal.sum();
                double avgPk = total == 0L ? 0.0d : pkSum / (double) total;
                int min = minPkValue();
                int max = maxPk.get();
                String avgText = format(avgPk);
                logger.info(
                        "Соль Phoenix таблицы {}: saltBytes={} ({}), строк={}, средний PK={} байт, minPK={}, maxPK={}, коротких={}",
                        table,
                        saltBytes,
                        label(saltSource),
                        total,
                        avgText,
                        min,
                        max,
                        shortRows.sum());
            }
        }

        private void refreshSaltMetadata(int newSaltBytes, TableValueSource newSaltSource) {
            if (saltBytes == newSaltBytes && saltSource == newSaltSource) {
                return;
            }
            synchronized (this) {
                saltBytes = newSaltBytes;
                saltSource = newSaltSource;
                // не сбрасываем счётчики: изменения соли редкие, а статистика полезна в динамике
            }
        }

        private void updateMin(int pkLength) {
            while (true) {
                int current = minPk.get();
                if (pkLength >= current && current != Integer.MAX_VALUE) {
                    return;
                }
                if (minPk.compareAndSet(current, pkLength)) {
                    return;
                }
            }
        }

        private void updateMax(int pkLength) {
            while (true) {
                int current = maxPk.get();
                if (pkLength <= current) {
                    return;
                }
                if (maxPk.compareAndSet(current, pkLength)) {
                    return;
                }
            }
        }

        private int minPkValue() {
            int value = minPk.get();
            return value == Integer.MAX_VALUE ? 0 : value;
        }

        /**
         * Возвращает пояснение источника значения соли для логирования.
         */
        private static String label(TableValueSource source) {
            return source == null ? "" : source.label();
        }

        private static String format(double value) {
            return String.format(java.util.Locale.ROOT, "%.1f", value);
        }
    }
}
