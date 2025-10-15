package kz.qazmarka.h2k.endpoint.processing;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;

/**
 * Фиксирует использование соли Phoenix (h2k.saltBytes) на горячем пути.
 * Считывает длину rowkey и оценивает эффективность соли, логируя предупреждения при аномалиях.
 */
final class SaltUsageObserver {

    private static final Logger LOG = LoggerFactory.getLogger(SaltUsageObserver.class);
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

    void observeRow(TableName table,
                    H2kConfig.TableOptionsSnapshot tableOpts,
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
        private volatile H2kConfig.ValueSource saltSource;
        private final LongAdder rows = new LongAdder();
        private final LongAdder pkBytesTotal = new LongAdder();
        private final AtomicInteger minPk = new AtomicInteger(Integer.MAX_VALUE);
        private final AtomicInteger maxPk = new AtomicInteger();
        private final LongAdder shortRows = new LongAdder();
        private final AtomicBoolean warnIssued = new AtomicBoolean();
        private final AtomicLong lastInfoLogged = new AtomicLong();

        void capture(TableName table,
                     int newSaltBytes,
                     H2kConfig.ValueSource newSaltSource,
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

            if (rowKeyLength <= saltBytes) {
                shortRows.increment();
                if (warnIssued.compareAndSet(false, true) && LOG.isWarnEnabled()) {
                    LOG.warn("Соль Phoenix таблицы {}: обнаружены строки длиной {} байт (saltBytes={}). Проверьте конфигурацию Avro.",
                            table,
                            rowKeyLength,
                            saltBytes);
                }
            }

            long total = rows.sum();
            long previous = lastInfoLogged.get();
            long delta = total - previous;
            if (total >= LOG_THRESHOLD
                    && delta >= LOG_THRESHOLD
                    && lastInfoLogged.compareAndSet(previous, total)
                    && LOG.isInfoEnabled()) {
                long pkSum = pkBytesTotal.sum();
                double avgPk = total == 0L ? 0.0d : pkSum / (double) total;
                int min = minPkValue();
                int max = maxPk.get();
                String avgText = format(avgPk);
                LOG.info(
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

        private void refreshSaltMetadata(int newSaltBytes, H2kConfig.ValueSource newSaltSource) {
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

        private static String label(H2kConfig.ValueSource source) {
            return source == null ? "" : source.label();
        }

        private static String format(double value) {
            return String.format(java.util.Locale.ROOT, "%.1f", value);
        }
    }
}
