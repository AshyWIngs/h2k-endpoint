package kz.qazmarka.h2k.endpoint.processing;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;

/**
 * Отвечает за агрегирование счётчиков WAL и расчёт throughput.
 * Предоставляет локальные счётчики на запись и итоговые метрики для JMX/логов.
 */
final class WalCounterService {

    private static final long THROUGHPUT_LOG_INTERVAL_NS = TimeUnit.SECONDS.toNanos(5);

    private final LongAdder entriesProcessed = new LongAdder();
    private final LongAdder rowsProcessed = new LongAdder();
    private final LongAdder cellsProcessed = new LongAdder();
    private final LongAdder rowsFiltered = new LongAdder();

    private final LongAdder entriesWindow = new LongAdder();
    private final LongAdder rowsWindow = new LongAdder();
    private final LongAdder cellsWindow = new LongAdder();
    private final LongAdder filteredRowsWindow = new LongAdder();
    private final AtomicLong throughputWindowStart = new AtomicLong(System.nanoTime());

    EntryCounters newEntryCounters() {
        return new EntryCounters();
    }

    EntrySummary completeEntry(EntryCounters counters) {
        entriesProcessed.increment();
        long rowsSeen = (long) counters.rowsSent + (long) counters.rowsFiltered;
        if (rowsSeen > 0L) {
            rowsProcessed.add(rowsSeen);
        }
        cellsProcessed.add(counters.cellsSeen);
        if (counters.rowsFiltered > 0) {
            rowsFiltered.add(counters.rowsFiltered);
        }
        return new EntrySummary(rowsSeen,
                counters.rowsSent,
                counters.rowsFiltered,
                counters.cellsSeen,
                counters.maxRowCellsSent,
                counters.maxRowCellsSeen);
    }

    void logThroughput(EntryCounters counters, Logger log) {
        entriesWindow.increment();
        addIfPositive(rowsWindow, counters.rowsSent);
        addIfPositive(cellsWindow, counters.cellsSent);
        addIfPositive(filteredRowsWindow, counters.rowsFiltered);

        long now = System.nanoTime();
        long windowStart = throughputWindowStart.get();
        long elapsed = now - windowStart;
        if (!windowReady(elapsed, windowStart, now)) {
            return;
        }

        long rowsPreview = rowsWindow.sum();
        long cellsPreview = cellsWindow.sum();
        long filteredPreview = filteredRowsWindow.sum();
        if (rowsPreview == 0L && cellsPreview == 0L && filteredPreview == 0L) {
            return;
        }

        long entries = entriesWindow.sumThenReset();
        long rows = rowsWindow.sumThenReset();
        long cells = cellsWindow.sumThenReset();
        long filteredRows = filteredRowsWindow.sumThenReset();

        emitThroughput(elapsed, entries, rows, cells, filteredRows, log);
    }

    /**
     * Служебный хук для тестов: вбрасывает абсолютный момент начала окна throughput.
     * Используется исключительно при юнит-тестировании для форсированного логирования.
     *
     * @param absoluteNanoTime значение {@link System#nanoTime()}, которое нужно сохранить
     */
    void overrideThroughputWindowStart(long absoluteNanoTime) {
        throughputWindowStart.set(absoluteNanoTime);
    }

    MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
                entriesProcessed.sum(),
                rowsProcessed.sum(),
                cellsProcessed.sum(),
                rowsFiltered.sum());
    }

    long entriesTotal() {
        return entriesProcessed.sum();
    }

    long rowsTotal() {
        return rowsProcessed.sum();
    }

    long cellsTotal() {
        return cellsProcessed.sum();
    }

    long rowsFilteredTotal() {
        return rowsFiltered.sum();
    }

    private static void addIfPositive(LongAdder target, int delta) {
        if (delta > 0) {
            target.add(delta);
        }
    }

    private boolean windowReady(long elapsed, long windowStart, long now) {
        return elapsed >= THROUGHPUT_LOG_INTERVAL_NS
                && throughputWindowStart.compareAndSet(windowStart, now);
    }

    private void emitThroughput(long elapsedNs,
                                long entries,
                                long rows,
                                long cells,
                                long filteredRows,
                                Logger log) {
        if (elapsedNs <= 0L || !log.isInfoEnabled()) {
            return;
        }
        double intervalSeconds = elapsedNs / 1_000_000_000.0;
        if (intervalSeconds <= 0D) {
            return;
        }
        double rowsPerSec = rows / intervalSeconds;
        double cellsPerSec = cells / intervalSeconds;

        log.info(
                "Скорость WAL: записей={}, строк={}, строк/с={}, ячеек={}, ячеек/с={}, отфильтровано_строк={}, интервал_мс={}",
                entries,
                rows,
                formatDecimal(rowsPerSec),
                cells,
                formatDecimal(cellsPerSec),
                filteredRows,
                TimeUnit.NANOSECONDS.toMillis(elapsedNs));
    }

    private static String formatDecimal(double value) {
        return String.format(Locale.ROOT, "%.1f", value);
    }

    static final class EntryCounters {
        int rowsSent;
        int cellsSent;
        int rowsFiltered;
        int cellsSeen;
        int maxRowCellsSeen;
        int maxRowCellsSent;
    }

    static final class EntrySummary {
        final long rowsSeen;
        final long rowsSent;
        final long rowsFiltered;
        final long cellsSeen;
        final int maxRowCellsSent;
        final int maxRowCellsSeen;

        EntrySummary(long rowsSeen,
                     int rowsSent,
                     int rowsFiltered,
                     int cellsSeen,
                     int maxRowCellsSent,
                     int maxRowCellsSeen) {
            this.rowsSeen = rowsSeen;
            this.rowsSent = rowsSent;
            this.rowsFiltered = rowsFiltered;
            this.cellsSeen = cellsSeen;
            this.maxRowCellsSent = maxRowCellsSent;
            this.maxRowCellsSeen = maxRowCellsSeen;
        }
    }

    static final class MetricsSnapshot {
        final long entries;
        final long rows;
        final long cells;
        final long filteredRows;

        MetricsSnapshot(long entries, long rows, long cells, long filteredRows) {
            this.entries = entries;
            this.rows = rows;
            this.cells = cells;
            this.filteredRows = filteredRows;
        }
    }
}
