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
    // ==== Периодические сводки (перемещено вверх для PMD) ====
    private static final long ENSURE_SUMMARY_INTERVAL_NS = TimeUnit.SECONDS.toNanos(60);
    private static final long SR_SUMMARY_INTERVAL_NS = TimeUnit.MINUTES.toNanos(5);
    private final AtomicLong nextEnsureSummaryAt = new AtomicLong(System.nanoTime());
    private final AtomicLong nextSrSummaryAt = new AtomicLong(System.nanoTime());

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
        counters.maxRowCellsSent);
    }

    void logThroughput(EntryCounters counters, Logger log, java.util.function.Supplier<java.util.Map<String, Long>> extraMetricsSupplier) {
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

        java.util.Map<String, Long> extras = (extraMetricsSupplier == null) ? java.util.Collections.emptyMap() : safeExtras(extraMetricsSupplier);
        emitThroughput(elapsed, entries, rows, cells, filteredRows, extras, log);
    }

    // Сохранение бинарной совместимости для тестов/старых вызовов
    void logThroughput(EntryCounters counters, Logger log) {
        logThroughput(counters, log, null);
    }

    private static java.util.Map<String, Long> safeExtras(java.util.function.Supplier<java.util.Map<String, Long>> s) {
        try {
            java.util.Map<String, Long> m = s.get();
            return (m == null) ? java.util.Collections.<String, Long>emptyMap() : m;
        } catch (RuntimeException e) {
            return java.util.Collections.emptyMap();
        }
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
        return elapsed >= THROUGHPUT_LOG_INTERVAL_NS && throughputWindowStart.compareAndSet(windowStart, now);
    }

    private void emitThroughput(long elapsedNs,
                                long entries,
                                long rows,
                                long cells,
                                long filteredRows,
                                java.util.Map<String, Long> extras,
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

        // Базовая строка throughput (совместима с тестами/документацией)
        log.info(
                "Скорость WAL: записей={}, строк={}, строк/с={}, ячеек={}, ячеек/с={}, отфильтровано_строк={}, интервал_мс={}",
                entries,
                rows,
                formatDecimal(rowsPerSec),
                cells,
                formatDecimal(cellsPerSec),
                filteredRows,
                TimeUnit.NANOSECONDS.toMillis(elapsedNs));

        // Дополнительные метрики — только в DEBUG, чтобы не шуметь в проде и не ломать парсинг
        if (log.isDebugEnabled() && extras != null && !extras.isEmpty()) {
            long ensureQueue = get(extras, "ensure.очередь.ожидает");
            long ensureBackoff = get(extras, "ensure.бэкофф.размер");
            long srFailures = get(extras, "sr.регистрация.ошибок");
            long cooldownSkipped = get(extras, "ensure.пропуски.из-за.паузы");
            // Локализованные подписи, значения берём из внутренних ключей метрик (совместимость JMX/тестов)
            log.debug(
                    "Скорость WAL (доп. метрики): очередь ensure={}, бэкофф ensure={}, ошибки регистрации SR={}, пропуски из-за cooldown={}",
                    ensureQueue, ensureBackoff, srFailures, cooldownSkipped);
        }
        // Периодические агрегированные сводки ensure/SR
        emitPeriodicSummaries(extras, log);
    }

    private static long get(java.util.Map<String, Long> m, String key) {
        if (m == null) return 0L;
        Long v = m.get(key);
        return v == null ? 0L : v;
    }

    // ==== Периодические сводки ====

    private void emitPeriodicSummaries(java.util.Map<String, Long> extras, Logger log) {
        if (extras == null || extras.isEmpty()) return;
        long now = System.nanoTime();
        // Сводка ensure (INFO)
    long targetEnsure = nextEnsureSummaryAt.get();
    boolean ensureDue = now >= targetEnsure && nextEnsureSummaryAt.compareAndSet(targetEnsure, now + ENSURE_SUMMARY_INTERVAL_NS);
    if (ensureDue && log.isInfoEnabled()) {
            long accepted = get(extras, "ensure.вызовов.принято");
            long rejected = get(extras, "ensure.вызовов.отклонено");
            long existsYes = get(extras, "существует.да");
            long existsNo = get(extras, "существует.нет");
            long existsUnknown = get(extras, "существует.неизвестно");
            long createdOk = get(extras, "создание.успех");
            long createdRace = get(extras, "создание.гонка");
            long createdFail = get(extras, "создание.ошибка");
            long backoffSize = get(extras, "ensure.бэкофф.размер");
            long queuePending = get(extras, "ensure.очередь.ожидает");
            log.info("Сводка ensure: принято={}, отклонено={}, существует[да/нет/неизв]={}/{}/{}, создание[ok/гонка/ош] ={}/{}/{}, бэкофф={}, очередь={}",
                    accepted, rejected, existsYes, existsNo, existsUnknown,
                    createdOk, createdRace, createdFail, backoffSize, queuePending);
        }
        // Сводка Schema Registry (DEBUG)
    long targetSr = nextSrSummaryAt.get();
    boolean srDue = now >= targetSr && nextSrSummaryAt.compareAndSet(targetSr, now + SR_SUMMARY_INTERVAL_NS);
    if (srDue && log.isDebugEnabled()) {
            long srOk = get(extras, "sr.регистрация.успехов");
            long srFail = get(extras, "sr.регистрация.ошибок");
            log.debug("Сводка SR: регистраций ок={}, ошибок={}", srOk, srFail);
        }
    }

    private static String formatDecimal(double value) {
        return String.format(Locale.ROOT, "%.1f", value);
    }

    static final class EntryCounters {
        int rowsSent;
        int cellsSent;
        int rowsFiltered;
        int cellsSeen;
        int maxRowCellsSent;
    }

    static final class EntrySummary {
        final long rowsSeen;
        final long rowsSent;
        final long rowsFiltered;
        final long cellsSeen;
        final int maxRowCellsSent;

        EntrySummary(long rowsSeen,
                     int rowsSent,
                     int rowsFiltered,
                     int cellsSeen,
                     int maxRowCellsSent) {
            this.rowsSeen = rowsSeen;
            this.rowsSent = rowsSent;
            this.rowsFiltered = rowsFiltered;
            this.cellsSeen = cellsSeen;
            this.maxRowCellsSent = maxRowCellsSent;
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
