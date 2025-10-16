package kz.qazmarka.h2k.endpoint.processing;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.Marker;

/**
 * Проверяет агрегирование и логирование метрик WAL.
 */
class WalCounterServiceTest {

    @Test
    @DisplayName("completeEntry агрегирует счётчики и даёт корректный snapshot")
    void aggregatesCounters() {
        WalCounterService service = new WalCounterService();
        WalCounterService.EntryCounters counters = service.newEntryCounters();
        counters.rowsSent = 2;
        counters.cellsSent = 5;
        counters.rowsFiltered = 1;
        counters.cellsSeen = 6;
        counters.maxRowCellsSent = 5;
        counters.maxRowCellsSeen = 6;

        WalCounterService.EntrySummary summary = service.completeEntry(counters);

        assertEquals(3L, summary.rowsSeen);
        assertEquals(2L, summary.rowsSent);
        assertEquals(1L, summary.rowsFiltered);
        assertEquals(6L, summary.cellsSeen);
        WalCounterService.MetricsSnapshot snapshot = service.snapshot();
        assertEquals(1L, snapshot.entries);
        assertEquals(3L, snapshot.rows);
        assertEquals(6L, snapshot.cells);
        assertEquals(1L, snapshot.filteredRows);
    }

    @Test
    @DisplayName("logThroughput выводит метрику и очищает окно после порога времени")
    void logsThroughputAfterInterval() {
        WalCounterService service = new WalCounterService();
        WalCounterService.EntryCounters counters = service.newEntryCounters();
        counters.rowsSent = 2;
        counters.cellsSent = 4;
        counters.rowsFiltered = 1;

        CapturingLogger logger = new CapturingLogger();
        forceElapsedWindow(service);

        service.logThroughput(counters, logger);

        assertEquals(1, logger.infoCount);
        assertNotNull(logger.lastFormat);
        assertEquals("Скорость WAL: записей={}, строк={}, строк/с={}, ячеек={}, ячеек/с={}, отфильтровано_строк={}, интервал_мс={}",
                logger.lastFormat);

        WalCounterService.EntryCounters second = service.newEntryCounters();
        service.logThroughput(second, logger);
        assertEquals(1, logger.infoCount, "повторный вызов без данных не должен логировать");
    }

    @Test
    @DisplayName("logThroughput не пишет лог до истечения окна")
    void doesNotLogBeforeInterval() {
        WalCounterService service = new WalCounterService();
        WalCounterService.EntryCounters counters = service.newEntryCounters();
        counters.rowsSent = 1;
        counters.cellsSent = 1;

        CapturingLogger logger = new CapturingLogger();

        service.logThroughput(counters, logger);

        assertEquals(0, logger.infoCount, "без достаточного интервала лог не должен появляться");
    }

    @Test
    @DisplayName("logThroughput пропускает пустое окно даже после форсированного сдвига")
    void skipsEmptyWindowAfterElapsedInterval() {
        WalCounterService service = new WalCounterService();
        WalCounterService.EntryCounters counters = service.newEntryCounters();

        CapturingLogger logger = new CapturingLogger();
        forceElapsedWindow(service);

        service.logThroughput(counters, logger);

        assertEquals(0, logger.infoCount, "пустое окно не должно приводить к логированию");
    }

    private static void forceElapsedWindow(WalCounterService service) {
        long offset = TimeUnit.SECONDS.toNanos(10);
        service.overrideThroughputWindowStart(System.nanoTime() - offset);
    }

    private static final class CapturingLogger implements Logger {
        String lastFormat;
        int infoCount;

        @Override
        public String getName() {
            return "test";
        }

        @Override
        public boolean isTraceEnabled() {
            return false;
        }

        @Override
        public void trace(String msg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void trace(String format, Object arg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void trace(String format, Object arg1, Object arg2) {
            /* не используется в тестовом логере */
        }

        @Override
        public void trace(String format, Object... arguments) {
            /* не используется в тестовом логере */
        }

        @Override
        public void trace(String msg, Throwable t) {
            /* не используется в тестовом логере */
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return false;
        }

        @Override
        public void trace(Marker marker, String msg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            /* не используется в тестовом логере */
        }

        @Override
        public void trace(Marker marker, String format, Object... arguments) {
            /* не используется в тестовом логере */
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            /* не используется в тестовом логере */
        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public void debug(String msg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void debug(String format, Object arg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void debug(String format, Object arg1, Object arg2) {
            /* не используется в тестовом логере */
        }

        @Override
        public void debug(String format, Object... arguments) {
            /* не используется в тестовом логере */
        }

        @Override
        public void debug(String msg, Throwable t) {
            /* не используется в тестовом логере */
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return false;
        }

        @Override
        public void debug(Marker marker, String msg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            /* не используется в тестовом логере */
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            /* не используется в тестовом логере */
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            /* не используется в тестовом логере */
        }

        @Override
        public boolean isInfoEnabled() {
            return true;
        }

        @Override
        public void info(String msg) {
            infoCount++;
            lastFormat = msg;
        }

        @Override
        public void info(String format, Object arg) {
            info(format, new Object[] { arg });
        }

        @Override
        public void info(String format, Object arg1, Object arg2) {
            info(format, new Object[] { arg1, arg2 });
        }

        @Override
        public void info(String format, Object... arguments) {
            infoCount++;
            lastFormat = format;
        }

        @Override
        public void info(String msg, Throwable t) {
            infoCount++;
            lastFormat = msg;
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return true;
        }

        @Override
        public void info(Marker marker, String msg) {
            info(msg);
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            info(format, arg);
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            info(format, arg1, arg2);
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            info(format, arguments);
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            info(msg, t);
        }

        @Override
        public boolean isWarnEnabled() {
            return false;
        }

        @Override
        public void warn(String msg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void warn(String format, Object arg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void warn(String format, Object arg1, Object arg2) {
            /* не используется в тестовом логере */
        }

        @Override
        public void warn(String format, Object... arguments) {
            /* не используется в тестовом логере */
        }

        @Override
        public void warn(String msg, Throwable t) {
            /* не используется в тестовом логере */
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return false;
        }

        @Override
        public void warn(Marker marker, String msg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            /* не используется в тестовом логере */
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            /* не используется в тестовом логере */
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            /* не используется в тестовом логере */
        }

        @Override
        public boolean isErrorEnabled() {
            return false;
        }

        @Override
        public void error(String msg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void error(String format, Object arg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void error(String format, Object arg1, Object arg2) {
            /* не используется в тестовом логере */
        }

        @Override
        public void error(String format, Object... arguments) {
            /* не используется в тестовом логере */
        }

        @Override
        public void error(String msg, Throwable t) {
            /* не используется в тестовом логере */
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return false;
        }

        @Override
        public void error(Marker marker, String msg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            /* не используется в тестовом логере */
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            /* не используется в тестовом логере */
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            /* не используется в тестовом логере */
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            /* не используется в тестовом логере */
        }
    }
}
