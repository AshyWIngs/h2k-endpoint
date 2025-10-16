package kz.qazmarka.h2k.endpoint.processing;

import java.nio.file.Paths;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.Marker;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;

/**
 * Проверяет делегирование событий из {@link WalObserverHub} к наблюдателям.
 */
class WalObserverHubTest {

    private static final TableName TABLE = TableName.valueOf("NS:TEST");

    @Test
    @DisplayName("finalizeEntry делегирует накопленные метрики всем наблюдателям")
    void finalizeEntryUpdatesObservers() {
        H2kConfig config = config();
        TableCapacityObserver capacityObserver = TableCapacityObserver.create(config);
        CfFilterObserver cfObserver = CfFilterObserver.create();
        TableOptionsObserver tableOptionsObserver = TableOptionsObserver.create();
        SaltUsageObserver saltObserver = SaltUsageObserver.create();
        WalObserverHub hub = WalObserverHub.forTest(capacityObserver, cfObserver, tableOptionsObserver, saltObserver);

        H2kConfig.TableOptionsSnapshot tableOptions = config.describeTableOptions(TABLE);
        H2kConfig.CfFilterSnapshot cfSnapshot = config.describeCfFilter(TABLE);
        WalCounterService.EntrySummary summary = new WalCounterService.EntrySummary(10L, 6, 4, 12, 5, 7);

        CapturingLogger logger = new CapturingLogger();
        AutoCloseable closeLogger = TableOptionsObserver.withLoggerForTest(logger);
        try {
            hub.finalizeEntry(TABLE, summary, true, cfSnapshot, tableOptions);
        } finally {
            closeQuietly(closeLogger);
        }

        Map<TableName, TableCapacityObserver.StatsSnapshot> capacitySnapshot = capacityObserver.snapshot();
        TableCapacityObserver.StatsSnapshot capacityStats = capacitySnapshot.get(TABLE);
        assertNotNull(capacityStats, "Ожидается запись статистики по ёмкости");
        assertEquals(5L, capacityStats.maxFields());
        assertEquals(6L, capacityStats.rowsObserved());

        Map<TableName, CfFilterObserver.Stats> cfStatsMap = cfObserver.snapshot();
        CfFilterObserver.Stats cfStats = cfStatsMap.get(TABLE);
        assertNotNull(cfStats, "Ожидается накопление статистики CF-фильтра");
        assertEquals(10L, cfStats.rowsTotal.sum());
        assertEquals(4L, cfStats.rowsFiltered.sum());

        assertEquals(1, logger.infoCount, "Первое наблюдение должно логироваться");
    }

    @Test
    @DisplayName("finalizeEntry пропускает наблюдателей, когда строки отсутствуют")
    void finalizeEntrySkipsWhenNoRows() {
        H2kConfig config = config();
        TableCapacityObserver capacityObserver = TableCapacityObserver.create(config);
        CfFilterObserver cfObserver = CfFilterObserver.create();
        TableOptionsObserver tableOptionsObserver = TableOptionsObserver.create();
        SaltUsageObserver saltObserver = SaltUsageObserver.create();
        WalObserverHub hub = WalObserverHub.forTest(capacityObserver, cfObserver, tableOptionsObserver, saltObserver);

        H2kConfig.TableOptionsSnapshot tableOptions = config.describeTableOptions(TABLE);
        H2kConfig.CfFilterSnapshot cfSnapshot = config.describeCfFilter(TABLE);
        WalCounterService.EntrySummary summary = new WalCounterService.EntrySummary(0L, 0, 0, 0, 0, 0);

        CapturingLogger logger = new CapturingLogger();
        AutoCloseable closeLogger = TableOptionsObserver.withLoggerForTest(logger);
        try {
            hub.finalizeEntry(TABLE, summary, true, cfSnapshot, tableOptions);
        } finally {
            closeQuietly(closeLogger);
        }

        assertTrue(capacityObserver.snapshot().isEmpty(), "Статистика не должна накапливаться");
        assertTrue(cfObserver.snapshot().isEmpty(), "CF-фильтр не должен обновляться");
        assertEquals(0, logger.infoCount, "Логирование не ожидается");
    }

    @Test
    @DisplayName("observeRow направляет длину rowkey в наблюдатель соли")
    void observeRowFeedsSaltObserver() {
        H2kConfig config = config();
        TableCapacityObserver capacityObserver = TableCapacityObserver.create(config);
        CfFilterObserver cfObserver = CfFilterObserver.create();
        TableOptionsObserver tableOptionsObserver = TableOptionsObserver.disabled();
        SaltUsageObserver saltObserver = SaltUsageObserver.create();
        WalObserverHub hub = WalObserverHub.forTest(capacityObserver, cfObserver, tableOptionsObserver, saltObserver);

        H2kConfig.TableOptionsSnapshot tableOptions = config.describeTableOptions(TABLE);

        hub.observeRow(TABLE, tableOptions, 12);

        assertEquals(1, saltObserver.observedTablesCountForTest(), "Должна появиться запись о наблюдении соли");
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            fail("Не удалось закрыть ресурс тестового логгера: " + e.getMessage());
        }
    }

    private static H2kConfig config() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.sr.urls", "http://mock");
        cfg.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        PhoenixTableMetadataProvider provider = new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) { return 2; }

            @Override
            public Integer capacityHint(TableName table) { return 4; }

            @Override
            public String[] columnFamilies(TableName table) { return new String[] { "d" }; }

            @Override
            public String[] primaryKeyColumns(TableName table) { return new String[] { "id" }; }
        };
        return H2kConfig.from(cfg, "mock:9092", provider);
    }

    private static final class CapturingLogger implements Logger {
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
            /* не используется в тестовой реализации */
        }

        @Override
        public void trace(String format, Object arg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void trace(String format, Object arg1, Object arg2) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void trace(String format, Object... arguments) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void trace(String msg, Throwable t) {
            /* не используется в тестовой реализации */
        }

        @Override
        public boolean isTraceEnabled(Marker marker) {
            return false;
        }

        @Override
        public void trace(Marker marker, String msg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void trace(Marker marker, String format, Object arg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void trace(Marker marker, String format, Object arg1, Object arg2) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void trace(Marker marker, String format, Object... argArray) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void trace(Marker marker, String msg, Throwable t) {
            /* не используется в тестовой реализации */
        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public void debug(String msg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void debug(String format, Object arg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void debug(String format, Object arg1, Object arg2) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void debug(String format, Object... arguments) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void debug(String msg, Throwable t) {
            /* не используется в тестовой реализации */
        }

        @Override
        public boolean isDebugEnabled(Marker marker) {
            return false;
        }

        @Override
        public void debug(Marker marker, String msg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void debug(Marker marker, String format, Object arg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void debug(Marker marker, String format, Object arg1, Object arg2) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void debug(Marker marker, String format, Object... arguments) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void debug(Marker marker, String msg, Throwable t) {
            /* не используется в тестовой реализации */
        }

        @Override
        public boolean isInfoEnabled() {
            return true;
        }

        @Override
        public void info(String msg) {
            infoCount++;
        }

        @Override
        public void info(String format, Object arg) {
            infoCount++;
        }

        @Override
        public void info(String format, Object arg1, Object arg2) {
            infoCount++;
        }

        @Override
        public void info(String format, Object... arguments) {
            infoCount++;
        }

        @Override
        public void info(String msg, Throwable t) {
            infoCount++;
        }

        @Override
        public boolean isInfoEnabled(Marker marker) {
            return true;
        }

        @Override
        public void info(Marker marker, String msg) {
            infoCount++;
        }

        @Override
        public void info(Marker marker, String format, Object arg) {
            infoCount++;
        }

        @Override
        public void info(Marker marker, String format, Object arg1, Object arg2) {
            infoCount++;
        }

        @Override
        public void info(Marker marker, String format, Object... arguments) {
            infoCount++;
        }

        @Override
        public void info(Marker marker, String msg, Throwable t) {
            infoCount++;
        }

        @Override
        public boolean isWarnEnabled() {
            return false;
        }

        @Override
        public void warn(String msg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void warn(String format, Object arg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void warn(String format, Object arg1, Object arg2) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void warn(String format, Object... arguments) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void warn(String msg, Throwable t) {
            /* не используется в тестовой реализации */
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return false;
        }

        @Override
        public void warn(Marker marker, String msg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            /* не используется в тестовой реализации */
        }

        @Override
        public boolean isErrorEnabled() {
            return false;
        }

        @Override
        public void error(String msg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void error(String format, Object arg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void error(String format, Object arg1, Object arg2) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void error(String format, Object... arguments) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void error(String msg, Throwable t) {
            /* не используется в тестовой реализации */
        }

        @Override
        public boolean isErrorEnabled(Marker marker) {
            return false;
        }

        @Override
        public void error(Marker marker, String msg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void error(Marker marker, String format, Object arg) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void error(Marker marker, String format, Object arg1, Object arg2) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void error(Marker marker, String format, Object... arguments) {
            /* не используется в тестовой реализации */
        }

        @Override
        public void error(Marker marker, String msg, Throwable t) {
            /* не используется в тестовой реализации */
        }
    }
}
