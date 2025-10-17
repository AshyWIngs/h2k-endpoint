package kz.qazmarka.h2k.endpoint.processing;

import java.nio.file.Paths;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.Marker;

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;

/**
 * Проверяет делегирование событий из {@link WalObserverHub} к наблюдателям.
 */
class WalObserverHubTest {

    private static final TableName TABLE = TableName.valueOf("NS:TEST");

    @Test
    @DisplayName("finalizeEntry делегирует накопленные метрики всем наблюдателям")
    void finalizeEntryUpdatesObservers() {
        ObserverScenario scenario = createScenario(false, false);
        WalObserverHub hub = scenario.hub;

    TableOptionsSnapshot tableOptions = scenario.tableOptions;
    CfFilterSnapshot cfSnapshot = scenario.cfSnapshot;
        WalCounterService.EntrySummary summary = new WalCounterService.EntrySummary(10L, 6, 4, 12, 5, 7);

        CapturingLogger logger = new CapturingLogger();
        AutoCloseable closeLogger = TableOptionsObserver.withLoggerForTest(logger);
        try {
            hub.finalizeEntry(TABLE, summary, true, cfSnapshot, tableOptions);
        } finally {
            closeQuietly(closeLogger);
        }

    Map<TableName, TableCapacityObserver.StatsSnapshot> capacitySnapshot = scenario.capacityObserver.snapshot();
        TableCapacityObserver.StatsSnapshot capacityStats = capacitySnapshot.get(TABLE);
        assertNotNull(capacityStats, "Ожидается запись статистики по ёмкости");
        assertEquals(5L, capacityStats.maxFields());
        assertEquals(6L, capacityStats.rowsObserved());

    Map<TableName, CfFilterObserver.Stats> cfStatsMap = scenario.cfObserver.snapshot();
        CfFilterObserver.Stats cfStats = cfStatsMap.get(TABLE);
        assertNotNull(cfStats, "Ожидается накопление статистики CF-фильтра");
        assertEquals(10L, cfStats.rowsTotal.sum());
        assertEquals(4L, cfStats.rowsFiltered.sum());

        assertEquals(1, logger.infoCount, "Первое наблюдение должно логироваться");
    }

    @Test
    @DisplayName("finalizeEntry пропускает наблюдателей, когда строки отсутствуют")
    void finalizeEntrySkipsWhenNoRows() {
        ObserverScenario scenario = createScenario(false, false);
        WalObserverHub hub = scenario.hub;

    TableOptionsSnapshot tableOptions = scenario.tableOptions;
    CfFilterSnapshot cfSnapshot = scenario.cfSnapshot;
        WalCounterService.EntrySummary summary = new WalCounterService.EntrySummary(0L, 0, 0, 0, 0, 0);

        CapturingLogger logger = new CapturingLogger();
        AutoCloseable closeLogger = TableOptionsObserver.withLoggerForTest(logger);
        try {
            hub.finalizeEntry(TABLE, summary, true, cfSnapshot, tableOptions);
        } finally {
            closeQuietly(closeLogger);
        }

        assertTrue(scenario.capacityObserver.snapshot().isEmpty(), "Статистика не должна накапливаться");
        assertTrue(scenario.cfObserver.snapshot().isEmpty(), "CF-фильтр не должен обновляться");
        assertEquals(0, logger.infoCount, "Логирование не ожидается");
    }

    @Test
    @DisplayName("observeRow направляет длину rowkey в наблюдатель соли")
    void observeRowFeedsSaltObserver() {
        ObserverScenario scenario = createScenario(true, false);

    scenario.hub.observeRow(TABLE, scenario.tableOptions, 12);

        assertEquals(1, scenario.saltObserver.observedTablesCountForTest(), "Должна появиться запись о наблюдении соли");
    }

    @Test
    @DisplayName("create возвращает заглушки, когда наблюдатели выключены конфигурацией")
    void createReturnsDisabledObserversWhenConfigDisables() {
        H2kConfig config = configWithObservers(false);
        WalObserverHub hub = WalObserverHub.create(config);

        assertFalse(hub.capacityObserverForTest().isEnabledForTest(), "TableCapacityObserver обязан быть отключён");
        assertFalse(hub.cfFilterObserverForTest().isEnabledForTest(), "CfFilterObserver обязан быть отключён");
        assertFalse(hub.tableOptionsObserverForTest().isEnabledForTest(), "TableOptionsObserver обязан быть отключён");
        assertFalse(hub.saltObserverForTest().isEnabledForTest(), "SaltUsageObserver обязан быть отключён");
    }

    @Test
    @DisplayName("create с отключёнными наблюдателями игнорирует события finalizeEntry и observeRow")
    void createDisabledObserversIgnoreEvents() throws Exception {
        H2kConfig config = configWithObservers(false);
        WalObserverHub hub = WalObserverHub.create(config);

    TableOptionsSnapshot tableOptions = config.describeTableOptions(TABLE);
    CfFilterSnapshot cfSnapshot = config.describeCfFilter(TABLE);

        CapturingLogger capacityLogger = new CapturingLogger();
        CapturingLogger saltLogger = new CapturingLogger();
        CapturingLogger optionsLogger = new CapturingLogger();

        try (AutoCloseable capacityHook = TableCapacityObserver.withLoggerForTest(capacityLogger);
             AutoCloseable saltHook = SaltUsageObserver.withLoggerForTest(saltLogger);
             AutoCloseable optionsHook = TableOptionsObserver.withLoggerForTest(optionsLogger)) {

            assertNotNull(capacityHook, "Хук ёмкости должен создаваться в тестовом режиме");
            assertNotNull(saltHook, "Хук проверки соли должен создаваться в тестовом режиме");
            assertNotNull(optionsHook, "Хук опций таблицы должен создаваться в тестовом режиме");

            WalCounterService.EntrySummary summary = new WalCounterService.EntrySummary(
                    320L,    // rowsSeen
                    0,       // rowsSent
                    320,     // rowsFiltered
                    1280,    // cellsSeen
                    0,       // maxRowCellsSent
                    16);     // maxRowCellsSeen

            hub.finalizeEntry(TABLE, summary, true, cfSnapshot, tableOptions);
            hub.observeRow(TABLE, tableOptions, 1); // длина меньше соли → должна была быть варнинговая запись
        }

        assertEquals(0, capacityLogger.warnCount, "Отключённый наблюдатель ёмкости не должен логировать предупреждения");
        assertEquals(0, optionsLogger.infoCount, "Отключённый наблюдатель опций не должен логировать изменения");
        assertEquals(0, saltLogger.warnCount, "Отключённый наблюдатель соли не должен логировать предупреждения");
    }

    @Test
    @DisplayName("finalizeEntry использует метрики rowsSeen, когда отправленных строк нет")
    void finalizeEntryUsesSeenMetricsWhenSentZero() {
        ObserverScenario scenario = createScenario(false, true);
        WalObserverHub hub = scenario.hub;

    TableOptionsSnapshot tableOptions = scenario.tableOptions;
    CfFilterSnapshot cfSnapshot = scenario.cfSnapshot;

        WalCounterService.EntrySummary summary = new WalCounterService.EntrySummary(
                5L,   // rowsSeen
                0,    // rowsSent
                5,    // rowsFiltered
                15,   // cellsSeen
                0,    // maxRowCellsSent
                3);   // maxRowCellsSeen

        CapturingLogger optionsLogger = new CapturingLogger();
        AutoCloseable loggerHook = TableOptionsObserver.withLoggerForTest(optionsLogger);
        try {
            hub.finalizeEntry(TABLE, summary, true, cfSnapshot, tableOptions);
        } finally {
            closeQuietly(loggerHook);
        }

        Map<TableName, TableCapacityObserver.StatsSnapshot> capacitySnapshot = scenario.capacityObserver.snapshot();
        TableCapacityObserver.StatsSnapshot stats = capacitySnapshot.get(TABLE);
        assertNotNull(stats, "Ожидается накопление статистики ёмкости");
        assertEquals(3L, stats.maxFields(), "Максимум полей должен опираться на maxRowCellsSeen");
        assertEquals(5L, stats.rowsObserved(), "Количество строк должно опираться на rowsSeen");

        CfFilterObserver.Stats cfStats = scenario.cfObserver.snapshot().get(TABLE);
        assertNotNull(cfStats, "Должна накопиться статистика CF-фильтра");
        assertEquals(5L, cfStats.rowsTotal.sum(), "rowsSeen должны учитываться даже без отправленных строк");
        assertEquals(5L, cfStats.rowsFiltered.sum(), "rowsFiltered должны сохраняться");

        assertEquals(1, optionsLogger.infoCount, "Первое наблюдение опций таблицы должно логироваться");
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
        return buildConfig(null);
    }

    private static H2kConfig configWithObservers(boolean enabled) {
        return buildConfig(enabled);
    }

    private static ObserverScenario createScenario(boolean disableTableOptions, boolean disableSalt) {
        H2kConfig config = config();
        TableCapacityObserver capacityObserver = TableCapacityObserver.create(config);
        CfFilterObserver cfObserver = CfFilterObserver.create();
        SaltUsageObserver saltObserver = disableSalt ? SaltUsageObserver.disabled() : SaltUsageObserver.create();
        WalObserverHub hub = WalObserverHub.forTest(capacityObserver, cfObserver,
                disableTableOptions ? TableOptionsObserver.disabled() : TableOptionsObserver.create(),
                saltObserver);
        return new ObserverScenario(config, hub, capacityObserver, cfObserver, saltObserver);
    }

    private static H2kConfig buildConfig(Boolean observersEnabled) {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.sr.urls", "http://mock");
        cfg.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        if (observersEnabled != null) {
            cfg.set(H2kConfig.Keys.OBSERVERS_ENABLED, observersEnabled.toString());
        }
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

    /**
     * Содержит сконфигурированный набор наблюдателей для тестового сценария.
     */
    private static final class ObserverScenario {
        final WalObserverHub hub;
        final TableCapacityObserver capacityObserver;
        final CfFilterObserver cfObserver;
        final SaltUsageObserver saltObserver;
    final TableOptionsSnapshot tableOptions;
    final CfFilterSnapshot cfSnapshot;

        ObserverScenario(H2kConfig config,
                         WalObserverHub hub,
                         TableCapacityObserver capacityObserver,
                         CfFilterObserver cfObserver,
                         SaltUsageObserver saltObserver) {
            this.hub = hub;
            this.capacityObserver = capacityObserver;
            this.cfObserver = cfObserver;
            this.saltObserver = saltObserver;
            this.tableOptions = config.describeTableOptions(TABLE);
            this.cfSnapshot = config.describeCfFilter(TABLE);
        }
    }

    private static final class CapturingLogger implements Logger {
        int infoCount;
        int warnCount;

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
            return true;
        }

        @Override
        public void warn(String msg) {
            warnCount++;
        }

        @Override
        public void warn(String format, Object arg) {
            warnCount++;
        }

        @Override
        public void warn(String format, Object arg1, Object arg2) {
            warnCount++;
        }

        @Override
        public void warn(String format, Object... arguments) {
            warnCount++;
        }

        @Override
        public void warn(String msg, Throwable t) {
            warnCount++;
        }

        @Override
        public boolean isWarnEnabled(Marker marker) {
            return true;
        }

        @Override
        public void warn(Marker marker, String msg) {
            warnCount++;
        }

        @Override
        public void warn(Marker marker, String format, Object arg) {
            warnCount++;
        }

        @Override
        public void warn(Marker marker, String format, Object arg1, Object arg2) {
            warnCount++;
        }

        @Override
        public void warn(Marker marker, String format, Object... arguments) {
            warnCount++;
        }

        @Override
        public void warn(Marker marker, String msg, Throwable t) {
            warnCount++;
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
