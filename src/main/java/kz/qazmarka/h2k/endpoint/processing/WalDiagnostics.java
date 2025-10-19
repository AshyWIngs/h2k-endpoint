package kz.qazmarka.h2k.endpoint.processing;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.TableMetadataView;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.config.TableValueSource;

/**
 * Лёгкая диагностика горячего пути: отслеживает базовую статистику и логирует
 * рекомендационные предупреждения по необходимости.
 */
final class WalDiagnostics {

    private static final Logger LOG = LoggerFactory.getLogger(WalDiagnostics.class);
    private static final long CF_SAMPLE_THRESHOLD = 500L;
    private static final double CF_LOW_RATIO = 0.01d;
    private static final double CF_HIGH_RATIO = 0.95d;

    private final boolean enabled;
    private final TableMetadataView metadata;
    private final ConcurrentHashMap<TableName, TableStats> stats = new ConcurrentHashMap<>();

    private WalDiagnostics(TableMetadataView metadata, boolean enabled) {
        this.metadata = metadata;
        this.enabled = enabled;
    }

    static WalDiagnostics create(TableMetadataView metadata) {
        boolean enabled = metadata != null && metadata.isObserversEnabled();
        return new WalDiagnostics(metadata, enabled);
    }

    static WalDiagnostics disabled() {
        return new WalDiagnostics(null, false);
    }

    void recordRow(TableName table,
                   TableOptionsSnapshot tableOptions,
                   int rowKeyLength) {
        if (!enabled || table == null || tableOptions == null) {
            return;
        }
        int saltBytes = tableOptions.saltBytes();
        if (saltBytes <= 0 || rowKeyLength <= 0) {
            return;
        }
        TableStats statsForTable = stats.computeIfAbsent(table, t -> new TableStats());
        statsForTable.observeSalt(table, saltBytes, tableOptions.saltSource(), rowKeyLength);
    }

    void recordEntry(TableName table,
                     WalCounterService.EntrySummary summary,
                     boolean filterActive,
                     CfFilterSnapshot cfSnapshot,
                     TableOptionsSnapshot tableOptions) {
        if (!enabled || table == null || summary == null) {
            return;
        }
        TableStats statsForTable = stats.computeIfAbsent(table, t -> new TableStats());
        statsForTable.observeEntry(table, summary, filterActive, cfSnapshot, tableOptions, metadata);
    }

    Map<TableName, TableStatsSnapshot> snapshot() {
        if (!enabled) {
            return java.util.Collections.emptyMap();
        }
        Map<TableName, TableStatsSnapshot> copy = new java.util.HashMap<>();
        stats.forEach((table, state) -> copy.put(table, state.snapshot()));
        return copy;
    }

    static final class TableStatsSnapshot {
        private final long rowsSeen;
        private final long rowsFiltered;
        private final int maxRowCellsSent;
        private final int saltBytes;
        private final WarningFlags warnings;

        TableStatsSnapshot(long rowsSeen,
                           long rowsFiltered,
                           int maxRowCellsSent,
                           int saltBytes,
                           WarningFlags warnings) {
            this.rowsSeen = rowsSeen;
            this.rowsFiltered = rowsFiltered;
            this.maxRowCellsSent = maxRowCellsSent;
            this.saltBytes = saltBytes;
            this.warnings = warnings;
        }

        long rowsSeen() {
            return rowsSeen;
        }

        long rowsFiltered() {
            return rowsFiltered;
        }

        int maxRowCellsSent() {
            return maxRowCellsSent;
        }

        int saltBytes() {
            return saltBytes;
        }

        boolean saltWarned() {
            return warnings.saltWarned;
        }

        boolean capacityWarned() {
            return warnings.capacityWarned;
        }

        boolean cfLowWarned() {
            return warnings.cfLowWarned;
        }

        boolean cfHighWarned() {
            return warnings.cfHighWarned;
        }
    }

    private static final class WarningFlags {
        private final boolean saltWarned;
        private final boolean capacityWarned;
        private final boolean cfLowWarned;
        private final boolean cfHighWarned;

        WarningFlags(boolean saltWarned,
                     boolean capacityWarned,
                     boolean cfLowWarned,
                     boolean cfHighWarned) {
            this.saltWarned = saltWarned;
            this.capacityWarned = capacityWarned;
            this.cfLowWarned = cfLowWarned;
            this.cfHighWarned = cfHighWarned;
        }
    }

    private static final class TableStats {
        private long rowsSeen;
        private long rowsFiltered;
        private int maxRowCellsSent;
    private boolean cfLowWarned;
    private boolean cfHighWarned;
    private boolean capacityWarned;
    private boolean saltWarned;
    private int saltBytes;

        synchronized void observeSalt(TableName table,
                                      int newSaltBytes,
                                      TableValueSource source,
                                      int rowKeyLength) {
            this.saltBytes = newSaltBytes;
            TableValueSource effectiveSource = source == null ? TableValueSource.DEFAULT : source;
            if (rowKeyLength <= newSaltBytes && !saltWarned) {
                saltWarned = true;
        if (LOG.isWarnEnabled()) {
                    LOG.warn("Соль Phoenix таблицы {}: обнаружены строки длиной {} байт (saltBytes={}, источник: {}). Проверьте схему Avro.",
                table,
                rowKeyLength,
                            newSaltBytes,
                            label(effectiveSource));
        }
            }
        }

        synchronized void observeEntry(TableName table,
                                       WalCounterService.EntrySummary summary,
                                       boolean filterActive,
                                       CfFilterSnapshot cfSnapshot,
                                       TableOptionsSnapshot tableOptions,
                                       TableMetadataView metadata) {
            rowsSeen += summary.rowsSeen;
            rowsFiltered += summary.rowsFiltered;
            if (summary.maxRowCellsSent > maxRowCellsSent) {
                maxRowCellsSent = summary.maxRowCellsSent;
            }
            maybeWarnCapacity(table, tableOptions, metadata);
            maybeWarnCf(table, filterActive, cfSnapshot);
        }

        private void maybeWarnCapacity(TableName table,
                                       TableOptionsSnapshot tableOptions,
                                       TableMetadataView metadata) {
            if (capacityWarned) {
                return;
            }
            int configuredHint = 0;
            if (metadata != null) {
                configuredHint = metadata.getCapacityHintFor(table);
            }
            int actualHint = tableOptions == null ? configuredHint : tableOptions.capacityHint();
            if (actualHint <= 0 || maxRowCellsSent > actualHint) {
                capacityWarned = true;
                TableValueSource source = tableOptions == null ? TableValueSource.DEFAULT : tableOptions.capacitySource();
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Таблица {}: замечено {} полей в строке, подсказка capacityHint={} (источник: {}). Рекомендуется обновить Avro-схему.",
                            table,
                            maxRowCellsSent,
                            actualHint,
                            label(source));
                }
            }
        }

        private void maybeWarnCf(TableName table,
                                 boolean filterActive,
                                 CfFilterSnapshot cfSnapshot) {
            if (!filterActive || rowsSeen < CF_SAMPLE_THRESHOLD || rowsSeen <= 0L) {
                return;
            }
            double ratio = rowsFiltered <= 0L ? 0.0d : (double) rowsFiltered / (double) rowsSeen;
            if (ratio < CF_LOW_RATIO && !cfLowWarned) {
                cfLowWarned = true;
                if (LOG.isWarnEnabled()) {
                    LOG.warn("CF-фильтр таблицы {} практически не работает: обработано {} строк, отфильтровано {} ({}%). Проверьте список CF '{}'.",
                            table,
                            rowsSeen,
                            rowsFiltered,
                            formatPercent(ratio),
                            csv(cfSnapshot));
                }
                return;
            }
            if (ratio >= CF_HIGH_RATIO && !cfHighWarned) {
                cfHighWarned = true;
                if (LOG.isWarnEnabled()) {
                    LOG.warn("CF-фильтр таблицы {} отбрасывает почти всё: обработано {} строк, отфильтровано {} ({}%). Проверьте конфигурацию CF '{}'.",
                            table,
                            rowsSeen,
                            rowsFiltered,
                            formatPercent(ratio),
                            csv(cfSnapshot));
                }
            }
        }

        synchronized TableStatsSnapshot snapshot() {
            WarningFlags flags = new WarningFlags(saltWarned, capacityWarned, cfLowWarned, cfHighWarned);
            return new TableStatsSnapshot(
                    rowsSeen,
                    rowsFiltered,
                    maxRowCellsSent,
                    saltBytes,
                    flags);
        }

        private static String csv(CfFilterSnapshot snapshot) {
            if (snapshot == null) {
                return "";
            }
            String csv = snapshot.csv();
            return csv == null ? "" : csv;
        }

        private static String formatPercent(double ratio) {
            return String.format(java.util.Locale.ROOT, "%.2f", ratio * 100.0d);
        }

        private static String label(TableValueSource source) {
            return source == null ? "" : source.label();
        }
    }
}
