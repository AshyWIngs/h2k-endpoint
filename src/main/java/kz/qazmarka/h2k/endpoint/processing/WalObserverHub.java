package kz.qazmarka.h2k.endpoint.processing;

import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.TableMetadataView;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;

/**
 * Инкапсулирует все наблюдатели WAL и обеспечивает единый интерфейс для строк и записей.
 */
final class WalObserverHub {

    private final WalDiagnostics diagnostics;

    private WalObserverHub(WalDiagnostics diagnostics) {
        this.diagnostics = diagnostics;
    }

    static WalObserverHub create(TableMetadataView metadata) {
        if (metadata == null || !metadata.isObserversEnabled()) {
            return new WalObserverHub(null);
        }
        return new WalObserverHub(WalDiagnostics.create(metadata));
    }

    void observeRow(TableName table,
                    TableOptionsSnapshot tableOptions,
                    int rowKeyLength) {
        if (diagnostics == null) {
            return;
        }
        diagnostics.recordRow(table, tableOptions, rowKeyLength);
    }

    void finalizeEntry(TableName table,
                        WalCounterService.EntrySummary summary,
                        boolean filterActive,
                        CfFilterSnapshot cfSnapshot,
                        TableOptionsSnapshot tableOptions) {
        if (diagnostics == null) {
            return;
        }
        if (summary.rowsSeen <= 0L) {
            return;
        }
        diagnostics.recordEntry(table, summary, filterActive, cfSnapshot, tableOptions);
    }
}
