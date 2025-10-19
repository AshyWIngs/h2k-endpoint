package kz.qazmarka.h2k.endpoint.processing;

import java.util.Objects;

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
        Objects.requireNonNull(metadata, "метаданные таблиц");
        return new WalObserverHub(WalDiagnostics.create(metadata));
    }

    /**
     * Создаёт концентратор наблюдателей с заранее подготовленными экземплярами.
     * Используется модульными тестами для контроля состояний.
     */
    static WalObserverHub forTest(WalDiagnostics diagnostics) {
        return new WalObserverHub(diagnostics == null ? WalDiagnostics.disabled() : diagnostics);
    }

    void observeRow(TableName table,
                    TableOptionsSnapshot tableOptions,
                    int rowKeyLength) {
        diagnostics.recordRow(table, tableOptions, rowKeyLength);
    }

    void finalizeEntry(TableName table,
                        WalCounterService.EntrySummary summary,
                        boolean filterActive,
                        CfFilterSnapshot cfSnapshot,
                        TableOptionsSnapshot tableOptions) {
        if (summary.rowsSeen <= 0L) {
            return;
        }
        diagnostics.recordEntry(table, summary, filterActive, cfSnapshot, tableOptions);
    }

    WalDiagnostics diagnosticsForTest() {
        return diagnostics;
    }
}
