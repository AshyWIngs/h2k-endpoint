package kz.qazmarka.h2k.endpoint.processing;

import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.TableMetadataView;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.config.TableValueSource;

/**
 * Проверки потокобезопасного диагностического блока WAL.
 */
class WalDiagnosticsTest {

    private static final TableOptionsSnapshot OPTIONS_WITH_SALT =
            new TableOptionsSnapshot(4, TableValueSource.AVRO, 6, TableValueSource.AVRO, null);

    @Test
    @DisplayName("Salt usage: короткие rowkey приводят к предупреждению и отражаются в снимке")
    void saltWarningReflectedInSnapshot() {
        WalDiagnostics diagnostics = WalDiagnostics.create(new DummyMetadata());
        TableName table = TableName.valueOf("ns", "salt");

        diagnostics.recordRow(table, OPTIONS_WITH_SALT, 3);

        Map<TableName, WalDiagnostics.TableStatsSnapshot> snapshot = diagnostics.snapshot();
        WalDiagnostics.TableStatsSnapshot stats = snapshot.get(table);
    assertTrue(stats.saltWarned(), "Флаг предупреждения по соли обязан выставляться");
    assertEquals(4, stats.saltBytes(), "Снимок должен сохранять текущий saltBytes");
    }

    @Test
    @DisplayName("Превышение фактического числа колонок инициирует рекомендацию по capacityHint")
    void capacityHintRecommendation() {
        WalDiagnostics diagnostics = WalDiagnostics.create(new DummyMetadata());
        TableName table = TableName.valueOf("ns", "capacity");
    WalCounterService.EntrySummary summary =
        new WalCounterService.EntrySummary(10, 10, 0, 20, 12);

        diagnostics.recordEntry(table, summary, false, null, OPTIONS_WITH_SALT);

        WalDiagnostics.TableStatsSnapshot stats = diagnostics.snapshot().get(table);
    assertTrue(stats.capacityWarned(), "Должно быть зафиксировано предупреждение по capacityHint");
    assertEquals(12, stats.maxRowCellsSent(), "Снимок сохраняет максимум отправленных ячеек");
    }

    @Test
    @DisplayName("CF-фильтр классифицирует низкую и высокую эффективность")
    void cfEffectivenessFlags() {
        WalDiagnostics diagnostics = WalDiagnostics.create(new DummyMetadata());
        TableName lowTable = TableName.valueOf("ns", "cf-low");

        // низкая эффективность (почти нет фильтрации)
        for (int i = 0; i < 20; i++) {
        WalCounterService.EntrySummary summary =
            new WalCounterService.EntrySummary(50, 50, 0, 100, 5);
            diagnostics.recordEntry(lowTable, summary, true, null, OPTIONS_WITH_SALT);
        }
        WalDiagnostics.TableStatsSnapshot low = diagnostics.snapshot().get(lowTable);
    assertTrue(low.cfLowWarned(), "Нужно предупредить о низкой эффективности CF");

        TableName highTable = TableName.valueOf("ns", "cf-high");
        for (int i = 0; i < 20; i++) {
        WalCounterService.EntrySummary summary =
            new WalCounterService.EntrySummary(50, 2, 48, 100, 5);
            diagnostics.recordEntry(highTable, summary, true, null, OPTIONS_WITH_SALT);
        }
        WalDiagnostics.TableStatsSnapshot high = diagnostics.snapshot().get(highTable);
    assertTrue(high.cfHighWarned(), "Нужно предупредить о слишком агрессивном фильтре");
    }

    @Test
    @DisplayName("Снимок включает базовые счётчики строк")
    void snapshotKeepsCounters() {
        WalDiagnostics diagnostics = WalDiagnostics.create(new DummyMetadata());
        TableName table = TableName.valueOf("ns", "counter");
    WalCounterService.EntrySummary summary =
        new WalCounterService.EntrySummary(20, 18, 2, 40, 6);

        diagnostics.recordEntry(table, summary, true, null, OPTIONS_WITH_SALT);

        WalDiagnostics.TableStatsSnapshot stats = diagnostics.snapshot().get(table);
    assertEquals(20L, stats.rowsSeen(), "Количество строк должно аккумулироваться");
    assertEquals(2L, stats.rowsFiltered(), "Фильтрованные строки терять нельзя");
    }

    private static final class DummyMetadata implements TableMetadataView {

        @Override
        public TableOptionsSnapshot describeTableOptions(TableName table) {
            return OPTIONS_WITH_SALT;
        }

        @Override
        public kz.qazmarka.h2k.config.CfFilterSnapshot describeCfFilter(TableName table) {
            return null;
        }

        @Override
        public int getCapacityHintFor(TableName table) {
            return 4;
        }

        @Override
        public boolean isObserversEnabled() {
            return true;
        }
    }
}
