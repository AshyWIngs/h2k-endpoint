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

    private final TableCapacityObserver capacityObserver;
    private final CfFilterObserver cfFilterObserver;
    private final TableOptionsObserver tableOptionsObserver;
    private final SaltUsageObserver saltObserver;

    private WalObserverHub(TableCapacityObserver capacityObserver,
                           CfFilterObserver cfFilterObserver,
                           TableOptionsObserver tableOptionsObserver,
                           SaltUsageObserver saltObserver) {
        this.capacityObserver = capacityObserver;
        this.cfFilterObserver = cfFilterObserver;
        this.tableOptionsObserver = tableOptionsObserver;
        this.saltObserver = saltObserver;
    }

    static WalObserverHub create(TableMetadataView metadata) {
        Objects.requireNonNull(metadata, "метаданные таблиц");
        if (metadata.isObserversEnabled()) {
            return new WalObserverHub(
                    TableCapacityObserver.create(metadata),
                    CfFilterObserver.create(),
                    TableOptionsObserver.create(),
                    SaltUsageObserver.create());
        }
        return new WalObserverHub(
                TableCapacityObserver.disabled(),
                CfFilterObserver.disabled(),
                TableOptionsObserver.disabled(),
                SaltUsageObserver.disabled());
    }

    /**
     * Создаёт концентратор наблюдателей с заранее подготовленными экземплярами.
     * Используется модульными тестами для контроля состояний.
     */
    static WalObserverHub forTest(TableCapacityObserver capacityObserver,
                                  CfFilterObserver cfFilterObserver,
                                  TableOptionsObserver tableOptionsObserver,
                                  SaltUsageObserver saltObserver) {
        return new WalObserverHub(capacityObserver, cfFilterObserver, tableOptionsObserver, saltObserver);
    }

    void observeRow(TableName table,
                    TableOptionsSnapshot tableOptions,
                    int rowKeyLength) {
        saltObserver.observeRow(table, tableOptions, rowKeyLength);
    }

    void finalizeEntry(TableName table,
                        WalCounterService.EntrySummary summary,
                        boolean filterActive,
                        CfFilterSnapshot cfSnapshot,
                        TableOptionsSnapshot tableOptions) {
        if (summary.rowsSeen <= 0L) {
            return;
        }
        int capacityCandidate = summary.maxRowCellsSent > 0
                ? summary.maxRowCellsSent
                : summary.maxRowCellsSeen;
        long rowsForCapacity = summary.rowsSent > 0L
                ? summary.rowsSent
                : summary.rowsSeen;
        if (capacityCandidate > 0) {
            capacityObserver.observe(table, capacityCandidate, rowsForCapacity);
        }
        cfFilterObserver.observe(table, summary.rowsSeen, summary.rowsFiltered, filterActive, cfSnapshot);
        tableOptionsObserver.observe(table, tableOptions, cfSnapshot);
    }

    /**
     * Возвращает наблюдатель ёмкости таблицы для нужд модульного тестирования.
     */
    TableCapacityObserver capacityObserverForTest() {
        return capacityObserver;
    }

    /**
     * Возвращает наблюдатель CF-фильтра для нужд модульного тестирования.
     */
    CfFilterObserver cfFilterObserverForTest() {
        return cfFilterObserver;
    }

    /**
     * Возвращает наблюдатель табличных опций для нужд модульного тестирования.
     */
    TableOptionsObserver tableOptionsObserverForTest() {
        return tableOptionsObserver;
    }

    /**
     * Возвращает наблюдатель использования соли для нужд модульного тестирования.
     */
    SaltUsageObserver saltObserverForTest() {
        return saltObserver;
    }
}
