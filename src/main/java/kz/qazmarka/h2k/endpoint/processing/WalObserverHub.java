package kz.qazmarka.h2k.endpoint.processing;

import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.config.H2kConfig;

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

    static WalObserverHub create(H2kConfig config) {
        if (config.isObserversEnabled()) {
            return new WalObserverHub(
                    TableCapacityObserver.create(config),
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
                    H2kConfig.TableOptionsSnapshot tableOptions,
                    int rowKeyLength) {
        saltObserver.observeRow(table, tableOptions, rowKeyLength);
    }

    void finalizeEntry(TableName table,
                        WalCounterService.EntrySummary summary,
                        boolean filterActive,
                        H2kConfig.CfFilterSnapshot cfSnapshot,
                        H2kConfig.TableOptionsSnapshot tableOptions) {
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
}
