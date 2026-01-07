package kz.qazmarka.h2k.endpoint.processing;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Обрабатывает одну строку WAL: проверяет CF-фильтр, обновляет счётчики и отправляет payload.
 */
final class WalRowProcessor {

    private final WalRowDispatcher dispatcher;
    private final WalObserverHub observers;

    WalRowProcessor(WalRowDispatcher dispatcher, WalObserverHub observers) {
        this.dispatcher = dispatcher;
        this.observers = observers;
    }

    void processRow(RowKeySlice.Mutable rowKey,
                    List<Cell> rowCells,
                    RowContext context)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (rowKey == null || rowCells == null || rowCells.isEmpty()) {
            return;
        }

        observers.observeRow(context.table, context.tableOptions, rowKey.getLength());

        WalCounterService.EntryCounters counters = context.counters;
        int cellsInRow = rowCells.size();
        counters.cellsSeen += cellsInRow;

        if (context.filterActive && !context.cfFilter.allows(rowCells)) {
            counters.rowsFiltered++;
            return;
        }

        dispatcher.dispatch(context.topic, context.table, context.walMeta, rowKey, rowCells, context.sender);
        counters.rowsSent++;
        counters.cellsSent += cellsInRow;
        if (cellsInRow > counters.maxRowCellsSent) {
            counters.maxRowCellsSent = cellsInRow;
        }
    }

    static final class RowContext {
        final String topic;
        final TableName table;
        final WalMeta walMeta;
        final BatchSender sender;
        final TableOptionsSnapshot tableOptions;
        final WalCfFilterCache cfFilter;
        final boolean filterActive;
        final WalCounterService.EntryCounters counters;

        @SuppressWarnings("java:S107")
        RowContext(String topic,
                   TableName table,
                   WalMeta walMeta,
                   BatchSender sender,
                   TableOptionsSnapshot tableOptions,
                   FilterState filterState,
                   WalCounterService.EntryCounters counters) {
            this.topic = topic;
            this.table = table;
            this.walMeta = walMeta;
            this.sender = sender;
            this.tableOptions = tableOptions;
            this.cfFilter = filterState.cfFilter;
            this.filterActive = filterState.filterActive;
            this.counters = counters;
        }
    }

    static final class FilterState {
        final WalCfFilterCache cfFilter;
        final boolean filterActive;

        FilterState(WalCfFilterCache cfFilter, boolean filterActive) {
            this.cfFilter = cfFilter;
            this.filterActive = filterActive;
        }
    }
}
