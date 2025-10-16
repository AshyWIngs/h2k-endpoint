package kz.qazmarka.h2k.endpoint.processing;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.config.H2kConfig;
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
        if (cellsInRow > counters.maxRowCellsSeen) {
            counters.maxRowCellsSeen = cellsInRow;
        }

        if (!context.cfFilter.isEmpty() && !context.cfFilter.allows(rowCells)) {
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
        final H2kConfig.TableOptionsSnapshot tableOptions;
        final WalCfFilterCache cfFilter;
        final WalCounterService.EntryCounters counters;

        RowContext(String topic,
                   TableName table,
                   WalMeta walMeta,
                   BatchSender sender,
                   H2kConfig.TableOptionsSnapshot tableOptions,
                   WalCfFilterCache cfFilter,
                   WalCounterService.EntryCounters counters) {
            this.topic = topic;
            this.table = table;
            this.walMeta = walMeta;
            this.sender = sender;
            this.tableOptions = tableOptions;
            this.cfFilter = cfFilter;
            this.counters = counters;
        }
    }
}
