package kz.qazmarka.h2k.endpoint.processing;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Тестовый помощник для обращения к package-private методам {@link WalEntryProcessor} из внешних пакетов.
 */
public final class WalEntryProcessorTestSupport {
    private WalEntryProcessorTestSupport() { }

    public static void sendRow(WalEntryProcessor processor,
                               String topic,
                               TableName table,
                               WalMeta walMeta,
                               RowKeySlice slice,
                               List<Cell> cells,
                               BatchSender sender) throws Exception {
        processor.sendRow(topic, table, walMeta, slice, cells, sender);
    }
}
