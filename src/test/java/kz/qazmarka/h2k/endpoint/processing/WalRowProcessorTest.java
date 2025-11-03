package kz.qazmarka.h2k.endpoint.processing;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.MockProducer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Проверяет построчную обработку WAL и корректность работы фильтра CF.
 */
class WalRowProcessorTest extends BaseWalProcessorTest {

    @Test
    @DisplayName("Пустой rowkey не приводит к отправке и метрики не изменяются")
    void skipsNullRowKey() throws Exception {
        H2kConfig config = builder().buildConfig();
        MockProducer<RowKeySlice, byte[]> producer = builder().buildProducer();
        WalRowProcessor processor = builder().buildProcessor(config, producer);
        
        WalCounterService.EntryCounters counters = builder().buildCounters();
        BatchSender sender = builder().buildBatchSender();
        WalRowProcessor.RowContext context = builder().buildContext(config, sender, counters);

        List<Cell> cells = builder().buildSingleCell(Bytes.toBytes("rk-null"), Bytes.toBytes("d"));
        processor.processRow(null, cells, context);

        assertEquals(0, counters.rowsSent);
        assertEquals(0, counters.rowsFiltered);
        assertEquals(0, counters.cellsSeen);
        assertTrue(producer.history().isEmpty(), "Kafka не должен получать данных без rowkey");
    }

    @Test
    @DisplayName("Фильтр по колоночным семействам блокирует строки")
    void cfFilterBlocksRow() throws Exception {
        H2kConfig config = builder().withAllowedCfs("allowed").buildConfig();
        MockProducer<RowKeySlice, byte[]> producer = builder().buildProducer();
        WalRowProcessor processor = builder().buildProcessor(config, producer);

        WalCounterService.EntryCounters counters = builder().buildCounters();
        BatchSender sender = builder().buildBatchSender();
        WalRowProcessor.RowContext context = builder().withAllowedCfs("allowed").buildContext(config, sender, counters);

        byte[] row = Bytes.toBytes("block-row-id");
        List<Cell> cells = Collections.singletonList(new KeyValue(row, Bytes.toBytes("blocked"), Bytes.toBytes("q"), 1L, Bytes.toBytes(1L)));
        RowKeySlice.Mutable rowKey = builder().buildRowKey(row);
        processor.processRow(rowKey, cells, context);

        assertTrue(producer.history().isEmpty(), "Kafka не должен получать данных с заблокированным CF");
        assertEquals(1, counters.rowsFiltered, "Должна быть зафиксирована одна отфильтрованная строка");
        assertEquals(0, counters.rowsSent, "Не должно быть отправленных строк");
        assertEquals(1, counters.cellsSeen, "Одна ячейка должна быть подсчитана");
    }    @Test
    @DisplayName("Разрешённая строка отправляется и метрики обновляются")
    void sendsRowWhenFilterAllows() throws Exception {
        H2kConfig config = builder().buildConfig();
        MockProducer<RowKeySlice, byte[]> producer = builder().buildProducer();
        WalRowProcessor processor = builder().buildProcessor(config, producer);

        WalCounterService.EntryCounters counters = builder().buildCounters();
        BatchSender sender = builder().buildBatchSender();
        WalRowProcessor.RowContext context = builder()
                .withWalMeta(100L, 200L)
                .buildContext(config, sender, counters);

        byte[] row = Bytes.toBytes("rk-allowed");
        List<Cell> cells = builder().buildSingleCell(row, Bytes.toBytes("d"));
        RowKeySlice.Mutable rowKey = builder().buildRowKey(row);

        processor.processRow(rowKey, cells, context);
        sender.flush();

        assertEquals(1, counters.rowsSent);
        assertEquals(0, counters.rowsFiltered);
        assertEquals(cells.size(), counters.cellsSent);
        assertEquals(1, producer.history().size(), "Kafka должен получить единственную строку");
        assertEquals("test-topic", producer.history().get(0).topic());
    }
}
