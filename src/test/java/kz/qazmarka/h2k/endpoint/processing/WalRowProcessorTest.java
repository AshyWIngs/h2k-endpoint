package kz.qazmarka.h2k.endpoint.processing;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.payload.serializer.avro.SchemaRegistryClientFactory;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Проверяет построчную обработку WAL и корректность работы фильтра CF.
 */
class WalRowProcessorTest {

    private static final Path SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath();
    private static final TableName TABLE = TableName.valueOf("INT_TEST_TABLE");

    @Test
    @DisplayName("Пустой rowkey не приводит к отправке и метрики не изменяются")
    void skipsNullRowKey() throws Exception {
        H2kConfig config = config();
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalRowProcessor processor = processor(config, producer);
        WalCounterService counterService = new WalCounterService();
        WalCounterService.EntryCounters counters = counterService.newEntryCounters();
        WalRowProcessor.RowContext context = new WalRowProcessor.RowContext(
                "test-topic",
                TABLE,
                WalMeta.EMPTY,
                new BatchSender(10, 5_000),
                config.describeTableOptions(TABLE),
                WalCfFilterCache.EMPTY,
                counters);

        List<Cell> cells = singleCell(Bytes.toBytes("rk-null"), Bytes.toBytes("d"));
        processor.processRow(null, cells, context);

        assertEquals(0, counters.rowsSent);
        assertEquals(0, counters.rowsFiltered);
        assertEquals(0, counters.cellsSeen);
        assertTrue(producer.history().isEmpty(), "Kafka не должен получать данных без rowkey");
    }

    @Test
    @DisplayName("CF-фильтр блокирует строку и увеличивает счётчик filtered")
    void cfFilterBlocksRow() throws Exception {
        H2kConfig config = config();
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalRowProcessor processor = processor(config, producer);
        WalCounterService counterService = new WalCounterService();
        WalCounterService.EntryCounters counters = counterService.newEntryCounters();
        WalCfFilterCache cfCache = WalCfFilterCache.build(new byte[][] { Bytes.toBytes("allowed") });
        WalRowProcessor.RowContext context = new WalRowProcessor.RowContext(
                "test-topic",
                TABLE,
                WalMeta.EMPTY,
                new BatchSender(10, 5_000),
                config.describeTableOptions(TABLE),
                cfCache,
                counters);

        byte[] row = Bytes.toBytes("rk-filtered");
        List<Cell> cells = Collections.singletonList(new KeyValue(row, Bytes.toBytes("blocked"), Bytes.toBytes("q"), 1L, Bytes.toBytes(1L)));
        RowKeySlice.Mutable rowKey = new RowKeySlice.Mutable(row, 0, row.length);

        processor.processRow(rowKey, cells, context);

        assertEquals(0, counters.rowsSent);
        assertEquals(1, counters.rowsFiltered);
        assertEquals(1, counters.cellsSeen);
        assertTrue(producer.history().isEmpty(), "Kafka не должен получать данные, отфильтрованные по CF");
    }

    @Test
    @DisplayName("Разрешённая строка отправляется и метрики обновляются")
    void sendsRowWhenFilterAllows() throws Exception {
        H2kConfig config = config();
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalRowProcessor processor = processor(config, producer);
        WalCounterService counterService = new WalCounterService();
        WalCounterService.EntryCounters counters = counterService.newEntryCounters();
        BatchSender sender = new BatchSender(10, 5_000);
        WalRowProcessor.RowContext context = new WalRowProcessor.RowContext(
                "test-topic",
                TABLE,
                new WalMeta(100L, 200L),
                sender,
                config.describeTableOptions(TABLE),
                WalCfFilterCache.EMPTY,
                counters);

        byte[] row = Bytes.toBytes("rk-allowed");
        List<Cell> cells = singleCell(row, Bytes.toBytes("d"));
        RowKeySlice.Mutable rowKey = new RowKeySlice.Mutable(row, 0, row.length);

        processor.processRow(rowKey, cells, context);
        sender.flush();

        assertEquals(1, counters.rowsSent);
        assertEquals(0, counters.rowsFiltered);
        assertEquals(cells.size(), counters.cellsSent);
        assertEquals(1, producer.history().size(), "Kafka должен получить единственную строку");
        assertEquals("test-topic", producer.history().get(0).topic());
    }

    private static WalRowProcessor processor(H2kConfig config, MockProducer<byte[], byte[]> producer) {
        SchemaRegistryClientFactory factory = (urls, clientConfig, capacity) -> new MockSchemaRegistryClient();
        PayloadBuilder builder = new PayloadBuilder(decoder(), config, factory);
        WalRowDispatcher dispatcher = new WalRowDispatcher(builder, producer);
        WalObserverHub observers = WalObserverHub.create(config);
        return new WalRowProcessor(dispatcher, observers);
    }

    private static List<Cell> singleCell(byte[] row, byte[] family) {
        ArrayList<Cell> cells = new ArrayList<>(1);
        cells.add(new KeyValue(row, family, Bytes.toBytes("value_long"), 1L, Bytes.toBytes(1L)));
        return cells;
    }

    private static H2kConfig config() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.schema.dir", SCHEMA_DIR.toString());
        cfg.set("h2k.avro.sr.urls", "http://mock-sr");
        return H2kConfig.from(cfg, "mock:9092", metadata());
    }

    private static PhoenixTableMetadataProvider metadata() {
        return new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) {
                return null;
            }

            @Override
            public Integer capacityHint(TableName table) {
                return 4;
            }

            @Override
            public String[] columnFamilies(TableName table) {
                return new String[] { "d" };
            }

            @Override
            public String[] primaryKeyColumns(TableName table) {
                return new String[] { "id" };
            }
        };
    }

    private static Decoder decoder() {
        return new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                if (value == null) {
                    return null;
                }
                if ("value_long".equalsIgnoreCase(qualifier)) {
                    return Bytes.toLong(value);
                }
                return new String(value, StandardCharsets.UTF_8);
            }

            @Override
            public void decodeRowKey(TableName table, RowKeySlice rowKey, int saltBytes, java.util.Map<String, Object> out) {
                out.put("id", new String(rowKey.toByteArray(), StandardCharsets.UTF_8));
            }
        };
    }
}
