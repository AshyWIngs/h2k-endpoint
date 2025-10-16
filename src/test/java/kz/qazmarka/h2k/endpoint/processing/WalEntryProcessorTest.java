package kz.qazmarka.h2k.endpoint.processing;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor.WalMetrics;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.payload.serializer.avro.SchemaRegistryClientFactory;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.SimpleDecoder;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Юнит‑тесты для внутренних помощников {@link WalEntryProcessor} (initialCapacity и фильтры WAL).
 */
class WalEntryProcessorTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("WalMetrics агрегирует строки и фильтрацию")
    void metricsAccumulates() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.sr.urls", "http://mock");
        cfg.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());

        SchemaRegistryClientFactory testFactory = (urls, props, capacity) -> new MockSchemaRegistryClient();

        PhoenixTableMetadataProvider provider = new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) { return null; }

            @Override
            public Integer capacityHint(TableName table) { return null; }

            @Override
            public String[] columnFamilies(TableName table) {
                return "T_AVRO".equalsIgnoreCase(table.getNameAsString()) ? new String[]{"d"} : SchemaRegistry.EMPTY;
            }
        };

        H2kConfig h2kConfig = H2kConfig.from(cfg, "mock:9092", provider);

        Decoder decoder = new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                Object raw = SimpleDecoder.INSTANCE.decode(table, qualifier, value);
                if (raw instanceof byte[]) {
                    return new String((byte[]) raw, StandardCharsets.UTF_8);
                }
                return raw;
            }

            @Override
            public Object decode(TableName table, byte[] qual, int qOff, int qLen, byte[] value, int vOff, int vLen) {
                Object raw = SimpleDecoder.INSTANCE.decode(table, qual, qOff, qLen, value, vOff, vLen);
                if (raw instanceof byte[]) {
                    return new String((byte[]) raw, StandardCharsets.UTF_8);
                }
                return raw;
            }

            @Override
            public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
                if (rk != null) {
                    out.put("id", new String(rk.toByteArray(), StandardCharsets.UTF_8));
                }
            }
        };

        PayloadBuilder builder = new PayloadBuilder(decoder, h2kConfig, testFactory);

        byte[] probeRow = bytes("probe");
        List<Cell> probeCells = Collections.singletonList(new KeyValue(probeRow,
                Bytes.toBytes("d"),
                Bytes.toBytes("value"),
                Bytes.toBytes("v")));
        GenericData.Record probeRecord = builder.buildRowPayload(TableName.valueOf("T_AVRO"),
                probeCells,
                RowKeySlice.whole(probeRow),
                0L,
                0L);
        assertEquals("probe", String.valueOf(probeRecord.get("id")));
        TopicManager topicManager = new TopicManager(h2kConfig, TopicEnsurer.disabled());
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer, h2kConfig);

        processWalEntry(processor, walEntry("row1", "d"), 10);

        WalMetrics metrics = processor.metrics();
        assertEquals(1, metrics.entries());
        assertEquals(1, metrics.rows());
        assertEquals(1, metrics.cells());
        assertEquals(0, metrics.filteredRows());

        processWalEntry(processor, walEntry("row2", "x"), 10);

        WalMetrics after = processor.metrics();
        assertEquals(2, after.entries());
        assertEquals(2, after.rows());
        assertEquals(2, after.cells());
        assertEquals(1, after.filteredRows());
    }

    private static WAL.Entry walEntry(String row, String cf) {
        byte[] rowBytes = bytes(row);
        byte[] cfBytes = bytes(cf);
        WALKey key = new WALKey(rowBytes, TableName.valueOf("T_AVRO"), 1L);
        WALEdit edit = new WALEdit();
        edit.add(new KeyValue(rowBytes, cfBytes, bytes("q"), 1L, bytes("v")));
        return new Entry(key, edit);
    }

    @Test
    @DisplayName("CF-фильтр отключён → строки всегда отправляются")
    void cfFilterDisabledAllowsAllRows() {
        WalScenario scenario = createScenario(new String[0]);
        WAL.Entry entry = walEntry("row-disabled", "x");

        processWalEntry(scenario.processor, entry, 3);

        assertEquals(1, scenario.producer.history().size(), "Строка должна быть отправлена при выключенном фильтре");
        WalEntryProcessor.WalMetrics metrics = scenario.processor.metrics();
        assertEquals(1, metrics.rows());
        assertEquals(0, metrics.filteredRows());
    }

    @Test
    @DisplayName("CF-фильтр удаляет строки без разрешённых семейств")
    void cfFilterRejectsDisallowedRows() {
        WalScenario scenario = createScenario(new String[]{"d"});
        WAL.Entry entry = walEntry("row-filtered", "x");

        processWalEntry(scenario.processor, entry, 3);

        assertTrue(scenario.producer.history().isEmpty(),
                "Отфильтрованная строка не должна публиковаться");
        WalEntryProcessor.WalMetrics metrics = scenario.processor.metrics();
        assertEquals(1, metrics.rows(), "Строка учитывается в метриках");
        assertEquals(1, metrics.filteredRows(), "Должна учитываться как отфильтрованная");
    }

    @Test
    @DisplayName("Большие строки увеличивают буфер rowBuffer, trim выполняется при ручном сбросе")
    void rowBufferUpsizeAndTrimMetrics() {
        WalScenario scenario = createScenario(new String[]{"d"});
        int cellsCount = 5_000;
        WAL.Entry entry = largeWalEntry("row-large", "d", cellsCount);

        processWalEntry(scenario.processor, entry, 3);

        WalEntryProcessor.WalMetrics metrics = scenario.processor.metrics();
        assertEquals(cellsCount, metrics.cells(), "Все ячейки должны быть обработаны");
        assertEquals(cellsCount, metrics.rows(), "Все строки должны быть учтены в метриках");
        assertTrue(scenario.processor.rowBufferUpsizeCount() > 0,
                "Ожидается увеличение буфера");

        int trimThreshold = WalEntryProcessor.rowBufferTrimThresholdForTest();
        forceRowBufferTrim(scenario, trimThreshold);
        assertTrue(scenario.processor.rowBufferTrimCount() > 0,
                "Ожидается усадка буфера после ручного сброса");
    }

    private static void processWalEntry(WalEntryProcessor processor, WAL.Entry entry, int batchSize) {
        try (BatchSender sender = new BatchSender(batchSize, 1_000)) {
            processor.process(entry, sender, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Обработка WAL прервана: " + e.getMessage(), e);
        } catch (ExecutionException | TimeoutException e) {
            fail("Не удалось обработать WAL: " + e.getMessage(), e);
        }
    }

    private static void forceRowBufferTrim(WalScenario scenario, int trimThreshold) {
        ArrayList<Cell> buffer = new ArrayList<>(trimThreshold);
        for (int i = 0; i < trimThreshold; i++) {
            buffer.add(null);
        }
        scenario.processor.resetRowBufferForTest(buffer, trimThreshold);
    }

    private static WalScenario createScenario(String[] cfFamilies) {
        PhoenixTableMetadataProvider provider = provider(cfFamilies);
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.sr.urls", "http://mock");
        cfg.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        cfg.set("h2k.topic.pattern", "${namespace}.${qualifier}");

        SchemaRegistryClientFactory factory = (urls, props, capacity) -> new MockSchemaRegistryClient();
        H2kConfig h2kConfig = H2kConfig.from(cfg, "mock:9092", provider);

        Decoder decoder = new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                Object raw = SimpleDecoder.INSTANCE.decode(table, qualifier, value);
                if (raw instanceof byte[]) {
                    return new String((byte[]) raw, StandardCharsets.UTF_8);
                }
                return raw;
            }

            @Override
            public Object decode(TableName table, byte[] qual, int qOff, int qLen, byte[] value, int vOff, int vLen) {
                Object raw = SimpleDecoder.INSTANCE.decode(table, qual, qOff, qLen, value, vOff, vLen);
                if (raw instanceof byte[]) {
                    return new String((byte[]) raw, StandardCharsets.UTF_8);
                }
                return raw;
            }

            @Override
            public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
                if (rk != null) {
                    out.put("id", new String(rk.toByteArray(), StandardCharsets.UTF_8));
                }
            }
        };

        PayloadBuilder payloadBuilder = new PayloadBuilder(decoder, h2kConfig, factory);
        TopicManager topicManager = new TopicManager(h2kConfig, TopicEnsurer.disabled());
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalEntryProcessor processor = new WalEntryProcessor(payloadBuilder, topicManager, producer, h2kConfig);
        return new WalScenario(processor, producer);
    }

    private static PhoenixTableMetadataProvider provider(String[] cfFamilies) {
        final String[] fams = cfFamilies == null ? new String[0] : cfFamilies.clone();
        return new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) { return null; }

            @Override
            public Integer capacityHint(TableName table) { return null; }

            @Override
            public String[] columnFamilies(TableName table) {
                return "T_AVRO".equalsIgnoreCase(table.getNameAsString()) ? fams.clone() : SchemaRegistry.EMPTY;
            }

            @Override
            public String[] primaryKeyColumns(TableName table) {
                return new String[]{"id"};
            }
        };
    }

    private static WAL.Entry largeWalEntry(String row, String cf, int cellsCount) {
        byte[] rowBytes = bytes(row);
        byte[] cfBytes = bytes(cf);
        TableName table = TableName.valueOf("T_AVRO");
        WALKey key = new WALKey(rowBytes, table, 1L);
        WALEdit edit = new WALEdit();
        byte[] qualifier = bytes("value");
        for (int i = 0; i < cellsCount; i++) {
            byte[] value = bytes("v" + i);
            edit.add(new KeyValue(rowBytes, cfBytes, qualifier, i, value));
        }
        return new Entry(key, edit);
    }

    private static final class WalScenario {
        final WalEntryProcessor processor;
        final MockProducer<byte[], byte[]> producer;

        WalScenario(WalEntryProcessor processor, MockProducer<byte[], byte[]> producer) {
            this.processor = processor;
            this.producer = producer;
        }
    }
}
