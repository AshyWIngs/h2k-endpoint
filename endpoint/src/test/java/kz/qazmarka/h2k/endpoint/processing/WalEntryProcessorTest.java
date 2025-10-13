package kz.qazmarka.h2k.endpoint.processing;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;

/**
 * Юнит‑тесты для внутренних помощников {@link WalEntryProcessor} (initialCapacity и фильтры WAL).
 */
class WalEntryProcessorTest {

    private static Cell cell(String row, String cf, long ts) {
        return new KeyValue(bytes(row), bytes(cf), bytes("q"), ts, bytes("v"));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static int[] hashes(byte[][] families) {
        if (families == null) {
            return null;
        }
        int[] hashes = new int[families.length];
        for (int i = 0; i < families.length; i++) {
            byte[] cf = families[i];
            hashes[i] = cf == null ? 0 : Bytes.hashCode(cf, 0, cf.length);
        }
        return hashes;
    }

    @Test
    @DisplayName("passCfFilter1(): один CF — true, если встречается целевое семейство")
    void filter_oneCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 50L),
                cell("r", "a", 100L)
        );
        byte[] cfA = bytes("a");
        assertTrue(WalEntryProcessor.passCfFilter1(cells, cfA));
        assertFalse(WalEntryProcessor.passCfFilter1(cells, bytes("b")));
        assertFalse(WalEntryProcessor.passCfFilter1(Collections.<Cell>emptyList(), cfA));
    }

    @Test
    @DisplayName("passCfFilter2(): true при совпадении с любым из двух семейств")
    void filter_twoCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 100L),
                cell("r", "b", 90L),
                cell("r", "c", 120L)
        );
        assertTrue(WalEntryProcessor.passCfFilter2(cells, bytes("a"), bytes("b")), "совпадение по первому CF");
        assertTrue(WalEntryProcessor.passCfFilter2(cells, bytes("x"), bytes("b")), "совпадение по второму CF");
        assertFalse(WalEntryProcessor.passCfFilter2(cells, bytes("x"), bytes("y")), "нет совпадений");
    }

    @Test
    @DisplayName("passCfFilterN(): три и более CF")
    void filter_manyCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 10L),
                cell("r", "b", 20L),
                cell("r", "c", 30L)
        );
        byte[][] fams1 = {bytes("a"), bytes("b"), bytes("c")};
        byte[][] fams2 = {bytes("z"), bytes("b"), bytes("y")};
        byte[][] fams3 = {bytes("x"), bytes("y"), bytes("z")};
        assertTrue(WalEntryProcessor.passCfFilterN(cells, fams1, hashes(fams1)));
        assertTrue(WalEntryProcessor.passCfFilterN(cells, fams2, hashes(fams2)));
        assertFalse(WalEntryProcessor.passCfFilterN(cells, fams3, hashes(fams3)));
        byte[][] famsEmpty = {bytes("a")};
        assertFalse(WalEntryProcessor.passCfFilterN(Collections.<Cell>emptyList(), famsEmpty, hashes(famsEmpty)));
    }

    @Test
    @DisplayName("WalMetrics агрегирует строки и фильтрацию")
    void metricsAccumulates() throws Exception {
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
            public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, java.util.Map<String, Object> out) {
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
        org.junit.jupiter.api.Assertions.assertEquals("probe", String.valueOf(probeRecord.get("id")));
        TopicManager topicManager = new TopicManager(h2kConfig, TopicEnsurer.disabled());
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer, h2kConfig);

        try (BatchSender sender = new BatchSender(10, 1_000)) {
            processor.process(walEntry("row1", "d"), sender, false);
        }

        WalMetrics metrics = processor.metrics();
        assertEquals(1, metrics.entries());
        assertEquals(1, metrics.rows());
        assertEquals(1, metrics.cells());
        assertEquals(0, metrics.filteredRows());

        try (BatchSender sender = new BatchSender(10, 1_000)) {
            processor.process(walEntry("row2", "x"), sender, false);
        }

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
}
