package kz.qazmarka.h2k.endpoint;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessorTestSupport;
import kz.qazmarka.h2k.endpoint.processing.WalMeta;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

class KafkaReplicationEndpointPayloadFormatTest {

    private static final Decoder STRING_DECODER = (table, qualifier, value) ->
            value == null ? null : new String(value, StandardCharsets.UTF_8);

    private static final Decoder PK_AWARE_DECODER = new Decoder() {
        @Override
        public Object decode(TableName table, String qualifier, byte[] value) {
            return STRING_DECODER.decode(table, qualifier, value);
        }

        @Override
        public Object decode(TableName table, byte[] qualifier, int qualifierOffset, int qualifierLength,
                             byte[] value, int valueOffset, int valueLength) {
            return STRING_DECODER.decode(table, qualifier, qualifierOffset, qualifierLength, value, valueOffset, valueLength);
        }

        @Override
        public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
            if (rk == null) {
                return;
            }
            byte[] keyBytes = rk.toByteArray();
            int start = Math.min(Math.max(0, saltBytes), keyBytes.length);
            String pk = new String(keyBytes, start, keyBytes.length - start, StandardCharsets.UTF_8);
            out.put("id", pk);
        }
    };
    private static final TableName TABLE = TableName.valueOf("T_AVRO");

    @Test
    @DisplayName("json_each_row формирует значение через PayloadSerializer")
    void jsonEachRowFormatUsesPayloadSerializer() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("h2k.payload.format", "json_each_row");
        H2kConfig cfg = H2kConfig.from(conf, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(PK_AWARE_DECODER, cfg);
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        TopicManager topicManager = new TopicManager(cfg, null);
        WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer, cfg);

        byte[] rowKey = "rk-json".getBytes(StandardCharsets.UTF_8);
        RowKeySlice slice = RowKeySlice.whole(rowKey);
        Cell cell = new KeyValue(rowKey, bytes("d"), bytes("value"), 123L, bytes("JSON"));
        List<Cell> cells = Collections.singletonList(cell);

        WalMeta walMeta = new WalMeta(123L, 456L);
        try (BatchSender sender = new BatchSender(10, 1_000)) {
            WalEntryProcessorTestSupport.sendRow(processor, "topic-json", TABLE, walMeta, slice, cells, sender);
        }

        assertEquals(1, producer.history().size(), "Ожидаем одну запись в MockProducer");
        ProducerRecord<byte[], byte[]> rec = producer.history().get(0);
        assertArrayEquals(rowKey, rec.key(), "Ключ Kafka должен совпадать с rowkey");

        byte[] expected = builder.buildRowPayloadBytes(TABLE, cells, slice, 123L, 456L);
        assertArrayEquals(expected, rec.value(), "Значение должно совпадать с результатом PayloadBuilder");

        String json = new String(rec.value(), StandardCharsets.UTF_8).trim();
        assertTrue(json.contains("\"value\""), "JSON должен содержать исходное значение: " + json);
    }

    @ParameterizedTest
    @ValueSource(strings = { "avro-binary", "avro_json" })
    @DisplayName("Avro форматы используют PayloadSerializer и корректную схему")
    void avroFormatsUsePayloadSerializer(String format) throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("h2k.payload.format", format);
        conf.set("h2k.avro.mode", "generic");
        conf.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        H2kConfig cfg = H2kConfig.from(conf, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(PK_AWARE_DECODER, cfg);
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        TopicManager topicManager = new TopicManager(cfg, null);
        WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer, cfg);

        byte[] rowKey = "pk-001".getBytes(StandardCharsets.UTF_8);
        RowKeySlice slice = RowKeySlice.whole(rowKey);
        Cell cell = new KeyValue(rowKey, bytes("d"), bytes("value"), 321L, bytes("OK"));
        List<Cell> cells = Collections.singletonList(cell);

        WalMeta walMeta = new WalMeta(321L, 654L);
        try (BatchSender sender = new BatchSender(10, 1_000)) {
            WalEntryProcessorTestSupport.sendRow(processor, "topic-avro", TABLE, walMeta, slice, cells, sender);
        }

        assertEquals(1, producer.history().size(), "Ожидаем одну запись в MockProducer");
        ProducerRecord<byte[], byte[]> rec = producer.history().get(0);
        assertArrayEquals(rowKey, rec.key(), "Ключ Kafka должен совпадать с rowkey");

        byte[] expected = builder.buildRowPayloadBytes(TABLE, cells, slice, 321L, 654L);
        assertArrayEquals(expected, rec.value(), "Значение должно совпадать с результатом PayloadBuilder");
        assertTrue(rec.value().length > 0, "Avro выход не должен быть пустым");

        AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
        Schema schema = registry.getByTable(TABLE.getNameAsString());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        org.apache.avro.io.Decoder decoder = format.equals("avro_json")
                ? DecoderFactory.get().jsonDecoder(schema, new String(expected, StandardCharsets.UTF_8))
                : DecoderFactory.get().binaryDecoder(expected, null);
        GenericRecord record = reader.read(null, decoder);
        assertEquals("pk-001", record.get("id").toString());
        assertEquals("OK", record.get("value").toString());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

}
