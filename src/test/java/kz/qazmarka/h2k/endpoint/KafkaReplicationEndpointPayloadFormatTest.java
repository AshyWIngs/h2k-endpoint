package kz.qazmarka.h2k.endpoint;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.internal.TopicManager;
import kz.qazmarka.h2k.endpoint.internal.WalEntryProcessor;
import kz.qazmarka.h2k.endpoint.internal.WalMeta;
import kz.qazmarka.h2k.kafka.producer.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.SimpleDecoder;
import kz.qazmarka.h2k.util.RowKeySlice;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaReplicationEndpointPayloadFormatTest {

    private static final Decoder STRING_DECODER = (table, qualifier, value) ->
            value == null ? null : new String(value, StandardCharsets.UTF_8);
    private static final TableName TABLE = TableName.valueOf("T_AVRO");

    @Test
    @DisplayName("json_each_row формирует значение через PayloadSerializer")
    void jsonEachRowFormatUsesPayloadSerializer() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("h2k.cf.list", "d");
        conf.set("h2k.payload.format", "json_each_row");
        H2kConfig cfg = H2kConfig.from(conf, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(SimpleDecoder.INSTANCE, cfg);
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        TopicManager topicManager = new TopicManager(cfg, null);
        WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer);

        byte[] rowKey = "rk-json".getBytes(StandardCharsets.UTF_8);
        RowKeySlice slice = new RowKeySlice(rowKey, 0, rowKey.length);
        Cell cell = new KeyValue(rowKey, bytes("d"), bytes("value"), 123L, bytes("JSON"));
        Map.Entry<RowKeySlice, List<Cell>> rowEntry = new AbstractMap.SimpleEntry<>(slice, Collections.singletonList(cell));

        WalMeta walMeta = new WalMeta(123L, 456L);
        BatchSender sender = new BatchSender(10, 1_000);
        try {
            invokeSendRow(processor, "topic-json", TABLE, walMeta, rowEntry, sender);
        } finally {
            closeQuietly(sender);
        }

        assertEquals(1, producer.history().size(), "Ожидаем одну запись в MockProducer");
        ProducerRecord<byte[], byte[]> record = producer.history().get(0);
        assertArrayEquals(rowKey, record.key(), "Ключ Kafka должен совпадать с rowkey");

        byte[] expected = builder.buildRowPayloadBytes(TABLE, rowEntry.getValue(), slice, 123L, 456L);
        assertArrayEquals(expected, record.value(), "Значение должно совпадать с результатом PayloadBuilder");

        String json = new String(record.value(), StandardCharsets.UTF_8).trim();
        assertTrue(json.contains("\"value\""), "JSON должен содержать исходное значение: " + json);
    }

    @ParameterizedTest
    @ValueSource(strings = { "avro-binary", "avro_json" })
    @DisplayName("Avro форматы используют PayloadSerializer и корректную схему")
    void avroFormatsUsePayloadSerializer(String format) throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("h2k.cf.list", "d");
        conf.set("h2k.payload.format", format);
        conf.set("h2k.avro.mode", "generic");
        conf.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        H2kConfig cfg = H2kConfig.from(conf, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        TopicManager topicManager = new TopicManager(cfg, null);
        WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer);

        byte[] rowKey = "rk-avro".getBytes(StandardCharsets.UTF_8);
        RowKeySlice slice = new RowKeySlice(rowKey, 0, rowKey.length);
        Cell cell = new KeyValue(rowKey, bytes("d"), bytes("value"), 321L, bytes("OK"));
        Map.Entry<RowKeySlice, List<Cell>> rowEntry = new AbstractMap.SimpleEntry<>(slice, Collections.singletonList(cell));

        WalMeta walMeta = new WalMeta(321L, 654L);
        BatchSender sender = new BatchSender(10, 1_000);
        try {
            invokeSendRow(processor, "topic-avro", TABLE, walMeta, rowEntry, sender);
        } finally {
            closeQuietly(sender);
        }

        assertEquals(1, producer.history().size(), "Ожидаем одну запись в MockProducer");
        ProducerRecord<byte[], byte[]> record = producer.history().get(0);
        assertArrayEquals(rowKey, record.key(), "Ключ Kafka должен совпадать с rowkey");

        byte[] expected = builder.buildRowPayloadBytes(TABLE, rowEntry.getValue(), slice, 321L, 654L);
        assertArrayEquals(expected, record.value(), "Значение должно совпадать с результатом PayloadBuilder");
        assertTrue(record.value().length > 0, "Avro выход не должен быть пустым");
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void invokeSendRow(WalEntryProcessor processor,
                                      String topic,
                                      TableName table,
                                      WalMeta walMeta,
                                      Map.Entry<RowKeySlice, List<Cell>> rowEntry,
                                      BatchSender sender) throws Exception {
        Method method = WalEntryProcessor.class
                .getDeclaredMethod("sendRow", String.class, TableName.class, WalMeta.class, Map.Entry.class, BatchSender.class);
        method.setAccessible(true);
        method.invoke(processor, topic, table, walMeta, rowEntry, sender);
    }

    private static void closeQuietly(BatchSender sender) {
        try {
            sender.close();
        } catch (Exception e) {
            throw new AssertionError("Не удалось корректно закрыть BatchSender", e);
        }
    }
}
