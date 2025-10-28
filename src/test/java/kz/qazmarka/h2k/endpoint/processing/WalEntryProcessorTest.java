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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.TestRawDecoder;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Юнит‑тесты внутренних помощников {@link WalEntryProcessor}: агрегированные метрики, буфер строк и
 * обработка пустых записей.
 */
class WalEntryProcessorTest {

    private static final TableName TABLE = TableName.valueOf("T_AVRO");

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("WalMetrics агрегирует строки и фильтрацию")
    void metricsAccumulates() {
        WalScenario scenario = createScenario(new String[]{"d"});

        byte[] probeRow = bytes("probe");
        List<Cell> probeCells = Collections.singletonList(new KeyValue(probeRow,
                Bytes.toBytes("d"),
                Bytes.toBytes("value"),
                Bytes.toBytes("v")));
        GenericData.Record probeRecord = scenario.builder.buildRowPayload(TABLE,
                probeCells,
                RowKeySlice.whole(probeRow),
                0L,
                0L);
        assertEquals("probe", String.valueOf(probeRecord.get("id")));
        WalEntryProcessor processor = scenario.processor;

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

    /** Проверяет, что пустая запись WAL игнорируется и метрики не изменяются. */
    @Test
    @DisplayName("Пустая запись WAL игнорируется без обновления счётчиков")
    void skipEmptyEntryIgnoresRecord() {
        WalScenario scenario = createScenario(new String[]{"d"});
        WALKey key = new WALKey(bytes("ignored"), TABLE, 42L);
        WALEdit emptyEdit = new WALEdit();
        WAL.Entry entry = new Entry(key, emptyEdit);

        processWalEntry(scenario.processor, entry, 3);

        WalEntryProcessor.WalMetrics metrics = scenario.processor.metrics();
        assertEquals(0, metrics.entries(), "Пустая запись не должна увеличивать счётчик записей");
        assertEquals(0, metrics.rows(), "Пустая запись не должна учитывать строки");
        assertTrue(scenario.producer.history().isEmpty(), "Kafka не должен получать сообщений для пустого WAL");
    }

    @Test
    @DisplayName("CF-фильтр пропускает строки только с разрешёнными семействами")
    void cfFilterAllowsWhitelistedFamilies() {
        WalScenario scenario = createScenario(new String[]{"d", "aux"});

        WAL.Entry allowed = walEntryWithFamilies("row-allowed", "aux");
        processWalEntry(scenario.processor, allowed, 4);

        WalEntryProcessor.WalMetrics afterAllowed = scenario.processor.metrics();
        assertEquals(1, afterAllowed.entries(), "Ожидается обработка первой записи WAL");
        assertEquals(1, afterAllowed.rows(), "Строка с допустимым CF должна учитываться");
        assertEquals(0, afterAllowed.filteredRows(), "При разрешённом CF фильтр не должен срабатывать");
        assertEquals(1, scenario.producer.history().size(), "Kafka должен получить сообщение с разрешённым CF");

        WAL.Entry denied = walEntryWithFamilies("row-denied", "forbidden");
        processWalEntry(scenario.processor, denied, 4);

        WalEntryProcessor.WalMetrics afterDenied = scenario.processor.metrics();
        assertEquals(2, afterDenied.entries(), "Счётчик записей WAL должен увеличиться");
        assertEquals(2, afterDenied.rows(), "Вторая строка засчитывается как просмотренная");
        assertEquals(1, afterDenied.filteredRows(), "Строка с запрещённым CF должна отфильтроваться");
        assertEquals(1, scenario.producer.history().size(), "Kafka не должен получать сообщения по запрещённому CF");
    }

    @Test
    @DisplayName("CF-фильтр отключён при пустом списке семейств")
    void cfFilterDisabledWhenFamiliesMissing() {
        WalScenario scenario = createScenario(new String[0]);
        WAL.Entry entry = walEntryWithFamilies("row-free", "unknown");

        processWalEntry(scenario.processor, entry, 2);

        WalEntryProcessor.WalMetrics metrics = scenario.processor.metrics();
        assertEquals(1, metrics.entries(), "Запись WAL должна быть обработана");
        assertEquals(1, metrics.rows(), "Строка должна пройти без фильтрации");
        assertEquals(0, metrics.filteredRows(), "Фильтр отключён и не должен считать строки");
        assertEquals(1, scenario.producer.history().size(), "Kafka должен получить сообщение при отключённом фильтре");
    }

    private static WAL.Entry walEntry(String row, String cf) {
        byte[] rowBytes = bytes(row);
        byte[] cfBytes = bytes(cf);
        WALKey key = new WALKey(rowBytes, TABLE, 1L);
        WALEdit edit = new WALEdit();
        edit.add(new KeyValue(rowBytes, cfBytes, bytes("q"), 1L, bytes("v")));
        return new Entry(key, edit);
    }

    private static WAL.Entry walEntryWithFamilies(String row, String... families) {
        byte[] rowBytes = bytes(row);
        WALKey key = new WALKey(rowBytes, TABLE, 1L);
        WALEdit edit = new WALEdit();
        if (families != null && families.length > 0) {
            for (String family : families) {
                byte[] cfBytes = bytes(family);
                edit.add(new KeyValue(rowBytes, cfBytes, bytes("q"), 1L, bytes("v-" + family)));
            }
        }
        return new Entry(key, edit);
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
    assertFalse(scenario.producer.history().isEmpty(),
        "После отправки большого набора строк Kafka должна получить сообщения");

        int trimThreshold = WalEntryProcessor.rowBufferTrimThresholdForTest();
        forceRowBufferTrim(scenario, trimThreshold);
        assertTrue(scenario.processor.rowBufferTrimCount() > 0,
                "Ожидается усадка буфера после ручного сброса");
    }

    @Test
    @DisplayName("sendRow публикует строку в Kafka и добавляет фьючерс")
    void sendRowPublishesRecord() throws Exception {
        WalScenario scenario = createScenario(new String[]{"d"});
        byte[] row = bytes("rk-send");
        List<Cell> cells = Collections.singletonList(new KeyValue(row,
                Bytes.toBytes("d"),
                Bytes.toBytes("value"),
                1L,
                Bytes.toBytes("payload")));
        RowKeySlice rowKey = RowKeySlice.whole(row);
        BatchSender sender = new BatchSender(2, 1_000);
        WalMeta meta = new WalMeta(123L, 456L);

        scenario.processor.sendRow(scenario.topicManager.resolveTopic(TABLE),
                TABLE,
                meta,
                rowKey,
                cells,
                sender);
        sender.flush();

        assertEquals(1, scenario.producer.history().size(), "Ожидается публикация строки");
        org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> produced = scenario.producer.history().get(0);
        assertEquals(scenario.topicManager.resolveTopic(TABLE), produced.topic());
        assertTrue(java.util.Arrays.equals(row, produced.key()), "Ключ должен совпасть с rowkey");
        assertTrue(produced.value() != null && produced.value().length > 0, "Payload не должен быть пустым");
    }

    @Test
    @DisplayName("Сбой отправки продьюсера приводит к ExecutionException при flush/close")
    void producerFailurePropagatesAsExecutionException() {
        // Конфигурация и метаданные как в обычном сценарии
        PhoenixTableMetadataProvider provider = provider(new String[]{"d"});
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.sr.urls", "http://mock");
        cfg.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        cfg.set("h2k.topic.pattern", "${namespace}.${qualifier}");

        H2kConfig h2kConfig = H2kConfig.from(cfg, "mock:9092", provider);

    PayloadBuilder payloadBuilder = new PayloadBuilder(decoder(), h2kConfig, new MockSchemaRegistryClient());
        TopicManager topicManager = new TopicManager(h2kConfig.getTopicSettings(), TopicEnsurer.disabled());

        // Продьюсер, у которого send() всегда возвращает exceptional future
        class FailingProducer implements org.apache.kafka.clients.producer.Producer<byte[], byte[]> {
            private java.util.concurrent.CompletableFuture<org.apache.kafka.clients.producer.RecordMetadata> failedFuture() {
                java.util.concurrent.CompletableFuture<org.apache.kafka.clients.producer.RecordMetadata> cf = new java.util.concurrent.CompletableFuture<>();
                cf.completeExceptionally(new IllegalStateException("simulated send failure"));
                return cf;
            }
            @Override public java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> rec) { return failedFuture(); }
            @Override public java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> rec, org.apache.kafka.clients.producer.Callback cb) { return failedFuture(); }
            @Override public void flush() { /* no-op for test */ }
            @Override public List<org.apache.kafka.common.PartitionInfo> partitionsFor(String topic) { return Collections.emptyList(); }
            @Override public Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics() { return Collections.emptyMap(); }
            @Override public void close() { /* no-op for test */ }
            @Override public void close(java.time.Duration timeout) { /* no-op for test */ }
            @Override public void initTransactions() { throw new UnsupportedOperationException("not used in test"); }
            @Override public void beginTransaction() { throw new UnsupportedOperationException("not used in test"); }
            @Override public void commitTransaction() { throw new UnsupportedOperationException("not used in test"); }
            @Override public void abortTransaction() { throw new UnsupportedOperationException("not used in test"); }
            @Override public void sendOffsetsToTransaction(Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets, String consumerGroupId) { throw new UnsupportedOperationException("not used in test"); }
        }
        org.apache.kafka.clients.producer.Producer<byte[], byte[]> failingProducer = new FailingProducer();

        WalEntryProcessor processor = new WalEntryProcessor(payloadBuilder, topicManager, failingProducer, h2kConfig);

        byte[] row = bytes("rk-fail");
        WALKey key = new WALKey(row, TABLE, 1L);
        WALEdit edit = new WALEdit();
        edit.add(new KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("q"), 1L, Bytes.toBytes("v")));
        WAL.Entry entry = new Entry(key, edit);

        // Ожидаем ExecutionException из-за exceptional future продьюсера
        ExecutionException thrown = org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class, () -> {
            try (BatchSender sender = new BatchSender(1, 500)) {
                processor.process(entry, sender, false);
            }
        });
        org.junit.jupiter.api.Assertions.assertTrue(thrown.getCause() instanceof IllegalStateException,
                "Причина должна сохранять исходное исключение отправки");
        org.junit.jupiter.api.Assertions.assertEquals("simulated send failure", thrown.getCause().getMessage());
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

        H2kConfig h2kConfig = H2kConfig.from(cfg, "mock:9092", provider);

    PayloadBuilder payloadBuilder = new PayloadBuilder(decoder(), h2kConfig, new MockSchemaRegistryClient());
        TopicManager topicManager = new TopicManager(h2kConfig.getTopicSettings(), TopicEnsurer.disabled());
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalEntryProcessor processor = new WalEntryProcessor(payloadBuilder, topicManager, producer, h2kConfig);
        return new WalScenario(processor, producer, payloadBuilder, topicManager);
    }

    private static PhoenixTableMetadataProvider provider(String[] cfFamilies) {
        String[] fams = cfFamilies == null ? SchemaRegistry.EMPTY : cfFamilies.clone();
        PhoenixTableMetadataProvider.TableMetadataBuilder tableBuilder = PhoenixTableMetadataProvider.builder()
                .table(TABLE)
                .primaryKeyColumns("id");
        if (fams.length > 0) {
            tableBuilder.columnFamilies(fams);
        }
        return tableBuilder.done().build();
    }

    private static Decoder decoder() {
        return new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                Object raw = TestRawDecoder.INSTANCE.decode(table, qualifier, value);
                if (raw instanceof byte[]) {
                    return new String((byte[]) raw, StandardCharsets.UTF_8);
                }
                return raw;
            }

            @Override
            public Object decode(TableName table, byte[] qual, int qOff, int qLen, byte[] value, int vOff, int vLen) {
                Object raw = TestRawDecoder.INSTANCE.decode(table, qual, qOff, qLen, value, vOff, vLen);
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
    }

    private static WAL.Entry largeWalEntry(String row, String cf, int cellsCount) {
        byte[] rowBytes = bytes(row);
        byte[] cfBytes = bytes(cf);
        WALKey key = new WALKey(rowBytes, TABLE, 1L);
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
        final PayloadBuilder builder;
        final TopicManager topicManager;

        WalScenario(WalEntryProcessor processor,
                    MockProducer<byte[], byte[]> producer,
                    PayloadBuilder builder,
                    TopicManager topicManager) {
            this.processor = processor;
            this.producer = producer;
            this.builder = builder;
            this.topicManager = topicManager;
        }
    }
}
