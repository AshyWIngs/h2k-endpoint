package kz.qazmarka.h2k.endpoint;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Интеграционный тест горячего пути WAL → PayloadBuilder → Kafka (Confluent Avro).
 */
class KafkaReplicationEndpointIntegrationTest extends BaseKafkaReplicationEndpointTest {

    @Test
    @DisplayName("WAL-запись сериализуется в Kafka-сообщение с PK и колонками")
    void walEntryProducesKafkaRecordWithPkAndColumns() throws Exception {
        H2kConfig config = builder().buildConfig();
        MockSchemaRegistryClient mockClient = builder().buildMockSchemaRegistryClient();
        PayloadBuilder payloadBuilder = builder().buildPayloadBuilder(config, mockClient);
        MockProducer<RowKeySlice, byte[]> producer = builder().buildMockProducer();
        TopicManager topicManager = builder().buildTopicManager(config);
        WalEntryProcessor processor = builder().buildWalEntryProcessor(payloadBuilder, topicManager, producer, config);

        byte[] row = Bytes.toBytes("row1");
        long ts = System.currentTimeMillis();
        List<Cell> cells = new java.util.ArrayList<>();
        cells.add(new org.apache.hadoop.hbase.KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("id"), ts, Bytes.toBytes("row1")));
        cells.add(new org.apache.hadoop.hbase.KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value_long"), ts, Bytes.toBytes(42L)));
        
        Entry entry = builder().buildWalEntry(TableName.valueOf("INT_TEST_TABLE"), row, cells);
        
        try (BatchSender sender = new BatchSender(1, 5_000)) {
            processor.process(entry, sender);
        }

        List<ProducerRecord<RowKeySlice, byte[]>> sent = producer.history();
        assertEquals(1, sent.size());

        ProducerRecord<RowKeySlice, byte[]> producedRecord = sent.get(0);
        assertEquals("INT_TEST_TABLE", producedRecord.topic());
        assertNotNull(producedRecord.key());

        byte[] valueBytes = producedRecord.value();
        assertNotNull(valueBytes);

        assertTrue(mockClient.getAllSubjects().contains("default:INT_TEST_TABLE"));

        GenericData.Record decoded = builder().decodeAvro(mockClient, valueBytes);
        assertNotNull(decoded);
        assertEquals("row1", decoded.get("id").toString());
        assertEquals(42L, decoded.get("value_long"));
    }    @Test
    @DisplayName("Инициализация без bootstrap приводит к IOException")
    void initFailsWhenBootstrapMissing() {
        Configuration cfg = new Configuration(false);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> KafkaReplicationEndpoint.readBootstrapOrThrow(cfg));
        assertTrue(ex.getMessage().contains(H2kConfig.Keys.BOOTSTRAP),
                "Сообщение должно упоминать ключ bootstrap");
    }

    @Test
    @DisplayName("Недоступный Schema Registry → PayloadBuilder бросает IllegalStateException")
    void schemaRegistryUnavailable() {
        H2kConfig config = builder().buildConfigWithoutMetadata();
        
        MockSchemaRegistryClient failingClient = new MockSchemaRegistryClient() {
            @Override
            public int register(String subject, org.apache.avro.Schema schema) {
                throw new IllegalStateException("SR down");
            }
        };
        
        try (PayloadBuilder payloadBuilder = builder().buildPayloadBuilder(config, failingClient)) {
            TableName table = TableName.valueOf("INT_TEST_TABLE");
            byte[] row = Bytes.toBytes("rk-fail");
            List<Cell> cells = new java.util.ArrayList<>();
            cells.add(new org.apache.hadoop.hbase.KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value_long"), Bytes.toBytes(1L)));
            RowKeySlice rowSlice = RowKeySlice.whole(row);
            
            IllegalStateException initError = assertThrows(IllegalStateException.class,
                    () -> payloadBuilder.buildRowPayloadBytes(table, cells, rowSlice, 0L, 0L),
                    "Ожидается ошибка регистрации схемы при недоступном Schema Registry");
            assertNotNull(initError);
        }
    }

    @Test
    @DisplayName("Ошибка ensureTopic не прерывает обработку WAL и не мешает отправке")
    void ensureTopicFailureDoesNotInterruptProcessing() throws Exception {
        H2kConfig config = builder().buildConfig();
        MockSchemaRegistryClient mockClient = builder().buildMockSchemaRegistryClient();
        PayloadBuilder payloadBuilder = builder().buildPayloadBuilder(config, mockClient);
        MockProducer<RowKeySlice, byte[]> producer = builder().buildMockProducer();
        
    AtomicInteger ensureAttempts = new AtomicInteger();
        AtomicReference<RuntimeException> ensureFailure = new AtomicReference<>();
    CountDownLatch ensureAttemptedLatch = new CountDownLatch(1);
    TopicManager topicManager = builder().buildTopicManagerWithFailingEnsurer(config, ensureAttempts, ensureFailure, ensureAttemptedLatch);
        WalEntryProcessor processor = builder().buildWalEntryProcessor(payloadBuilder, topicManager, producer, config);

        byte[] row = Bytes.toBytes("rk-ensure");
        long ts = 123L;
        List<Cell> cells = new java.util.ArrayList<>();
        cells.add(new org.apache.hadoop.hbase.KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value_long"), ts, Bytes.toBytes(1L)));
        Entry entry = builder().buildWalEntry(TableName.valueOf("INT_TEST_TABLE"), row, cells);

        try (BatchSender sender = new BatchSender(1, 5_000)) {
            processor.process(entry, sender);
        }

    assertTrue(ensureAttemptedLatch.await(1, TimeUnit.SECONDS),
        "ensureTopic обязан стартовать асинхронно в рамках теста");

        assertEquals(1, producer.history().size(), "Сообщение должно быть отправлено несмотря на ошибку ensure");
        assertTrue(ensureAttempts.get() > 0, "ensureTopic обязан вызываться хотя бы один раз");
        RuntimeException failure = ensureFailure.get();
        assertNotNull(failure, "Исключение ensureTopic должно фиксироваться для контроля обработчика");
        assertTrue(failure instanceof IllegalStateException, "Тип исключения ensureTopic должен оставаться IllegalStateException");
        assertTrue(failure.getMessage().contains("Симуляция сбоя ensure топика"),
                "Сообщение исключения должно помогать диагностировать проблему ensure");
    }
}
