package kz.qazmarka.h2k.endpoint;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer.EnsureDelegate;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.payload.serializer.avro.SchemaRegistryClientFactory;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Интеграционный тест горячего пути WAL → PayloadBuilder → Kafka (Confluent Avro).
 */
class KafkaReplicationEndpointIntegrationTest {

    private static final Path SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath();

    @Test
    @DisplayName("WAL-запись сериализуется в Confluent Avro с PK и данными Phoenix")
    void walEntryProducesKafkaRecordWithPkAndColumns() throws Exception {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.schema.dir", SCHEMA_DIR.toString());
        cfg.set("h2k.avro.sr.urls", "http://mock-sr");

        PhoenixTableMetadataProvider metadata = new PhoenixTableMetadataProvider() {
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

        H2kConfig config = H2kConfig.from(cfg, "mock:9092", metadata);

        MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();
        SchemaRegistryClientFactory factory = (urls, clientConfig, identityMapCapacity) -> mockClient;

        Decoder decoder = new Decoder() {
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
            public void decodeRowKey(TableName table, RowKeySlice rowKey, int saltBytes, Map<String, Object> out) {
                assertNotNull(rowKey, "rowkey должен присутствовать");
                out.put("id", new String(rowKey.toByteArray(), StandardCharsets.UTF_8));
            }
        };

        PayloadBuilder builder = new PayloadBuilder(decoder, config, factory);
    TopicManager topicManager = new TopicManager(config.getTopicSettings(), TopicEnsurer.disabled());
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer, config);

        TableName table = TableName.valueOf("INT_TEST_TABLE");
        byte[] row = Bytes.toBytes("rk-777");
        long cellTs = 1_678_901_234L;

        List<Cell> cells = new ArrayList<>();
        cells.add(new KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value_long"), cellTs, Bytes.toBytes(42L)));

        Entry entry = walEntry(table, row, cells);

        try (BatchSender sender = new BatchSender(1, 5_000)) {
            processor.process(entry, sender, false);
        }

        List<ProducerRecord<byte[], byte[]>> history = producer.history();
        assertEquals(1, history.size(), "ожидается единственная продюсерская запись");
        ProducerRecord<byte[], byte[]> produced = history.get(0);
        assertEquals("INT_TEST_TABLE", produced.topic(), "имя топика должно совпадать с шаблоном по умолчанию");

        GenericData.Record avro = decodeAvro(mockClient, produced.value());
        assertEquals("rk-777", String.valueOf(avro.get("id")));
        assertEquals(42L, avro.get("value_long"));
        assertEquals(cellTs, avro.get("_event_ts"));

        Collection<String> subjects = mockClient.getAllSubjects();
        assertEquals(1, subjects.size(), "ожидается один subject в Schema Registry");
        String subject = subjects.iterator().next();
        assertEquals("default:INT_TEST_TABLE", subject, "subject должен совпадать с именем таблицы");
    }

    @Test
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
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.schema.dir", SCHEMA_DIR.toString());
        cfg.set("h2k.avro.sr.urls", "http://mock-sr");

        H2kConfig config = H2kConfig.from(cfg, "mock:9092");

        SchemaRegistryClientFactory failingFactory = (urls, clientConfig, capacity) -> {
            throw new IllegalStateException("SR down");
        };

        Decoder decoder = defaultDecoder();

        IllegalStateException initError = assertThrows(IllegalStateException.class,
                () -> new PayloadBuilder(decoder, config, failingFactory),
                "Ожидается ошибка инициализации при недоступном Schema Registry");
        assertNotNull(initError);
    }

    @Test
    @DisplayName("Ошибка ensureTopic не прерывает обработку WAL и не мешает отправке")
    void ensureTopicFailureDoesNotInterruptProcessing() throws Exception {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.schema.dir", SCHEMA_DIR.toString());
        cfg.set("h2k.avro.sr.urls", "http://mock-sr");

        H2kConfig config = H2kConfig.from(cfg, "mock:9092", defaultMetadataProvider());

        Decoder decoder = defaultDecoder();
        SchemaRegistryClientFactory factory = (urls, clientConfig, identityMapCapacity) -> new MockSchemaRegistryClient();
        PayloadBuilder builder = new PayloadBuilder(decoder, config, factory);

        AtomicInteger ensureAttempts = new AtomicInteger();
        AtomicReference<RuntimeException> ensureFailure = new AtomicReference<>();
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

        try (TopicEnsurer failingEnsurer = makeFailingEnsurer(ensureAttempts, ensureFailure)) {
            TopicManager topicManager = new TopicManager(config.getTopicSettings(), failingEnsurer);
            WalEntryProcessor processor = new WalEntryProcessor(builder, topicManager, producer, config);

            TableName table = TableName.valueOf("INT_TEST_TABLE");
            byte[] row = Bytes.toBytes("rk-ensure");
            long ts = 123L;
            List<Cell> cells = new ArrayList<>();
            cells.add(new KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value_long"), ts, Bytes.toBytes(1L)));
            Entry entry = walEntry(table, row, cells);

            try (BatchSender sender = new BatchSender(1, 5_000)) {
                processor.process(entry, sender, false);
            }

            assertEquals(1, producer.history().size(), "Сообщение должно быть отправлено несмотря на ошибку ensure");
        }

    assertTrue(ensureAttempts.get() > 0, "ensureTopic обязан вызываться хотя бы один раз");
        RuntimeException failure = ensureFailure.get();
        assertNotNull(failure, "Исключение ensureTopic должно фиксироваться для контроля обработчика");
        assertTrue(failure instanceof IllegalStateException, "Тип исключения ensureTopic должен оставаться IllegalStateException");
        assertTrue(failure.getMessage().contains("Симуляция сбоя ensure топика"),
                "Сообщение исключения должно помогать диагностировать проблему ensure");
    }

    private static Entry walEntry(TableName table, byte[] row, List<Cell> cells) {
        WALEdit edit = new WALEdit();
        for (Cell cell : cells) {
            edit.add(cell);
        }
        WALKey key = new WALKey(row, table, System.currentTimeMillis());
        return new WAL.Entry(key, edit);
    }

    private static GenericData.Record decodeAvro(MockSchemaRegistryClient client, byte[] value) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        assertEquals(0, buffer.get(), "Confluent payload обязан начинаться с magic byte");
        int schemaId = buffer.getInt();
        byte[] avroBytes = new byte[buffer.remaining()];
        buffer.get(avroBytes);

        Schema schema = client.getById(schemaId);
        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
        return reader.read(null, DecoderFactory.get().binaryDecoder(avroBytes, null));
    }

    private static Decoder defaultDecoder() {
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
            public void decodeRowKey(TableName table, RowKeySlice rowKey, int saltBytes, Map<String, Object> out) {
                assertNotNull(rowKey, "rowkey должен присутствовать");
                out.put("id", new String(rowKey.toByteArray(), StandardCharsets.UTF_8));
            }
        };
    }

    private static PhoenixTableMetadataProvider defaultMetadataProvider() {
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
                return new String[]{"d"};
            }

            @Override
            public String[] primaryKeyColumns(TableName table) {
                return new String[]{"id"};
            }
        };
    }

    /**
     * Фабрика заглушки ensure, которая всегда бросает {@link IllegalStateException} и фиксирует факт вызова.
     * Это позволяет тесту проверять, что обработчик WAL не проглатывает ошибку ensure молча.
     */
    private static TopicEnsurer makeFailingEnsurer(AtomicInteger ensureAttempts,
                                                   AtomicReference<RuntimeException> failureRef) {
        EnsureDelegate failingDelegate = topic -> {
            if (ensureAttempts != null) {
                ensureAttempts.incrementAndGet();
            }
            IllegalStateException failure = new IllegalStateException("Симуляция сбоя ensure топика: " + topic);
            if (failureRef != null) {
                failureRef.set(failure);
            }
            throw failure;
        };
        return TopicEnsurer.testingDelegate(failingDelegate);
    }
}
