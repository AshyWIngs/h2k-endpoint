package kz.qazmarka.h2k.endpoint;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer.EnsureDelegate;
import kz.qazmarka.h2k.kafka.serializer.RowKeySliceSerializer;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Билдер тестовых данных для создания компонентов KafkaReplicationEndpoint.
 */
class KafkaReplicationEndpointTestBuilder {

    private static final Path SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath();
    
    private final String bootstrapServers = "mock:9092";
    private final String schemaRegistryUrls = "http://mock-sr";
    private final TableName tableName = TableName.valueOf("INT_TEST_TABLE");
    private final int capacityHint = 4;
    private final String columnFamilies = "d";
    private final String primaryKeyColumns = "id";
    
    
    /**
     * Создаёт Configuration с заданными параметрами.
     */
    Configuration buildConfiguration() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", bootstrapServers);
        cfg.set("h2k.avro.schema.dir", SCHEMA_DIR.toString());
        cfg.set("h2k.avro.sr.urls", schemaRegistryUrls);
        return cfg;
    }
    
    /**
     * Создаёт PhoenixTableMetadataProvider.
     */
    PhoenixTableMetadataProvider buildMetadataProvider() {
        return PhoenixTableMetadataProvider.builder()
                .table(tableName)
                .capacityHint(capacityHint)
                .columnFamilies(columnFamilies)
                .primaryKeyColumns(primaryKeyColumns)
                .done()
                .build();
    }
    
    /**
     * Создаёт H2kConfig с заданными параметрами.
     */
    H2kConfig buildConfig() {
        return H2kConfig.from(buildConfiguration(), bootstrapServers, buildMetadataProvider());
    }
    
    /**
     * Создаёт H2kConfig без метаданных (для тестов ошибок).
     */
    H2kConfig buildConfigWithoutMetadata() {
        return H2kConfig.from(buildConfiguration(), bootstrapServers);
    }
    
    /**
     * Создаёт стандартный Decoder для тестов.
     */
    Decoder buildDecoder() {
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
    
    /**
     * Создаёт MockSchemaRegistryClient.
     */
    MockSchemaRegistryClient buildMockSchemaRegistryClient() {
        return new MockSchemaRegistryClient();
    }
    
    /**
     * Создаёт PayloadBuilder с заданными параметрами.
     */
    PayloadBuilder buildPayloadBuilder(H2kConfig config, MockSchemaRegistryClient client) {
        return new PayloadBuilder(buildDecoder(), config, client);
    }
    
    /**
     * Создаёт MockProducer для Kafka.
     */
    MockProducer<RowKeySlice, byte[]> buildMockProducer() {
        return new MockProducer<>(true, new RowKeySliceSerializer(), new ByteArraySerializer());
    }
    
    /**
     * Создаёт TopicManager с опциональным TopicEnsurer.
     */
    TopicManager buildTopicManager(H2kConfig config) {
        return new TopicManager(config.getTopicSettings(), TopicEnsurer.disabled());
    }
    
    /**
     * Создаёт TopicManager с failing TopicEnsurer для тестов ошибок.
     * Позволяет передать latch, который будет разблокирован при первой попытке ensure,
     * чтобы ожидать асинхронный вызов без использования Thread.sleep.
     */
    TopicManager buildTopicManagerWithFailingEnsurer(H2kConfig config,
                                                     AtomicInteger ensureAttempts,
                                                     AtomicReference<RuntimeException> failureRef,
                                                     CountDownLatch ensureAttemptedLatch) {
        TopicEnsurer failingEnsurer = makeFailingEnsurer(ensureAttempts, failureRef, ensureAttemptedLatch);
        return new TopicManager(config.getTopicSettings(), failingEnsurer);
    }
    
    /**
     * Создаёт WalEntryProcessor.
     */
    WalEntryProcessor buildWalEntryProcessor(PayloadBuilder payloadBuilder,
                                              TopicManager topicManager,
                                              MockProducer<RowKeySlice, byte[]> producer,
                                              H2kConfig config) {
        return new WalEntryProcessor(payloadBuilder, topicManager, producer, config);
    }
    
    /**
     * Создаёт WAL.Entry с заданными параметрами.
     */
    WAL.Entry buildWalEntry(TableName table, byte[] row, List<Cell> cells) {
        WALEdit edit = new WALEdit();
        for (Cell cell : cells) {
            edit.add(cell);
        }
        WALKey key = new WALKey(row, table, System.currentTimeMillis());
        return new WAL.Entry(key, edit);
    }
    
    /**
     * Декодирует Confluent Avro из байтового массива.
     */
    GenericData.Record decodeAvro(MockSchemaRegistryClient client, byte[] value) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        assertEquals(0, buffer.get(), "Confluent payload обязан начинаться с magic byte");
        int schemaId = buffer.getInt();
        byte[] avroBytes = new byte[buffer.remaining()];
        buffer.get(avroBytes);

        Schema schema = client.getById(schemaId);
        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
        return reader.read(null, DecoderFactory.get().binaryDecoder(avroBytes, null));
    }
    
    /**
     * Создаёт failing TopicEnsurer для тестов ошибок.
     */
    private static TopicEnsurer makeFailingEnsurer(AtomicInteger ensureAttempts,
                                                   AtomicReference<RuntimeException> failureRef,
                                                   CountDownLatch ensureAttemptedLatch) {
        EnsureDelegate failingDelegate = topic -> {
            if (ensureAttempts != null) {
                ensureAttempts.incrementAndGet();
            }
            if (ensureAttemptedLatch != null) {
                ensureAttemptedLatch.countDown();
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
