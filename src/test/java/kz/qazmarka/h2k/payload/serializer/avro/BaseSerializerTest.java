package kz.qazmarka.h2k.payload.serializer.avro;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfigBuilder;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Базовый класс для тестов {@link ConfluentAvroPayloadSerializer}.
 * Содержит общие утилиты и helper методы для устранения дублирования.
 */
abstract class BaseSerializerTest {

    protected static final Path SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath();

    /**
     * Создаёт конфигурацию с заданной ёмкостью кеша.
     */
    protected H2kConfig configWithCacheCapacity(int capacity) {
        Map<String, String> props = new HashMap<>();
        props.put("client.cache.capacity", String.valueOf(capacity));
        
        return new H2kConfigBuilder("mock:9092")
                .avro()
                .schemaDir(SCHEMA_DIR.toString())
                .schemaRegistryUrls(Collections.singletonList("http://mock-sr"))
                .properties(props)
                .done()
                .build();
    }

    /**
     * Возвращает значение метрики успешных регистраций схем.
     */
    protected long metricsRegistered(ConfluentAvroPayloadSerializer serializer) {
        return serializer.metrics().registeredSchemas();
    }

    /**
     * Возвращает значение метрики ошибок регистрации схем.
     */
    protected long metricsFailures(ConfluentAvroPayloadSerializer serializer) {
        return serializer.metrics().registrationFailures();
    }

    /**
     * Ожидает достижения заданного количества вызовов register().
     */
    protected void awaitRegisterCalls(RecordingSchemaRegistryClient client, int expected, long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (client.registerCalls() < expected) {
            if (System.nanoTime() > deadline) {
                fail(String.format("Ожидалось %d вызовов register(), но было %d за %dms",
                        expected, client.registerCalls(), timeoutMs));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    /**
     * Ожидает увеличения метрики успешных регистраций на заданное значение.
     */
    protected long awaitRegisteredDelta(ConfluentAvroPayloadSerializer serializer,
                                       long baseBefore,
                                       int expectedDelta,
                                       long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (true) {
            long current = metricsRegistered(serializer);
            long delta = current - baseBefore;
            if (delta >= expectedDelta) {
                return delta;
            }
            if (System.nanoTime() > deadline) {
                fail(String.format("Ожидалось delta=%d в метрике registered, но было %d за %dms",
                        expectedDelta, delta, timeoutMs));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    /**
     * Вспомогательный метод для пауз в тестах.
     */
    protected void sleepMs(long ms) {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(ms));
    }
    
    /**
     * Проверяет что payload соответствует Confluent формату и возвращает schemaId.
     */
    protected int assertConfluentFormat(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        assertEquals(0, buffer.get(), "первый байт — magic byte");
        int schemaId = buffer.getInt();
        assertTrue(schemaId > 0, "schemaId должен быть > 0");
        return schemaId;
    }

    /**
     * Десериализует Confluent Avro payload и возвращает значение указанного поля.
     */
    protected ByteBuffer deserializePayload(byte[] confluentPayload,
                                           RecordingSchemaRegistryClient client,
                                           String field) {
        return assertDoesNotThrow(() -> {
            ByteBuffer buffer = ByteBuffer.wrap(confluentPayload);
            assertEquals(0, buffer.get(), "ожидается magic byte Confluent");
            int schemaId = buffer.getInt();
            Schema schema = client.getById(schemaId);
            byte[] avroBytes = new byte[buffer.remaining()];
            buffer.get(avroBytes);
            GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            GenericData.Record restored = reader.read(null, DecoderFactory.get().binaryDecoder(avroBytes, null));
            return (ByteBuffer) restored.get(field);
        });
    }

    /**
     * Создаёт новый builder для упрощённого построения тестовых данных.
     */
    protected SerializerTestBuilder builder() {
        return new SerializerTestBuilder();
    }
    
    /**
     * Создаёт TableName с записью и схемой для типичного теста.
     */
    protected TestContext buildTestContext() {
        AvroSchemaRegistry localRegistry = builder().buildLocalRegistry();
        TableName table = builder().buildTableName();
        Schema schema = localRegistry.getByTable(table.getQualifierAsString());
        GenericData.Record avroRecord = builder().buildAvroRecord(schema);
        
        return new TestContext(localRegistry, table, schema, avroRecord);
    }
    
    /**
     * Вспомогательный класс для хранения компонентов теста.
     */
    protected static class TestContext {
        public final AvroSchemaRegistry localRegistry;
        public final TableName table;
        public final Schema schema;
        public final GenericData.Record avroRecord;
        
        TestContext(AvroSchemaRegistry localRegistry, TableName table, 
                   Schema schema, GenericData.Record avroRecord) {
            this.localRegistry = localRegistry;
            this.table = table;
            this.schema = schema;
            this.avroRecord = avroRecord;
        }
    }
}
