package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfigBuilder;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.TestRawDecoder;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Тесты для {@link ConfluentAvroPayloadSerializer}: регистрация схем, кеширование,
 * обработка сбоев Schema Registry и поддержка BinarySlice.
 */
class ConfluentAvroPayloadSerializerTest {

    private static final Path SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath();

    @Test
    @DisplayName("serialize(): регистрирует схему один раз и возвращает байты Confluent формата")
    void serializeRegistersSchemaAndCachesWriter() {
        MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();

        H2kConfig cfg = new H2kConfigBuilder("mock:9092")
                .avro()
                .schemaDir(SCHEMA_DIR.toString())
                .schemaRegistryUrls(Collections.singletonList("http://mock-sr"))
                .properties(Collections.singletonMap("client.cache.capacity", "1000"))
                .done()
                .build();

        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        try (ConfluentAvroPayloadSerializer serializer = new ConfluentAvroPayloadSerializer(
                cfg.getAvroSettings(),
                localRegistry,
                mockClient)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            assertEquals(Collections.singletonList("http://mock-sr"), serializer.registryUrlsForTest(),
                    "список URL должен совпадать с настройками");
            assertEquals(1000, serializer.identityMapCapacityForTest(),
                    "ёмкость кеша клиента Schema Registry должна совпадать");
            assertTrue(serializer.clientConfigForTest().isEmpty(),
                    "для сценария без аутентификации конфигурация клиента должна быть пустой");

            Schema tableSchema = localRegistry.getByTable("INT_TEST_TABLE");
            GenericData.Record avroRecord = new GenericData.Record(tableSchema);
            avroRecord.put("id", "rk-1");
            avroRecord.put("value_long", 42L);
            avroRecord.put("_event_ts", 123L);

            TableName table = TableName.valueOf("INT_TEST_TABLE");

            byte[] payload1 = serializer.serialize(table, avroRecord);
            byte[] payload2 = serializer.serialize(table, avroRecord);

            assertNotNull(payload1);
            assertArrayEquals(payload1, payload2, "одинаковые записи должны сериализоваться детерминированно");

            List<String> subjects = new ArrayList<>(assertDoesNotThrow(mockClient::getAllSubjects));
            assertEquals(1, subjects.size(), "ожидается ровно один subject");
            assertEquals("default:INT_TEST_TABLE", subjects.get(0), "subject должен совпадать с именем таблицы");

            ByteBuffer buffer = ByteBuffer.wrap(payload1);
            assertEquals(0, buffer.get(), "первый байт — magic byte");
            int schemaId = buffer.getInt();
            assertTrue(schemaId > 0, "schemaId должен быть > 0");

            byte[] avroBytes = new byte[buffer.remaining()];
            buffer.get(avroBytes);

            Schema remoteSchema = assertDoesNotThrow(() -> mockClient.getById(schemaId));
            GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(remoteSchema);
            GenericData.Record restored = assertDoesNotThrow(
                    () -> reader.read(null, DecoderFactory.get().binaryDecoder(avroBytes, null)));
            assertEquals("rk-1", restored.get("id").toString());
            assertEquals(42L, restored.get("value_long"));
            assertEquals(123L, restored.get("_event_ts"));

            long successDelta = metricsRegistered(serializer) - successBefore;
            long failureDelta = metricsFailures(serializer) - failureBefore;
            assertEquals(1, successDelta, "ожидается успешная регистрация схемы");
            assertEquals(0, failureDelta, "ошибок регистрации быть не должно");
        }
    }

    @Test
    @DisplayName("Конструктор формирует basic-auth конфигурацию клиента Schema Registry")
    void constructorBuildsBasicAuthConfiguration() {
        Map<String, String> auth = new HashMap<>();
        auth.put("basic.username", "user");
        auth.put("basic.password", "pass");

        Map<String, String> props = new HashMap<>();
        props.put("client.cache.capacity", "16");

        H2kConfig cfg = new H2kConfigBuilder("mock:9092")
                .avro()
                .schemaDir(SCHEMA_DIR.toString())
                .schemaRegistryUrls(Collections.singletonList("http://mock-sr"))
                .schemaRegistryAuth(auth)
                .properties(props)
                .done()
                .build();

        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        try (ConfluentAvroPayloadSerializer serializer = new ConfluentAvroPayloadSerializer(
                cfg.getAvroSettings(),
                localRegistry,
                new MockSchemaRegistryClient())) {

            Map<String, Object> config = serializer.clientConfigForTest();
            assertEquals("USER_INFO", config.get(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE));
            assertEquals("user:pass", config.get(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG));
            assertEquals("user:pass", config.get("schema.registry.basic.auth.user.info"));
            assertEquals(16, serializer.identityMapCapacityForTest(),
                    "ёмкость кеша должна считываться из настроек");
        }
    }

    @Test
    @DisplayName("serialize(): повторный вызов использует кеш и учитывает метрики при успешном сравнении fingerprint")
    void serializeReusesCacheAndReportsMetrics() {
        RecordingSchemaRegistryClient client = new RecordingSchemaRegistryClient();
        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        H2kConfig cfg = configWithCacheCapacity(32);
        TableName table = TableName.valueOf("INT_TEST_TABLE");
        Schema tableSchema = localRegistry.getByTable(table.getQualifierAsString());

        client.setLatestMetadata(table.getNameWithNamespaceInclAsString(),
                new SchemaMetadata(42, 3, tableSchema.toString(false)));

        try (ConfluentAvroPayloadSerializer serializer =
                     new ConfluentAvroPayloadSerializer(cfg.getAvroSettings(), localRegistry, client)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            GenericData.Record avroRecord = new GenericData.Record(tableSchema);
            avroRecord.put("id", "rk-1");
            avroRecord.put("value_long", 42L);
            avroRecord.put("_event_ts", 123L);

            byte[] payload1 = serializer.serialize(table, avroRecord);
            byte[] payload2 = serializer.serialize(table, avroRecord);

            assertArrayEquals(payload1, payload2, "кешированный writer должен выдавать идентичные байты");
            assertEquals(1, client.registerCalls(), "register() должен сработать один раз");
            assertEquals(1, client.latestCalls(), "getLatestSchemaMetadata() вызывается один раз");

            long successDelta = metricsRegistered(serializer) - successBefore;
            long failureDelta = metricsFailures(serializer) - failureBefore;
            assertEquals(2, successDelta, "ожидаются операции сравнения fingerprint и успешная регистрация");
            assertEquals(0, failureDelta, "ошибок регистрации быть не должно");
        }
    }

    @Test
    @DisplayName("serialize(): ошибки регистрации Schema Registry пробрасываются и учитываются в метриках")
    void serializePropagatesSchemaRegistryFailures() {
        RecordingSchemaRegistryClient client = new RecordingSchemaRegistryClient();
        client.failRegisterWith(new RestClientException("SR down", 503, 50301));

        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        H2kConfig cfg = configWithCacheCapacity(8);
        TableName table = TableName.valueOf("INT_TEST_TABLE");
        Schema tableSchema = localRegistry.getByTable(table.getQualifierAsString());

        try (ConfluentAvroPayloadSerializer serializer =
                     new ConfluentAvroPayloadSerializer(cfg.getAvroSettings(), localRegistry, client)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            GenericData.Record avroRecord = new GenericData.Record(tableSchema);
            avroRecord.put("id", "rk-err");
            avroRecord.put("value_long", 7L);
            avroRecord.put("_event_ts", 77L);

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> serializer.serialize(table, avroRecord),
                    "Ожидается исключение при ошибке регистрации схемы");
            assertTrue(ex.getMessage().contains("не удалось зарегистрировать схему"),
                    "сообщение должно описывать невозможность регистрации схемы");
            assertTrue(ex.getCause() instanceof RestClientException,
                    "ожидается RestClientException в качестве первопричины");
            assertTrue(ex.getCause().getMessage().contains("SR down"),
                    "сообщение RestClientException должно содержать описание сбоя");

            long successDelta = metricsRegistered(serializer) - successBefore;
            long failureDelta = metricsFailures(serializer) - failureBefore;
            assertEquals(0, successDelta, "успешных сравнений fingerprint не было");
            assertEquals(1, failureDelta, "должна учитываться одна ошибка регистрации");
            assertEquals(1, client.registerCalls(), "попытка регистрации ожидается ровно одна");
        }
    }

    @Test
    @DisplayName("serialize(): запись с неожиданной схемой отклоняется до сериализации")
    void serializeRejectsUnexpectedSchemaInstance() {
        RecordingSchemaRegistryClient client = new RecordingSchemaRegistryClient();
        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        H2kConfig cfg = configWithCacheCapacity(16);
        TableName table = TableName.valueOf("INT_TEST_TABLE");
        Schema schema = localRegistry.getByTable(table.getQualifierAsString());
        client.setLatestMetadata(table.getNameWithNamespaceInclAsString(),
                new SchemaMetadata(10, 1, schema.toString(false)));

        try (ConfluentAvroPayloadSerializer serializer =
                     new ConfluentAvroPayloadSerializer(cfg.getAvroSettings(), localRegistry, client)) {

            GenericData.Record original = new GenericData.Record(schema);
            original.put("id", "rk-ok");
            original.put("value_long", 1L);
            original.put("_event_ts", 2L);
            serializer.serialize(table, original);

            Schema clonedSchema = new Schema.Parser().parse(schema.toString());
            GenericData.Record mismatched = new GenericData.Record(clonedSchema);
            mismatched.put("id", "rk-ok");
            mismatched.put("value_long", 1L);
            mismatched.put("_event_ts", 2L);

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> serializer.serialize(table, mismatched));
            assertTrue(ex.getMessage().contains("неожиданную схему"),
                    "ожидался текст про неожиданную схему");
            assertEquals(1, client.registerCalls(),
                    "повторная попытка регистрации схемы не требуется при ошибке валидации");
        }
    }

    @Test
    @DisplayName("serialize(): ошибки writer'а Avro заворачиваются в IllegalStateException")
    void serializeWrapsDatumWriterFailures() {
        RecordingSchemaRegistryClient client = new RecordingSchemaRegistryClient();
        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        H2kConfig cfg = configWithCacheCapacity(12);
        TableName table = TableName.valueOf("INT_TEST_TABLE");
        Schema schema = localRegistry.getByTable(table.getQualifierAsString());
        client.setLatestMetadata(table.getNameWithNamespaceInclAsString(),
                new SchemaMetadata(11, 4, schema.toString(false)));

        try (ConfluentAvroPayloadSerializer serializer =
                     new ConfluentAvroPayloadSerializer(cfg.getAvroSettings(), localRegistry, client)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            GenericData.Record broken = new GenericData.Record(schema);
            broken.put("id", 123); // ожидалась строка
            broken.put("value_long", 1L);
            broken.put("_event_ts", 2L);

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> serializer.serialize(table, broken));
            assertTrue(ex.getMessage().contains("Avro: ошибка сериализации записи"),
                    "ошибка должна быть обёрнута в IllegalStateException с диагностикой");
            assertEquals(1, client.registerCalls(),
                    "схема регистрируется единожды до попытки записи");

            long successDelta = metricsRegistered(serializer) - successBefore;
            long failureDelta = metricsFailures(serializer) - failureBefore;
            assertEquals(2, successDelta, "сравнение fingerprint и регистрация прошли до ошибки writer");
            assertEquals(0, failureDelta, "ошибок регистрации не возникло");
        }
    }

    @Test
    @DisplayName("serialize(): при кратковременном сбое Schema Registry используется кеш и планируется повторная регистрация")
    void serializeFallsBackToCachedSchemaDuringSrOutage() {
        RecordingSchemaRegistryClient client = new RecordingSchemaRegistryClient();
        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        H2kConfig cfg = configWithCacheCapacity(16);
        TableName table = TableName.valueOf("INT_TEST_TABLE");
        Schema schema = localRegistry.getByTable(table.getQualifierAsString());

        String subject = table.getNameWithNamespaceInclAsString();
        client.setLatestMetadata(subject, new SchemaMetadata(91, 5, schema.toString(false)));
        client.failRegisterWith(new RestClientException("temporary SR issue", 503, 50301), 1);

        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", "rk-cache");
        avroRecord.put("value_long", 9L);
        avroRecord.put("_event_ts", 99L);

        ConfluentAvroPayloadSerializer.RetrySettings retrySettings =
                ConfluentAvroPayloadSerializer.RetrySettings.forTests(5, 10, 3);

        try (ConfluentAvroPayloadSerializer serializer =
                     new ConfluentAvroPayloadSerializer(cfg.getAvroSettings(), localRegistry, client, retrySettings)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            byte[] payload = assertDoesNotThrow(() -> serializer.serialize(table, avroRecord));
            assertNotNull(payload, "fallback должен возвращать полезную нагрузку");
            assertEquals(1, client.registerCalls(), "первая попытка регистрации должна завершиться ошибкой");

            ByteBuffer buffer = ByteBuffer.wrap(payload);
            assertEquals(0, buffer.get(), "ожидается Confluent magic byte");
            assertEquals(91, buffer.getInt(), "fallback обязан использовать id из кэша Schema Registry");

            awaitRegisterCalls(client, 2, 500);

            long successDelta = awaitRegisteredDelta(serializer, successBefore, 2, 500);
            long failureDelta = metricsFailures(serializer) - failureBefore;
            assertEquals(2, successDelta,
                    "ожидаем успешное сравнение fingerprint и последующую повторную регистрацию");
            assertEquals(1, failureDelta, "фиксируем одну ошибку регистрации");
        }
    }

    @Test
    @DisplayName("serialize(): планировщик останавливается после достижения максимального числа повторных попыток")
    void serializeStopsRetryingAfterMaxAttempts() {
        RecordingSchemaRegistryClient client = new RecordingSchemaRegistryClient();
        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        H2kConfig cfg = configWithCacheCapacity(16);
        TableName table = TableName.valueOf("INT_TEST_TABLE");
        Schema schema = localRegistry.getByTable(table.getQualifierAsString());

        String subject = table.getNameWithNamespaceInclAsString();
        client.setLatestMetadata(subject, new SchemaMetadata(77, 2, schema.toString(false)));
        client.failRegisterWith(new RestClientException("SR offline", 503, 50302));

        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", "rk-retry");
        avroRecord.put("value_long", 11L);
        avroRecord.put("_event_ts", 111L);

        ConfluentAvroPayloadSerializer.RetrySettings retrySettings =
                ConfluentAvroPayloadSerializer.RetrySettings.forTests(5, 10, 3);

        try (ConfluentAvroPayloadSerializer serializer =
                     new ConfluentAvroPayloadSerializer(cfg.getAvroSettings(), localRegistry, client, retrySettings)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            byte[] payload = assertDoesNotThrow(() -> serializer.serialize(table, avroRecord));
            assertNotNull(payload, "fallback обязан сериализовать запись из кэша");

            awaitRegisterCalls(client, 1 + retrySettings.maxAttempts(), 500);
            sleepMs(50);
            assertEquals(1 + retrySettings.maxAttempts(), client.registerCalls(),
                    "после достижения лимита повторных попыток новые задачи не ставятся");

            long successDelta = awaitRegisteredDelta(serializer, successBefore, 1, 500);
            long failureDelta = metricsFailures(serializer) - failureBefore;
            assertEquals(1, successDelta,
                    "успешно выполнено только сравнение fingerprint при чтении метаданных");
            assertEquals(1 + retrySettings.maxAttempts(), failureDelta,
                    "каждая попытка регистрации должна учитываться как ошибка");
        }
    }

    @Test
    @DisplayName("serialize(): BinarySlice из RowPayloadAssembler сериализуется как bytes union без ошибок")
    void serializeHandlesBinarySliceFromAssembler() {
        RecordingSchemaRegistryClient client = new RecordingSchemaRegistryClient();
        H2kConfig cfg = new H2kConfigBuilder("mock:9092")
                .avro()
                .schemaDir(SCHEMA_DIR.toString())
                .schemaRegistryUrls(Collections.singletonList("http://mock-sr"))
                .done()
                .build();

        Decoder decoder = new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                if ("payload".equalsIgnoreCase(qualifier)) {
                    return null; // заставить RowPayloadAssembler использовать BinarySlice
                }
                return TestRawDecoder.INSTANCE.decode(table, qualifier, value);
            }

            @Override
            public Object decode(TableName table,
                                 byte[] qual,
                                 int qOff,
                                 int qLen,
                                 byte[] value,
                                 int vOff,
                                 int vLen) {
                String name = Bytes.toString(qual, qOff, qLen);
                if ("payload".equalsIgnoreCase(name)) {
                    return null;
                }
                return TestRawDecoder.INSTANCE.decode(table, qual, qOff, qLen, value, vOff, vLen);
            }

            @Override
            public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
                out.put("id", Bytes.toString(rk.getArray(), rk.getOffset(), rk.getLength()));
            }
        };

        PayloadBuilder builder = new PayloadBuilder(decoder, cfg, client);
        TableName table = TableName.valueOf("default", "T_ROW");
        byte[] row = Bytes.toBytes("rk-1");
        byte[] family = Bytes.toBytes("data");
        byte[] payload = "binary-data".getBytes(StandardCharsets.UTF_8);
        Cell payloadCell = new KeyValue(row, family, Bytes.toBytes("payload"), 123L, payload);

        byte[] serialized = builder.buildRowPayloadBytes(
                table,
                Collections.singletonList(payloadCell),
                RowKeySlice.whole(row),
                1L,
                2L);

        assertNotNull(serialized, "Сериализация BinarySlice не должна возвращать null");

        ByteBuffer payloadBuffer = deserializePayload(serialized, client, "payload");
        assertEquals(payload.length, payloadBuffer.remaining(), "Размер payload должен совпадать");
        byte[] restored = new byte[payloadBuffer.remaining()];
        payloadBuffer.duplicate().get(restored);
        assertEquals("binary-data", new String(restored, StandardCharsets.UTF_8));
    }

    private static H2kConfig configWithCacheCapacity(int capacity) {
        Map<String, String> props = new HashMap<>();
        props.put("client.cache.capacity", Integer.toString(capacity));
        return new H2kConfigBuilder("mock:9092")
                .avro()
                .schemaDir(SCHEMA_DIR.toString())
                .schemaRegistryUrls(Collections.singletonList("http://mock-sr"))
                .properties(props)
                .done()
                .build();
    }

    private static long metricsRegistered(ConfluentAvroPayloadSerializer serializer) {
        return serializer.metrics().registeredSchemas();
    }

    private static long metricsFailures(ConfluentAvroPayloadSerializer serializer) {
        return serializer.metrics().registrationFailures();
    }

    private static long awaitRegisteredDelta(ConfluentAvroPayloadSerializer serializer,
                                             long baseline,
                                             long expectedDelta,
                                             long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            long delta = metricsRegistered(serializer) - baseline;
            if (delta >= expectedDelta) {
                return delta;
            }
            sleepMs(5);
        }
        long delta = metricsRegistered(serializer) - baseline;
        fail("Не дождались registeredSchemas delta >= " + expectedDelta + ", текущее значение: " + delta);
        return delta;
    }

    private static void awaitRegisterCalls(RecordingSchemaRegistryClient client,
                                           int expected,
                                           long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (client.registerCalls() >= expected) {
                return;
            }
            sleepMs(5);
        }
        fail("Не дождались register() == " + expected + " за " + timeoutMs + " мс");
    }

    private static void sleepMs(long millis) {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(millis));
        if (Thread.interrupted()) {
            fail("Ожидание было прервано");
        }
    }

    private static final class RecordingSchemaRegistryClient extends MockSchemaRegistryClient {
        private final Map<String, SchemaMetadata> predefinedMetadata = new HashMap<>();
        private int registerCalls;
        private int latestCalls;
        private RestClientException registerRestException;
        private boolean alwaysFailRegister;
        private int remainingRegisterFailures;

        void setLatestMetadata(String subject, SchemaMetadata metadata) {
            predefinedMetadata.put(subject, metadata);
        }

        void failRegisterWith(RestClientException ex) {
            this.registerRestException = ex;
            this.alwaysFailRegister = true;
            this.remainingRegisterFailures = Integer.MAX_VALUE;
        }

        void failRegisterWith(RestClientException ex, int attempts) {
            this.registerRestException = ex;
            this.alwaysFailRegister = false;
            this.remainingRegisterFailures = attempts;
        }

        void clearRegisterFailure() {
            this.registerRestException = null;
            this.alwaysFailRegister = false;
            this.remainingRegisterFailures = 0;
        }

        int registerCalls() {
            return registerCalls;
        }

        int latestCalls() {
            return latestCalls;
        }

        @Override
        public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
                throws IOException, RestClientException {
            latestCalls++;
            SchemaMetadata metadata = predefinedMetadata.get(subject);
            if (metadata != null) {
                return metadata;
            }
            return super.getLatestSchemaMetadata(subject);
        }

        @Override
        public synchronized int register(String subject, Schema schema)
                throws IOException, RestClientException {
            registerCalls++;
            if (registerRestException != null) {
                if (alwaysFailRegister) {
                    throw registerRestException;
                }
                if (remainingRegisterFailures > 0) {
                    remainingRegisterFailures--;
                    RestClientException ex = registerRestException;
                    if (remainingRegisterFailures == 0) {
                        registerRestException = null;
                    }
                    throw ex;
                }
            }
            int id = super.register(subject, schema);
            predefinedMetadata.put(subject, new SchemaMetadata(id, 1, schema.toString(false)));
            clearRegisterFailure();
            return id;
        }
    }

    private static ByteBuffer deserializePayload(byte[] confluentPayload,
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
}
