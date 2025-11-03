package kz.qazmarka.h2k.payload.serializer.avro;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfigBuilder;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.TestRawDecoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Тесты для {@link ConfluentAvroPayloadSerializer}: регистрация схем, кеширование,
 * обработка сбоев Schema Registry и поддержка BinarySlice.
 */
class ConfluentAvroPayloadSerializerTest extends BaseSerializerTest {

        /** Тестовый URL Schema Registry, используемый в сценариях без аутентификации. */
        private static final String DEFAULT_SR_URL = "http://mock-sr";
        /** Имя поля с числовым значением, присутствующим в тестовых записях. */
        private static final String FIELD_VALUE_LONG = "value_long";
        /** Имя временного поля события, ожидаемого в Avro-записях. */
        private static final String FIELD_EVENT_TS = "_event_ts";
        /** Имя бинарного поля, задействованного в проверке BinarySlice. */
        private static final String FIELD_PAYLOAD = "payload";

    @Test
    @DisplayName("serialize(): регистрирует схему один раз и возвращает байты Confluent формата")
    void serializeRegistersSchemaAndCachesWriter() {
        MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();

        H2kConfig cfg = builder()
                .withRegistryUrls(Collections.singletonList(DEFAULT_SR_URL))
                .withClientProperties(Collections.singletonMap("client.cache.capacity", "1000"))
                .buildConfig();

        TestContext ctx = buildTestContext();
        try (ConfluentAvroPayloadSerializer serializer = new ConfluentAvroPayloadSerializer(
                cfg.getAvroSettings(),
                ctx.localRegistry,
                mockClient)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            assertEquals(Collections.singletonList(DEFAULT_SR_URL), serializer.registryUrlsForTest(),
                    "список URL должен совпадать с настройками");
            assertEquals(1000, serializer.identityMapCapacityForTest(),
                    "ёмкость кеша клиента Schema Registry должна совпадать");
            assertTrue(serializer.clientConfigForTest().isEmpty(),
                    "для сценария без аутентификации конфигурация клиента должна быть пустой");

            byte[] payload1 = serializer.serialize(ctx.table, ctx.avroRecord);
            byte[] payload2 = serializer.serialize(ctx.table, ctx.avroRecord);

            assertNotNull(payload1);
            assertArrayEquals(payload1, payload2, "одинаковые записи должны сериализоваться детерминированно");

            List<String> subjects = new ArrayList<>(assertDoesNotThrow(mockClient::getAllSubjects));
            assertEquals(1, subjects.size(), "ожидается ровно один subject");
            assertEquals("default:INT_TEST_TABLE", subjects.get(0), "subject должен совпадать с именем таблицы");

            int schemaId = assertConfluentFormat(payload1);

            ByteBuffer buffer = ByteBuffer.wrap(payload1);
            buffer.position(5); // skip magic byte (1) + schemaId (4)
            byte[] avroBytes = new byte[buffer.remaining()];
            buffer.get(avroBytes);

            Schema remoteSchema = assertDoesNotThrow(() -> mockClient.getById(schemaId));
            GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(remoteSchema);
            GenericData.Record restored = assertDoesNotThrow(
                    () -> reader.read(null, DecoderFactory.get().binaryDecoder(avroBytes, null)));
            assertEquals("rk-1", restored.get("id").toString());
            assertEquals(42L, restored.get(FIELD_VALUE_LONG));
            assertEquals(123L, restored.get(FIELD_EVENT_TS));

            long successDelta = metricsRegistered(serializer) - successBefore;
            long failureDelta = metricsFailures(serializer) - failureBefore;
            assertEquals(1, successDelta, "ожидается успешная регистрация схемы");
            assertEquals(0, failureDelta, "ошибок регистрации быть не должно");
        }
    }

    @Test
    @DisplayName("Конструктор формирует basic-auth конфигурацию клиента Schema Registry")
    void constructorBuildsBasicAuthConfiguration() {
        H2kConfig cfg = builder()
                .withRegistryUrls(Collections.singletonList(DEFAULT_SR_URL))
                .withAuth("user", "pass")
                .withClientProperties(Collections.singletonMap("client.cache.capacity", "16"))
                .buildConfig();

        TestContext ctx = buildTestContext();
        try (ConfluentAvroPayloadSerializer serializer = new ConfluentAvroPayloadSerializer(
                cfg.getAvroSettings(),
                ctx.localRegistry,
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
        TestContext ctx = buildTestContext();

        RecordingSchemaRegistryClient client = builder()
                .withCacheCapacity(32)
                .withPredefinedMetadata(42, 3, ctx.schema)
                .buildMockClient();

        try (ConfluentAvroPayloadSerializer serializer = builder()
                .withCacheCapacity(32)
                .withLocalRegistry(ctx.localRegistry)
                .buildSerializer(client)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            byte[] payload1 = serializer.serialize(ctx.table, ctx.avroRecord);
            byte[] payload2 = serializer.serialize(ctx.table, ctx.avroRecord);

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

        TestContext ctx = buildTestContext();

        try (ConfluentAvroPayloadSerializer serializer = builder()
                .withCacheCapacity(8)
                .withLocalRegistry(ctx.localRegistry)
                .buildSerializer(client)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            GenericData.Record avroRecord = builder()
                    .withRecordField("id", "rk-err")
                    .withRecordField(FIELD_VALUE_LONG, 7L)
                    .withRecordField(FIELD_EVENT_TS, 77L)
                    .buildAvroRecord(ctx.schema);

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> serializer.serialize(ctx.table, avroRecord),
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
        TestContext ctx = buildTestContext();

        RecordingSchemaRegistryClient client = builder()
                .withPredefinedMetadata(10, 1, ctx.schema)
                .buildMockClient();

        try (ConfluentAvroPayloadSerializer serializer = builder()
                .withLocalRegistry(ctx.localRegistry)
                .buildSerializer(client)) {

            GenericData.Record original = builder().buildAvroRecord(ctx.schema);
            serializer.serialize(ctx.table, original);

            Schema clonedSchema = new Schema.Parser().parse(ctx.schema.toString());
            GenericData.Record mismatched = new GenericData.Record(clonedSchema);
            mismatched.put("id", "rk-ok");
            mismatched.put(FIELD_VALUE_LONG, 1L);
            mismatched.put(FIELD_EVENT_TS, 2L);

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> serializer.serialize(ctx.table, mismatched));
            assertTrue(ex.getMessage().contains("неожиданную схему"),
                    "ожидался текст про неожиданную схему");
            assertEquals(1, client.registerCalls(),
                    "повторная попытка регистрации схемы не требуется при ошибке валидации");
        }
    }

    @Test
    @DisplayName("serialize(): ошибки writer'а Avro заворачиваются в IllegalStateException")
    void serializeWrapsDatumWriterFailures() {
        TestContext ctx = buildTestContext();

        RecordingSchemaRegistryClient client = builder()
                .withCacheCapacity(12)
                .withPredefinedMetadata(11, 4, ctx.schema)
                .buildMockClient();

        try (ConfluentAvroPayloadSerializer serializer = builder()
                .withCacheCapacity(12)
                .withLocalRegistry(ctx.localRegistry)
                .buildSerializer(client)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            GenericData.Record broken = new GenericData.Record(ctx.schema);
            broken.put("id", 123); // ожидалась строка
            broken.put(FIELD_VALUE_LONG, 1L);
            broken.put(FIELD_EVENT_TS, 2L);

            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> serializer.serialize(ctx.table, broken));
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
        TestContext ctx = buildTestContext();

        RecordingSchemaRegistryClient client = builder()
                .withPredefinedMetadata(91, 5, ctx.schema)
                .buildMockClient();
        client.failRegisterWith(new RestClientException("temporary SR issue", 503, 50301), 1);

        GenericData.Record avroRecord = builder()
                .withRecordField("id", "rk-cache")
                .withRecordField(FIELD_VALUE_LONG, 9L)
                .withRecordField(FIELD_EVENT_TS, 99L)
                .buildAvroRecord(ctx.schema);

        ConfluentAvroPayloadSerializer.RetrySettings retrySettings =
                ConfluentAvroPayloadSerializer.RetrySettings.forTests(5, 10, 3);

        try (ConfluentAvroPayloadSerializer serializer = builder()
                .withLocalRegistry(ctx.localRegistry)
                .buildSerializer(client, retrySettings)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            byte[] payload = assertDoesNotThrow(() -> serializer.serialize(ctx.table, avroRecord));
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
        TestContext ctx = buildTestContext();

        RecordingSchemaRegistryClient client = builder()
                .withPredefinedMetadata(77, 2, ctx.schema)
                .buildMockClient();
        client.failRegisterWith(new RestClientException("SR offline", 503, 50302));

        GenericData.Record avroRecord = builder()
                .withRecordField("id", "rk-retry")
                .withRecordField(FIELD_VALUE_LONG, 11L)
                .withRecordField(FIELD_EVENT_TS, 111L)
                .buildAvroRecord(ctx.schema);

        ConfluentAvroPayloadSerializer.RetrySettings retrySettings =
                ConfluentAvroPayloadSerializer.RetrySettings.forTests(5, 10, 3);

        try (ConfluentAvroPayloadSerializer serializer = builder()
                .withLocalRegistry(ctx.localRegistry)
                .buildSerializer(client, retrySettings)) {

            long successBefore = metricsRegistered(serializer);
            long failureBefore = metricsFailures(serializer);

            byte[] payload = assertDoesNotThrow(() -> serializer.serialize(ctx.table, avroRecord));
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
                .schemaRegistryUrls(Collections.singletonList(DEFAULT_SR_URL))
                .done()
                .build();

        Decoder decoder = new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                if (FIELD_PAYLOAD.equalsIgnoreCase(qualifier)) {
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
                if (FIELD_PAYLOAD.equalsIgnoreCase(name)) {
                    return null;
                }
                return TestRawDecoder.INSTANCE.decode(table, qual, qOff, qLen, value, vOff, vLen);
            }

            @Override
            public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
                out.put("id", Bytes.toString(rk.getArray(), rk.getOffset(), rk.getLength()));
            }
        };

        try (PayloadBuilder builder = new PayloadBuilder(decoder, cfg, client)) {
            TableName table = TableName.valueOf("default", "T_ROW");
            byte[] row = Bytes.toBytes("rk-1");
            byte[] family = Bytes.toBytes("data");
            byte[] payload = "binary-data".getBytes(StandardCharsets.UTF_8);
            Cell payloadCell = new KeyValue(row, family, Bytes.toBytes(FIELD_PAYLOAD), 123L, payload);

            byte[] serialized = builder.buildRowPayloadBytes(
                    table,
                    Collections.singletonList(payloadCell),
                    RowKeySlice.whole(row),
                    1L,
                    2L);

            assertNotNull(serialized, "Сериализация BinarySlice не должна возвращать null");

            ByteBuffer payloadBuffer = deserializePayload(serialized, client, FIELD_PAYLOAD);
            assertEquals(payload.length, payloadBuffer.remaining(), "Размер payload должен совпадать");
            byte[] restored = new byte[payloadBuffer.remaining()];
            payloadBuffer.duplicate().get(restored);
            assertEquals("binary-data", new String(restored, StandardCharsets.UTF_8));
        }
    }
}
