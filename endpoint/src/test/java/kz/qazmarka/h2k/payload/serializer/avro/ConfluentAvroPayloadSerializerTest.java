package kz.qazmarka.h2k.payload.serializer.avro;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Тесты для {@link ConfluentAvroPayloadSerializer}: регистрация схем, кеширование и передача настроек клиента.
 */
class ConfluentAvroPayloadSerializerTest {

    private static final Path SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath();

    @Test
    @DisplayName("serialize(): регистрирует схему один раз и возвращает байты Confluent формата")
    void serializeRegistersSchemaAndCachesWriter() throws Exception {
        MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();
        SchemaRegistryClientFactory factory = (urls, clientConfig, identityMapCapacity) -> {
            assertEquals(Collections.singletonList("http://mock-sr"), urls, "список SR URL");
            assertEquals(1000, identityMapCapacity);
            return mockClient;
        };

        H2kConfig cfg = new H2kConfig.Builder("mock:9092")
                .avro()
                .schemaDir(SCHEMA_DIR.toString())
                .schemaRegistryUrls(Collections.singletonList("http://mock-sr"))
                .properties(Collections.singletonMap("client.cache.capacity", "1000"))
                .done()
                .build();

        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        ConfluentAvroPayloadSerializer serializer = new ConfluentAvroPayloadSerializer(cfg, factory, localRegistry);

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

        List<String> subjects = new ArrayList<>(mockClient.getAllSubjects());
        assertEquals(1, subjects.size(), "ожидается ровно один subject");
        String subject = subjects.get(0);
        assertEquals("default:INT_TEST_TABLE", subject, "subject должен совпадать с именем таблицы");

        ByteBuffer buffer = ByteBuffer.wrap(payload1);
        assertEquals(0, buffer.get(), "первый байт — magic byte");
        int schemaId = buffer.getInt();
        assertTrue(schemaId > 0, "schemaId должен быть > 0");

        byte[] avroBytes = new byte[buffer.remaining()];
        buffer.get(avroBytes);

        Schema remoteSchema = mockClient.getById(schemaId);
        GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(remoteSchema);
        GenericData.Record restored = reader.read(null, DecoderFactory.get().binaryDecoder(avroBytes, null));

        assertEquals("rk-1", String.valueOf(restored.get("id")));
        assertEquals(42L, restored.get("value_long"));
        assertEquals(123L, restored.get("_event_ts"));
        assertEquals(0, serializer.metrics().registrationFailures());
    }

    @Test
    @DisplayName("Конструктор передаёт basic-auth в фабрику клиента Schema Registry")
    void factoryReceivesAuthConfiguration() {
        Map<String, String> auth = new HashMap<>();
        auth.put("basic.username", "user");
        auth.put("basic.password", "pass");

        Map<String, String> props = new HashMap<>();
        props.put("client.cache.capacity", "16");

        Map<String, Object> capturedConfig = new HashMap<>();

        SchemaRegistryClientFactory factory = (urls, clientConfig, identityMapCapacity) -> {
            capturedConfig.putAll(clientConfig);
            assertEquals(Collections.singletonList("http://mock-sr"), urls);
            assertEquals(16, identityMapCapacity);
            return new MockSchemaRegistryClient();
        };

        H2kConfig cfg = new H2kConfig.Builder("mock:9092")
                .avro()
                .schemaDir(SCHEMA_DIR.toString())
                .schemaRegistryUrls(Collections.singletonList("http://mock-sr"))
                .schemaRegistryAuth(auth)
                .properties(props)
                .done()
                .build();

        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(SCHEMA_DIR);
        ConfluentAvroPayloadSerializer created = new ConfluentAvroPayloadSerializer(cfg, factory, localRegistry);
        assertNotNull(created.metrics(), "метрики сериализатора должны быть доступны");

        assertEquals("USER_INFO", capturedConfig.get(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE));
        assertEquals("user:pass", capturedConfig.get(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG));
        assertEquals("user:pass", capturedConfig.get("schema.registry.basic.auth.user.info"));
    }
}
