package kz.qazmarka.h2k.payload.serializer.avro;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.TableName;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfigBuilder;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Билдер тестовых данных для упрощения создания тестовых данных
 * в {@link ConfluentAvroPayloadSerializerTest}.
 * Устраняет дублирование кода setup-логики.
 */
final class SerializerTestBuilder {

    private static final Path SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath();
    
    private final String tableName = "INT_TEST_TABLE";
    private int cacheCapacity = 16;
    private List<String> registryUrls = Collections.singletonList("http://mock-sr:8081");
    private Map<String, String> authConfig = Collections.emptyMap();
    private Map<String, String> clientProperties = Collections.emptyMap();
    private final Map<String, Object> recordFields = new HashMap<>();
    private final RecordingSchemaRegistryClient mockClient = null;
    private SchemaMetadata predefinedMetadata = null;
    private AvroSchemaRegistry localRegistry = null;
    
    SerializerTestBuilder() {
        // Значения по умолчанию для типичного теста
        recordFields.put("id", "rk-1");
        recordFields.put("value_long", 42L);
        recordFields.put("_event_ts", 123L);
    }
    /**
     * Устанавливает ёмкость кеша Schema Registry клиента.
     */
    SerializerTestBuilder withCacheCapacity(int capacity) {
        this.cacheCapacity = capacity;
        return this;
    }
    
    /**
     * Устанавливает список URL Schema Registry.
     */
    SerializerTestBuilder withRegistryUrls(List<String> urls) {
        this.registryUrls = urls;
        return this;
    }
    
    /**
     * Устанавливает аутентификацию для Schema Registry.
     */
    SerializerTestBuilder withAuth(String username, String password) {
        Map<String, String> auth = new HashMap<>();
        auth.put("basic.username", username);
        auth.put("basic.password", password);
        this.authConfig = auth;
        return this;
    }
    
    /**
     * Устанавливает custom AvroSchemaRegistry (вместо создания нового).
     */
    SerializerTestBuilder withLocalRegistry(AvroSchemaRegistry registry) {
        this.localRegistry = registry;
        return this;
    }
    
    /**
     * Устанавливает дополнительные свойства клиента.
     */
    SerializerTestBuilder withClientProperties(Map<String, String> props) {
        this.clientProperties = props;
        return this;
    }
    
    /**
     * Устанавливает значения полей для Avro записи.
     */
    SerializerTestBuilder withRecordField(String fieldName, Object value) {
        this.recordFields.put(fieldName, value);
        return this;
    }
    
    /**
     * Устанавливает предопределённые метаданные схемы для mock клиента.
     */
    SerializerTestBuilder withPredefinedMetadata(int schemaId, int version, Schema schema) {
        this.predefinedMetadata = new SchemaMetadata(schemaId, version, schema.toString(false));
        return this;
    }
    
    /**
     * Создаёт H2kConfig с заданными параметрами.
     */
    H2kConfig buildConfig() {
        Map<String, String> props = new HashMap<>(clientProperties);
        // Устанавливаем cacheCapacity только если не указано явно в clientProperties
        if (!props.containsKey("client.cache.capacity")) {
            props.put("client.cache.capacity", String.valueOf(cacheCapacity));
        }
        
        H2kConfigBuilder.AvroOptions avroBuilder = new H2kConfigBuilder("mock:9092")
                .avro()
                .schemaDir(SCHEMA_DIR.toString())
                .schemaRegistryUrls(registryUrls)
                .properties(props);
        
        if (!authConfig.isEmpty()) {
            avroBuilder.schemaRegistryAuth(authConfig);
        }
        
        return avroBuilder.done().build();
    }
    
    /**
     * Создаёт AvroSchemaRegistry для локального чтения схем.
     */
    AvroSchemaRegistry buildLocalRegistry() {
        if (localRegistry != null) {
            return localRegistry;
        }
        return new AvroSchemaRegistry(SCHEMA_DIR);
    }
    
    /**
     * Создаёт TableName из текущего имени таблицы.
     */
    TableName buildTableName() {
        return TableName.valueOf(tableName);
    }
    
    /**
     * Создаёт GenericData.Record с заданными полями.
     */
    GenericData.Record buildAvroRecord(Schema schema) {
        GenericData.Record avroRecord = new GenericData.Record(schema);
        recordFields.forEach(avroRecord::put);
        return avroRecord;
    }
    
    /**
     * Создаёт RecordingSchemaRegistryClient с опциональными предустановками.
     */
    RecordingSchemaRegistryClient buildMockClient() {
        if (mockClient != null) {
            return mockClient;
        }
        
        RecordingSchemaRegistryClient client = new RecordingSchemaRegistryClient();
        
        if (predefinedMetadata != null) {
            String subject = buildTableName().getNameWithNamespaceInclAsString();
            client.setLatestMetadata(subject, predefinedMetadata);
        }
        
        return client;
    }
    
    /**
     * Создаёт ConfluentAvroPayloadSerializer с заданным mock клиентом.
     */
    ConfluentAvroPayloadSerializer buildSerializer(RecordingSchemaRegistryClient client) {
        return new ConfluentAvroPayloadSerializer(
                buildConfig().getAvroSettings(),
                buildLocalRegistry(),
                client
        );
    }
    
    /**
     * Создаёт ConfluentAvroPayloadSerializer с retry settings.
     */
    ConfluentAvroPayloadSerializer buildSerializer(
            RecordingSchemaRegistryClient client,
            ConfluentAvroPayloadSerializer.RetrySettings retrySettings) {
        return new ConfluentAvroPayloadSerializer(
                buildConfig().getAvroSettings(),
                buildLocalRegistry(),
                client,
                retrySettings
        );
    }
}
