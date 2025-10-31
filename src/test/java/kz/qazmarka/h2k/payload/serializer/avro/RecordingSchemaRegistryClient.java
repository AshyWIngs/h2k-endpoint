package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Mock реализация Schema Registry Client с возможностью записи статистики вызовов
 * и симуляции различных сценариев ошибок для тестов.
 */
final class RecordingSchemaRegistryClient extends MockSchemaRegistryClient {
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
