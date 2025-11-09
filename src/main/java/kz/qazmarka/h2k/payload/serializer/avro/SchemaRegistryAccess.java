package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.IOException;
import java.util.Objects;

import org.apache.avro.Schema;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Оборачивает вызовы {@link SchemaRegistryClient}, чтобы изолировать работу с удалённым Schema Registry
 * и облегчить рефакторинг сериализатора. Класс иммутабелен и потокобезопасен.
 */
final class SchemaRegistryAccess {

    private final SchemaRegistryClient client;

    SchemaRegistryAccess(SchemaRegistryClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    /**
     * Регистрирует схему в удалённом Schema Registry и возвращает присвоенный идентификатор.
     *
     * @param subject имя subject в Schema Registry
     * @param schema Avro-схема для регистрации
     * @return идентификатор схемы
     * @throws RestClientException при ошибке Schema Registry
     * @throws IOException при сетевых сбоях
     */
    int register(String subject, Schema schema) throws RestClientException, IOException {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(schema, "schema");
        return client.register(subject, schema);
    }

    /**
     * Возвращает последнюю зарегистрированную версию схемы для указанного subject.
     *
     * @param subject имя subject в Schema Registry
     * @return метаданные последней схемы или исключение, если схема отсутствует/недоступна
     * @throws RestClientException при ошибке Schema Registry
     * @throws IOException при сетевых сбоях
     */
    SchemaMetadata latest(String subject) throws RestClientException, IOException {
        Objects.requireNonNull(subject, "subject");
        return client.getLatestSchemaMetadata(subject);
    }
}
