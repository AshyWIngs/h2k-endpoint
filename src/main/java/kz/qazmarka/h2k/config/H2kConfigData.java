package kz.qazmarka.h2k.config;

import java.util.Objects;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;

/**
 * Компактный снимок секций конфигурации, подготавливаемый загрузчиком перед созданием {@link H2kConfig}.
 * Содержит плоские DTO без бизнес-логики, что упрощает тестирование и переиспользование секций.
 */
final class H2kConfigData {
    final String bootstrap;
    final TopicNamingSettings topic;
    final AvroSettings avro;
    final EnsureSettings ensure;
    final ProducerAwaitSettings producer;
    final PhoenixTableMetadataProvider metadataProvider;
    final boolean observersEnabled;

    H2kConfigData(String bootstrap,
                  TopicNamingSettings topic,
                  AvroSettings avro,
                  EnsureSettings ensure,
                  ProducerAwaitSettings producer,
                  PhoenixTableMetadataProvider metadataProvider,
                  boolean observersEnabled) {
        this.bootstrap = Objects.requireNonNull(bootstrap, "bootstrap не может быть null");
        this.topic = Objects.requireNonNull(topic, "topic не может быть null");
        this.avro = Objects.requireNonNull(avro, "avro не может быть null");
        this.ensure = Objects.requireNonNull(ensure, "ensure не может быть null");
        this.producer = Objects.requireNonNull(producer, "producer не может быть null");
        this.metadataProvider = metadataProvider;
        this.observersEnabled = observersEnabled;
    }
}
