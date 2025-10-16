package kz.qazmarka.h2k.payload.builder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.serializer.avro.ConfluentAvroPayloadSerializer;
import kz.qazmarka.h2k.payload.serializer.avro.SchemaRegistryClientFactory;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Управляет построением карты payload и выбором сериализатора. Вся тяжёлая логика сборки
 * вынесена в {@link RowPayloadAssembler}, что упрощает поддержку и уменьшает нагрузку на горячем пути.
 */
public final class PayloadBuilder {

    private final H2kConfig cfg;
    private final ConfluentAvroPayloadSerializer serializer;
    private final RowPayloadAssembler assembler;

    /**
     * @param decoder декодер Phoenix, предоставляющий значения колонок и PK
     * @param cfg     неизменяемая конфигурация h2k
     */
    public PayloadBuilder(Decoder decoder, H2kConfig cfg) {
        this(decoder, cfg, SchemaRegistryClientFactory.cached());
    }

    /**
     * @param decoder                     декодер Phoenix
     * @param cfg                         конфигурация h2k
     * @param schemaRegistryClientFactory фабрика клиентов Schema Registry (для режима Confluent)
     */
    public PayloadBuilder(Decoder decoder,
                          H2kConfig cfg,
                          SchemaRegistryClientFactory schemaRegistryClientFactory) {
        this.cfg = Objects.requireNonNull(cfg, "конфигурация h2k");
        Objects.requireNonNull(schemaRegistryClientFactory, "schemaRegistryClientFactory");
        AvroSchemaRegistry localRegistry = new AvroSchemaRegistry(resolveSchemaDir(cfg));
        this.serializer = new ConfluentAvroPayloadSerializer(cfg, schemaRegistryClientFactory, localRegistry);
        this.assembler = new RowPayloadAssembler(decoder, cfg, localRegistry);
    }

    /**
     * Собирает payload и сериализует результат в байты, используя сериализатор из конфигурации.
     */
    public byte[] buildRowPayloadBytes(TableName table,
                                       List<Cell> cells,
                                       RowKeySlice rowKey,
                                       long walSeq,
                                       long walWriteTime) {
        Objects.requireNonNull(table, "таблица");
        GenericData.Record avroRecord = buildRowPayload(table, cells, rowKey, walSeq, walWriteTime);
        return serializer.serialize(table, avroRecord);
    }

    /**
     * Формирует карту payload без сериализации — удобно для тестов и альтернативных сериализаторов.
     */
    public GenericData.Record buildRowPayload(TableName table,
                                              List<Cell> cells,
                                              RowKeySlice rowKey,
                                              long walSeq,
                                              long walWriteTime) {
        Objects.requireNonNull(table, "таблица");
        return assembler.assemble(table, cells, rowKey, walSeq, walWriteTime);
    }

    public long schemaRegistryRegisteredCount() {
        return serializer.metrics().registeredSchemas();
    }

    public long schemaRegistryFailedCount() {
        return serializer.metrics().registrationFailures();
    }

    /**
     * Человекочитаемое описание активного сериализатора и ключевых параметров Avro.
     */
    public String describeSerializer() {
        StringBuilder sb = new StringBuilder(160);
        sb.append("payload.format=AVRO_BINARY");
        sb.append(", serializer.class=").append(serializer.getClass().getName());
        sb.append(", schema.registry.urls=").append(cfg.getAvroSchemaRegistryUrls());
        sb.append(", schema.registry.auth=")
                .append(cfg.getAvroSrAuth().isEmpty() ? "disabled" : "configured");
        if (!cfg.getAvroProps().isEmpty()) {
            sb.append(", schema.registry.props=").append(cfg.getAvroProps().keySet());
        }
        return sb.toString();
    }

    private Path resolveSchemaDir(H2kConfig cfg) {
        String dir = cfg.getAvroSchemaDir();
        if (dir == null || dir.trim().isEmpty()) {
            return Paths.get("conf", "avro");
        }
        return Paths.get(dir.trim());
    }
}
