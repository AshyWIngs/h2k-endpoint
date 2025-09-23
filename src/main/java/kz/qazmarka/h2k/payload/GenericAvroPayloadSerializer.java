package kz.qazmarka.h2k.payload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.AvroSchemaRegistry;

/**
 * Сериализатор Avro для режима {@code generic}: читает локальные {@code *.avsc}
 * и кэширует готовые сериализаторы по таблицам.
 *
 * Основные характеристики
 *  - Потокобезопасен: использует {@link ConcurrentHashMap} для кэша.
 *  - Минимум аллокаций: повторно использует {@link AvroSerializer}, который
 *    сам хранит буферы в {@code ThreadLocal}.
 *  - Поддерживает вывод в бинарном формате Avro и в JSON (авро-вариант).
 */
final class GenericAvroPayloadSerializer implements TableAwarePayloadSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(GenericAvroPayloadSerializer.class);

    /** Поддерживаемые варианты вывода. */
    enum Encoding { BINARY, JSON }

    private final AvroSchemaRegistry registry;
    private final ConcurrentHashMap<String, AvroSerializer> serializerByTable = new ConcurrentHashMap<>();
    private final Encoding encoding;
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    GenericAvroPayloadSerializer(H2kConfig cfg, Encoding encoding) {
        Objects.requireNonNull(cfg, "cfg");
        if (cfg.getAvroMode() != H2kConfig.AvroMode.GENERIC) {
            throw new IllegalStateException("Avro: режим '" + cfg.getAvroMode() + "' не поддерживается этим сериализатором");
        }
        this.encoding = Objects.requireNonNull(encoding, "encoding");

        final String dir = cfg.getAvroSchemaDir();
        Path baseDir;
        if (dir == null || dir.trim().isEmpty()) {
            baseDir = Paths.get("conf", "avro");
        } else {
            baseDir = Paths.get(dir.trim());
        }
        this.registry = new AvroSchemaRegistry(baseDir);
        LOG.debug("Avro: режим generic, каталог схем={}, абсолютный путь={}", baseDir, baseDir.toAbsolutePath());
    }

    @Override
    public byte[] serialize(TableName table, Map<String, ?> obj) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(obj, "payload");
        final String tableKey = table.getNameAsString();

        AvroSerializer serializer = serializerByTable.get(tableKey);
        if (serializer == null) {
            serializer = createSerializer(tableKey);
            AvroSerializer prev = serializerByTable.putIfAbsent(tableKey, serializer);
            if (prev != null) {
                serializer = prev; // гонка: используем существующий
            }
        }

        if (encoding == Encoding.BINARY) {
            return serializer.serialize(obj);
        }
        return serializeJson(serializer, obj, tableKey);
    }

    private AvroSerializer createSerializer(String tableKey) {
        Schema schema;
        try {
            schema = registry.getByTable(tableKey);
        } catch (IllegalStateException ex) {
            throw new IllegalStateException(
                    "Avro: не удалось загрузить схему для таблицы '" + tableKey + "': " + ex.getMessage(), ex);
        }
        LOG.debug("Avro: схема загружена для таблицы='{}', количество полей={}.", tableKey, schema.getFields().size());
        return new AvroSerializer(schema);
    }

    private byte[] serializeJson(AvroSerializer serializer, Map<String, ?> obj, String tableKey) {
        Schema schema;
        try {
            schema = registry.getByTable(tableKey);
        } catch (IllegalStateException ex) {
            throw new IllegalStateException(
                    "Avro: не удалось загрузить схему для таблицы '" + tableKey + "': " + ex.getMessage(), ex);
        }

        final byte[] binaryPayload = serializer.serialize(obj);
        final Decoder decoder = decoderFactory.binaryDecoder(binaryPayload, null);
        final org.apache.avro.generic.GenericDatumReader<GenericRecord> reader =
                new org.apache.avro.generic.GenericDatumReader<>(schema);
        final GenericRecord genericRecord;
        try {
            genericRecord = reader.read(null, decoder);
        } catch (IOException io) {
            throw new IllegalStateException("Avro: не удалось декодировать запись перед выводом JSON: " + io.getMessage(), io);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
        try {
            Encoder jsonEncoder = encoderFactory.jsonEncoder(schema, baos);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            writer.write(genericRecord, jsonEncoder);
            jsonEncoder.flush();
        } catch (IOException io) {
            throw new IllegalStateException("Avro: ошибка сериализации в JSON: " + io.getMessage(), io);
        }
        baos.write(0x0A); // перевод строки для совместимости с JSONEachRow
        return baos.toByteArray();
    }

    @Override
    public String format() {
        return encoding == Encoding.BINARY ? "avro-binary" : "avro-json";
    }

    @Override
    public String contentType() {
        return encoding == Encoding.BINARY ? "application/avro-binary" : "application/json";
    }
}
