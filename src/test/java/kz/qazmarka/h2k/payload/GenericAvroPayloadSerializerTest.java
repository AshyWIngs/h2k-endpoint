package kz.qazmarka.h2k.payload;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.AvroSchemaRegistry;
import kz.qazmarka.h2k.schema.Decoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Интеграционные тесты Avro-сериализации в режиме generic.
 */
class GenericAvroPayloadSerializerTest {

    private static final TableName TABLE = TableName.valueOf("T_AVRO");
    private static final Decoder STRING_DECODER = (table, qualifier, value) ->
            value == null ? null : new String(value, StandardCharsets.UTF_8);

    private static final String SHARED_SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString();

    private static H2kConfig prepareConfig(String format) {
        Configuration conf = new Configuration(false);
        conf.set("h2k.cf.list", "d");
        conf.set("h2k.payload.format", format);
        conf.set("h2k.avro.mode", "generic");
        conf.set("h2k.avro.schema.dir", SHARED_SCHEMA_DIR);
        return H2kConfig.from(conf, "dummy:9092");
    }

    @Test
    @DisplayName("Avro generic: бинарный вывод соответствует схеме")
    void avroBinary_matchesSchema() throws Exception {
        H2kConfig configBinary = prepareConfig("avro-binary");
        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, configBinary);
        long ts = 123L;
        byte[] rowKey = Bytes.toBytes("rk1");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), ts, Bytes.toBytes("OK"));
        List<Cell> cells = Collections.singletonList(cell);
        byte[] bytes = builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), ts, ts);

        AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get(configBinary.getAvroSchemaDir()));
        Schema schema = registry.getByTable(TABLE.getNameAsString());

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        org.apache.avro.io.Decoder avroDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord avroRecord = reader.read(null, avroDecoder);

        assertEquals("OK", avroRecord.get("value").toString());
        assertEquals(ts, ((Long) avroRecord.get("event_version")).longValue());
    }

    @Test
    @DisplayName("Avro generic: JSON вывод разбирается обратным преобразованием")
    void avroJson_roundTrip() throws Exception {
        H2kConfig configJson = prepareConfig("avro-json");
        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, configJson);
        long ts = 321L;
        byte[] rowKey = Bytes.toBytes("rk2");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), ts, Bytes.toBytes("JSON"));
        List<Cell> cells = Collections.singletonList(cell);
        byte[] bytes = builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), ts, ts);

        AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get(configJson.getAvroSchemaDir()));
        Schema schema = registry.getByTable(TABLE.getNameAsString());

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        org.apache.avro.io.Decoder avroDecoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(bytes));
        GenericRecord avroRecord = reader.read(null, avroDecoder);

        assertEquals("JSON", avroRecord.get("value").toString());
        assertEquals(ts, ((Long) avroRecord.get("event_version")).longValue());
        assertEquals('\n', (char) bytes[bytes.length - 1], "JSON вывод должен заканчиваться переводом строки");
    }

    @Test
    @DisplayName("Avro generic: отсутствие схемы даёт понятную ошибку")
    void avroMissingSchemaFails() {
        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", "avro-binary");
        c.set("h2k.avro.mode", "generic");
        c.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro-missing").toAbsolutePath().toString());
        H2kConfig cfg = H2kConfig.from(c, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
        byte[] rowKey = Bytes.toBytes("rk3");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("oops"));
        List<Cell> cells = Collections.singletonList(cell);

        RowKeySlice rowKeySlice = RowKeySlice.whole(rowKey);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                builder.buildRowPayloadBytes(TABLE, cells, rowKeySlice, 1L, 1L)
        );
        assertTrue(ex.getMessage().contains("не удалось загрузить схему"));
    }

    @Test
    @DisplayName("Avro generic: тип не соответствует схеме — бросается IllegalStateException")
    void avroSchemaTypeMismatch() throws Exception {
        Path tmp = Files.createTempDirectory("avro-mismatch");
        writeSchema(tmp, "{\n"
                + "  \"type\": \"record\",\n"
                + "  \"name\": \"T_AVRO\",\n"
                + "  \"fields\": [\n"
                + "    {\"name\": \"value\", \"type\": [\"null\", \"long\"], \"default\": null}\n"
                + "  ]\n"
                + "}");

        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", "avro-binary");
        c.set("h2k.avro.mode", "generic");
        c.set("h2k.avro.schema.dir", tmp.toString());
        H2kConfig cfg = H2kConfig.from(c, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
        byte[] rowKey = Bytes.toBytes("rk-mismatch");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("NOT_A_NUMBER"));
        List<Cell> cells = Collections.singletonList(cell);

        RowKeySlice rowKeySlice = RowKeySlice.whole(rowKey);
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                builder.buildRowPayloadBytes(TABLE, cells, rowKeySlice, 1L, 1L));
        assertTrue(ex.getMessage().contains("поле"));
    }

    @Test
    @DisplayName("Avro generic: схема с optional полем поддерживается без изменений payload")
    void avroSchemaOptionalField() throws Exception {
        Path tmp = Files.createTempDirectory("avro-optional");
        writeSchema(tmp, "{\n"
                + "  \"type\": \"record\",\n"
                + "  \"name\": \"T_AVRO\",\n"
                + "  \"fields\": [\n"
                + "    {\"name\": \"value\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
                + "    {\"name\": \"extra\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
                + "  ]\n"
                + "}");

        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", "avro-binary");
        c.set("h2k.avro.mode", "generic");
        c.set("h2k.avro.schema.dir", tmp.toString());
        H2kConfig cfg = H2kConfig.from(c, "dummy:9092");

        PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
        byte[] rowKey = Bytes.toBytes("rk-optional");
        Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("OK"));
        List<Cell> cells = Collections.singletonList(cell);

        byte[] out = builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), 1L, 1L);

        Schema schema = new AvroSchemaRegistry(tmp).getByTable(TABLE.getNameAsString());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord avroRecord = reader.read(null, DecoderFactory.get().binaryDecoder(out, null));
        assertEquals("OK", avroRecord.get("value").toString());
        assertNull(avroRecord.get("extra"), "Дополнительное поле должно сериализоваться как null по умолчанию");
    }

    private static void writeSchema(Path dir, String schemaJson) throws Exception {
        Files.write(dir.resolve("t_avro.avsc"), schemaJson.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
