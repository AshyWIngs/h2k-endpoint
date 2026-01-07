package kz.qazmarka.h2k.payload.builder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfigBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.TestRawDecoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Юнит‑тесты {@link PayloadBuilder}: проверяем сборку payload и диагностический вывод.
 */
class PayloadBuilderTest {

    private static H2kConfig configWithSr() {
        String schemaDir = Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString();
        return new H2kConfigBuilder("bootstrap-test")
                .avro()
                .schemaDir(schemaDir)
                .schemaRegistryUrls(Collections.singletonList("http://mock"))
                .done()
                .build();
    }

    @Test
    @DisplayName("PK добавляется в payload, null-значения ячеек игнорируются")
    void pkInjectedAndNullValuesSkipped() {
        try (PayloadBuilder builder = new PayloadBuilder(new StubDecoder(), configWithSr(), new MockSchemaRegistryClient())) {

        List<Cell> cells = new ArrayList<>();
        byte[] row = Bytes.toBytes("rk-1");
        cells.add(new KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value"),
                "VAL".getBytes(StandardCharsets.UTF_8)));

        org.apache.avro.generic.GenericData.Record avroRecord = builder.buildRowPayload(
                TableName.valueOf("T_AVRO"), cells, RowKeySlice.whole(row), 0L, 0L);

        assertAll(
                () -> assertEquals("rk-1", avroRecord.get("id"), "ожидается первичный ключ"),
        () -> assertEquals("VAL", String.valueOf(avroRecord.get("value"))),
        () -> assertNull(avroRecord.getSchema().getField("pn"),
            "схема не содержит неожиданных полей")
        );
    }
    }

    @Test
    @DisplayName("Повторное использование списка ячеек не влияет на собранный payload")
    void rowBufferReuseDoesNotAffectPayload() {
        Decoder decoder = (table, qualifier, value) -> value == null
                ? null
                : new String(value, StandardCharsets.UTF_8);
        try (PayloadBuilder builder = new PayloadBuilder(decoder, configWithSr(), new MockSchemaRegistryClient())) {

        List<Cell> cells = new ArrayList<>();
        byte[] row = Bytes.toBytes("rk-2");
        cells.add(new KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value"), Bytes.toBytes("value")));

        org.apache.avro.generic.GenericData.Record avroRecord = builder.buildRowPayload(
                TableName.valueOf("T_AVRO"), cells, RowKeySlice.whole(row), 7L, 9L);

        cells.clear();

        assertEquals("value", String.valueOf(avroRecord.get("value")));
        }
    }

    @Test
    @DisplayName("Одинаковый вход даёт одинаковые Avro bytes")
    void buildRowPayloadBytesDeterministic() {
        try (PayloadBuilder builder = new PayloadBuilder(new StubDecoder(), configWithSr(), new MockSchemaRegistryClient())) {
            List<Cell> cells = new ArrayList<>();
            byte[] row = Bytes.toBytes("rk-3");
            cells.add(new KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value"),
                    "VAL".getBytes(StandardCharsets.UTF_8)));

            byte[] first = builder.buildRowPayloadBytes(
                    TableName.valueOf("T_AVRO"), cells, RowKeySlice.whole(row), 7L, 9L);
            byte[] second = builder.buildRowPayloadBytes(
                    TableName.valueOf("T_AVRO"), cells, RowKeySlice.whole(row), 7L, 9L);

            assertArrayEquals(first, second, "Avro bytes должны быть детерминированы");
        }
    }

    @Test
    @DisplayName("describeSerializer() описывает Avro Confluent без legacy-ключа avro.mode")
    void describeSerializerHighlightsAvroMode() {
        try (PayloadBuilder builder = new PayloadBuilder(TestRawDecoder.INSTANCE, configWithSr(), new MockSchemaRegistryClient())) {

        String info = builder.describeSerializer();
        assertTrue(info.contains("payload.format=AVRO_BINARY"), info);
        assertTrue(info.contains("schema.registry.urls=[http://mock]"), info);
        assertTrue(info.contains("schema.registry.auth=disabled"), info);
        assertEquals(-1, info.indexOf("avro.mode"), "diagnostic summary не должен содержать legacy поле avro.mode");
        }
    }

    private static final class StubDecoder implements Decoder {
        @Override
        public Object decode(TableName table, String qualifier, byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public void decodeRowKey(TableName table,
                                 RowKeySlice slice,
                                 int pkCount,
                                 Map<String, Object> out) {
            out.put("id", new String(slice.toByteArray(), StandardCharsets.UTF_8));
        }
    }
}
