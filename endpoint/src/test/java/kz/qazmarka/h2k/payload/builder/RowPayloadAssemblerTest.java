package kz.qazmarka.h2k.payload.builder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Тесты RowPayloadAssembler закрывают сценарии:
 *  • сборка payload с PK, значениями и метаданными WAL;
 *  • обработка delete-ячейки и бинарных значений без копий;
 *  • повторное использование сборщика PK между строками (нет утечек состояний).
 */
class RowPayloadAssemblerTest {

    private static final TableName TABLE = TableName.valueOf("default", "T_ROW");
    private static final byte[] ROW_BYTES = Bytes.toBytes("row-1");
    private static final byte[] FAMILY = Bytes.toBytes("data");

    @Test
    void assembleShouldPopulatePkValuesAndWalMetadata() {
        TestContext ctx = newContext();
        List<Cell> cells = new ArrayList<>();
        cells.add(putCell("version", 123L, bytes("42")));
        Cell payloadCell = putCell("payload", 321L, bytes("abc"));
        cells.add(payloadCell);

        RowKeySlice rowKey = RowKeySlice.whole(bytes("id-1"));
        GenericData.Record actual = ctx.assembler.assemble(TABLE, cells, rowKey, 777L, 888L);

        assertEquals("id-1", actual.get("id"));
        assertEquals(42L, actual.get("version"));

        ByteBuffer payloadBuffer = (ByteBuffer) actual.get("payload");
        assertSame(payloadCell.getValueArray(), payloadBuffer.array(),
                "BinarySlice должен использовать исходный буфер ячейки");
        assertEquals(payloadCell.getValueLength(), payloadBuffer.remaining(),
                "Payload должен передаваться без копий и с корректной длиной");
        byte[] restored = new byte[payloadBuffer.remaining()];
        payloadBuffer.duplicate().get(restored);
        assertEquals("abc", new String(restored, UTF_8));

        assertEquals(321L, actual.get(PayloadFields.EVENT_TS));
        assertEquals(Boolean.FALSE, actual.get(PayloadFields.DELETE));
        assertEquals(777L, actual.get(PayloadFields.WAL_SEQ));
        assertEquals(888L, actual.get(PayloadFields.WAL_WRITE_TIME));
    }

    @Test
    void assembleShouldMarkDeleteAndSkipDecoderForDeleteCells() {
        TestContext ctx = newContext();
        List<Cell> cells = new ArrayList<>();
        cells.add(putCell("version", 100L, bytes("100")));
        cells.add(deleteCell("payload", 200L));

        RowKeySlice rowKey = RowKeySlice.whole(bytes("id-del"));
        GenericData.Record actual = ctx.assembler.assemble(TABLE, cells, rowKey, 500L, 600L);

        assertEquals("id-del", actual.get("id"));
        assertEquals(100L, actual.get("version"), "Put-ячейка должна быть декодирована");
        assertEquals(Boolean.TRUE, actual.get(PayloadFields.DELETE), "Delete-флаг должен быть выставлен");
        assertEquals(200L, actual.get(PayloadFields.EVENT_TS), "Максимальная метка времени учитывает delete");
        assertEquals(1, ctx.decoder.decodeCalls("version"), "Декодер должен вызываться только для Put-ячейки");
        assertEquals(0, ctx.decoder.decodeCalls("payload"), "Delete-ячейка не декодируется");
    }

    @Test
    void assemblerReusesPkCollectorBetweenRows() {
        TestContext ctx = newContext();
        List<Cell> firstRow = new ArrayList<>();
        firstRow.add(putCell("version", 10L, bytes("1")));
        RowKeySlice rk1 = RowKeySlice.whole(bytes("first"));
        GenericData.Record firstActual = ctx.assembler.assemble(TABLE, firstRow, rk1, 1L, 1L);
        assertEquals("first", firstActual.get("id"));
        assertEquals(1L, firstActual.get("version"));

        List<Cell> secondRow = new ArrayList<>();
        RowKeySlice rk2 = RowKeySlice.whole(bytes("second"));
        GenericData.Record secondActual = ctx.assembler.assemble(TABLE, secondRow, rk2, 2L, 2L);
        assertEquals("second", secondActual.get("id"));
        assertNull(secondActual.get("version"), "Значение из предыдущей строки не должно протекать");
        assertNull(secondActual.get(PayloadFields.EVENT_TS), "Без ячеек нет событийного времени");
        assertEquals(Boolean.FALSE, secondActual.get(PayloadFields.DELETE), "Флаг delete сбрасывается");
    }

    @Test
    void assembleShouldFailWhenRowKeyMissing() {
        TestContext ctx = newContext();
        List<Cell> noCells = new ArrayList<>();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> ctx.assembler.assemble(TABLE, noCells, null, 0L, 0L),
                "Отсутствующий rowkey должен приводить к ошибке");
        assertTrue(ex.getMessage().contains("rowkey"), "Диагностика должна указывать на отсутствие rowkey");
    }

    private TestContext newContext() {
        StubDecoder decoder = new StubDecoder();
        RowPayloadAssembler assembler = new RowPayloadAssembler(
                decoder,
                config(),
                new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro")));
        return new TestContext(decoder, assembler);
    }

    private H2kConfig config() {
        PhoenixTableMetadataProvider metadata = new PhoenixTableMetadataProvider() {
            private final String[] pk = new String[]{"id"};

            @Override
            public String[] primaryKeyColumns(TableName table) {
                return pk.clone();
            }

            @Override
            public Integer saltBytes(TableName table) {
                return 0;
            }

            @Override
            public Integer capacityHint(TableName table) {
                return 0;
            }

            @Override
            public String[] columnFamilies(TableName table) {
                return new String[]{"data"};
            }
        };

        H2kConfig.Builder builder = new H2kConfig.Builder("mock:9092");
        builder.tables().metadataProvider(metadata).done();
        return builder.build();
    }

    private Cell putCell(String qualifier, long ts, byte[] value) {
        return new KeyValue(ROW_BYTES, FAMILY, qualifier.getBytes(UTF_8), ts, Type.Put, value);
    }

    private Cell deleteCell(String qualifier, long ts) {
        return new KeyValue(ROW_BYTES, FAMILY, qualifier.getBytes(UTF_8), ts, Type.DeleteColumn, new byte[0]);
    }

    private static byte[] bytes(String value) {
            return value.getBytes(UTF_8);
    }

    private static final class StubDecoder implements Decoder {
        private final Map<String, Integer> calls = new HashMap<>();

        @Override
        public Object decode(TableName table, String qualifier, byte[] value) {
            String key = qualifier.toLowerCase(Locale.ROOT);
            calls.merge(key, 1, Integer::sum);
            if ("version".equals(key)) {
                if (value == null || value.length == 0) {
                    return null;
                }
                 return parseAsciiLong(value);
            }
            if ("payload".equals(key)) {
                return null; // спровоцировать BinarySlice
            }
            return value == null ? null : new String(value, UTF_8);
        }

        @Override
        public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
            String id = Bytes.toString(rk.getArray(), rk.getOffset(), rk.getLength());
            out.put("id", id);
        }

        int decodeCalls(String qualifier) {
            return calls.getOrDefault(qualifier.toLowerCase(Locale.ROOT), 0);
        }

        private long parseAsciiLong(byte[] value) {
            long result = 0;
            boolean negative = false;
            int idx = 0;
            if (value[idx] == '-') {
                negative = true;
                idx++;
            }
            for (; idx < value.length; idx++) {
                int digit = value[idx] - '0';
                if (digit < 0 || digit > 9) {
                    throw new NumberFormatException("Невозможно распарсить long из ascii: "
                            + Bytes.toStringBinary(value));
                }
                result = result * 10 + digit;
            }
            return negative ? -result : result;
        }
    }

    private static final class TestContext {
        final StubDecoder decoder;
        final RowPayloadAssembler assembler;

        TestContext(StubDecoder decoder, RowPayloadAssembler assembler) {
            this.decoder = decoder;
            this.assembler = assembler;
        }
    }
}
