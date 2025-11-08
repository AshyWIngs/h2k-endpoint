package kz.qazmarka.h2k.payload.builder;

import java.nio.ByteBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfigBuilder;
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
        try (TestContext ctx = newContext()) {
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
    }

    @Test
    void assembleShouldMarkDeleteAndSkipDecoderForDeleteCells() {
        try (TestContext ctx = newContext()) {
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
    }

    @Test
    void assembleShouldSkipColumnsMarkedAsIgnoredInSchema() {
        try (TestContext ctx = newContext()) {
            List<Cell> cells = new ArrayList<>();
            cells.add(putCell("skip_me", 101L, bytes("ignored")));
            cells.add(putCell("version", 102L, bytes("2")));

            RowKeySlice rowKey = RowKeySlice.whole(bytes("skip-row"));
            GenericData.Record actual = ctx.assembler.assemble(TABLE, cells, rowKey, 10L, 20L);

            assertEquals("skip-row", actual.get("id"));
            assertEquals(2L, actual.get("version"));
            assertNull(actual.get("skip_me"), "Поле с h2k.payload.skip должно оставаться по умолчанию");
            assertEquals(0, ctx.decoder.decodeCalls("skip_me"), "Декодер не должен вызываться для пропускаемых колонок");
        }
    }

    @Test
    void qualifierCacheSupportsParallelAssembly() throws Exception {
        RowPayloadAssembler.QualifierCacheProbe cache = RowPayloadAssembler.qualifierCacheProbeForTest();
        int workers = 4;
        int iterations = 128;
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        AtomicReference<String> shared = new AtomicReference<>();
        try {
            List<Future<Void>> futures = new java.util.ArrayList<>(workers);
            for (int t = 0; t < workers; t++) {
                final int threadId = t;
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        byte[] qualifier = ("version-" + threadId + "-" + i).getBytes(UTF_8);
                        String first = cache.intern(qualifier, 0, qualifier.length);
                        String second = cache.intern(qualifier, 0, qualifier.length);
                        assertSame(first, second, "Повторный вызов должен вернуть тот же экземпляр строки");
                    }
                    byte[] sharedBytes = "shared-field".getBytes(UTF_8);
                    String sharedValue = cache.intern(sharedBytes, 0, sharedBytes.length);
                    String previous = shared.getAndSet(sharedValue);
                    if (previous != null) {
                        assertSame(previous, sharedValue, "Все потоки должны получить единый экземпляр shared qualifier");
                    }
                    return null;
                }));
            }
            for (Future<Void> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void assemblerReusesPkCollectorBetweenRows() {
        try (TestContext ctx = newContext()) {
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
    }

    @Test
    void assembleShouldFailWhenRowKeyMissing() {
        try (TestContext ctx = newContext()) {
            List<Cell> noCells = new ArrayList<>();
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> ctx.assembler.assemble(TABLE, noCells, null, 0L, 0L),
                    "Отсутствующий rowkey должен приводить к ошибке");
            assertTrue(ex.getMessage().contains("rowkey"), "Диагностика должна указывать на отсутствие rowkey");
        }
    }

    @Test
    void assembleShouldDecodeRowKeyWhenSaltConfigured() {
        SaltAwareDecoder saltAware = new SaltAwareDecoder();
        Decoder decoder = saltAware;
        try (RowPayloadAssembler assembler = new RowPayloadAssembler(
                decoder,
                configWithProvider(metadataProvider(1)),
                new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro")))) {

            byte[] rowKeyBytes = new byte[]{(byte) 0x7F, 'i', 'd', '-', 's', 'a', 'l', 't'};
            GenericData.Record avroRecord = assembler.assemble(
                    TABLE,
                    Collections.emptyList(),
                    RowKeySlice.whole(rowKeyBytes),
                    0L,
                    0L);

            assertEquals("id-salt", avroRecord.get("id"));
            assertEquals(1, saltAware.lastSaltBytes(),
                    "Decoder должен получать число байтов соли из конфигурации");
        }
    }

    @Test
    void assembleShouldPassZeroSaltWhenMetadataMissing() {
        SaltAwareDecoder saltAware = new SaltAwareDecoder();
        Decoder decoder = saltAware;
        try (RowPayloadAssembler assembler = new RowPayloadAssembler(
                decoder,
                configWithProvider(metadataProvider(null)),
                new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro")))) {

            byte[] rowKeyBytes = Bytes.toBytes("plain-rowkey");
            GenericData.Record avroRecord = assembler.assemble(
                    TABLE,
                    Collections.emptyList(),
                    RowKeySlice.whole(rowKeyBytes),
                    0L,
                    0L);

            assertEquals("plain-rowkey", avroRecord.get("id"));
            assertEquals(0, saltAware.lastSaltBytes(),
                    "При отсутствии соли в метаданных RowPayloadAssembler должен передавать 0");
        }
    }

    @Test
    void assembleShouldFailWhenRowKeyShorterThanSaltBytes() {
        SaltAwareDecoder saltAware = new SaltAwareDecoder();
        Decoder decoder = saltAware;
        try (RowPayloadAssembler assembler = new RowPayloadAssembler(
                decoder,
                configWithProvider(metadataProvider(1)),
                new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro")))) {

            byte[] rowKeyBytes = new byte[]{0x42}; // только байт соли, без PK
            List<Cell> emptyCells = Collections.emptyList();
            RowKeySlice rowKeySlice = RowKeySlice.whole(rowKeyBytes);
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> assembler.assemble(
                            TABLE,
                            emptyCells,
                            rowKeySlice,
                            0L,
                            0L));
            assertTrue(ex.getMessage().contains("PK 'id'"),
                    "Диагностика должна указывать на отсутствие декодированного PK");
            assertEquals(1, saltAware.lastSaltBytes(),
                    "Даже при ошибке декодер получает ожидаемое число байтов соли");
        }
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
        return configWithProvider(metadataProvider(0));
    }

    private H2kConfig configWithProvider(PhoenixTableMetadataProvider metadata) {
    H2kConfigBuilder builder = new H2kConfigBuilder("mock:9092");
        builder.tableMetadataProvider(metadata);
        return builder.build();
    }

    private PhoenixTableMetadataProvider metadataProvider(final Integer saltBytes) {
        PhoenixTableMetadataProvider.TableMetadataBuilder builder =
                PhoenixTableMetadataProvider.builder()
                        .table(TABLE)
                        .columnFamilies("data")
                        .primaryKeyColumns("id")
                        .capacityHint(0);
        if (saltBytes != null) {
            builder.saltBytes(saltBytes);
        }
        return builder.done().build();
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
            calls.merge(key, 1, (current, add) -> current + add);
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

    private static final class SaltAwareDecoder implements Decoder {
        private int lastSaltBytes = -1;

        @Override
        public Object decode(TableName table, String qualifier, byte[] value) {
            return null;
        }

        @Override
        public void decodeRowKey(TableName table, RowKeySlice rk, int saltBytes, Map<String, Object> out) {
            lastSaltBytes = saltBytes;
            if (rk == null || rk.getLength() <= saltBytes) {
                return;
            }
            int offset = rk.getOffset() + saltBytes;
            int length = rk.getLength() - saltBytes;
            String id = Bytes.toString(rk.getArray(), offset, length);
            out.put("id", id);
        }

        int lastSaltBytes() {
            return lastSaltBytes;
        }
    }

    private static final class TestContext implements AutoCloseable {
        final StubDecoder decoder;
        final RowPayloadAssembler assembler;

        TestContext(StubDecoder decoder, RowPayloadAssembler assembler) {
            this.decoder = decoder;
            this.assembler = assembler;
        }

        @Override
        public void close() {
            assembler.close();
        }
    }
}
