package kz.qazmarka.h2k.payload.builder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.SimpleDecoder;
import kz.qazmarka.h2k.util.RowKeySlice;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import kz.qazmarka.h2k.payload.serializer.avro.SchemaRegistryClientFactory;

/**
 * Юнит‑тесты для расчёта начальной ёмкости корневой LinkedHashMap
 * в {@code PayloadBuilder.computeInitialCapacity(...)}.
 *
 * Назначение:
 *  • Зафиксировать публичный контракт расчёта capacity без использования дробной арифметики (только целые).
 *  • Подтвердить корректность на граничных значениях (отрицательные, нули, большие входы).
 *  • Проверить монотонность (capacity не убывает при росте estimated/hint).
 *
 * Контракт (утверждаем тестами):
 *  • {@code target = max(estimated, hint > 0 ? hint : 0)} — отрицательный hint эквивалентен отсутствию подсказки.
 *  • {@code initial = 1 + ceil(target / 0.75)} — минимум 1.
 *  • Эквивалентная целочисленная формула без double: {@code initial = 1 + (4*target + 2)/3}.
 *  • Кэп сверху: {@code initial <= 1<<30} (макс. для HashMap в Java 8).
 *
 * Методика проверки:
 *  • Используем эталонный расчёт {@link #expectedCapacity(int, int)} на long, чтобы исключить переполнения int.
 *  • Сравниваем значения точечно (CSV/параметризованные тесты) и сеткой на диапазонах.
 *  • Проверяем свойства (монотонность, cap, минимум) отдельными тестами.
 *
 * Замечания по производительности:
 *  • В эталоне и в реализации не используется плавающая точка — только целочисленная математика.
 *  • Формула {@code (4*t + 2)/3} даёт то же, что {@code ceil(4*t/3)} и избегает двойной арифметики.
 *
 * Связанные требования проекта:
 *  • h2k.capacity.hints — подсказка максимального числа непустых полей; при включённых метаполях учитываются базовые + WAL + rowkey.
 *  • Главный приоритет — производительность и сдержанный GC: корректная предразмеренность LinkedHashMap снижает аллокации/ре‑хеш.
 */
class PayloadBuilderTest {

    private static final int MAX_HASH_CAP = 1 << 30;

    private static final SchemaRegistryClientFactory TEST_SR_FACTORY =
            (urls, props, capacity) -> new MockSchemaRegistryClient();

    private static int compute(int estimated, int hint) {
        return kz.qazmarka.h2k.util.Maps.computeInitialCapacity(estimated, hint);
    }

    private static H2kConfig configWithSr() {
        String schemaDir = Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString();
        return new H2kConfig.Builder("test-bootstrap")
                .avro()
                .schemaDir(schemaDir)
                .schemaRegistryUrls(Collections.singletonList("http://mock"))
                .done()
                .build();
    }

    /**
     * Эталонный расчёт capacity, полностью на целых типах.
     *
     * Назначение:
     *  • Получить «истинное» ожидаемое значение для сравнения с реализацией без привлечения double.
     *
     * Контракт:
     *  • target = max(estimated, hint>0 ? hint : 0)
     *  • initial = 1 + ceil(target / 0.75)
     *  • Эквивалент: initial = 1 + (4*target + 2)/3
     *  • Минимум = 1; максимум = {@link #MAX_HASH_CAP}
     *
     * Детали реализации:
     *  • Используем long при промежуточных вычислениях: (4*target + 2) помещается в 64 бита и не переполняется.
     *  • Добавка «+2» в числителе реализует «ceil» для деления на 3.
     *
     * @param estimated оценка числа ключей (может быть ≤0)
     * @param hint подсказка (≤0 трактуется как отсутствие подсказки)
     * @return ожидаемая initialCapacity для LinkedHashMap
     */
    private static int expectedCapacity(int estimated, int hint) {
        final int target = Math.max(estimated, hint > 0 ? hint : 0);
        if (target <= 0) return 1;
        final long n = ((long) target << 2) + 2; // 4*target + 2
        long cap = 1 + n / 3L;
        final long MAX = (long) MAX_HASH_CAP;
        if (cap > MAX) cap = MAX;
        return (int) cap;
    }

    /**
     * Назначение: при отсутствии подсказки берём estimated.
     * Покрывает: ветку hint <= 0.
     */
    @Test
    @DisplayName("Если подсказки нет, используется оценка")
    void usesEstimateWhenNoHint() {
        int est = 20;
        int hint = 0;
        int expected = expectedCapacity(est, hint);
        assertEquals(expected, compute(est, hint));
    }

    /**
     * Назначение: при hint > estimated доминирует подсказка.
     * Покрывает: выбор target = hint.
     */
    @Test
    @DisplayName("Если подсказка больше — доминирует над оценкой")
    void respectsLargerHint() {
        int est = 20;
        int hint = 40;
        int expected = expectedCapacity(est, hint);
        assertEquals(expected, compute(est, hint));
    }

    /**
     * Назначение: проверка округления вверх и корректной обработки нулей/отрицательных значений.
     * Покрывает: базовые точки, равные значения, нули, отрицательные.
     */
    @ParameterizedTest(name = "estimated={0}, hint={1}")
    @CsvSource({
            // базовые точки
            "1, 0",
            "15, 0",
            "50, 100",
            // равные значения
            "24, 24",
            // граничные/нулевые
            "0, 0",
            "-5, 0",
            "0, -7",
            "-3, -9",
    })
    @DisplayName("Корректное округление и обработка нулей/отрицательных")
    void roundsUpProperly(int estimated, int hint) {
        assertEquals(expectedCapacity(estimated, hint), compute(estimated, hint));
    }

    /**
     * Назначение: отрицательные estimated/hint не занижают результат.
     * Покрывает: минимум 1 и ветвление по hint<=0.
     */
    @Test
    @DisplayName("Отрицательные значения не занижают результат (мин=1)")
    void ignoresNegativeValues() {
        assertEquals(expectedCapacity(-1, -1), compute(-1, -1));
        assertEquals(expectedCapacity(-1, 10), compute(-1, 10));
        assertEquals(expectedCapacity(10, -1), compute(10, -1));
    }

    /**
     * Назначение: кэп по максимуму для HashMap (1<<30).
     * Покрывает: большие входы, пересечение с капом.
     */
    @Test
    @DisplayName("Кэп по максимуму HashMap (1<<30)")
    void capsAtMax() {
        int est = Integer.MAX_VALUE / 2;
        int hint = Integer.MAX_VALUE;
        assertEquals(expectedCapacity(est, hint), compute(est, hint));
    }

    /**
     * Назначение: монотонность по estimated (capacity не убывает).
     * Покрывает: линейный прогон по est при фиксированном hint.
     */
    @Test
    @DisplayName("Монотонность по estimated: неубывающее значение")
    void monotonicInEstimated() {
        final int hint = 32;
        int prev = compute(0, hint);
        for (int est = 1; est <= 160; est++) {
            int cur = compute(est, hint);
            assertTrue(cur >= prev, "capacity должна быть неубывающей по estimated");
            prev = cur;
        }
    }

    /**
     * Назначение: монотонность по hint (capacity не убывает).
     * Покрывает: линейный прогон по hint при фиксированном est.
     */
    @Test
    @DisplayName("Монотонность по hint: неубывающее значение")
    void monotonicInHint() {
        final int est = 24;
        int prev = compute(est, 0);
        for (int h = 1; h <= 160; h++) {
            int cur = compute(est, h);
            assertTrue(cur >= prev, "capacity должна быть неубывающей по hint");
            prev = cur;
        }
    }
    /**
     * Назначение: экстремальные границы MIN/MAX int.
     * Покрывает: минимум 1, верхний кэп, безопасные вычисления без переполнений.
     */
    @Test
    @DisplayName("Экстремальные границы: MIN/MAX int корректно обрабатываются (cap и минимум 1)")
    void extremeBounds() {
        // Очень большие положительные: срабатывает кэп 1<<30
        assertEquals(expectedCapacity(Integer.MAX_VALUE, 0), compute(Integer.MAX_VALUE, 0));
        assertEquals(expectedCapacity(0, Integer.MAX_VALUE), compute(0, Integer.MAX_VALUE));
        assertEquals(expectedCapacity(Integer.MAX_VALUE, Integer.MAX_VALUE), compute(Integer.MAX_VALUE, Integer.MAX_VALUE));

        // Сильные отрицательные: минимум 1
        assertEquals(expectedCapacity(Integer.MIN_VALUE, 0), compute(Integer.MIN_VALUE, 0));
        assertEquals(expectedCapacity(0, Integer.MIN_VALUE), compute(0, Integer.MIN_VALUE));
        assertEquals(expectedCapacity(Integer.MIN_VALUE, Integer.MIN_VALUE), compute(Integer.MIN_VALUE, Integer.MIN_VALUE));
    }

    /**
     * Назначение: отрицательная подсказка эквивалентна отсутствию подсказки.
     * Покрывает: разные отрицательные значения hint.
     */
    @ParameterizedTest(name = "negative hint {0} должен вести себя как 0")
    @ValueSource(ints = {0, -1, -2, -10, -100, Integer.MIN_VALUE})
    @DisplayName("Отрицательная подсказка эквивалентна отсутствию подсказки (hint<=0 → 0)")
    void negativeHintBehavesAsZero(int hint) {
        int est = 37;
        int withZero = compute(est, 0);
        int withNeg  = compute(est, hint);
        assertEquals(withZero, withNeg);
    }

    /**
     * Назначение: широкая сетка малых значений на совпадение с эталонной формулой.
     * Покрывает: множество комбинаций estimated×hint.
     */
    @Test
    @DisplayName("Сетка малых значений: совпадение с эталонной формулой на широком диапазоне")
    void gridSanitySmall() {
        int[] estValues  = {-5, -1, 0, 1, 2, 3, 7, 10, 24, 25};
        int[] hintValues = {-5, -1, 0, 1, 2, 3, 7, 10, 24, 25};
        for (int est : estValues) {
            for (int hint : hintValues) {
                int expected = expectedCapacity(est, hint);
                int actual   = compute(est, hint);
                assertEquals(expected, actual,
                        "Mismatch for est=" + est + ", hint=" + hint);
            }
        }
    }

    /**
     * Назначение: результат всегда >= 1.
     * Покрывает: разные «пути» получения target (через est и через hint).
     */
    @ParameterizedTest(name = "target={0}: результат не меньше 1")
    @ValueSource(ints = {-100, -1, 0, 1, 2, 3, 4, 100})
    @DisplayName("Результат всегда >= 1 (включая отрицательные/нулевые входы)")
    void resultIsAtLeastOne(int t) {
        // Используем два пути получения target: через estimated и через hint
        int a = compute(t, 0);
        int b = compute(0, t);
        assertTrue(a >= 1, "estimated=" + t + ", hint=0");
        assertTrue(b >= 1, "estimated=0, hint=" + t);
    }

    /**
     * Назначение: около порогов округления capacity не убывает.
     * Покрывает: точки, где меняется ceil(4*t/3).
     */
    @Test
    @DisplayName("Поблизости порогов округления capacity не убывает при росте target")
    void nearRoundingThresholdsNonDecreasing() {
        // Проверим окрестности нескольких точек, где ceil(4*t/3) меняется
        int[] targets = {0,1,2,3,4,5,6,7,8,9,10,11,12,24,25,26,27,28,29,30};
        int prev = -1;
        for (int t : targets) {
            int actual = compute(t, 0);
            if (prev != -1) {
                assertTrue(actual >= prev, "t=" + t + " capacity=" + actual + " prev=" + prev);
            }
            prev = actual;
        }
    }

    @Test
    @DisplayName("PK-поля попадают в Value, null-значения ячеек пропускаются")
    void pkFieldsAreInjectedAndNullsSkipped() {
        PayloadBuilder builder = new PayloadBuilder(new StubDecoder(), configWithSr(), TEST_SR_FACTORY);

        java.util.List<Cell> cells = new java.util.ArrayList<>();
        byte[] row = Bytes.toBytes("rk-1");
        cells.add(new KeyValue(row, Bytes.toBytes("d"), Bytes.toBytes("value"), "VAL".getBytes(StandardCharsets.UTF_8)));

        org.apache.avro.generic.GenericData.Record avroRecord =
                builder.buildRowPayload(TableName.valueOf("T_AVRO"), cells, RowKeySlice.whole(row), 0L, 0L);
        Object idVal = avroRecord.get("id");

        org.junit.jupiter.api.Assertions.assertAll(
                () -> org.junit.jupiter.api.Assertions.assertEquals("rk-1", idVal, "ожидается поле id"),
                () -> org.junit.jupiter.api.Assertions.assertEquals("VAL", String.valueOf(avroRecord.get("value"))),
                () -> org.junit.jupiter.api.Assertions.assertNull(avroRecord.getSchema().getField("pn"), "лишние поля отсутствуют")
        );
    }

    @Test
    @DisplayName("Сборка payload не зависит от повторного использования буфера ячеек")
    void payloadIndependentFromRowBufferReuse() {
        Decoder decoder = (table, qualifier, value) -> value == null ? null : new String(value, StandardCharsets.UTF_8);
        H2kConfig cfg = configWithSr();
        PayloadBuilder builder = new PayloadBuilder(decoder, cfg, TEST_SR_FACTORY);

        java.util.List<Cell> cells = new java.util.ArrayList<>();
        byte[] row = Bytes.toBytes("rk-2");
        cells.add(new KeyValue(row,
                Bytes.toBytes("d"),
                Bytes.toBytes("value"),
                Bytes.toBytes("value")));

        org.apache.avro.generic.GenericData.Record avroRecord = builder.buildRowPayload(TableName.valueOf("T_AVRO"),
                cells, RowKeySlice.whole(row), 7L, 9L);

        cells.clear();

        org.junit.jupiter.api.Assertions.assertEquals("value", String.valueOf(avroRecord.get("value")));
    }

    @Test
    @DisplayName("describeSerializer() отражает Avro-only режим без поля avro.mode")
    void describeSerializerOutputsConfluentSummary() {
        H2kConfig cfg = configWithSr();
        PayloadBuilder builder = new PayloadBuilder(SimpleDecoder.INSTANCE, cfg, TEST_SR_FACTORY);

        String info = builder.describeSerializer();
        assertTrue(info.contains("payload.format=AVRO_BINARY"), info);
        assertTrue(info.contains("schema.registry.urls=[http://mock]"), info);
        assertTrue(info.contains("schema.registry.auth=disabled"), info);
        assertEquals(-1, info.indexOf("avro.mode"), "строка не должна содержать legacy-ключ avro.mode");
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
                                 java.util.Map<String, Object> out) {
            out.put("id", new String(slice.toByteArray(), StandardCharsets.UTF_8));
        }
    }
}
