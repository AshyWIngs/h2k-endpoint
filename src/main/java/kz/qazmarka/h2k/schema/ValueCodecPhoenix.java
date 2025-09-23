package kz.qazmarka.h2k.schema;

import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.types.PDataType;

import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Кодек Phoenix для скалярных (и некоторых массивных) типов.
 *
 * Строгие правила
 * - Фиксированные типы требуют строго заданной длины: {@code UNSIGNED_INT} = 4 байта,
 *   {@code UNSIGNED_TINYINT} = 1 байт, {@code UNSIGNED_SMALLINT} = 2 байта и т.п.
 * - При несоответствии длины/типа выбрасывается {@link IllegalStateException} с диагностикой на русском.
 * - Временные типы декодируются по правилам Phoenix 4.14/4.15 (мс с эпохи для {@code TIMESTAMP/DATE/TIME}).
 *
 * Назначение
 * - Декодер значений колонок через Phoenix {@link PDataType}, опираясь на {@link SchemaRegistry}.
 * - Поддерживает семантику Phoenix для широкого набора типов (UNSIGNED‑типы, TIMESTAMP/DATE/TIME, ARRAY и т.п.).
 * - Унифицирует результат: TIMESTAMP/DATE/TIME → epoch millis (long); любой Phoenix ARRAY → {@code List<Object>};
 *   VARBINARY/BINARY → {@code byte[]} как есть; прочие типы — как вернул {@code PDataType}.
 *
 * Диагностика и исключения
 * - Если тип колонки известен (получен из {@link SchemaRegistry}), но байты не соответствуют формату,
 *   метод {@link #decode(TableName, String, byte[])} выбрасывает {@link IllegalStateException} с контекстом
 *   (таблица, колонка, тип).
 * - Для фиксированных типов дополнительно проверяется длина байтового представления ({@code getByteSize}).
 * - Входные {@code table} и {@code qualifier} обязательны: при {@code null} выбрасывается {@link NullPointerException}.
 * - Если тип колонки в реестре неизвестен, используется дефолтный {@code VARCHAR}; фактическое WARN-логирование
 *   выполняется внутри {@code PhoenixColumnTypeRegistry}.
 *
 * Производительность и GC
 * - Двухуровневый кэш соответствий (table → qualifier → {@code PDataType}) минимизирует промахи и аллокации.
 * - Нормализация строкового имени типа выполняется один раз на колонку (при первом доступе).
 * - Конвертация массивов делает минимум аллокаций: {@code Object[]} не копируются, примитивы боксируются линейно.
 * - Диагностические строки формируются только в редких ветках (ошибки/неизвестные типы).
 *
 * Логи
 * - WARN по неизвестным типам выдаётся на уровне реестра типов ({@code PhoenixColumnTypeRegistry}).
 *
 * Потокобезопасность
 * - Используются потокобезопасные структуры; класс безопасен для многопоточности в RegionServer.
 */
public final class ValueCodecPhoenix implements Decoder {

    private final SchemaRegistry registry;
    private final PhoenixColumnTypeRegistry types;
    private final PhoenixPkParser pkParser;

    public ValueCodecPhoenix(SchemaRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.types = new PhoenixColumnTypeRegistry(this.registry);
        this.pkParser = new PhoenixPkParser(this.registry, this.types);
    }

    /**
     * Декодирует значение колонки согласно Phoenix‑типу из реестра.
     *
     * Унификация результата
     *  • TIMESTAMP/DATE/TIME → миллисекунды epoch (long);
     *  • любой Phoenix ARRAY → {@code List<Object>} (без копии для Object[], минимальная коробка для примитивов);
     *  • VARBINARY/BINARY → {@code byte[]} как есть;
     *  • прочие типы возвращаются как есть (строки/числа/Boolean), как их выдал {@link PDataType}.
     *
     * Контракты и ошибки
     *  • {@code value == null} → возвращается {@code null} без попытки декодирования;
     *  • при несовпадении объявленного типа и фактических байтов — {@link IllegalStateException} с контекстом;
     *  • {@code table} и {@code qualifier} обязательны (при {@code null} — {@link NullPointerException}).
     *
     * @param table     имя таблицы (не {@code null})
     * @param qualifier имя колонки (не {@code null})
     * @param value     байты значения; {@code null} возвращается как {@code null}
     * @return нормализованное значение в соответствии с правилами выше
     * @throws NullPointerException  если {@code table} или {@code qualifier} равны {@code null}
     * @throws IllegalStateException если {@link PDataType#toObject(byte[], int, int)} выбросил исключение
     */
    @Override
    public Object decode(TableName table, String qualifier, byte[] value) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(qualifier, "qualifier");

        if (value == null) return null;

        final PDataType<?> t = types.resolve(table, qualifier);

        // Предварительная быстрая валидация длины для фиксированных типов Phoenix (int/long/boolean/...).
        // Это позволяет рано выявить ситуацию, когда байты принадлежат другому типу (например, VARCHAR),
        // и избежать "тихих" неверных декодирований. Накладные расходы нулевые на горячем пути.
        final Integer expectedSize = t.getByteSize();
        if (expectedSize != null && expectedSize != value.length) { // auto-unboxing, избыток intValue() не нужен
            throw new IllegalStateException(
                "Несоответствие длины значения для " + table + "." + qualifier
                + ": тип=" + t + " ожидает " + expectedSize + " байт(а), получено " + value.length
            );
        }

        // Преобразуем байты через Phoenix-тип, чтобы сохранить семантику Phoenix; добавляем диагностический контекст
        final Object obj;
        try {
            obj = t.toObject(value, 0, value.length);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Не удалось преобразовать значение через Phoenix: " + table + "." + qualifier + ", тип=" + t, e);
        }

        return PhoenixValueNormalizer.normalizeValue(obj, table, qualifier);
    }

    /**
     * Равенство кодеков определяется ссылочной идентичностью реестра типов.
     * Считаем два экземпляра эквивалентными только если это один и тот же класс
     * и оба указывают на один и тот же {@code registry} (по ссылке).
     *
     * @param o другой объект для сравнения
     * @return {@code true}, если {@code this == o} или оба экземпляра одного класса с тем же {@code registry}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || o.getClass() != ValueCodecPhoenix.class) return false;
        ValueCodecPhoenix other = (ValueCodecPhoenix) o;
        // Эквивалентность кодеков определяем по ссылочной идентичности реестра
        return this.registry == other.registry;
    }

    /**
     * Хеш-код согласован с {@link #equals(Object)}: вычисляется на основе ссылочной
     * идентичности {@code registry}. Это гарантирует, что равные объекты имеют одинаковый хеш.
     *
     * @return хеш-код текущего экземпляра
     */
    @Override
    public int hashCode() {
        // Хеш-функция согласована с equals: опираемся на идентичность реестра
        return System.identityHashCode(this.registry);
    }

    /**
     * Делегирует разбор Phoenix rowkey в {@link PhoenixPkParser}, сохраняя контракт {@link Decoder#decodeRowKey}.
     */
    @Override
    public void decodeRowKey(TableName table,
                             RowKeySlice rk,
                             int saltBytes,
                             Map<String, Object> out) {
        pkParser.decodeRowKey(table, rk, saltBytes, out);
    }
}
