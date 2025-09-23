package kz.qazmarka.h2k.payload;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.Decoder;
import kz.qazmarka.h2k.util.Bytes;
import kz.qazmarka.h2k.util.Maps;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Сборщик JSON‑payload для отправки в Kafka в формате JSONEachRow.
 *
 * Назначение
 *  - На основе данных WAL формирует одну JSON-строку на одну строку HBase: ключи — квалификаторы колонок.
 *  - обрабатывает все ячейки строки; фильтрация по целевому CF предполагается на уровне источника/сканера.
 *
 * PK из rowkey
 *  - Перед сериализацией выполняется декодирование rowkey: {@link Decoder#decodeRowKey}
 *    обогащает карту значений расшифрованными компонентами PK (например, "c","t","opd"), чтобы PK были видны
 *    в Value как обычные колонки.
 *  - Детали извлечения PK: см. {@link kz.qazmarka.h2k.schema.ValueCodecPhoenix#decodeRowKey(org.apache.hadoop.hbase.TableName, kz.qazmarka.h2k.util.RowKeySlice, int, java.util.Map)}.
 *
 * Сериализация и метаданные
 *  - Нулевые значения колонок не сериализуются, если не включено cfg.isJsonSerializeNulls().
 *  - Колонки, не описанные в схеме, не отбрасываются: если декодер вернул {@code null}, но значение непустое —
 *    сохраняем как UTF‑8 строку.
 *  - Метаполя добавляются по флагам конфигурации (см. README и {@link H2kConfig}): h2k.include.meta.*,
 *    h2k.include.rowkey и др.
 *
 * Производительность и потокобезопасность
 *  - Без побочных эффектов; удобно тестировать.
 *  - Используется {@link LinkedHashMap} с заранее рассчитанной ёмкостью, порядок ключей стабилен.
 *  - Строка qualifier создаётся один раз на ячейку напрямую из буфера без промежуточного копирования.
 *  - Декодер вызывается для всех ячеек строки (фильтрация CF — на уровне источника/сканера).
 */
public final class PayloadBuilder {


    private final Decoder decoder;
    private final H2kConfig cfg;
    private final AtomicReference<PayloadSerializer> cachedSerializer = new java.util.concurrent.atomic.AtomicReference<>();

    /**
     * @param decoder декодер значений клеток целевого CF (byte[] → Java‑типы)
     * @param cfg     снимок конфигурации h2k.* (флаги сериализации/метаданных)
     * @throws NullPointerException если любой из аргументов равен null
     */
    public PayloadBuilder(Decoder decoder, H2kConfig cfg) {
        this.decoder = Objects.requireNonNull(decoder, "декодер");
        this.cfg = Objects.requireNonNull(cfg, "конфигурация h2k");
    }

    /**
     * Упаковка результата с использованием сериализатора, выбранного по конфигурации
     * (cfg.getPayloadFormat()/cfg.getSerializerFactoryClass()). Кэширует созданный инстанс.
     */
    public byte[] buildRowPayloadBytes(TableName table,
                                       List<Cell> cells,
                                       RowKeySlice rowKey,
                                       long walSeq,
                                       long walWriteTime) {
        PayloadSerializer ser = resolveSerializerFromConfig();
        return buildRowPayloadBytes(table, cells, rowKey, walSeq, walWriteTime, ser);
    }

    /**
     * Упаковка результата {@link #buildRowPayload(TableName, List, RowKeySlice, long, long)}
     * в байты с помощью заданного сериализатора. Метод не меняет логику построения payload:
     * он лишь делегирует сериализацию (JSONEachRow/Avro и т.п.) внешней стратегии.
     *
     * @param table таблица HBase
     * @param cells клетки строки
     * @param rowKey срез rowkey
     * @param walSeq sequenceId записи WAL
     * @param walWriteTime время записи в WAL, мс
     * @param serializer реализация {@link PayloadSerializer} (например, {@code JsonEachRowSerializer})
     * @return сериализованный payload
     * @throws NullPointerException если {@code table} или {@code serializer} равны null
     */
    public byte[] buildRowPayloadBytes(TableName table,
                                       List<Cell> cells,
                                       RowKeySlice rowKey,
                                       long walSeq,
                                       long walWriteTime,
                                       PayloadSerializer serializer) {
        Objects.requireNonNull(table, "таблица");
        Objects.requireNonNull(serializer, "serializer");
        Map<String, Object> obj = buildRowPayload(table, cells, rowKey, walSeq, walWriteTime);
        if (serializer instanceof TableAwarePayloadSerializer) {
            TableAwarePayloadSerializer ta = (TableAwarePayloadSerializer) serializer;
            return ta.serialize(table, obj);
        }
        return serializer.serialize(obj);
    }

    /**
     * Создаёт корневую LinkedHashMap с ёмкостью, подобранной по «подсказке» из H2kConfig (если задана)
     * и/или оценке фактического числа ключей (данные + метаданные). Расчёт ёмкости делегируется в
     * {@link Maps#initialCapacity(int)} для предсказуемого минимального числа расширений.
     *
     * Подсказка берётся из {@code cfg.getCapacityHintFor(table)} (0 — подсказка отсутствует).
     * Итоговая ёмкость ограничивается внутренним максимумом HashMap/LinkedHashMap.
     *
     * @param table              таблица HBase, для которой строится payload
     * @param estimatedKeysCount оценка общего числа ключей в payload (данные колонок + служебные поля)
     * @return новая упорядоченная мапа под корневой JSON‑объект
     */
    private Map<String, Object> newRootMap(TableName table, int estimatedKeysCount) {
        final int hint = cfg.getCapacityHintFor(table); // 0, если подсказка не задана
        final int target = Math.max(estimatedKeysCount, hint > 0 ? hint : 0);
        final int initialCapacity = Maps.initialCapacity(target);
        return new LinkedHashMap<>(initialCapacity, 0.75f);
    }

    /**
     * Основной метод сборки payload (одно событие по rowkey).
     * Порядок ключей стабилен ({@link LinkedHashMap}). Null‑значения колонок включаются в JSON только
     * при {@code cfg.isJsonSerializeNulls()}.
     *
     * Ёмкость результирующей мапы рассчитывается заранее из числа ячеек и флагов включения метаданных/rowkey.
     *
     * @param table        таблица HBase (для ключей _table/_namespace/_qualifier и для декодера)
     * @param cells        список ячеек этой строки (обычно только целевой CF; фильтрация выполняется на источнике)
     * @param rowKey       срез rowkey (может быть {@code null}, если источник не предоставляет ключ)
     * @param walSeq       sequenceId записи WAL (или {@code < 0}, если недоступен)
     * @param walWriteTime время записи в WAL в мс (или {@code < 0}, если недоступно)
     * @return упорядоченная карта с данными колонок и служебными полями
     * @throws NullPointerException если {@code table} равен {@code null}
     */
    public Map<String, Object> buildRowPayload(TableName table,
                                               List<Cell> cells,
                                               RowKeySlice rowKey,
                                               long walSeq,
                                               long walWriteTime) {
        Objects.requireNonNull(table, "таблица");
        // Кэшируем флаги конфига (уменьшаем количество ветвлений в этом методе)
        final boolean includeMeta    = cfg.isIncludeMeta();
        final boolean includeWalMeta = includeMeta && cfg.isIncludeMetaWal();
        final boolean includeRowKey  = cfg.isIncludeRowKey();
        final boolean rowkeyB64      = cfg.isRowkeyBase64();

        // Оцениваем число ключей в результате и заранее задаём ёмкость мапы
        final int cellsCount = (cells == null ? 0 : cells.size());
        final boolean includeRowKeyPresent = includeRowKey && rowKey != null;

        int cap = 1 /*event_version*/
                + cellsCount
                + (includeMeta ? 5 : 0)         /*_table,_namespace,_qualifier,_cf,_cells_total*/
                + (includeRowKeyPresent ? 1 : 0)/*_rowkey*/
                + (includeWalMeta ? 2 : 0);     /*_wal_seq,_wal_write_time*/

        final Map<String, Object> obj = newRootMap(table, cap);

        // Метаданные таблицы (если включены)
        addMetaIfEnabled(includeMeta, obj, table, cellsCount);

        // PK как отдельные поля значения (c/t/opd и т.п.) — извлекаем из rowkey по схеме Phoenix.
        // Включаем всегда (это бизнес-данные), если rowkey доступен и схема знает состав PK.
        decodePkFromRowKey(table, rowKey, obj);

        // Расшифровка ячеек: добавляет поля CF в obj и возвращает агрегаты
        CellStats stats = decodeCells(table, cells, obj);

        // Итоговые служебные поля
        addCellsCfIfMeta(obj, includeMeta, stats.cfCells);
        // event_version пишем только если были обработанные ячейки целевого CF
        if (stats.cfCells > 0) {
            obj.put(PayloadFields.EVENT_VERSION, stats.maxTs);
        }
        addDeleteFlagIfNeeded(obj, stats.hasDelete);

        // RowKey (если включён и присутствует). При необходимости снимаем префикс соли Phoenix.
        final int saltBytes = cfg.getSaltBytesFor(table);
        addRowKeyIfPresent(includeRowKeyPresent, obj, rowKey, rowkeyB64, saltBytes);

        // Метаданные WAL (если включены)
        addWalMeta(includeWalMeta, obj, walSeq, walWriteTime);

        return obj;
    }

    /**
     * Декодирует составной PK из {@link RowKeySlice} в именованные поля значения.
     * Реализация декодирования и знание порядка/типов колонок PK — на стороне {@link Decoder}.
     * Если rowkey отсутствует/пустой или таблица не описана в схеме — метод ничего не делает.
     * Детали декодирования Phoenix rowkey: {@link kz.qazmarka.h2k.schema.ValueCodecPhoenix#decodeRowKey(org.apache.hadoop.hbase.TableName, kz.qazmarka.h2k.util.RowKeySlice, int, java.util.Map)}.
     */
    private void decodePkFromRowKey(TableName table, RowKeySlice rk, Map<String, Object> out) {
        if (rk == null) {
            return;
        }
        // Если у таблицы есть соль Phoenix — декодер обязан корректно её учесть (сдвиг/обрезка).
        final int saltBytes = cfg.getSaltBytesFor(table);
        // Decoder API: decodeRowKey(table, rowKeySlice, saltBytes, outValues)
        // Контракт декодера: безопасно-небросающий; при отсутствии описания PK — no-op.
        decoder.decodeRowKey(table, rk, saltBytes, out);
    }

    /**
     * Декодирует клетки целевых CF и заполняет {@code obj} декодированными значениями колонок.
     * Обновляет агрегаты (максимальную метку времени, число ячеек CF, флаг удаления).
     *
     * Имплементационная заметка: обработка делегируется в {@link #processCell(TableName, Cell, Map, CellStats, boolean)},
     * где используется «быстрая» перегрузка декодера без копирования value; {@code CellUtil.cloneValue} не вызывается.
     *
     * @param table таблица для декодера и построения имён
     * @param cells все ячейки строки (все CF)
     * @param obj   результирующая карта для добавления полей
     * @return агрегаты по строке
     */
    private CellStats decodeCells(TableName table, List<Cell> cells, Map<String, Object> obj) {
        CellStats s = new CellStats();
        if (cells == null || cells.isEmpty()) {
            return s;
        }

        final boolean serializeNulls = cfg.isJsonSerializeNulls();

        for (Cell cell : cells) {
            processCell(table, cell, obj, s, serializeNulls);
        }
        return s;
    }

    /**
     * Обрабатывает одну ячейку:
     *  - обрабатывает все ячейки (фильтрация CF выполняется на уровне источника/сканера);
     *  - увеличивает счётчики и обновляет {@code maxTs};
     *  - для операций удаления выставляет флаг и не добавляет колонку;
     *  - для обычных значений вызывает декодер и кладёт результат в {@code obj};
     *  - если декодер вернул {@code null}, но значение непустое — сохраняем как UTF‑8 строку.
     *
     * Имплементационная заметка: используется «быстрая» перегрузка декодера
     * {@link Decoder#decode(TableName, byte[], int, int, byte[], int, int)} (без копирования value).
     * Преобразование в строку выполняется только в фоллбэке, когда декодер вернул {@code null}.
     *
     * @param table           таблица (контекст декодера)
     * @param cell            текущая ячейка
     * @param obj             результирующая карта
     * @param s               агрегаты по CF
     * @param serializeNulls  добавлять ли колонку с null‑значением
     */
    private void processCell(TableName table,
                            Cell cell,
                            Map<String, Object> obj,
                            CellStats s,
                            boolean serializeNulls) {
        // Считаем все ячейки релевантными (фильтрация CF — на уровне источника/сканера)
        s.cfCells++;
        long ts = cell.getTimestamp();
        if (ts > s.maxTs) s.maxTs = ts;
        if (CellUtil.isDelete(cell)) {
            s.hasDelete = true;
            return;
        }

        final byte[] qa = cell.getQualifierArray();
        final int    qo = cell.getQualifierOffset();
        final int    ql = cell.getQualifierLength();

        final String q = new String(qa, qo, ql, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);

        final byte[] va = cell.getValueArray();
        final int    vo = cell.getValueOffset();
        final int    vl = cell.getValueLength();

        // Попытка декодировать известный тип: быстрая перегрузка без копирования value
        Object decoded = decoder.decode(table, qa, qo, ql, va, vo, vl);
        if (decoded != null) {
            obj.put(q, decoded);
            return;
        }

        // Фоллбэк: если байты непустые, кладём как UTF-8 строку (для пользовательских/неописанных колонок)
        if (va != null && vl > 0) {
            obj.put(q, new String(va, vo, vl, StandardCharsets.UTF_8));
            return;
        }

        // Иначе добавляем null только если это разрешено конфигурацией
        if (serializeNulls && (!obj.containsKey(q) || obj.get(q) == null)) {
            obj.put(q, null);
        }
    }

    /**
     * Добавляет базовые метаданные таблицы: _table, _namespace, _qualifier, _cf и _cells_total.
     */
    private void addMetaFields(Map<String, Object> obj, TableName table, int totalCells) {
          String ns = table.getNamespaceAsString();
          String qn = table.getQualifierAsString();
          obj.put(PayloadFields.TABLE, table.getNameAsString());
          obj.put(PayloadFields.NAMESPACE, ns);
          obj.put(PayloadFields.QUALIFIER, qn);
          // CSV со списком целевых CF, см. H2kConfig
          obj.put(PayloadFields.CF, cfg.getCfNamesCsv());
          obj.put(PayloadFields.CELLS_TOTAL, totalCells);
    }

    /** Добавляет метаданные таблицы, если соответствующий флаг включён. */
    private void addMetaIfEnabled(boolean includeMeta, Map<String, Object> obj, TableName table, int totalCells) {
        if (includeMeta) {
            addMetaFields(obj, table, totalCells);
        }
    }

    /** Добавляет счётчик ячеек по целевому CF, только если включены метаданные. */
    private static void addCellsCfIfMeta(Map<String, Object> obj, boolean includeMeta, int cfCells) {
        if (includeMeta) {
            obj.put(PayloadFields.CELLS_CF, cfCells);
        }
    }

    /** Устанавливает флаг удаления, если в партии встречалась операция удаления по строке. */
    private static void addDeleteFlagIfNeeded(Map<String, Object> obj, boolean hasDelete) {
        if (hasDelete) {
            obj.put(PayloadFields.DELETE, true);
        }
    }

    /**
     * Добавляет единое поле {@code _rowkey} (HEX или Base64 по конфигурации), если ключ присутствует и включён.
     * Учитывает соль Phoenix: первые {@code saltBytes} байт ключа пропускаются без аллокаций (смещение/длина).
     *
     * @param includeRowKeyPresent флаг «rowkey включён и присутствует»
     * @param obj                  результирующая карта
     * @param rk                   срез исходного rowkey
     * @param base64               {@code true} — кодировать Base64; {@code false} — как HEX
     * @param saltBytes            число байт соли Phoenix в начале ключа (0 — соли нет)
     */
    private static void addRowKeyIfPresent(boolean includeRowKeyPresent,
                                           Map<String, Object> obj,
                                           RowKeySlice rk,
                                           boolean base64,
                                           int saltBytes) {
        if (!includeRowKeyPresent || rk == null) {
            return;
        }
        // Безопасно «снимаем» соль Phoenix (если задана для таблицы): сдвигаем off/len, не создавая новых объектов.
        int len = rk.getLength();
        int salt = saltBytes > 0 ? Math.min(saltBytes, len) : 0;
        int off  = rk.getOffset() + salt;
        int effLen = len - salt;

        if (effLen <= 0) {
            // Нечего выводить — исходный ключ короче или равен длине соли; молча пропускаем, чтобы не шуметь.
            return;
        }
        if (base64) {
            obj.put(PayloadFields.ROWKEY, Bytes.base64(rk.getArray(), off, effLen));
        } else {
            obj.put(PayloadFields.ROWKEY, Bytes.toHex(rk.getArray(), off, effLen));
        }
    }

    /** Добавляет метаданные WAL (_wal_seq, _wal_write_time), если включены и значения неотрицательны. */
    private static void addWalMeta(boolean includeWalMeta,
                                   Map<String, Object> obj,
                                   long walSeq,
                                   long walWriteTime) {
        if (!includeWalMeta) {
            return;
        }
        if (walSeq >= 0L) {
            obj.put(PayloadFields.WAL_SEQ, walSeq);
        }
        if (walWriteTime >= 0L) {
            obj.put(PayloadFields.WAL_WRITE_TIME, walWriteTime);
        }
    }

    /** Агрегаты по CF. */
    private static final class CellStats {
        /** Максимальная метка времени среди обработанных ячеек целевого CF. */
        long maxTs;
        /** В партии встречалась операция удаления по строке. */
        boolean hasDelete;
        /** Сколько ячеек целевого CF было обработано. */
        int cfCells;
    }

    /** Минимальная фабрика сериализаторов для DIP/SPI. */
    public interface PayloadSerializerFactory {
        PayloadSerializer create(H2kConfig cfg);
    }

    /** Builds a serializer instance according to config (no caching, no synchronization). */
    private PayloadSerializer createSerializerFromConfig() {
        PayloadSerializer custom = instantiateFromFqcn();
        if (custom != null) {
            return custom;
        }

        H2kConfig.PayloadFormat fmt = cfg.getPayloadFormat();
        if (fmt == null || fmt == H2kConfig.PayloadFormat.JSON_EACH_ROW) {
            return new JsonEachRowSerializer();
        }

        switch (fmt) {
            case AVRO_BINARY:
                return createAvroBinarySerializer();
            case AVRO_JSON:
                return createAvroJsonSerializer();
            default:
                throw new IllegalStateException("Неизвестный формат payload: " + fmt);
        }
    }

    private PayloadSerializer instantiateFromFqcn() {
        String fqcn = cfg.getSerializerFactoryClass();
        if (fqcn == null || fqcn.trim().isEmpty()) {
            return null;
        }
        try {
            Class<?> cls = Class.forName(fqcn);
            if (PayloadSerializerFactory.class.isAssignableFrom(cls)) {
                PayloadSerializerFactory factory = (PayloadSerializerFactory) cls.getDeclaredConstructor().newInstance();
                return Objects.requireNonNull(factory.create(cfg), "factory returned null");
            }
            if (PayloadSerializer.class.isAssignableFrom(cls)) {
                @SuppressWarnings("unchecked")
                Class<? extends PayloadSerializer> serializerClass = (Class<? extends PayloadSerializer>) cls;
                return serializerClass.getDeclaredConstructor().newInstance();
            }
        } catch (ReflectiveOperationException | ClassCastException ignore) {
            // Fall through to default path
        }
        return null;
    }

    private PayloadSerializer createAvroBinarySerializer() {
        if (cfg.getAvroMode() == H2kConfig.AvroMode.CONFLUENT) {
            return new ConfluentAvroPayloadSerializer(cfg);
        }
        return new GenericAvroPayloadSerializer(cfg, GenericAvroPayloadSerializer.Encoding.BINARY);
    }

    private PayloadSerializer createAvroJsonSerializer() {
        if (cfg.getAvroMode() == H2kConfig.AvroMode.CONFLUENT) {
            throw new IllegalStateException("Avro: режим confluent поддерживает только avro-binary");
        }
        return new GenericAvroPayloadSerializer(cfg, GenericAvroPayloadSerializer.Encoding.JSON);
    }

    /**
     * Returns a cached PayloadSerializer instance; thread-safe and lock-free.
     * Uses AtomicReference with CAS to initialize once.
     */
    private PayloadSerializer resolveSerializerFromConfig() {
        PayloadSerializer existing = cachedSerializer.get();
        if (existing != null) return existing;

        PayloadSerializer created = createSerializerFromConfig();
        // If another thread won the race, use the winner
        if (cachedSerializer.compareAndSet(null, created)) {
            return created;
        }
        return cachedSerializer.get();
    }
}
