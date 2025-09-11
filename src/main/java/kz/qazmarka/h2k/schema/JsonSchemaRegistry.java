package kz.qazmarka.h2k.schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * JsonSchemaRegistry — реестр типов колонок, загружаемый из JSON-файла.
 *
 * Назначение
 *  - сопоставляет имя таблицы и квалификатор колонки с строковым типом Phoenix (например, "VARCHAR", "UNSIGNED_TINYINT");
 *  - терпимо относится к регистру: публикует алиасы имён таблиц/колонок (исходное/UPPER/lower);
 *  - обеспечивает быстрый доступ на «горячем пути» через кэш {TableName -> карта колонок}.
 *
 * Формат файла (пример):
 * {
 *   "TBL_NAME": {
 *     "columns": {
 *       "col1": "VARCHAR",
 *       "col2": "UNSIGNED_TINYINT",
 *       "created_at": "TIMESTAMP"
 *     }
 *   },
 *   "OTHER_TBL": { "columns": { ... } }
 * }
 *
 * Особенности реализации
 *  - для имён таблиц и колонок публикуются алиасы: исходное, верхний и нижний регистры;
 *  - строковые значения типов канонизируются в верхний регистр (Locale.ROOT);
 *  - после загрузки структура иммутабельна (unmodifiable) и атомарно обновляется методом {@link #refresh()};
 *  - чтение потокобезопасно без синхронизации: используется {@link java.util.concurrent.atomic.AtomicReference}
 *    для публикации корня и локальный {@link java.util.concurrent.ConcurrentMap}‑кэш по таблицам.
 *  - типы массивов Phoenix (например, "VARCHAR ARRAY") считаются корректными и не генерируют предупреждений;
 *  - default-namespace можно опускать: записи вида "DEFAULT:TBL" и "TBL" будут доступны при поиске.
 *  - секция {@code "pk": [...] } (необязательно) может содержать имена колонок PK; метод
 *    {@link #primaryKeyColumns(org.apache.hadoop.hbase.TableName)} вернёт их в порядке следования;
 *    при отсутствии секции возвращается пустой массив.
 *  - контракт аргументов: методы API требуют не-null параметров table/qualifier; при null выбрасывается {@link NullPointerException}.
 *  - реестр не выполняет глобальных эвристик — только поиск по паре (table, qualifier) с нормализацией регистра и поддержкой короткого имени (после ':').
 *
 * Совместимость
 *  - Требуется Java 8.
 */
public final class JsonSchemaRegistry implements SchemaRegistry {

    /** Логгер реестра; все сообщения — на русском языке. */
    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaRegistry.class);
    /** Ключ JSON-секции с определениями колонок. */
    private static final String KEY_COLUMNS = "columns";
    /** Ключ JSON-секции с определением PK (массив строк); поддерживаются варианты регистра "pk"/"PK". */
    private static final String KEY_PK = "pk";


    /**
     * Допустимые имена типов (канонизированные в UPPER/Locale.ROOT).
     * Это мягкая валидация: при неизвестном типе пишем WARN и продолжаем.
     */
    private static final Set<String> ALLOWED_TYPES;
    static {
        Set<String> s = new HashSet<>(64);
        // Базовые числовые
        s.add("TINYINT"); s.add("UNSIGNED_TINYINT");
        s.add("SMALLINT"); s.add("UNSIGNED_SMALLINT");
        s.add("INTEGER"); s.add("INT"); s.add("UNSIGNED_INT");
        s.add("BIGINT"); s.add("UNSIGNED_LONG");
        s.add("FLOAT"); s.add("UNSIGNED_FLOAT");
        s.add("DOUBLE"); s.add("UNSIGNED_DOUBLE");
        s.add("DECIMAL");
        // Строки/байты
        s.add("CHAR"); s.add("VARCHAR");
        s.add("BINARY"); s.add("VARBINARY");
        // Даты/время
        s.add("DATE"); s.add("TIME"); s.add("TIMESTAMP");
        // Прочее
        s.add("BOOLEAN");
        // Разрешим и массивы, если в схеме появятся
        s.add("ARRAY");
        ALLOWED_TYPES = Collections.unmodifiableSet(s);
    }

    /**
     * Множество уже залогированных «неизвестных» типов, чтобы не засорять WARN-ами.
     * Используется только для подавления повторных предупреждений и не влияет на функциональность.
     */
    private static final Set<String> UNKNOWN_TYPES_LOGGED = ConcurrentHashMap.newKeySet();

    /**
     * Возвращает true, если тип допустим:
     *  - входит в белый список скалярных типов;
     *  - либо является массивом Phoenix (например, "VARCHAR ARRAY").
     */
    private static boolean isAllowedType(String tname) {
        // Страховка от случайного null, чтобы не ловить NPE при внешних вызовах/изменениях парсинга
        if (tname == null) {
            return false;
        }
        if (ALLOWED_TYPES.contains(tname)) {
            return true;
        }
        // Разрешаем массивы: "VARCHAR ARRAY", "INTEGER ARRAY", ...
        return tname.endsWith(" ARRAY");
    }

    /**
     * Абсолютный или относительный путь к JSON-файлу схемы — источник для {@link #refresh()}.
     */
    private final String sourcePath;

    /**
     * Основная структура:
     * tableName -> (qualifier -> PHOENIX_TYPE_NAME)
     *
     * Иммутабельная карта; ссылка на неё хранится в AtomicReference и
     * переустанавливается атомарно при обновлении (см. {@link #refresh()}).
     * Это даёт корректную видимость между потоками без синхронизации на чтении.
     */
    private final AtomicReference<Map<String, Map<String, String>>> schemaRef;

    /** Публикация алиасов таблицы → массив имён PK (иммутабельно). */
    private final AtomicReference<Map<String, String[]>> pkRef = new AtomicReference<>();

    /**
     * Локальный кэш {TableName -> карта колонок}. Обнуляется при {@link #refresh()}.
     * Устраняет повторную конкатенацию строк имён таблиц на горячем пути.
     */
    private final ConcurrentMap<TableName, Map<String, String>> tableCache = new ConcurrentHashMap<>(64);

    /**
     * Создаёт реестр и сразу выполняет {@link #refresh()}.
     * @param path путь к JSON-файлу (UTF-8), не {@code null}
     */
    public JsonSchemaRegistry(String path) {
        this.sourcePath = java.util.Objects.requireNonNull(path, "schemaPath");
        this.schemaRef = new AtomicReference<>(Collections.emptyMap());
        this.pkRef.set(Collections.emptyMap());
        refresh();
    }

    /**
     * Переинициализирует реестр из {@link #sourcePath}.
     * Атомарно публикует новые иммутабельные структуры и очищает кэш таблиц.
     * При любой ошибке остаётся предыдущий снимок.
     */
    @Override
    public void refresh() {
        final Map<String, Map<String, String>> oldCols = schemaRef.get();
        final Map<String, String[]> oldPk = pkRef.get();
        try {
            ParsedSchema parsed = parseFromDisk(sourcePath);
            schemaRef.set(parsed.columnsByTable);
            pkRef.set(parsed.pkByTable);
            tableCache.clear();
        } catch (java.io.IOException | RuntimeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.warn("Не удалось загрузить файл схемы '{}'; сохранён предыдущий снимок", sourcePath, e);
            } else {
                LOG.warn("Не удалось загрузить файл схемы '{}'; сохранён предыдущий снимок ({}: {})",
                         sourcePath, e.getClass().getSimpleName(), e.getMessage());
            }
            schemaRef.set(oldCols);
            pkRef.set(oldPk);
        }
    }

    /**
     * Внутренний снимок разобранной схемы.
     * Содержит иммутабельные представления колонок и PK, публикуемые атомарно.
     */
    private static final class ParsedSchema {
        final Map<String, Map<String, String>> columnsByTable;
        final Map<String, String[]> pkByTable;
        ParsedSchema(Map<String, Map<String, String>> c, Map<String, String[]> p) {
            this.columnsByTable = c; this.pkByTable = p;
        }
    }

    /**
     * Читает JSON-файл и возвращает корневой объект.
     * @param path путь к файлу (UTF-8)
     * @return корневой {@code JsonObject}
     * @throws IOException при ошибке чтения или парсинга
     */
    private JsonObject readRootJson(String path) throws IOException {
        String json = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        return JsonParser.parseString(json).getAsJsonObject();
    }

    /** Разбирает секцию "columns": нормализует квалификаторы в lower-case и типы в UPPER, с мягкой валидацией. */
    private Map<String, String> parseColumns(JsonObject tableObj, String tableKey, String path) {
        Map<String, String> cols = new HashMap<>();
        JsonElement colsEl = tableObj.get(KEY_COLUMNS);
        if (colsEl != null && colsEl.isJsonObject()) {
            for (Map.Entry<String, JsonElement> c : colsEl.getAsJsonObject().entrySet()) {
                final String qLower = c.getKey().toLowerCase(Locale.ROOT);
                final String rawType = c.getValue().isJsonNull() ? null : c.getValue().getAsString();
                final String tUpper  = (rawType == null) ? null : rawType.trim().toUpperCase(Locale.ROOT);
                if (tUpper != null
                        && !tUpper.isEmpty()
                        && !isAllowedType(tUpper)
                        && UNKNOWN_TYPES_LOGGED.add(tUpper)) {
                    LOG.warn("Неизвестный тип '{}' для колонки '{}' в таблице '{}' (файл '{}') — будет использован как есть",
                            tUpper, c.getKey(), tableKey, path);
                }
                cols.put(qLower, tUpper);
            }
        }
        return cols;
    }

    /** Разбирает секцию "pk"/"PK" и возвращает массив имён PK (или пустой массив). */
    private String[] parsePk(JsonObject tableObj) {
        String[] pk = new String[0];
        JsonElement pkEl = tableObj.has(KEY_PK) ? tableObj.get(KEY_PK) : tableObj.get("PK");
        if (pkEl != null && pkEl.isJsonArray()) {
            JsonArray a = pkEl.getAsJsonArray();
            ArrayList<String> tmp = new ArrayList<>(a.size());
            for (int i = 0; i < a.size(); i++) {
                String q = a.get(i).getAsString();
                if (q != null && !q.trim().isEmpty()) tmp.add(q.trim());
            }
            pk = tmp.toArray(new String[0]);
        }
        return pk;
    }

    /**
     * Парсит JSON‑файл на диске и строит иммутабельные карты:
     *  • {@code tableAlias -> (qualifierAlias -> TYPE)}
     *  • {@code tableAlias -> String[] PK}
     * Табличные алиасы: исходное имя, UPPER, lower, а также «короткое» имя (после двоеточия), если есть.
     * Имена квалификаторов нормализуются до lower-case — один раз на парсинге.
     */
    private ParsedSchema parseFromDisk(String path) throws IOException {
        JsonObject root = readRootJson(path);
        return buildParsedSchema(root, path);
    }

    /**
     * Строит {@code ParsedSchema} из уже прочитанного корня JSON.
     * Выделен отдельно для снижения когнитивной сложности (S1199).
     * @param root корневой объект
     * @param path исходный путь (для диагностики)
     * @return иммутабельный снимок разобранной схемы
     */
    private ParsedSchema buildParsedSchema(JsonObject root, String path) {
        Map<String, Map<String, String>> colsTmp = new HashMap<>();
        Map<String, String[]> pkTmp = new HashMap<>();

        for (Map.Entry<String, JsonElement> e : root.entrySet()) {
            if (!e.getValue().isJsonObject()) {
                continue;
            }
            String tableKey = e.getKey();
            JsonObject tableObj = e.getValue().getAsJsonObject();

            processTableEntry(colsTmp, pkTmp, tableKey, tableObj, path);
        }

        return new ParsedSchema(Collections.unmodifiableMap(colsTmp),
                                Collections.unmodifiableMap(pkTmp));
    }

    /** Обрабатывает одну запись таблицы из корня JSON: парсит колонки/PK и публикует алиасы. */
    private void processTableEntry(Map<String, Map<String, String>> colsOut,
                                   Map<String, String[]> pkOut,
                                   String tableKey,
                                   JsonObject tableObj,
                                   String path) {
        Map<String, String> colsView = Collections.unmodifiableMap(parseColumns(tableObj, tableKey, path));
        String[] pk = parsePk(tableObj);
        publishTableAliases(colsOut, pkOut, tableKey, colsView, pk);
    }

    /** Публикует алиасы таблицы: исходное/UPPER/lower и «короткое» имя. */
    private static void publishTableAliases(Map<String, Map<String, String>> colsOut,
                                            Map<String, String[]> pkOut,
                                            String tableKey,
                                            Map<String, String> colsView,
                                            String[] pk) {
        addAliases(colsOut, pkOut, tableKey, colsView, pk);
        int idx = tableKey.indexOf(':');
        if (idx > 0 && idx + 1 < tableKey.length()) {
            addAliases(colsOut, pkOut, tableKey.substring(idx + 1), colsView, pk);
        }
    }

    /** Публикует исходное/UPPER/lower варианты имени. */
    private static void addAliases(Map<String, Map<String, String>> colsOut,
                                   Map<String, String[]> pkOut,
                                   String name,
                                   Map<String, String> colsView,
                                   String[] pk) {
        if (name == null || name.isEmpty()) return;
        putAlias(colsOut, pkOut, name, colsView, pk);
        String up = name.toUpperCase(Locale.ROOT);
        if (!up.equals(name)) putAlias(colsOut, pkOut, up, colsView, pk);
        String low = name.toLowerCase(Locale.ROOT);
        if (!low.equals(name) && !low.equals(up)) putAlias(colsOut, pkOut, low, colsView, pk);
    }

    /**
     * Registers a single alias mapping for both structures.
     *
     * <p>Both maps receive the same immutable references to avoid extra allocations:
     * alias -> {@code colsView} and alias -> {@code pk}. The values are not copied.</p>
     *
     * @param colsOut destination map: table alias -> immutable map of columns (qualifier -> type)
     * @param pkOut   destination map: table alias -> immutable array of PK column names
     * @param alias   table alias to publish (as-is)
     * @param colsView immutable view of columns to associate with the alias
     * @param pk      immutable PK array to associate with the alias
     */
    private static void putAlias(Map<String, Map<String, String>> colsOut,
                                 Map<String, String[]> pkOut,
                                 String alias,
                                 Map<String, String> colsView,
                                 String[] pk) {
        colsOut.put(alias, colsView);
        pkOut.put(alias, pk);
    }

    /**
     * Возвращает массив имён PK‑колонок (может быть пустым, но не {@code null}).
     * Поиск выполняется по полному и «короткому» имени таблицы (часть после ':').
     * Возвращается копия массива, чтобы защитить неизменяемость снимка.
     *
     * @param table имя таблицы (не {@code null})
     * @return массив имён PK в порядке следования в схеме; пустой массив, если PK не задан
     * @throws NullPointerException если {@code table} равен {@code null}
     */
    @Override
    public String[] primaryKeyColumns(TableName table) {
        java.util.Objects.requireNonNull(table, "table");
        Map<String, String[]> local = pkRef.get();
        String raw = table.getNameAsString();
        String[] found = local.get(raw);
        if (found == null) {
            int idx = raw.indexOf(':');
            if (idx >= 0 && idx + 1 < raw.length()) {
                found = local.get(raw.substring(idx + 1));
            }
        }
        return (found != null) ? found.clone() : new String[0];
    }


    /**
     * Возвращает иммутабельную карту колонок для таблицы: пробует полное имя и короткое (после ':').
     *
     * @param t объект имени таблицы
     * @return карта колонок или пустая карта
     */
    private Map<String, String> resolveColumnsForTable(TableName t) {
        Map<String, Map<String, String>> local = schemaRef.get();
        String raw = t.getNameAsString();
        Map<String, String> found = local.get(raw);
        if (found == null) {
            int idx = raw.indexOf(':');
            if (idx >= 0 && idx + 1 < raw.length()) {
                found = local.get(raw.substring(idx + 1));
            }
        }
        return (found != null) ? found : Collections.emptyMap();
    }

    /**
     * Возвращает строковый тип колонки (PHOENIX_TYPE_NAME) по таблице и имени колонки.
     * Поиск по lower-case ключу (квалификатор нормализуется к lower-case).
     *
     * @return тип как строка или {@code null}, если не найдено
     * @throws NullPointerException если любой из параметров равен {@code null}
     */
    @Override
    public String columnType(TableName table, String qualifier) {
        java.util.Objects.requireNonNull(table, "table");
        java.util.Objects.requireNonNull(qualifier, "qualifier");

        final Map<String, String> cols = tableCache.computeIfAbsent(table, this::resolveColumnsForTable);
        if (cols.isEmpty()) return null;
        return cols.get(qualifier.toLowerCase(Locale.ROOT));
    }

    /**
     * Краткая диагностика состояния реестра: число таблиц, размер кэша и путь к источнику.
     * @return строка для DEBUG‑логов
     */
    @Override
    public String toString() {
        Map<String, Map<String, String>> schema = schemaRef.get();
        return new StringBuilder(96)
                .append("JsonSchemaRegistry{")
                .append("tables=").append(schema.size())
                .append(", cache=").append(tableCache.size())
                .append(", source='").append(sourcePath).append('\'')
                .append('}')
                .toString();
    }
}