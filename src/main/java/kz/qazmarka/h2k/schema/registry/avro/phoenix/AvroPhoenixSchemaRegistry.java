package kz.qazmarka.h2k.schema.registry.avro.phoenix;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Реестр Phoenix-метаданных, считываемых из Avro-схем.
 */
public final class AvroPhoenixSchemaRegistry implements SchemaRegistry, PhoenixTableMetadataProvider {

    private static final AtomicReference<Logger> LOG = new AtomicReference<>(LoggerFactory.getLogger(AvroPhoenixSchemaRegistry.class));

    private static final String PROP_PK = "h2k.pk";
    private static final String PROP_TYPE = "h2k.phoenixType";
    private static final String PROP_SALT = "h2k.saltBytes";
    private static final String PROP_CAPACITY = "h2k.capacityHint";
    private static final String PROP_CF_LIST = "h2k.cf.list";
    private static final String WARN_CF_FILTER_DISABLED = "Avro-схема {}: h2k.cf.list не содержит валидных имён — фильтр будет отключён";
    private static final ObjectMapper PK_MAPPER = new ObjectMapper();

    private final AvroSchemaRegistry avroRegistry;
    private final ConcurrentMap<String, TableMetadata> cache = new ConcurrentHashMap<>();

    /**
     * @param avroRegistry локальный реестр Avro-схем (обязательный)
     */
    public AvroPhoenixSchemaRegistry(AvroSchemaRegistry avroRegistry) {
        this.avroRegistry = Objects.requireNonNull(avroRegistry, "Аргумент 'avroRegistry' не может быть null");
    }

    @Override
    /** Возвращает тип Phoenix для указанной таблицы/колонки, считывая его из Avro-схемы. */
    public String columnType(TableName table, String qualifier) {
        TableMetadata meta = metadataFor(table);
        return meta.columnType(qualifier);
    }

    @Override
    /** Возвращает массив имён PK, заданный в Avro-схеме. */
    public String[] primaryKeyColumns(TableName table) {
        return metadataFor(table).primaryKey();
    }

    @Override
    /** Возвращает количество байт соли rowkey, указанное в Avro-схеме. */
    public Integer saltBytes(TableName table) {
        return metadataFor(table).saltBytes();
    }

    @Override
    /** Возвращает подсказку ёмкости payload, считанную из Avro-схемы. */
    public Integer capacityHint(TableName table) {
        return metadataFor(table).capacityHint();
    }

    @Override
    public String[] columnFamilies(TableName table) {
        return metadataFor(table).columnFamilies();
    }

    private static Logger logger() {
        return LOG.get();
    }

    private static String normalizeColumnKey(String qualifier) {
        if (qualifier == null) {
            return "";
        }
        return qualifier.trim().toUpperCase(Locale.ROOT);
    }

    /** Позволяет временно подменить логгер для модульных тестов. */
    static AutoCloseable withLoggerForTest(Logger testLogger) {
        if (testLogger == null) {
            throw new IllegalArgumentException("Аргумент 'testLogger' не может быть null");
        }
        Logger previous = LOG.getAndSet(testLogger);
        return () -> LOG.set(previous);
    }

    /** Читает и кеширует метаданные таблицы, используя локальный .avsc. */
    private TableMetadata metadataFor(TableName table) {
        String key = table.getNameAsString().toUpperCase(Locale.ROOT);
        return cache.computeIfAbsent(key, k -> loadMetadata(table));
    }

    /** Загружает Avro-схему и извлекает из неё типы, PK, соль и подсказку ёмкости. */
    private TableMetadata loadMetadata(TableName table) {
        Path schemaPath = avroRegistry.schemaPath(table.getNameAsString());
        Schema schema = avroRegistry.getByTable(table.getNameAsString());
        Map<String, String> types = extractColumnTypes(schema);
        String[] pk = readPk(schema);
        Integer salt = readSaltBytes(schema, table);
        Integer capacity = readCapacityHint(schema, table);
        String[] cfFamilies = readColumnFamilies(schema, table);
        SchemaDebugInfo debugInfo = new SchemaDebugInfo(table, schema, schemaPath, logger())
                .withPk(pk)
                .withSalt(salt)
                .withCapacity(capacity)
                .withCfFamilies(cfFamilies);
        logSchemaDetails(debugInfo);
        return new TableMetadata(types, pk, salt, capacity, cfFamilies);
    }

    private Map<String, String> extractColumnTypes(Schema schema) {
        Map<String, String> types = new HashMap<>(Math.max(8, schema.getFields().size()));
        for (Field field : schema.getFields()) {
            String type = field.getProp(PROP_TYPE);
            if (type == null || type.trim().isEmpty()) {
                continue;
            }
            String trimmed = type.trim().toUpperCase(Locale.ROOT);
            String name = normalizeColumnKey(field.name());
            types.put(name, trimmed);
        }
        return types;
    }

    private static final class SchemaDebugInfo {
        final TableName table;
        final Schema schema;
        final Path schemaPath;
        final Logger log;
        String[] pk;
        Integer salt;
        Integer capacity;
        String[] cfFamilies;

        SchemaDebugInfo(TableName table,
                        Schema schema,
                        Path schemaPath,
                        Logger log) {
            this.table = table;
            this.schema = schema;
            this.schemaPath = schemaPath;
            this.pk = EMPTY;
            this.cfFamilies = EMPTY;
            this.log = log;
        }

        SchemaDebugInfo withPk(String[] pk) {
            this.pk = pk == null ? EMPTY : pk;
            return this;
        }

        SchemaDebugInfo withSalt(Integer salt) {
            this.salt = salt;
            return this;
        }

        SchemaDebugInfo withCapacity(Integer capacity) {
            this.capacity = capacity;
            return this;
        }

        SchemaDebugInfo withCfFamilies(String[] cfFamilies) {
            this.cfFamilies = (cfFamilies == null) ? EMPTY : cfFamilies;
            return this;
        }
    }

    private void logSchemaDetails(SchemaDebugInfo info) {
        if (!info.log.isDebugEnabled()) {
            return;
        }
        String pkText = info.pk.length == 0 ? "-" : String.join(",", info.pk);
        String saltText = info.salt == null ? "-" : info.salt.toString();
        String capacityText = info.capacity == null ? "-" : info.capacity.toString();
        String cfText = info.cfFamilies.length == 0 ? "-" : String.join(",", info.cfFamilies);
        info.log.debug("Avro-схема {} загружена: файл={}, полей={}, pk={}, соль={}, capacity={}, cf={}, cacheSize={}",
                info.table.getNameAsString(),
                info.schemaPath.toAbsolutePath(),
                info.schema.getFields().size(),
                pkText,
                saltText,
                capacityText,
                cfText,
                avroRegistry.cacheSize());
    }

    /** Считывает CF из Avro-свойства: поддерживается только CSV-строка, остальные типы отключают фильтр. */
    private String[] readColumnFamilies(Schema schema, TableName table) {
        Object raw = firstNonNull(schema.getObjectProp(PROP_CF_LIST), schema.getProp(PROP_CF_LIST));
        if (raw == null) {
            return EMPTY;
        }
        if (!(raw instanceof CharSequence)) {
            Logger log = logger();
            log.warn("Avro-схема {}: h2k.cf.list имеет неподдерживаемый тип — фильтр будет отключён",
                    table.getNameAsString());
            log.debug("h2k.cf.list: неподдерживаемый тип: {}", raw.getClass().getName());
            return EMPTY;
        }
        return sanitizeCfCsv(raw.toString(), table);
    }

    /**
     * Нормализует список CF из CSV-строки: разбивает по запятым, обрезает пробелы,
     * отбрасывает пустые, устраняет дубликаты. Если после очистки список пуст —
     * фильтр CF отключается.
     */
    private String[] sanitizeCfCsv(String raw, TableName table) {
        if (raw == null) {
            return EMPTY;
        }
        String text = raw.trim();
        if (text.isEmpty()) {
            return EMPTY;
        }
        String[] parts = text.split(",");
        java.util.List<String> values = new java.util.ArrayList<>(parts.length);
        for (String part : parts) {
            String normalized = normalizeCfName(part);
            if (normalized != null) {
                values.add(normalized);
            }
        }
        if (values.isEmpty()) {
            logger().warn(WARN_CF_FILTER_DISABLED, table.getNameAsString());
            return EMPTY;
        }
        return deduplicate(values);
    }

    private static String normalizeCfName(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String[] deduplicate(java.util.List<String> values) {
        if (values.isEmpty()) {
            return EMPTY;
        }
        java.util.LinkedHashSet<String> unique = new java.util.LinkedHashSet<>(values);
        return unique.toArray(new String[0]);
    }

    private Integer readSaltBytes(Schema schema, TableName table) {
        Object raw = firstNonNull(schema.getObjectProp(PROP_SALT), schema.getProp(PROP_SALT));
        if (raw == null) {
            return null;
        }
        Integer value = coerceInteger(raw);
        if (value == null) {
            logInvalidNumeric(table, PROP_SALT, raw);
            return null;
        }
        if (value < 0) {
            logger().warn("Avro-схема {}: h2k.saltBytes < 0 — значение будет проигнорировано", table.getNameAsString());
            logger().debug("h2k.saltBytes: отрицательное значение: {}", value);
            return null;
        }
        if (value > 8) {
            logger().warn("Avro-схема {}: h2k.saltBytes > 8 — значение будет ограничено 8", table.getNameAsString());
            logger().debug("h2k.saltBytes: исходное значение ограничено: {}", value);
            return 8;
        }
        return value;
    }

    private Integer readCapacityHint(Schema schema, TableName table) {
        Object raw = firstNonNull(schema.getObjectProp(PROP_CAPACITY), schema.getProp(PROP_CAPACITY));
        if (raw == null) {
            return null;
        }
        Integer value = coerceInteger(raw);
        if (value == null) {
            logInvalidNumeric(table, PROP_CAPACITY, raw);
            return null;
        }
        if (value < 0) {
            logger().warn("Avro-схема {}: h2k.capacityHint < 0 — значение будет проигнорировано", table.getNameAsString());
            logger().debug("h2k.capacityHint: отрицательное значение: {}", value);
            return null;
        }
        return value;
    }

    private static String[] readPk(Schema schema) {
        Object raw = schema.getObjectProp(PROP_PK);
        if (raw != null) {
            String[] fromObject = readPkFromObjectProp(schema, raw);
            if (fromObject.length > 0 || schema.getProp(PROP_PK) == null) {
                return fromObject;
            }
        }
        return readPkFromSchemaProperty(schema.getProp(PROP_PK));
    }

    private static String[] readPkFromObjectProp(Schema schema, Object raw) {
        if (raw instanceof JsonNode) {
            return readPkFromJsonNode((JsonNode) raw);
        }
        if (raw instanceof java.util.List<?>) {
            return copyPkFromList((java.util.List<?>) raw);
        }
        if (raw instanceof CharSequence) {
            String text = raw.toString().trim();
            return text.isEmpty() ? EMPTY : readPkFromString(text);
        }
        Logger log = logger();
        log.warn("Avro-схема {}: h2k.pk имеет неподдерживаемый тип — значение будет проигнорировано", schema.getFullName());
        if (raw != null) {
            log.debug("h2k.pk: неподдерживаемый тип: {}", raw.getClass().getName());
        } else if (log.isDebugEnabled()) {
            log.debug("h2k.pk: неподдерживаемый тип: null");
        }
        return EMPTY;
    }

    private static String[] readPkFromSchemaProperty(String property) {
        if (property == null) {
            return EMPTY;
        }
        String text = property.trim();
        return text.isEmpty() ? EMPTY : readPkFromString(text);
    }

    private static String normalizePkEntry(String raw) {
        if (raw == null) {
            return null;
        }
        String v = raw.trim();
        return v.isEmpty() ? null : v;
    }

    private static String[] readPkFromJsonNode(JsonNode node) {
        if (!node.isArray()) {
            return EMPTY;
        }
        String[] pk = new String[node.size()];
        int idx = 0;
        for (int i = 0; i < node.size(); i++) {
            String v = normalizePkEntry(node.get(i).asText());
            if (v != null) {
                pk[idx++] = v;
            }
        }
        return trimPkArray(pk, idx);
    }

    private static String[] copyPkFromList(java.util.List<?> list) {
        if (list.isEmpty()) {
            return EMPTY;
        }
        String[] pk = new String[list.size()];
        int idx = 0;
        for (Object o : list) {
            String v = normalizePkEntry(String.valueOf(o));
            if (v != null) {
                pk[idx++] = v;
            }
        }
        return trimPkArray(pk, idx);
    }

    private static String[] readPkFromString(String json) {
        try {
            JsonNode node = PK_MAPPER.readTree(json);
            return readPkFromJsonNode(node);
        } catch (java.io.IOException e) {
            Logger log = logger();
            // Короткое предупреждение без длинного содержимого строки; подробности уходим в DEBUG.
            log.warn("Не удалось распарсить h2k.pk из Avro-схемы — значение будет проигнорировано: {}", e.getMessage());
            log.debug("Трассировка парсинга h2k.pk, исходное значение='{}'", json, e);
            return EMPTY;
        }
    }

    private static String[] trimPkArray(String[] pk, int size) {
        if (size == 0) {
            return EMPTY;
        }
        if (size == pk.length) {
            return pk;
        }
        String[] trimmed = new String[size];
        System.arraycopy(pk, 0, trimmed, 0, size);
        return trimmed;
    }

    private static Object firstNonNull(Object a, Object b) {
        return (a != null) ? a : b;
    }

    private static Integer coerceInteger(Object raw) {
        if (raw instanceof Number) {
            return ((Number) raw).intValue();
        }
        if (raw instanceof String) {
            String s = ((String) raw).trim();
            if (s.isEmpty()) {
                return null;
            }
            try {
                return Integer.valueOf(s);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    private void logInvalidNumeric(TableName table, String prop, Object raw) {
        Logger log = logger();
        log.warn("Avro-схема {}: свойство {} имеет некорректное значение — будет проигнорировано",
        table.getNameAsString(), prop);
        log.debug("Некорректное числовое свойство {}: исходное значение='{}'", prop, raw);
    }

    private static final class TableMetadata {
        private final Map<String, String> columnTypes;
        private final String[] pk;
        private final Integer saltBytes;
        private final Integer capacityHint;
        private final String[] cfFamilies;

        TableMetadata(Map<String, String> columnTypes,
                      String[] pk,
                      Integer saltBytes,
                      Integer capacityHint,
                      String[] cfFamilies) {
            this.columnTypes = columnTypes.isEmpty()
                    ? Collections.emptyMap()
                    : Collections.unmodifiableMap(new HashMap<>(columnTypes));
            this.pk = normalizePk(pk);
            this.saltBytes = saltBytes;
            this.capacityHint = capacityHint;
            this.cfFamilies = (cfFamilies == null || cfFamilies.length == 0)
                    ? EMPTY
                    : cfFamilies.clone();
        }

        String columnType(String name) {
            if (name == null) {
                return null;
            }
            return columnTypes.get(normalizeColumnKey(name));
        }

        String[] primaryKey() {
            if (pk.length > 0) {
                return pk.clone();
            }
            return EMPTY;
        }

        private static String[] normalizePk(String[] pk) {
            if (pk == null || pk.length == 0) {
                return EMPTY;
            }
            String[] normalized = new String[pk.length];
            int size = 0;
            for (String entry : pk) {
                String trimmed = trimEntry(entry);
                if (trimmed != null) {
                    normalized[size++] = trimmed;
                }
            }
            return size == 0 ? EMPTY : shrink(normalized, size);
        }

        private static String trimEntry(String value) {
            if (value == null) {
                return null;
            }
            String trimmed = value.trim();
            return trimmed.isEmpty() ? null : trimmed;
        }

        private static String[] shrink(String[] source, int size) {
            if (size == source.length) {
                return source;
            }
            String[] result = new String[size];
            System.arraycopy(source, 0, result, 0, size);
            return result;
        }

        Integer saltBytes() {
            return saltBytes;
        }

        Integer capacityHint() {
            return capacityHint;
        }

        String[] columnFamilies() {
            if (cfFamilies.length > 0) {
                String[] copy = new String[cfFamilies.length];
                System.arraycopy(cfFamilies, 0, copy, 0, cfFamilies.length);
                return copy;
            }
            return EMPTY;
        }
    }
}
