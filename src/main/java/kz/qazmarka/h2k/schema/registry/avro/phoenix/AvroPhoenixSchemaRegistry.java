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
    /** Возвращает подсказку ёмкости JSON, считанную из Avro-схемы. */
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

    /** Позволяет временно подменить логгер для модульных тестов. */
    static AutoCloseable withLoggerForTest(Logger testLogger) {
        if (testLogger == null) {
            throw new IllegalArgumentException("testLogger == null");
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
        Map<String, String> types = new HashMap<>(Math.max(8, schema.getFields().size() * 3));
        for (Field field : schema.getFields()) {
            String type = field.getProp(PROP_TYPE);
            if (type == null || type.trim().isEmpty()) {
                continue;
            }
            String trimmed = type.trim().toUpperCase(Locale.ROOT);
            String name = field.name();
            types.put(name, trimmed);
            types.put(name.toLowerCase(Locale.ROOT), trimmed);
            types.put(name.toUpperCase(Locale.ROOT), trimmed);
        }
        String[] pk = readPk(schema);
        Integer salt = readSaltBytes(schema, table);
        Integer capacity = readCapacityHint(schema, table);
        String[] cfFamilies = readColumnFamilies(schema, table);
        Logger logger = logger();
        if (logger.isDebugEnabled()) {
            String pkText = pk.length == 0 ? "-" : String.join(",", pk);
            String saltText = salt == null ? "-" : salt.toString();
            String capacityText = capacity == null ? "-" : capacity.toString();
            String cfText = cfFamilies.length == 0 ? "-" : String.join(",", cfFamilies);
            logger.debug("Avro-схема {} загружена: файл={}, полей={}, pk={}, соль={}, capacity={}, cf={}, cacheSize={}",
                    table.getNameAsString(),
                    schemaPath.toAbsolutePath(),
                    schema.getFields().size(),
                    pkText,
                    saltText,
                    capacityText,
                    cfText,
                    avroRegistry.cacheSize());
        }
        return new TableMetadata(types, pk, salt, capacity, cfFamilies);
    }

    private String[] readColumnFamilies(Schema schema, TableName table) {
        Object raw = firstNonNull(schema.getObjectProp(PROP_CF_LIST), schema.getProp(PROP_CF_LIST));
        if (raw == null) {
            return SchemaRegistry.EMPTY;
        }
        if (raw instanceof JsonNode) {
            return sanitizeCfFromJson((JsonNode) raw, table);
        }
        if (isLegacyJacksonNode(raw)) {
            return sanitizeCfFromLegacy(raw, table);
        }
        if (raw instanceof java.util.List<?>) {
            return sanitizeCfFromList((java.util.List<?>) raw);
        }
        String text = String.valueOf(raw);
        return sanitizeCfCsv(text, table);
    }

    String[] sanitizeCfFromJson(JsonNode node, TableName table) {
        if (!node.isArray()) {
            logger().warn("Avro-схема {}: h2k.cf.list задан в виде JSON, но не является массивом — значение проигнорировано", table.getNameAsString());
            return SchemaRegistry.EMPTY;
        }
        java.util.List<String> values = new java.util.ArrayList<>(node.size());
        for (int i = 0; i < node.size(); i++) {
            JsonNode item = node.get(i);
            if (item == null || item.isNull()) {
                continue;
            }
            String val = normalizeCfName(item.asText());
            if (val != null) {
                values.add(val);
            }
        }
        return deduplicate(values);
    }

    private String[] sanitizeCfFromLegacy(Object node, TableName table) {
        try {
            java.lang.reflect.Method isArray = node.getClass().getMethod("isArray");
            if (!Boolean.TRUE.equals(isArray.invoke(node))) {
                logger().warn("Avro-схема {}: h2k.cf.list legacy Jackson значение не является массивом — проигнорировано", table.getNameAsString());
                return SchemaRegistry.EMPTY;
            }
            java.lang.reflect.Method sizeMethod = node.getClass().getMethod("size");
            int size = ((Number) sizeMethod.invoke(node)).intValue();
            java.util.List<String> values = new java.util.ArrayList<>(size);
            java.lang.reflect.Method getMethod = node.getClass().getMethod("get", int.class);
            for (int i = 0; i < size; i++) {
                Object element = getMethod.invoke(node, i);
                if (element == null) {
                    continue;
                }
                java.lang.reflect.Method asText = element.getClass().getMethod("asText");
                String text = (String) asText.invoke(element);
                String normalized = normalizeCfName(text);
                if (normalized != null) {
                    values.add(normalized);
                }
            }
            return deduplicate(values);
        } catch (ReflectiveOperationException ex) {
            Logger logger = logger();
            logger.warn("Avro-схема {}: не удалось прочитать legacy JSON h2k.cf.list — значение проигнорировано", table.getNameAsString());
            if (logger.isDebugEnabled()) {
                logger.debug("Трассировка чтения legacy h2k.cf.list", ex);
            }
            return SchemaRegistry.EMPTY;
        }
    }

    private String[] sanitizeCfFromList(java.util.List<?> list) {
        if (list.isEmpty()) {
            return SchemaRegistry.EMPTY;
        }
        java.util.List<String> values = new java.util.ArrayList<>(list.size());
        for (Object o : list) {
            String normalized = normalizeCfName(o == null ? null : String.valueOf(o));
            if (normalized != null) {
                values.add(normalized);
            }
        }
        return deduplicate(values);
    }

    private String[] sanitizeCfCsv(String raw, TableName table) {
        if (raw == null) {
            return SchemaRegistry.EMPTY;
        }
        String text = raw.trim();
        if (text.isEmpty()) {
            return SchemaRegistry.EMPTY;
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
            logger().warn("Avro-схема {}: h2k.cf.list не содержит валидных имён — фильтр будет отключён", table.getNameAsString());
            return SchemaRegistry.EMPTY;
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
            return SchemaRegistry.EMPTY;
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
            logger().warn("Avro-схема {}: h2k.saltBytes={} < 0 — значение будет проигнорировано", table.getNameAsString(), value);
            return null;
        }
        if (value > 8) {
            logger().warn("Avro-схема {}: h2k.saltBytes={} > 8 — значение будет ограничено 8", table.getNameAsString(), value);
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
            logger().warn("Avro-схема {}: h2k.capacityHint={} < 0 — значение будет проигнорировано", table.getNameAsString(), value);
            return null;
        }
        return value;
    }

    private static String[] readPk(Schema schema) {
        Object raw = schema.getObjectProp(PROP_PK);
        if (raw instanceof com.fasterxml.jackson.databind.JsonNode) {
            return readPkFromJsonNode((com.fasterxml.jackson.databind.JsonNode) raw);
        }
        if (isLegacyJacksonNode(raw)) {
            return readPkFromLegacyJsonNode(raw);
        }
        if (raw instanceof java.util.List<?>) {
            java.util.List<?> list = (java.util.List<?>) raw;
            return copyPkFromList(list);
        }
        String json = schema.getProp(PROP_PK);
        if (json != null && !json.trim().isEmpty()) {
            return readPkFromString(json.trim());
        }
        return SchemaRegistry.EMPTY;
    }

    private static String normalizePkEntry(String raw) {
        if (raw == null) {
            return null;
        }
        String v = raw.trim();
        return v.isEmpty() ? null : v;
    }

    private static String[] readPkFromJsonNode(com.fasterxml.jackson.databind.JsonNode node) {
        if (!node.isArray()) {
            return SchemaRegistry.EMPTY;
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
            return SchemaRegistry.EMPTY;
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
            Logger logger = logger();
            logger.warn("Не удалось распарсить h2k.pk из Avro-схемы ({}): {}", json, e.toString());
            if (logger.isDebugEnabled()) {
                logger.debug("Трассировка парсинга h2k.pk", e);
            }
            return SchemaRegistry.EMPTY;
        }
    }

    private static String[] trimPkArray(String[] pk, int size) {
        if (size == 0) {
            return SchemaRegistry.EMPTY;
        }
        if (size == pk.length) {
            return pk;
        }
        String[] trimmed = new String[size];
        System.arraycopy(pk, 0, trimmed, 0, size);
        return trimmed;
    }

    private static boolean isLegacyJacksonNode(Object raw) {
        if (raw == null) {
            return false;
        }
        String name = raw.getClass().getName();
        return name.startsWith("org.codehaus.jackson.");
    }

    private static String[] readPkFromLegacyJsonNode(Object node) {
        try {
            java.lang.reflect.Method isArray = node.getClass().getMethod("isArray");
            if (!Boolean.TRUE.equals(isArray.invoke(node))) {
                return SchemaRegistry.EMPTY;
            }
            java.lang.reflect.Method sizeMethod = node.getClass().getMethod("size");
            int size = ((Number) sizeMethod.invoke(node)).intValue();
            String[] pk = new String[size];
            java.lang.reflect.Method getMethod = node.getClass().getMethod("get", int.class);
            int idx = 0;
            for (int i = 0; i < size; i++) {
                Object element = getMethod.invoke(node, i);
                if (element == null) {
                    continue;
                }
                java.lang.reflect.Method asText = element.getClass().getMethod("asText");
                String text = (String) asText.invoke(element);
                String v = normalizePkEntry(text);
                if (v != null) {
                    pk[idx++] = v;
                }
            }
            return trimPkArray(pk, idx);
        } catch (ReflectiveOperationException e) {
            Logger logger = logger();
            logger.warn("Не удалось прочитать h2k.pk из legacy Jackson: {}", e.toString());
            if (logger.isDebugEnabled()) {
                logger.debug("Трассировка legacy Jackson", e);
            }
            return SchemaRegistry.EMPTY;
        }
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
        logger().warn("Avro-схема {}: свойство {} имеет некорректное значение '{}' — будет проигнорировано",
                table.getNameAsString(), prop, raw);
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
            if (columnTypes.isEmpty()) {
                this.columnTypes = Collections.emptyMap();
            } else {
                this.columnTypes = Collections.unmodifiableMap(new HashMap<>(columnTypes));
            }
            this.pk = normalizePk(pk);
            this.saltBytes = saltBytes;
            this.capacityHint = capacityHint;
            this.cfFamilies = (cfFamilies == null || cfFamilies.length == 0)
                    ? SchemaRegistry.EMPTY
                    : cfFamilies.clone();
        }

        String columnType(String name) {
            if (name == null) {
                return null;
            }
            String type = columnTypes.get(name);
            if (type != null) {
                return type;
            }
            type = columnTypes.get(name.toLowerCase(Locale.ROOT));
            if (type != null) {
                return type;
            }
            type = columnTypes.get(name.toUpperCase(Locale.ROOT));
            if (type != null) {
                return type;
            }
            return null;
        }

        String[] primaryKey() {
            if (pk.length > 0) {
                return pk.clone();
            }
            return SchemaRegistry.EMPTY;
        }

        private static String[] normalizePk(String[] pk) {
            if (pk == null || pk.length == 0) {
                return SchemaRegistry.EMPTY;
            }
            String[] res = new String[pk.length];
            int idx = 0;
            for (String val : pk) {
                if (val == null) {
                    continue;
                }
                String trimmed = val.trim();
                if (!trimmed.isEmpty()) {
                    res[idx++] = trimmed;
                }
            }
            if (idx == 0) {
                return SchemaRegistry.EMPTY;
            }
            if (idx != res.length) {
                String[] trimmed = new String[idx];
                System.arraycopy(res, 0, trimmed, 0, idx);
                return trimmed;
            }
            return res;
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
            return SchemaRegistry.EMPTY;
        }
    }
}
