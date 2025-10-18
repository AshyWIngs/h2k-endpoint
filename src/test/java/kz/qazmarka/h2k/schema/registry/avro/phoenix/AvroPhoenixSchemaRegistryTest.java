package kz.qazmarka.h2k.schema.registry.avro.phoenix;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.config.TableValueSource;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

class AvroPhoenixSchemaRegistryTest {

    private static final TableName TABLE_AVRO = TableName.valueOf("T_AVRO");

    @Test
    void shouldReadTypesFromAvroSchema() {
        AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro);

        assertEquals("VARCHAR", registry.columnType(TABLE_AVRO, "value"));
        assertNull(registry.columnType(TABLE_AVRO, "missing"));
        assertArrayEquals(new String[]{"id"}, registry.primaryKeyColumns(TABLE_AVRO));
        PhoenixTableMetadataProvider provider = registry;
        assertEquals(Integer.valueOf(0), provider.saltBytes(TABLE_AVRO));
        assertEquals(Integer.valueOf(4), provider.capacityHint(TABLE_AVRO));
    }

    @Test
    void shouldFailWhenSchemaMissing() {
        AvroPhoenixSchemaRegistry registry = registryWithTestSchemas();
        TableName missing = TableName.valueOf("T_AVRO_MISSING");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> registry.columnType(missing, "id"));
        assertTrue(ex.getMessage().contains("Не найдена Avro‑схема"), "Диагностика должна указывать на отсутствующий .avsc");
    }

    @Test
    void shouldIgnoreBlankPhoenixType() {
        AvroPhoenixSchemaRegistry registry = registryWithTestSchemas();
        TableName table = TableName.valueOf("T_AVRO_INVALID_TYPE");

        assertEquals("VARCHAR", registry.columnType(table, "id"));
        assertNull(registry.columnType(table, "value"), "Пустой h2k.phoenixType должен игнорироваться");
    }

    @Test
    void shouldWarnAndFallbackWhenCfJsonIsNotArray() throws Exception {
        TableName table = TableName.valueOf("T_AVRO_BAD_CF_JSON");
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode invalidNode = mapper.readTree("{\"family\":\"data\"}");
        AvroPhoenixSchemaRegistry registry = registryWithTestSchemas();

        try (WarnCapture capture = WarnCapture.capture()) {
            String[] families = registry.sanitizeCfFromJson(invalidNode, table);
            assertArrayEquals(SchemaRegistry.EMPTY, families);
            assertTrue(capture.warnCount() > 0, "WARN должен быть зафиксирован");
            assertTrue(capture.contains("T_AVRO_BAD_CF_JSON"), "WARN должен ссылаться на имя таблицы");
            assertTrue(capture.contains("h2k.cf.list") && capture.contains("не является массивом"),
                    "WARN должен описывать некорректный JSON");
        }
    }

    @Test
    void shouldWarnAndFallbackWhenCfCsvHasNoValidValues() {
        AvroPhoenixSchemaRegistry registry = registryWithTestSchemas();
        TableName table = TableName.valueOf("T_AVRO_BAD_CF_CSV");

        try (WarnCapture capture = WarnCapture.capture()) {
            String[] families = registry.columnFamilies(table);
            assertArrayEquals(SchemaRegistry.EMPTY, families);
            assertEquals(1, capture.warnCount(), "Ожидаем единичный WARN при пустом CSV");
            assertTrue(capture.contains("T_AVRO_BAD_CF_CSV"), "WARN должен ссылаться на имя таблицы");
            assertTrue(capture.contains("не содержит валидных имён"),
                    "WARN должен описывать фильтр без валидных значений");
        }
    }

    @Test
    void shouldNormalizeAndDeduplicateJsonCfList() {
        TableName table = TableName.valueOf("T_AVRO_GOOD_CF_JSON");
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.node.ArrayNode node = mapper.createArrayNode();
        node.add(" data ");
        node.addNull();
        node.add("data");
        node.add("");
        node.add("meta");
        node.add(" meta ");
        AvroPhoenixSchemaRegistry registry = registryWithTestSchemas();

        try (WarnCapture capture = WarnCapture.capture()) {
            String[] families = registry.sanitizeCfFromJson(node, table);
            assertArrayEquals(new String[]{"data", "meta"}, families);
            assertEquals(0, capture.warnCount(), "Корректный массив не должен генерировать WARN");
        }
    }

    @Test
    void shouldNormalizeCsvCfList(@TempDir Path tempDir) throws Exception {
        TableName table = TableName.valueOf("T_META_CF");
        Path schemaPath = tempDir.resolve("t_meta_cf.avsc");
        Files.write(schemaPath, schemaJson("T_META_CF", null, null, " cf1 , cf2 , cf1 ").getBytes(StandardCharsets.UTF_8));

        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(new AvroSchemaRegistry(tempDir));
        try (WarnCapture capture = WarnCapture.capture()) {
            String[] families = registry.columnFamilies(table);
            assertArrayEquals(new String[]{"cf1", "cf2"}, families);
            assertEquals(0, capture.warnCount(), "Корректный CSV не должен генерировать WARN");
        }
    }

    @Test
    void shouldParseLegacyJsonCfListFromAvroSchema(@TempDir Path tempDir) throws Exception {
        TableName table = TableName.valueOf("T_META_CF_JSON");
        Path schemaPath = tempDir.resolve("t_meta_cf_json.avsc");
        Files.write(schemaPath, schemaJsonRawCf("T_META_CF_JSON", null, null, "[\"data\", \" meta \", \"\", \"data\"]").getBytes(StandardCharsets.UTF_8));

        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(new AvroSchemaRegistry(tempDir));
        try (WarnCapture capture = WarnCapture.capture()) {
            String[] families = registry.columnFamilies(table);
            assertArrayEquals(new String[]{"data", "meta"}, families);
            assertEquals(0, capture.warnCount(), "Корректный JSON-массив не должен генерировать WARN");
        }
    }

    @Test
    void shouldWarnAndDisableWhenJsonArrayNormalizesToEmpty(@TempDir Path tempDir) throws Exception {
        TableName table = TableName.valueOf("T_META_CF_JSON_EMPTY");
        Path schemaPath = tempDir.resolve("t_meta_cf_json_empty.avsc");
        // JSON-массив с пустыми/пробельными значениями и null -> после нормализации фильтр отключается
        Files.write(schemaPath, schemaJsonRawCf("T_META_CF_JSON_EMPTY", null, null, "[\" \", null, \"   \", \"\"]").getBytes(StandardCharsets.UTF_8));

        AvroSchemaRegistry avro = new AvroSchemaRegistry(tempDir);
    Schema schema = avro.getByTable(table.getNameAsString());
    Object raw = schema.getObjectProp("h2k.cf.list");
    String rawType = raw == null ? "null" : raw.getClass().getName();
    assertTrue(raw instanceof List,
        "Свойство h2k.cf.list должно десериализоваться в java.util.List (class=" + rawType + ")");

        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro);
        try (WarnCapture capture = WarnCapture.capture()) {
            String[] families = registry.columnFamilies(table);
            assertArrayEquals(SchemaRegistry.EMPTY, families, "Возвращены CF: " + Arrays.toString(families));
            assertEquals(1, capture.warnCount(), "Пустой JSON-массив должен отключать фильтр с единичным WARN");
            assertTrue(capture.contains("не содержит валидных имён"), "WARN должен описывать пустой фильтр");
        }
    }

    @Test
    void shouldWarnAndDisableWhenListCfNormalizesToEmpty() {
        AvroPhoenixSchemaRegistry registry = registryWithTestSchemas();
        TableName table = TableName.valueOf("T_META_CF_LIST_EMPTY");
        List<Object> raw = Arrays.asList("   ", null, "\t");

        try (WarnCapture capture = WarnCapture.capture()) {
            String[] families = registry.sanitizeCfFromList(raw, table);
            assertArrayEquals(SchemaRegistry.EMPTY, families);
            assertEquals(1, capture.warnCount(), "Пустой java.util.List должен приводить к единичному WARN");
            assertTrue(capture.contains("не содержит валидных имён"), "WARN должен описывать пустой фильтр");
        }
    }

    @Test
    void shouldNormalizeMixedTokensInJsonCfList(@TempDir Path tempDir) throws Exception {
        TableName table = TableName.valueOf("T_META_CF_JSON_MIXED");
        Path schemaPath = tempDir.resolve("t_meta_cf_json_mixed.avsc");
    Files.write(schemaPath, schemaJsonRawCf("T_META_CF_JSON_MIXED", null, null,
        "[\" cf1 \", \"\", \"CF1\", null, \"cf2\", \" cf2 \", \"\\t\"]").getBytes(StandardCharsets.UTF_8));

        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(new AvroSchemaRegistry(tempDir));
        try (WarnCapture capture = WarnCapture.capture()) {
            String[] families = registry.columnFamilies(table);
            // CF регистрозависимы, поэтому "CF1" сохраняется отдельным элементом.
            assertArrayEquals(new String[]{"cf1", "CF1", "cf2"}, families,
                    "Возвращены CF: " + Arrays.toString(families));
            assertEquals(0, capture.warnCount(), "Валидные значения с шумом не должны генерировать WARN");
        }
    }

    @Test
    void shouldUseAvroSaltAndCapacityWhenAvailable(@TempDir Path tempDir) throws Exception {
        TableName table = TableName.valueOf("T_META_POS");
        Path schemaPath = tempDir.resolve("t_meta_pos.avsc");
        Files.write(schemaPath, schemaJson("T_META_POS", 1, 12).getBytes(StandardCharsets.UTF_8));

        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(new AvroSchemaRegistry(tempDir));
        assertEquals(Integer.valueOf(1), registry.saltBytes(table));
        assertEquals(Integer.valueOf(12), registry.capacityHint(table));

        Configuration cfg = new Configuration(false);
        cfg.set("h2k.capacity.hints.DEFAULT:T_META_POS", "999"); // legacy конфиг игнорируется

        H2kConfig config = H2kConfig.from(cfg, "mock:9092", registry);
    TableOptionsSnapshot snapshot = config.describeTableOptions(table);
        assertEquals(1, snapshot.saltBytes());
    assertEquals(TableValueSource.AVRO, snapshot.saltSource());
        assertEquals(12, snapshot.capacityHint());
    assertEquals(TableValueSource.AVRO, snapshot.capacitySource());
    }

    @Test
    void shouldWarnAndFallbackForNegativeSaltOrCapacity(@TempDir Path tempDir) throws Exception {
        TableName table = TableName.valueOf("T_META_NEG");
        Path schemaPath = tempDir.resolve("t_meta_neg.avsc");
        Files.write(schemaPath, schemaJson("T_META_NEG", -3, -7).getBytes(StandardCharsets.UTF_8));

        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(new AvroSchemaRegistry(tempDir));
        try (WarnCapture capture = WarnCapture.capture()) {
            assertNull(registry.saltBytes(table), "Отрицательная соль должна игнорироваться");
            assertNull(registry.capacityHint(table), "Отрицательная подсказка должна игнорироваться");
            assertTrue(capture.warnCount() >= 2, "Ожидаем хотя бы два предупреждения (соль и подсказка)");
            assertTrue(capture.contains("h2k.saltBytes"), "WARN должен упоминать h2k.saltBytes");
            assertTrue(capture.contains("h2k.capacityHint"), "WARN должен упоминать h2k.capacityHint");
        }

    H2kConfig config = H2kConfig.from(new Configuration(false), "mock:9092", registry);
    TableOptionsSnapshot snapshot = config.describeTableOptions(table);
        assertEquals(0, snapshot.saltBytes());
    assertEquals(TableValueSource.DEFAULT, snapshot.saltSource());
        assertEquals(0, snapshot.capacityHint());
    assertEquals(TableValueSource.DEFAULT, snapshot.capacitySource());
    }

    @Test
    void shouldFallbackToDefaultsWhenSaltAndCapacityMissing(@TempDir Path tempDir) throws Exception {
        TableName table = TableName.valueOf("T_META_EMPTY");
        Path schemaPath = tempDir.resolve("t_meta_empty.avsc");
        Files.write(schemaPath, schemaJson("T_META_EMPTY", null, null).getBytes(StandardCharsets.UTF_8));

        AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(new AvroSchemaRegistry(tempDir));
        try (WarnCapture capture = WarnCapture.capture()) {
            assertNull(registry.saltBytes(table));
            assertNull(registry.capacityHint(table));
            assertEquals(0, capture.warnCount(), "Отсутствие свойств не должно генерировать WARN");
        }

    H2kConfig config = H2kConfig.from(new Configuration(false), "mock:9092", registry);
    TableOptionsSnapshot snapshot = config.describeTableOptions(table);
        assertEquals(0, snapshot.saltBytes());
    assertEquals(TableValueSource.DEFAULT, snapshot.saltSource());
        assertEquals(0, snapshot.capacityHint());
    assertEquals(TableValueSource.DEFAULT, snapshot.capacitySource());
    }

    private static String schemaJson(String recordName, Integer saltBytes, Integer capacityHint) {
        return schemaJson(recordName, saltBytes, capacityHint, null);
    }

    private static String schemaJson(String recordName, Integer saltBytes, Integer capacityHint, String cfList) {
        return schemaJsonInternal(recordName, saltBytes, capacityHint, cfList == null ? null : "\"" + cfList + "\"");
    }

    private static String schemaJsonRawCf(String recordName, Integer saltBytes, Integer capacityHint, String cfRawJson) {
        return schemaJsonInternal(recordName, saltBytes, capacityHint, cfRawJson);
    }

    private static String schemaJsonInternal(String recordName, Integer saltBytes, Integer capacityHint, String cfValueLiteral) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"type\": \"record\",\n");
        sb.append("  \"name\": \"").append(recordName).append("\",\n");
        sb.append("  \"namespace\": \"kz.qazmarka.temp\",\n");
        if (saltBytes != null) {
            sb.append("  \"h2k.saltBytes\": ").append(saltBytes).append(",\n");
        }
        if (capacityHint != null) {
            sb.append("  \"h2k.capacityHint\": ").append(capacityHint).append(",\n");
        }
        if (cfValueLiteral != null) {
            sb.append("  \"h2k.cf.list\": ").append(cfValueLiteral).append(",\n");
        }
        sb.append("  \"h2k.pk\": [\"id\"],\n");
        sb.append("  \"fields\": [\n");
        sb.append("    {\n");
        sb.append("      \"name\": \"id\",\n");
        sb.append("      \"type\": \"string\",\n");
        sb.append("      \"h2k.phoenixType\": \"VARCHAR\"\n");
        sb.append("    },\n");
        sb.append("    {\n");
        sb.append("      \"name\": \"value\",\n");
        sb.append("      \"type\": [\"null\", \"string\"],\n");
        sb.append("      \"default\": null,\n");
        sb.append("      \"h2k.phoenixType\": \"VARCHAR\"\n");
        sb.append("    }\n");
        sb.append("  ]\n");
        sb.append("}\n");
        return sb.toString();
    }

    private AvroPhoenixSchemaRegistry registryWithTestSchemas() {
        AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
        return new AvroPhoenixSchemaRegistry(avro);
    }

    /**
     * Перехватывает WARN-логирование AvroPhoenixSchemaRegistry через подмену SLF4J-логгера.
     */
    private static final class WarnCapture implements AutoCloseable {
        private final AtomicInteger warns;
        private final List<String> messages;
        private final AutoCloseable restorer;

        private WarnCapture(AtomicInteger warns, List<String> messages, AutoCloseable restorer) {
            this.warns = warns;
            this.messages = messages;
            this.restorer = restorer;
        }

        @Override
        public void close() {
            try {
                restorer.close();
            } catch (Exception e) {
                throw new IllegalStateException("Не удалось восстановить исходный логгер", e);
            }
        }

        static WarnCapture capture() {
            Logger delegate = LoggerFactory.getLogger(AvroPhoenixSchemaRegistry.class);
            AtomicInteger warns = new AtomicInteger(0);
            List<String> messages = new ArrayList<>();
            Logger proxy = (Logger) Proxy.newProxyInstance(
                    delegate.getClass().getClassLoader(),
                    new Class<?>[]{Logger.class},
                    new WarnInterceptor(delegate, warns, messages));
            AutoCloseable restorer = AvroPhoenixSchemaRegistry.withLoggerForTest(proxy);
            return new WarnCapture(warns, messages, restorer);
        }

        int warnCount() {
            return warns.get();
        }

        boolean contains(String fragment) {
            for (String msg : messages) {
                if (msg != null && msg.contains(fragment)) {
                    return true;
                }
            }
            return false;
        }

    }

    private static final class WarnInterceptor implements InvocationHandler {
        private final Logger delegate;
        private final AtomicInteger warns;
        private final List<String> messages;

        WarnInterceptor(Logger delegate, AtomicInteger warns, List<String> messages) {
            this.delegate = delegate;
            this.warns = warns;
            this.messages = messages;
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
            if ("warn".equals(method.getName())) {
                warns.incrementAndGet();
                messages.add(resolveMessage(args));
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                throw cause != null ? cause : e;
            }
        }

        private String resolveMessage(Object[] args) {
            if (args == null || args.length == 0) {
                return null;
            }
            int offset = 0;
            if (args[0] instanceof org.slf4j.Marker) {
                offset = 1;
            }
            if (offset >= args.length) {
                return null;
            }
            Object first = args[offset];
            if (!(first instanceof String)) {
                return first == null ? null : first.toString();
            }
            Object[] fmtArgs = (args.length - offset - 1) == 0
                    ? new Object[0]
                    : Arrays.copyOfRange(args, offset + 1, args.length);
            FormattingTuple tuple = MessageFormatter.arrayFormat((String) first, fmtArgs);
            return tuple.getMessage();
        }
    }
}
