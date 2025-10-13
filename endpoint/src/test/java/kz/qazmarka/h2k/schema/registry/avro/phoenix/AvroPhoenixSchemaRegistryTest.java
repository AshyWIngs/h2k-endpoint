package kz.qazmarka.h2k.schema.registry.avro.phoenix;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

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
        java.lang.reflect.Method sanitize = AvroPhoenixSchemaRegistry.class
                .getDeclaredMethod("sanitizeCfFromJson", com.fasterxml.jackson.databind.JsonNode.class, TableName.class);
        sanitize.setAccessible(true);
        AvroPhoenixSchemaRegistry registry = registryWithTestSchemas();

        try (WarnCapture capture = new WarnCapture()) {
            String[] families = (String[]) sanitize.invoke(registry, invalidNode, table);
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

        try (WarnCapture capture = new WarnCapture()) {
            String[] families = registry.columnFamilies(table);
            assertArrayEquals(SchemaRegistry.EMPTY, families);
            assertTrue(capture.warnCount() > 0, "WARN должен быть зафиксирован");
            assertTrue(capture.contains("T_AVRO_BAD_CF_CSV"), "WARN должен ссылаться на имя таблицы");
            assertTrue(capture.contains("не содержит валидных имён"),
                    "WARN должен описывать фильтр без валидных значений");
        }
    }

    private AvroPhoenixSchemaRegistry registryWithTestSchemas() {
        AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
        return new AvroPhoenixSchemaRegistry(avro);
    }

    /**
     * Перехватывает WARN-логирование AvroPhoenixSchemaRegistry через подмену SLF4J-логгера.
     */
    private static final class WarnCapture implements AutoCloseable {
        private final Field logField;
        private final Logger original;
        private final Logger proxy;
        private final AtomicInteger warns = new AtomicInteger(0);
        private final List<String> messages = new ArrayList<>();

        WarnCapture() {
            try {
                logField = AvroPhoenixSchemaRegistry.class.getDeclaredField("LOG");
                logField.setAccessible(true);
                Field modifiers = Field.class.getDeclaredField("modifiers");
                modifiers.setAccessible(true);
                modifiers.setInt(logField, logField.getModifiers() & ~Modifier.FINAL);

                original = (Logger) logField.get(null);
                proxy = (Logger) Proxy.newProxyInstance(
                        original.getClass().getClassLoader(),
                        new Class<?>[]{Logger.class},
                        new WarnInterceptor(original, warns, messages));
                logField.set(null, proxy);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Не удалось подменить логгер AvroPhoenixSchemaRegistry", e);
            }
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

        @Override
        public void close() {
            try {
                logField.set(null, original);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Не удалось восстановить исходный логгер", e);
            }
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
            } catch (ReflectiveOperationException e) {
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
