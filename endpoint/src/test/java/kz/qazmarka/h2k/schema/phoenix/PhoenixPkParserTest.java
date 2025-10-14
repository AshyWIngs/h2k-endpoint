package kz.qazmarka.h2k.schema.phoenix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

class PhoenixPkParserTest {

    private static final TableName TABLE = TableName.valueOf("NS:PK");

    @Test
    @DisplayName("Недостаток байтов для фиксированного сегмента PK → предупреждение и отсутствие значений")
    void fixedSegmentTooShortWarnsOnce() throws Exception {
        resetWarnings();
        FakeRegistry registry = new FakeRegistry()
                .primaryKey("ID")
                .type("ID", "UNSIGNED_INT");
        PhoenixPkParser parser = new PhoenixPkParser(registry, new PhoenixColumnTypeRegistry(registry));

        Map<String, Object> out = new HashMap<>();
        parser.decodeRowKey(TABLE, RowKeySlice.whole(new byte[] {0x01, 0x02}), 0, out);

        assertTrue(out.isEmpty(), "PK не должен быть декодирован при нехватке байтов");
        assertEquals(1, pkColumnWarned().size(), "Ожидается одиночное предупреждение о колонке");
        assertEquals(1, pkWarned().size(), "Предупреждение о недекодированных PK должно быть записано один раз");
    }

    @Test
    @DisplayName("PK с null-именем в реестре вызывает предупреждение по индексу")
    void nullPkColumnNameWarned() throws Exception {
        resetWarnings();
        FakeRegistry registry = new FakeRegistry()
                .primaryKey(new String[]{null})
                .type("IGNORED", "VARCHAR");
        PhoenixPkParser parser = new PhoenixPkParser(registry, new PhoenixColumnTypeRegistry(registry));

        Map<String, Object> out = new HashMap<>();
        parser.decodeRowKey(TABLE, RowKeySlice.whole(new byte[] {0x00}), 0, out);

        assertTrue(out.isEmpty(), "PK не должен быть декодирован при null-столбце");

        Set<?> warnedColumns = pkColumnWarned();
        assertEquals(1, warnedColumns.size());
        Object key = warnedColumns.iterator().next();
        assertEquals("#0", readField(key, "column"), "Ожидалось предупреждение для колонки по индексу");
        assertTrue(pkWarned().contains(TABLE.getNameWithNamespaceInclAsString()),
                "Должно быть зафиксировано предупреждение о недекодированном PK");
    }

    private static void resetWarnings() throws Exception {
        clearWarnSet("PK_WARNED");
        clearWarnSet("PK_COLUMN_WARNED");
    }

    private static void clearWarnSet(String fieldName) throws Exception {
        Set<?> set = warnSet(fieldName);
        set.clear();
    }

    private static Set<Object> pkColumnWarned() throws Exception {
        Set<?> raw = warnSet("PK_COLUMN_WARNED");
        Set<Object> copy = new HashSet<>();
        for (Object element : raw) {
            copy.add(element);
        }
        return copy;
    }

    private static Set<String> pkWarned() throws Exception {
        Set<?> raw = warnSet("PK_WARNED");
        Set<String> copy = new HashSet<>();
        for (Object element : raw) {
            copy.add(String.class.cast(element));
        }
        return copy;
    }

    private static Set<?> warnSet(String fieldName) throws Exception {
        Field f = PhoenixPkParser.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return (Set<?>) f.get(null);
    }

    private static Object readField(Object target, String field) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return f.get(target);
    }

    private static final class FakeRegistry implements SchemaRegistry {
        private String[] pk = SchemaRegistry.EMPTY;
        private final Map<String, String> types = new HashMap<>();

        FakeRegistry primaryKey(String... names) {
            this.pk = names == null ? SchemaRegistry.EMPTY : names.clone();
            return this;
        }

        FakeRegistry type(String qualifier, String type) {
            if (qualifier != null) {
                types.put(qualifier, type);
            }
            return this;
        }

        @Override
        public String columnType(TableName table, String qualifier) {
            return types.get(qualifier);
        }

        @Override
        public String[] primaryKeyColumns(TableName table) {
            return pk == null ? SchemaRegistry.EMPTY : pk.clone();
        }
    }
}
