package kz.qazmarka.h2k.schema.phoenix;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

class PhoenixPkParserTest {

    private static final TableName TABLE = TableName.valueOf("NS:PK");

    @Test
    @DisplayName("Недостаток байтов для фиксированного сегмента PK → предупреждение и отсутствие значений")
    void fixedSegmentTooShortWarnsOnce() {
        PhoenixPkParser.clearWarnCachesForTest();
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
    void nullPkColumnNameWarned() {
        PhoenixPkParser.clearWarnCachesForTest();
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
        PhoenixPkParser.WarnedColumn warnedColumn = (PhoenixPkParser.WarnedColumn) key;
    assertEquals(TABLE.getNameWithNamespaceInclAsString(), warnedColumn.table(),
        "Ожидалось предупреждение именно для текущей таблицы");
        assertEquals("#0", warnedColumn.column(), "Ожидалось предупреждение для колонки по индексу");
        assertTrue(pkWarned().contains(TABLE.getNameWithNamespaceInclAsString()),
                "Должно быть зафиксировано предупреждение о недекодированном PK");
    }

    private static Set<Object> pkColumnWarned() {
        Set<PhoenixPkParser.WarnedColumn> raw = PhoenixPkParser.pkColumnWarnedSnapshotForTest();
        Set<Object> copy = new HashSet<>();
        for (PhoenixPkParser.WarnedColumn element : raw) {
            copy.add(element);
        }
        return copy;
    }

    private static Set<String> pkWarned() {
        return PhoenixPkParser.pkWarnedSnapshotForTest();
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
