package kz.qazmarka.h2k.schema.registry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PhoenixTableMetadataProviderBuilderTest {

    private static final TableName TABLE = TableName.valueOf("NS", "TAB");

    @Test
    @DisplayName("columnFamilies очищает null/пустые значения и убирает дубли без учёта регистра")
    void columnFamiliesSanitized() {
        PhoenixTableMetadataProvider provider = PhoenixTableMetadataProvider.builder()
                .table(TABLE)
                .columnFamilies("  cf ", null, "", "CF", "cf")
                .primaryKeyColumns("id")
                .done()
                .build();

        assertArrayEquals(new String[]{"cf"}, provider.columnFamilies(TABLE));
    }

    @Test
    @DisplayName("primaryKeyColumns триммит значения и убирает повторы без учёта регистра")
    void primaryKeysSanitized() {
        PhoenixTableMetadataProvider provider = PhoenixTableMetadataProvider.builder()
                .table(TABLE)
                .columnFamilies("d")
                .primaryKeyColumns(" id ", "ID", "id2", "  ID2  ")
                .done()
                .build();

        assertArrayEquals(new String[]{"id", "id2"}, provider.primaryKeyColumns(TABLE));
    }
}
