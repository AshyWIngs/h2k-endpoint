package kz.qazmarka.h2k.schema.phoenix;

import java.util.Objects;

/**
 * Универсальный ключ «таблица + колонка» для кешей предупреждений.
 */
final class TableColumnKey {

    private final String table;
    private final String column;
    private final int hash;

    TableColumnKey(String table, String column) {
        this.table = table;
        this.column = column;
        this.hash = Objects.hash(table, column);
    }

    String table() {
        return table;
    }

    String column() {
        return column;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TableColumnKey)) {
            return false;
        }
        TableColumnKey other = (TableColumnKey) obj;
        return hash == other.hash
                && Objects.equals(table, other.table)
                && Objects.equals(column, other.column);
    }
}
