package kz.qazmarka.h2k.schema.registry;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.hadoop.hbase.TableName;

/**
 * Поставщик табличных метаданных Phoenix, не привязанный к конкретному формату конфигурации.
 * Позволяет получать параметры, которые зависят от таблицы (байты соли, подсказки ёмкости и т.п.).
 */
public interface PhoenixTableMetadataProvider {

    /** Пустой поставщик, всегда возвращающий {@code null}. */
    PhoenixTableMetadataProvider NOOP = new PhoenixTableMetadataProvider() {
        @Override
        public Integer saltBytes(TableName table) { return null; }

        @Override
        public Integer capacityHint(TableName table) { return null; }

        @Override
        public String[] columnFamilies(TableName table) { return SchemaRegistry.EMPTY; }

        @Override
        public String[] primaryKeyColumns(TableName table) { return SchemaRegistry.EMPTY; }
    };

    /**
     * @param table таблица Phoenix/HBase
     * @return количество байт соли rowkey или {@code null}, если значение неизвестно
     */
    Integer saltBytes(TableName table);

    /**
     * @param table таблица Phoenix/HBase
     * @return подсказка ёмкости корневого JSON (ожидаемое число полей) или {@code null}, если значение неизвестно
     */
    Integer capacityHint(TableName table);

    /**
     * @param table таблица Phoenix/HBase
     * @return перечисление column family, которое следует реплицировать, или пустой массив для отключения фильтра
     */
    default String[] columnFamilies(TableName table) {
        return SchemaRegistry.EMPTY;
    }

    /**
     * @param table таблица Phoenix/HBase
     * @return массив имён PK-колонок или пустой массив, если информация недоступна
     */
    default String[] primaryKeyColumns(TableName table) {
        return SchemaRegistry.EMPTY;
    }

    /**
     * Создаёт билдер, позволяющий описать метаданные для нескольких таблиц
     * и получить неизменяемый {@link PhoenixTableMetadataProvider}.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * Быстрая фабрика для одного table->metadata без явного вызова {@link Builder#build()}.
     */
    static PhoenixTableMetadataProvider single(TableName table,
                                               Consumer<TableMetadataBuilder> customizer) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(customizer, "customizer");
        Builder builder = builder();
        customizer.accept(builder.table(table));
        return builder.build();
    }

    /**
     * Билдер для описания таблиц и их метаданных.
     */
    final class Builder {
        private final Map<TableName, TableMetadataBuilder> tableBuilders = new LinkedHashMap<>();

        private Builder() {
        }

        public TableMetadataBuilder table(TableName table) {
            Objects.requireNonNull(table, "table");
            return tableBuilders.computeIfAbsent(table, t -> new TableMetadataBuilder(this));
        }

        public PhoenixTableMetadataProvider build() {
            Map<TableName, TableMetadata> meta = new LinkedHashMap<>(tableBuilders.size());
            for (Map.Entry<TableName, TableMetadataBuilder> entry : tableBuilders.entrySet()) {
                meta.put(entry.getKey(), entry.getValue().build());
            }
            return new ImmutableProvider(meta);
        }
    }

    /**
     * Билдер для метаданных конкретной таблицы.
     */
    final class TableMetadataBuilder {
        private final Builder parent;
        private Integer saltBytes;
        private Integer capacityHint;
        private String[] columnFamilies = SchemaRegistry.EMPTY;
        private String[] primaryKeyColumns = SchemaRegistry.EMPTY;

        private TableMetadataBuilder(Builder parent) {
            this.parent = parent;
        }

        public TableMetadataBuilder saltBytes(Integer saltBytes) {
            this.saltBytes = saltBytes;
            return this;
        }

        public TableMetadataBuilder capacityHint(Integer capacityHint) {
            this.capacityHint = capacityHint;
            return this;
        }

        public TableMetadataBuilder columnFamilies(String... columnFamilies) {
            this.columnFamilies = sanitize(columnFamilies);
            return this;
        }

        public TableMetadataBuilder primaryKeyColumns(String... primaryKeyColumns) {
            this.primaryKeyColumns = sanitize(primaryKeyColumns);
            return this;
        }

        public Builder done() {
            return parent;
        }

        private TableMetadata build() {
            return new TableMetadata(saltBytes,
                    capacityHint,
                    copy(columnFamilies),
                    copy(primaryKeyColumns));
        }

        private static String[] sanitize(String[] source) {
            if (source == null || source.length == 0) {
                return SchemaRegistry.EMPTY;
            }
            if (source.length == 1 && source[0] == null) {
                return SchemaRegistry.EMPTY;
            }
            return Arrays.stream(source)
                    .filter(Objects::nonNull)
                    .toArray(String[]::new);
        }

        private static String[] copy(String[] source) {
            if (source == null || source.length == 0) {
                return SchemaRegistry.EMPTY;
            }
            return Arrays.copyOf(source, source.length);
        }
    }

    /**
     * Приватная неизменяемая реализация поставщика.
     */
    final class ImmutableProvider implements PhoenixTableMetadataProvider {
        private final Map<TableName, TableMetadata> byTable;

        private ImmutableProvider(Map<TableName, TableMetadata> byTable) {
            this.byTable = Collections.unmodifiableMap(new LinkedHashMap<>(byTable));
        }

        @Override
        public Integer saltBytes(TableName table) {
            return lookup(table).saltBytes();
        }

        @Override
        public Integer capacityHint(TableName table) {
            return lookup(table).capacityHint();
        }

        @Override
        public String[] columnFamilies(TableName table) {
            return lookup(table).columnFamilies();
        }

        @Override
        public String[] primaryKeyColumns(TableName table) {
            return lookup(table).primaryKeyColumns();
        }

        private TableMetadata lookup(TableName table) {
            TableMetadata metadata = byTable.get(table);
            return metadata == null ? TableMetadata.EMPTY : metadata;
        }
    }

    /**
     * Набор метаданных одной таблицы.
     */
    final class TableMetadata {
        static final TableMetadata EMPTY =
                new TableMetadata(null, null, SchemaRegistry.EMPTY, SchemaRegistry.EMPTY);

        private final Integer saltBytes;
        private final Integer capacityHint;
        private final String[] columnFamilies;
        private final String[] primaryKeyColumns;

        private TableMetadata(Integer saltBytes,
                              Integer capacityHint,
                              String[] columnFamilies,
                              String[] primaryKeyColumns) {
            this.saltBytes = saltBytes;
            this.capacityHint = capacityHint;
            this.columnFamilies = columnFamilies;
            this.primaryKeyColumns = primaryKeyColumns;
        }

        Integer saltBytes() {
            return saltBytes;
        }

        Integer capacityHint() {
            return capacityHint;
        }

        String[] columnFamilies() {
            return columnFamilies;
        }

        String[] primaryKeyColumns() {
            return primaryKeyColumns;
        }
    }

}
