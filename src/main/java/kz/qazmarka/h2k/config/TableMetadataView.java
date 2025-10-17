package kz.qazmarka.h2k.config;

import org.apache.hadoop.hbase.TableName;

/**
 * Минимальный интерфейс доступа к табличным параметрам для горячего пути репликации.
 * Позволяет использовать облегчённые DTO вместо прямой зависимости от {@link H2kConfig}.
 */
public interface TableMetadataView {
    TableOptionsSnapshot describeTableOptions(TableName table);

    CfFilterSnapshot describeCfFilter(TableName table);

    int getCapacityHintFor(TableName table);

    boolean isObserversEnabled();
}
