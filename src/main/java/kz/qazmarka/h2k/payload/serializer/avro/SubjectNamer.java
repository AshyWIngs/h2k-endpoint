package kz.qazmarka.h2k.payload.serializer.avro;

import java.util.Locale;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;

/**
 * Формирует subject для Schema Registry с учётом префикса/суффикса и выбранной стратегии.
 * Иммутабельный и потокобезопасный помощник: вся логика преобразования сосредоточена здесь,
 * что упрощает тестирование и дальнейшее расширение.
 */
final class SubjectNamer {

    /** Стратегия по умолчанию совпадает с существующим поведением «table». */
    static final String DEFAULT_STRATEGY = "table";
    private static final String TABLE_PARAM = DEFAULT_STRATEGY;

    private final Logger log;
    private final String originalStrategy;
    private final String strategyLower;
    private final String prefix;
    private final String suffix;

    SubjectNamer(Logger log, String strategy, String prefix, String suffix) {
        this.log = Objects.requireNonNull(log, "log");
        this.originalStrategy = (strategy == null || strategy.isEmpty()) ? DEFAULT_STRATEGY : strategy;
        this.strategyLower = this.originalStrategy.toLowerCase(Locale.ROOT);
        this.prefix = prefix == null ? "" : prefix;
        this.suffix = suffix == null ? "" : suffix;
    }

    /**
     * Возвращает subject для указанной таблицы, учитывая настройки.
     *
     * @param table имя таблицы HBase (namespace:tablename)
     * @return subject для регистрации/сериализации
     */
    String subjectFor(TableName table) {
        Objects.requireNonNull(table, TABLE_PARAM);
        String base = tableNameForStrategy(table);
        return prefix + base + suffix;
    }

    private String tableNameForStrategy(TableName table) {
        switch (strategyLower) {
            case DEFAULT_STRATEGY:
                return tableName(table, CaseMode.ORIGINAL);
            case "table-upper":
                return tableName(table, CaseMode.UPPER);
            case "table-lower":
                return tableName(table, CaseMode.LOWER);
            default:
                log.warn("Avro Confluent: неизвестная стратегия subject '{}', использую {}", originalStrategy, DEFAULT_STRATEGY);
                return tableName(table, CaseMode.ORIGINAL);
        }
    }

    private String tableName(TableName table, CaseMode mode) {
        String name = table.getNameWithNamespaceInclAsString();
        switch (mode) {
            case UPPER:
                return name.toUpperCase(Locale.ROOT);
            case LOWER:
                return name.toLowerCase(Locale.ROOT);
            default:
                return name;
        }
    }

    private enum CaseMode {
        ORIGINAL,
        UPPER,
        LOWER
    }
}
