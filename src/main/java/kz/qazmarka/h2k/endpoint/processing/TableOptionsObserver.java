package kz.qazmarka.h2k.endpoint.processing;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.config.TableValueSource;

/**
 * Отслеживает использование табличных опций (соль, capacityHint, cf.list) и логирует изменения.
 * Лёгкий наблюдатель: хранит последнее увиденное состояние на таблицу и пишет INFO только при изменении.
 */
/**
 * Потокобезопасный наблюдатель табличных опций.
 *
 * Конкурентный доступ:
 * - Карта состояний на таблицу — ConcurrentHashMap.
 * - Изменение снимка конкретной таблицы защищено synchronized на объекте состояния,
 *   чтобы логировать консистентные значения одним сообщением и избежать гонок записи.
 * - Логгер хранится в AtomicReference для безопасной подмены в тестах.
 */
final class TableOptionsObserver {

    private static final AtomicReference<Logger> LOGGER = new AtomicReference<>(LoggerFactory.getLogger(TableOptionsObserver.class));

    private final ConcurrentHashMap<TableName, TableState> stateByTable = new ConcurrentHashMap<>();
    private final boolean enabled;

    private TableOptionsObserver(boolean enabled) {
        this.enabled = enabled;
    }

    static TableOptionsObserver create() {
        return new TableOptionsObserver(true);
    }

    static TableOptionsObserver disabled() {
        return new TableOptionsObserver(false);
    }

    /**
     * Хук для тестов: возвращает признак активности наблюдателя.
     */
    boolean isEnabledForTest() {
        return enabled;
    }

    /**
     * Сохраняет снимок табличных опций и логирует его при первом появлении или изменении.
     *
     * @param table       таблица Phoenix/HBase
     * @param tableOpts   снимок опций (соль/ёмкость/CF)
     * @param cfSnapshot  снимок CF-фильтра
     */
    void observe(TableName table,
                 TableOptionsSnapshot tableOpts,
                 CfFilterSnapshot cfSnapshot) {
        if (!enabled) {
            return;
        }
        if (table == null || tableOpts == null) {
            return;
        }
        TableState state = stateByTable.computeIfAbsent(table, t -> new TableState());
        state.updateAndLog(table, tableOpts, cfSnapshot);
    }

    /**
     * Мутируемое состояние одной таблицы; доступ синхронизирован внешним блоком в updateAndLog().
     */
    private static final class TableState {
        private static final int UNINITIALIZED = Integer.MIN_VALUE;

        private int saltBytes = UNINITIALIZED;
        private TableValueSource saltSource;
        private int capacityHint = UNINITIALIZED;
        private TableValueSource capacitySource;
        private String cfCsv;
        private TableValueSource cfSource;

                void updateAndLog(TableName table,
                                                    TableOptionsSnapshot tableOpts,
                                                    CfFilterSnapshot cfSnapshot) {
            synchronized (this) {
                int newSalt = tableOpts.saltBytes();
                TableValueSource newSaltSource = tableOpts.saltSource();
                int newCapacity = tableOpts.capacityHint();
                TableValueSource newCapacitySource = tableOpts.capacitySource();
                String newCfCsv = safeCsv(cfSnapshot);
                TableValueSource newCfSource = (cfSnapshot == null) ? TableValueSource.DEFAULT : cfSnapshot.source();

                boolean changed = (saltBytes != newSalt)
                        || (saltSource != newSaltSource)
                        || (capacityHint != newCapacity)
                        || (capacitySource != newCapacitySource)
                        || differentCsv(newCfCsv)
                        || (cfSource != newCfSource);

                if (!changed) {
                    return;
                }

                saltBytes = newSalt;
                saltSource = newSaltSource;
                capacityHint = newCapacity;
                capacitySource = newCapacitySource;
                cfCsv = newCfCsv;
                cfSource = newCfSource;

                Logger log = LOGGER.get();
                if (log.isInfoEnabled()) {
                    log.info("Таблица {}: saltBytes={} ({}), capacityHint={} ({}), cf.list='{}' ({})",
                            table,
                            newSalt,
                            label(newSaltSource),
                            newCapacity,
                            label(newCapacitySource),
                            newCfCsv.isEmpty() ? "-" : newCfCsv,
                            label(newCfSource));
                }
            }
        }

        private boolean differentCsv(String candidate) {
            if (cfCsv == null) {
                return candidate != null && !candidate.isEmpty();
            }
            return !cfCsv.equals(candidate);
        }

        /**
         * Возвращает источник значения табличной опции для логирования.
         */
        private static String label(TableValueSource source) {
            return source == null ? "" : source.label();
        }

        private static String safeCsv(CfFilterSnapshot snapshot) {
            if (snapshot == null) {
                return "";
            }
            String csv = snapshot.csv();
            return csv == null ? "" : csv;
        }
    }

    static AutoCloseable withLoggerForTest(Logger testLogger) {
        Logger previous = LOGGER.getAndSet(testLogger);
        return () -> LOGGER.set(previous);
    }
}
