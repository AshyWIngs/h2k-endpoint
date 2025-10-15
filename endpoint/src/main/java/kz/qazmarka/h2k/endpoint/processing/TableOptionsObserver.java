package kz.qazmarka.h2k.endpoint.processing;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;

/**
 * Отслеживает использование табличных опций (соль, capacityHint, cf.list) и логирует изменения.
 * Лёгкий наблюдатель: хранит последнее увиденное состояние на таблицу и пишет INFO только при изменении.
 */
final class TableOptionsObserver {

    private static final Logger LOG = LoggerFactory.getLogger(TableOptionsObserver.class);

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
     * Сохраняет снимок табличных опций и логирует его при первом появлении или изменении.
     *
     * @param table       таблица Phoenix/HBase
     * @param tableOpts   снимок опций (соль/ёмкость/CF)
     * @param cfSnapshot  снимок CF-фильтра
     */
    void observe(TableName table,
                 H2kConfig.TableOptionsSnapshot tableOpts,
                 H2kConfig.CfFilterSnapshot cfSnapshot) {
        if (!enabled) {
            return;
        }
        if (table == null || tableOpts == null) {
            return;
        }
        TableState state = stateByTable.computeIfAbsent(table, t -> new TableState());
        state.updateAndLog(table, tableOpts, cfSnapshot);
    }

    private static final class TableState {
        private static final int UNINITIALIZED = Integer.MIN_VALUE;

        private int saltBytes = UNINITIALIZED;
        private H2kConfig.ValueSource saltSource;
        private int capacityHint = UNINITIALIZED;
        private H2kConfig.ValueSource capacitySource;
        private String cfCsv;
        private H2kConfig.ValueSource cfSource;

        void updateAndLog(TableName table,
                          H2kConfig.TableOptionsSnapshot tableOpts,
                          H2kConfig.CfFilterSnapshot cfSnapshot) {
            synchronized (this) {
                int newSalt = tableOpts.saltBytes();
                H2kConfig.ValueSource newSaltSource = tableOpts.saltSource();
                int newCapacity = tableOpts.capacityHint();
                H2kConfig.ValueSource newCapacitySource = tableOpts.capacitySource();
                String newCfCsv = safeCsv(cfSnapshot);
                H2kConfig.ValueSource newCfSource = (cfSnapshot == null) ? H2kConfig.ValueSource.DEFAULT : cfSnapshot.source();

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

                if (LOG.isInfoEnabled()) {
                    LOG.info("Таблица {}: saltBytes={} ({}), capacityHint={} ({}), cf.list='{}' ({})",
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

        private static String label(H2kConfig.ValueSource source) {
            return source == null ? "" : source.label();
        }

        private static String safeCsv(H2kConfig.CfFilterSnapshot snapshot) {
            if (snapshot == null) {
                return "";
            }
            String csv = snapshot.csv();
            return csv == null ? "" : csv;
        }
    }
}
