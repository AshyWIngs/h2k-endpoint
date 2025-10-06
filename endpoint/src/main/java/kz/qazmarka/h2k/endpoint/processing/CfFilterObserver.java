package kz.qazmarka.h2k.endpoint.processing;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;

/**
 * Пассивно оценивает эффективность фильтрации CF: считает строки/отфильтрованные строки
 * по каждой таблице и выводит предупреждение, если фильтр бесполезен.
 */
final class CfFilterObserver {

    private static final Logger LOG = LoggerFactory.getLogger(CfFilterObserver.class);
    private static final long MIN_ROWS_TO_LOG = 500L;
    private static final double MIN_RATIO = 0.01d; // 1%

    private final ConcurrentHashMap<TableName, Stats> statsByTable = new ConcurrentHashMap<>();
    private final Set<TableName> ineffectiveWarned = ConcurrentHashMap.newKeySet();

    private CfFilterObserver() {
    }

    static CfFilterObserver create() {
        return new CfFilterObserver();
    }

    /**
     * Накапливает статистику по эффективности фильтра CF для конкретной таблицы.
     * Метод вызывается на горячем пути после обработки партии строк и полностью потокобезопасен.
     *
     * @param table        имя таблицы
     * @param rowsTotal    сколько строк было рассмотрено (включая отфильтрованные)
     * @param rowsFiltered сколько строк фильтр исключил из отправки
     * @param filterActive фактически ли активна фильтрация (наличие заданных CF)
     * @param cfSnapshot   снимок настроек CF для таблицы
     */
    void observe(TableName table,
                 long rowsTotal,
                 long rowsFiltered,
                 boolean filterActive,
                 H2kConfig.CfFilterSnapshot cfSnapshot) {
        if (!filterActive || table == null || cfSnapshot == null) {
            return;
        }
        if (rowsTotal <= 0L) {
            return;
        }
        Stats stats = statsByTable.computeIfAbsent(table, t -> new Stats());
        stats.rowsTotal.add(rowsTotal);
        stats.rowsFiltered.add(rowsFiltered);
        long total = stats.rowsTotal.sum();
        long filtered = stats.rowsFiltered.sum();
        if (total >= MIN_ROWS_TO_LOG) {
            logEffectiveness(table, stats, total, filtered, cfSnapshot);
            double ratio = filtered == 0L ? 0.0d : (double) filtered / (double) total;
            if (ratio < MIN_RATIO && ineffectiveWarned.add(table) && LOG.isWarnEnabled()) {
                String cfCsv = safeCsv(cfSnapshot);
                LOG.warn(
                        "CF-фильтр для таблицы {} почти неэффективен: обработано {} строк, отфильтровано {} ({}%). Рассмотрите корректировку списка CF '{}'.",
                        table,
                        total,
                        filtered,
                        formatPercent(ratio),
                        cfCsv.isEmpty() ? "-" : cfCsv);
            }
        }
    }

    long ineffectiveTables() {
        return ineffectiveWarned.size();
    }

    Map<TableName, Stats> snapshot() {
        return new ConcurrentHashMap<>(statsByTable);
    }

    /**
     * Потокобезопасный накопитель статистики по конкретной таблице.
     */
    static final class Stats {
        final LongAdder rowsTotal = new LongAdder();
        final LongAdder rowsFiltered = new LongAdder();
        final AtomicLong lastLoggedRows = new AtomicLong();
    }

    /**
     * Логирует текущее отношение отфильтрованных строк к общему числу, не чаще одного раза
     * на заданный объём входных данных, чтобы не засорять журналы.
     */
    private void logEffectiveness(TableName table,
                                  Stats stats,
                                  long total,
                                  long filtered,
                                  H2kConfig.CfFilterSnapshot cfSnapshot) {
        long lastLogged = stats.lastLoggedRows.get();
        if (lastLogged != 0L && total - lastLogged < MIN_ROWS_TO_LOG) {
            return;
        }
        if (!stats.lastLoggedRows.compareAndSet(lastLogged, total)) {
            return;
        }
        if (LOG.isInfoEnabled()) {
            double ratio = filtered == 0L ? 0.0d : (double) filtered / (double) total;
            String csv = safeCsv(cfSnapshot);
            String scope = cfSnapshot.source().label();
            LOG.info(
                    "CF-фильтр таблицы {}: эффективность {}% (строк={}, отфильтровано={}, список={} — {})",
                    table,
                    formatPercent(ratio),
                    total,
                    filtered,
                    csv.isEmpty() ? "-" : csv,
                    scope);
        }
    }

    /**
     * Форматирует долю в проценты с двумя знаками после запятой.
     */
    private static String formatPercent(double ratio) {
        return String.format(Locale.ROOT, "%.2f", ratio * 100.0d);
    }

    private static String safeCsv(H2kConfig.CfFilterSnapshot snapshot) {
        if (snapshot == null) {
            return "";
        }
        String csv = snapshot.csv();
        return csv == null ? "" : csv;
    }
}
