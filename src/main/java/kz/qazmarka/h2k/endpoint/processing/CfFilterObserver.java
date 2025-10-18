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

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.TableValueSource;

/**
 * Пассивно оценивает эффективность фильтрации CF: считает строки/отфильтрованные строки
 * по каждой таблице и выводит предупреждение, если фильтр бесполезен.
 */
final class CfFilterObserver {

    private static final Logger LOG = LoggerFactory.getLogger(CfFilterObserver.class);
    private static final long MIN_ROWS_TO_LOG = 500L;
    private static final double MIN_RATIO = 0.01d; // 1%
    /**
     * Порог «слишком эффективного» фильтра — когда отфильтровано 95%+ строк. Это может указывать на
     * избыточно широкую конфигурацию CF и риски пропуска нужных данных. Логируем один раз на таблицу.
     */
    private static final double TOO_EFFECTIVE_RATIO = 0.95d; // 95%

    private final ConcurrentHashMap<TableName, Stats> statsByTable = new ConcurrentHashMap<>();
    private final Set<TableName> ineffectiveWarned = ConcurrentHashMap.newKeySet();
    private final Set<TableName> tooEffectiveWarned = ConcurrentHashMap.newKeySet();
    private final boolean enabled;

    private CfFilterObserver(boolean enabled) {
        this.enabled = enabled;
    }

    static CfFilterObserver create() {
        return new CfFilterObserver(true);
    }

    static CfFilterObserver disabled() {
        return new CfFilterObserver(false);
    }

    /**
     * Служебный индикатор для модульных тестов: показывает, работает ли наблюдатель.
     */
    boolean isEnabledForTest() {
        return enabled;
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
                 CfFilterSnapshot cfSnapshot) {
        if (!enabled) return;
        if (!filterActive || table == null || cfSnapshot == null) return;
        if (rowsTotal <= 0L) return;

        Stats stats = statsByTable.computeIfAbsent(table, t -> new Stats());
        stats.rowsTotal.add(rowsTotal);
        stats.rowsFiltered.add(rowsFiltered);

        long total = stats.rowsTotal.sum();
        if (total < MIN_ROWS_TO_LOG) return;

        long filtered = stats.rowsFiltered.sum();
        double ratio = filtered <= 0L ? 0.0d : (double) filtered / (double) total;
        evaluateAndWarn(table, stats, total, filtered, ratio, cfSnapshot);
    }

    /**
     * Выполняет логирование эффективности и разовые предупреждения для неэффективного/слишком эффективного фильтра.
     */
    private void evaluateAndWarn(TableName table,
                                 Stats stats,
                                 long total,
                                 long filtered,
                                 double ratio,
                                 CfFilterSnapshot cfSnapshot) {
        logEffectiveness(table, stats, total, filtered, ratio, cfSnapshot);
        if (!LOG.isWarnEnabled()) {
            return;
        }
        if (ratio < MIN_RATIO && ineffectiveWarned.add(table)) {
            String cfCsv = safeCsv(cfSnapshot);
            LOG.warn(
                    "CF-фильтр для таблицы {} почти неэффективен: обработано {} строк, отфильтровано {} ({}%). Рассмотрите корректировку списка CF '{}'.",
                    table, total, filtered, formatPercent(ratio), cfCsv.isEmpty() ? "-" : cfCsv);
            return;
        }
        if (ratio >= TOO_EFFECTIVE_RATIO && tooEffectiveWarned.add(table)) {
            String cfCsv = safeCsv(cfSnapshot);
            LOG.warn(
                    "CF-фильтр для таблицы {} возможно слишком агрессивен: обработано {} строк, отфильтровано {} ({}%). Проверьте конфигурацию CF '{}', чтобы избежать потери нужных событий.",
                    table, total, filtered, formatPercent(ratio), cfCsv.isEmpty() ? "-" : cfCsv);
        }
    }

    long ineffectiveTables() {
        if (!enabled) {
            return 0L;
        }
        return ineffectiveWarned.size();
    }

    long tooEffectiveTables() {
        if (!enabled) {
            return 0L;
        }
        return tooEffectiveWarned.size();
    }

    Map<TableName, Stats> snapshot() {
        if (!enabled) {
            return java.util.Collections.emptyMap();
        }
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
                                  double ratio,
                                  CfFilterSnapshot cfSnapshot) {
        long lastLogged = stats.lastLoggedRows.get();
        if (lastLogged != 0L && total - lastLogged < MIN_ROWS_TO_LOG) {
            return;
        }
        if (!stats.lastLoggedRows.compareAndSet(lastLogged, total)) {
            return;
        }
        if (LOG.isInfoEnabled()) {
            String csv = safeCsv(cfSnapshot);
            TableValueSource scope = (cfSnapshot == null) ? TableValueSource.DEFAULT : cfSnapshot.source();
            LOG.info(
                    "CF-фильтр таблицы {}: эффективность {}% (строк={}, отфильтровано={}, список={} — {})",
                    table,
                    formatPercent(ratio),
                    total,
                    filtered,
                    csv.isEmpty() ? "-" : csv,
                    label(scope));
        }
    }

    /**
     * Форматирует долю в проценты с двумя знаками после запятой.
     */
    private static String formatPercent(double ratio) {
        return String.format(Locale.ROOT, "%.2f", ratio * 100.0d);
    }

    private static String safeCsv(CfFilterSnapshot snapshot) {
        if (snapshot == null) {
            return "";
        }
        String csv = snapshot.csv();
        return csv == null ? "" : csv;
    }

    /**
     * Возвращает метку источника значений CF для логирования.
     */
    private static String label(TableValueSource source) {
        return source == null ? "" : source.label();
    }
}
