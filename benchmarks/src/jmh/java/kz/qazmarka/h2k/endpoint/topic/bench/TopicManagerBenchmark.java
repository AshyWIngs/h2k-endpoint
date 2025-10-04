package kz.qazmarka.h2k.endpoint.topic.bench;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;

/**
 * Бенчмарки кеша {@link TopicManager}: оцениваем стоимость разрешения топика для HBase-таблицы.
 *
 * Сценарии:
 *  - {@code resolveCached} — повторяем обращения к одной таблице, чтобы проверить скорость кеша.
 *  - {@code resolveUnique} — проходим по пулу из 64 таблиц (хеш-коллизии умеренные).
 *  - {@code ensureNoop} — оцениваем оверхед проверок ensure при отключённом режиме.
 *
 * Формат отчёта JMH: {@code Score} — среднее время одной операции в наносекундах,
 * {@code Error} — доверительный интервал 99%. Рост >10% по сравнению с baseline сигнализирует о деградации кеша.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TopicManagerBenchmark {

    @State(Scope.Thread)
    public static class CachedState {
        TopicManager manager;
        TableName table;

        @Setup(Level.Trial)
        public void setUp() {
            manager = new TopicManager(baseConfig(), TopicEnsurer.disabled());
            table = TableName.valueOf("bench", "metrics");
            manager.resolveTopic(table); // прогрев кеша
        }
    }

    @State(Scope.Thread)
    public static class UniqueState {
        TopicManager manager;
        TableName[] tables;
        int cursor;

        @Setup(Level.Trial)
        public void setUp() {
            manager = new TopicManager(baseConfig(), TopicEnsurer.disabled());
            tables = new TableName[64];
            for (int i = 0; i < tables.length; i++) {
                tables[i] = TableName.valueOf("bench", "metrics_" + i);
            }
        }
    }

    @Benchmark
    public String resolveCached(CachedState state) {
        return state.manager.resolveTopic(state.table);
    }

    @Benchmark
    public String resolveUnique(UniqueState state) {
        TableName table = state.tables[state.cursor];
        state.cursor = (state.cursor + 1) & (state.tables.length - 1);
        return state.manager.resolveTopic(table);
    }

    @Benchmark
    public boolean ensureNoop(CachedState state) {
        state.manager.ensureTopicIfNeeded("bench.metrics");
        return state.manager.ensureEnabled();
    }

    private static H2kConfig baseConfig() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "bench:9092");
        cfg.set("h2k.topic.pattern", "${namespace}.${qualifier}");
        return H2kConfig.from(cfg, "bench:9092");
    }
}
