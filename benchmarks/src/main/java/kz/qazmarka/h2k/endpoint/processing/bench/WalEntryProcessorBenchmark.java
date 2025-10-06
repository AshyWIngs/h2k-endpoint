package kz.qazmarka.h2k.endpoint.processing.bench;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.SimpleDecoder;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;

/**
 * Бенчмарки горячего пути {@link WalEntryProcessor#process}: измеряем среднюю стоимость обработки
 * компактных и «толстых» WAL-записей, а также накладные расходы фильтрации по Column Family.
 *
 * Формат вывода JMH аналогичен {@link kz.qazmarka.h2k.kafka.producer.batch.bench.BatchSenderBenchmark}:
 *  - {@code Score} — среднее время одной обработки (микросекунды);
 *  - {@code Error} — доверительный интервал (99%).
 *
 * Фиксируйте baseline на стенде и отслеживайте относительные изменения: рост выше 10–15% — сигнал
 * к повторной оптимизации горячего пути или пересмотру фильтров.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WalEntryProcessorBenchmark {

    @State(Scope.Thread)
    public static class ProcessorState {
        private static final TableName TABLE = TableName.valueOf("bench", "metrics");
        private static final byte[] CF_ACTIVE = Bytes.toBytes("d");
        private static final byte[] CF_SKIP = Bytes.toBytes("x");

        WalEntryProcessor processorNoFilter;
        WalEntryProcessor processorWithFilter;
        BatchSender senderNoFilter;
        BatchSender senderWithFilter;
        Entry smallEntryNoFilter;
        Entry smallEntryHit;
        Entry smallEntryMiss;
        Entry wideEntry;

        @Setup(Level.Trial)
        public void setUp() {
            Configuration baseConfig = new Configuration(false);
            baseConfig.set("h2k.kafka.bootstrap.servers", "bench:9092");
            baseConfig.set("h2k.topic.pattern", "${namespace}.${qualifier}");

            H2kConfig configNoFilter = H2kConfig.from(new Configuration(baseConfig), "bench:9092", PhoenixTableMetadataProvider.NOOP);
            PayloadBuilder builderNoFilter = new PayloadBuilder(SimpleDecoder.INSTANCE, configNoFilter);
            TopicManager topicManagerNoFilter = new TopicManager(configNoFilter, TopicEnsurer.disabled());
            MockProducer<byte[], byte[]> producerNoFilter = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
            processorNoFilter = new WalEntryProcessor(builderNoFilter, topicManagerNoFilter, producerNoFilter, configNoFilter);
            senderNoFilter = new BatchSender(256, 1_000, true, false);

            PhoenixTableMetadataProvider filterProvider = new PhoenixTableMetadataProvider() {
                @Override
                public Integer saltBytes(TableName table) { return null; }

                @Override
                public Integer capacityHint(TableName table) { return null; }

                @Override
                public String[] columnFamilies(TableName table) { return new String[]{Bytes.toString(CF_ACTIVE)}; }
            };

            H2kConfig configWithFilter = H2kConfig.from(new Configuration(baseConfig), "bench:9092", filterProvider);
            PayloadBuilder builderWithFilter = new PayloadBuilder(SimpleDecoder.INSTANCE, configWithFilter);
            TopicManager topicManagerWithFilter = new TopicManager(configWithFilter, TopicEnsurer.disabled());
            MockProducer<byte[], byte[]> producerWithFilter = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
            processorWithFilter = new WalEntryProcessor(builderWithFilter, topicManagerWithFilter, producerWithFilter, configWithFilter);
            senderWithFilter = new BatchSender(256, 1_000, true, false);

            smallEntryNoFilter = walEntry(1, CF_ACTIVE);
            smallEntryHit = walEntry(1, CF_ACTIVE);
            smallEntryMiss = walEntry(1, CF_SKIP);
            wideEntry = walEntry(64, CF_ACTIVE);
        }

        @Setup(Level.Invocation)
        public void resetSender() {
            senderNoFilter.resetCounters();
            senderNoFilter.resumeAutoFlush();
            senderWithFilter.resetCounters();
            senderWithFilter.resumeAutoFlush();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws ExecutionException, TimeoutException {
            try {
                senderNoFilter.flush();
                senderWithFilter.flush();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Benchmark teardown interrupted", e);
            }
        }

        private static Entry walEntry(int cells, byte[] cf) {
            byte[] row = Bytes.toBytes("row" + cells);
            WALKey key = new WALKey(row, TABLE, ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
            WALEdit edit = new WALEdit();
            for (int i = 0; i < cells; i++) {
                byte[] qualifier = Bytes.toBytes("q" + i);
                byte[] value = ("value-" + i).getBytes(StandardCharsets.UTF_8);
                edit.add(new KeyValue(row, cf, qualifier, System.currentTimeMillis(), value));
            }
            return new Entry(key, edit);
        }
    }

    /**
     * Базовый сценарий: запись WAL с одной строкой и минимальным набором ячеек.
     * Score отражает среднее время конвейера без фильтрации; рост относительно baseline на 10% и более
     * означает, что горячий путь заметно подорожал.
     */
    @Benchmark
    public void processSmallRow(ProcessorState state, Blackhole bh) {
        state.processorNoFilter.process(state.smallEntryNoFilter, state.senderNoFilter, false);
        bh.consume(state.senderNoFilter.tryFlush());
    }

    /**
     * Толстая запись без фильтрации: имитируем таблицы с большим количеством колонок.
     * Важно отслеживать изменение Score — сценарий показывает, как обрабатываются «широкие» строки
     * с десятками ячеек. Резкий рост сигнализирует о деградации в сборке payload или работе буферов.
     */
    @Benchmark
    public void processWideRow(ProcessorState state, Blackhole bh) {
        state.processorNoFilter.process(state.wideEntry, state.senderNoFilter, false);
        bh.consume(state.senderNoFilter.tryFlush());
    }

    /**
     * Фильтр CF включён, ячейки проходят (целевой Column Family присутствует).
     * Score показывает накладные расходы на проверку фильтра при положительном срабатывании.
     * Рост >15% относительно processSmallRow — повод перепроверить кэш фильтра.
     */
    @Benchmark
    public void processWithFilterHit(ProcessorState state, Blackhole bh) {
        state.processorWithFilter.process(state.smallEntryHit, state.senderWithFilter, false);
        bh.consume(state.senderWithFilter.tryFlush());
    }

    /**
     * Фильтр CF включён, все ячейки отбрасываются (CF отличается). Проверяем расходы на фильтрацию.
     * Небольшой Score подтверждает, что «пустые» строки быстро отбрасываются; рост указывает
     * на деградацию хешей/сравнений.
     */
    @Benchmark
    public void processWithFilterMiss(ProcessorState state, Blackhole bh) {
        state.processorWithFilter.process(state.smallEntryMiss, state.senderWithFilter, false);
        bh.consume(state.senderWithFilter.tryFlush());
    }
}
