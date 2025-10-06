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

        WalEntryProcessor processor;
        BatchSender sender;
        Entry smallEntry;
        Entry wideEntry;
        byte[][] cfAllowed;
        byte[][] cfForbidden;

        @Setup(Level.Trial)
        public void setUp() {
            Configuration configuration = new Configuration(false);
            configuration.set("h2k.kafka.bootstrap.servers", "bench:9092");
            configuration.set("h2k.topic.pattern", "${namespace}.${qualifier}");
            configuration.set("h2k.cf.list", "d");

            H2kConfig h2kConfig = H2kConfig.from(configuration, "bench:9092");
            PayloadBuilder builder = new PayloadBuilder(SimpleDecoder.INSTANCE, h2kConfig);
            TopicManager topicManager = new TopicManager(h2kConfig, TopicEnsurer.disabled());
            MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

            processor = new WalEntryProcessor(builder, topicManager, producer, h2kConfig);
            sender = new BatchSender(256, 1_000, true, false);

            smallEntry = walEntry(1);
            wideEntry = walEntry(64);
            cfAllowed = new byte[][]{CF_ACTIVE};
            cfForbidden = new byte[][]{CF_SKIP};
        }

        @Setup(Level.Invocation)
        public void resetSender() {
            sender.resetCounters();
            sender.resumeAutoFlush();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws ExecutionException, TimeoutException {
            try {
                sender.flush();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Benchmark teardown interrupted", e);
            }
        }

        private static Entry walEntry(int cells) {
            byte[] row = Bytes.toBytes("row" + cells);
            WALKey key = new WALKey(row, TABLE, ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
            WALEdit edit = new WALEdit();
            for (int i = 0; i < cells; i++) {
                byte[] qualifier = Bytes.toBytes("q" + i);
                byte[] value = ("value-" + i).getBytes(StandardCharsets.UTF_8);
                edit.add(new KeyValue(row, CF_ACTIVE, qualifier, System.currentTimeMillis(), value));
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
        state.processor.process(state.smallEntry, state.sender, false, false, null);
        bh.consume(state.sender.tryFlush());
    }

    /**
     * Толстая запись без фильтрации: имитируем таблицы с большим количеством колонок.
     * Важно отслеживать изменение Score — сценарий показывает, как обрабатываются «широкие» строки
     * с десятками ячеек. Резкий рост сигнализирует о деградации в сборке payload или работе буферов.
     */
    @Benchmark
    public void processWideRow(ProcessorState state, Blackhole bh) {
        state.processor.process(state.wideEntry, state.sender, false, false, null);
        bh.consume(state.sender.tryFlush());
    }

    /**
     * Фильтр CF включён, ячейки проходят (целевой Column Family присутствует).
     * Score показывает накладные расходы на проверку фильтра при положительном срабатывании.
     * Рост >15% относительно processSmallRow — повод перепроверить кэш фильтра.
     */
    @Benchmark
    public void processWithFilterHit(ProcessorState state, Blackhole bh) {
        state.processor.process(state.smallEntry, state.sender, false, true, state.cfAllowed);
        bh.consume(state.sender.tryFlush());
    }

    /**
     * Фильтр CF включён, все ячейки отбрасываются (CF отличается). Проверяем расходы на фильтрацию.
     * Небольшой Score подтверждает, что «пустые» строки быстро отбрасываются; рост указывает
     * на деградацию хешей/сравнений.
     */
    @Benchmark
    public void processWithFilterMiss(ProcessorState state, Blackhole bh) {
        state.processor.process(state.smallEntry, state.sender, false, true, state.cfForbidden);
        bh.consume(state.sender.tryFlush());
    }
}
