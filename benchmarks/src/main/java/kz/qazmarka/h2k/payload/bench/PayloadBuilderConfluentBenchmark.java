package kz.qazmarka.h2k.payload.bench;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.payload.serializer.avro.SchemaRegistryClientFactory;
import kz.qazmarka.h2k.schema.decoder.SimpleDecoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Измеряет стоимость сериализации payload в формате Confluent Avro (основной путь для продовой нагрузки).
 * Рассматриваем два сценария:
 *  - «горячий» — схема уже зарегистрирована, используется кеш {@link PayloadBuilder};
 *  - «холодный» — перед каждым вызовом заново создаём {@link PayloadBuilder} и клиента SR, чтобы увидеть цену регистрации.
 *
 * Формат вывода JMH: {@code Score} показывает среднее время одной сериализации (микросекунды),
 * {@code Error} — доверительный интервал 99%. Рост более чем на 10–15% по сравнению с baseline
 * сигнализирует о регрессии в Avro-конвейере.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PayloadBuilderConfluentBenchmark {

    private static final String SCHEMA_FILE_NAME = "bench_orders.avsc";
    private static final String SCHEMA_JSON = "{" +
            "\"type\":\"record\"," +
            "\"name\":\"BenchOrder\"," +
            "\"namespace\":\"kz.qazmarka.bench\"," +
            "\"fields\":[" +
            "{\"name\":\"_event_ts\",\"type\":[\"null\",\"long\"],\"default\":null}," +
            "{\"name\":\"status\",\"type\":[\"null\",\"bytes\"],\"default\":null}," +
            "{\"name\":\"amount\",\"type\":[\"null\",\"bytes\"],\"default\":null}" +
            "]" +
            "}";
    private static final TableName TABLE = TableName.valueOf("BENCH_ORDERS");
    private static final byte[] ROW_KEY = Bytes.toBytes("row-1");
    private static final long WAL_SEQ = 42L;
    private static final long WAL_WRITE_TIME = 1_690_000_000_000L;
    private static final byte[] CF = Bytes.toBytes("D");

    private static final String BENCH_BOOTSTRAP = "bench:9092";

    /**
     * Создаёт фабрику Schema Registry, выдающую новый {@code MockSchemaRegistryClient} на каждый вызов.
     * Метод устойчив к shading: сначала пробует оригинальный класс Confluent, затем релокированный.
     */
    private static SchemaRegistryClientFactory mockFactory() {
        return new SchemaRegistryClientFactory() {
            @Override
            public SchemaRegistryClient create(List<String> urls,
                                               java.util.Map<String, Object> clientConfig,
                                               int identityMapCapacity) {
                return instantiateMock();
            }

            private SchemaRegistryClient instantiateMock() {
                String[] candidates = {
                        "io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient",
                        "kz.qazmarka.shaded.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient"
                };
                for (String className : candidates) {
                    try {
                        Class<?> clazz = Class.forName(className);
                        if (!SchemaRegistryClient.class.isAssignableFrom(clazz)) {
                            continue;
                        }
                        Object instance = clazz.getDeclaredConstructor().newInstance();
                        return SchemaRegistryClient.class.cast(instance);
                    } catch (ReflectiveOperationException ignore) {
                        // пробуем следующий кандидат
                    }
                }
                throw new IllegalStateException("Не удалось создать MockSchemaRegistryClient (original/shaded)");
            }
        };
    }

    /** Состояние для «горячего» сценария: Schema Registry уже прогрет. */
    @State(Scope.Thread)
    public static class HotState {
        PayloadBuilder builder;
        List<Cell> cells;
        RowKeySlice rowKey;
        Path schemaDir;

        @Setup(Level.Trial)
        public void setUp() {
            try {
                schemaDir = Files.createTempDirectory("avro-bench-hot");
                writeSchema(schemaDir.resolve(SCHEMA_FILE_NAME));
                Configuration cfg = baseConfig(schemaDir);
                H2kConfig h2kConfig = H2kConfig.from(cfg, BENCH_BOOTSTRAP);
                builder = new PayloadBuilder(SimpleDecoder.INSTANCE, h2kConfig, mockFactory());
                cells = Collections.unmodifiableList(sampleCells());
                rowKey = new RowKeySlice.Mutable(ROW_KEY, 0, ROW_KEY.length);
                // Прогрев: первая сериализация зарегистрирует схему в Mock SR.
                builder.buildRowPayloadBytes(TABLE, cells, rowKey, WAL_SEQ, WAL_WRITE_TIME);
            } catch (IOException e) {
                throw new UncheckedIOException("Не удалось подготовить бенчмарк", e);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            deleteDirectoryQuietly(schemaDir);
        }
    }

    /** Состояние для измерения стоимости регистрации схемы (каждый вызов заново создаёт builder). */
    @State(Scope.Thread)
    public static class ColdState {
        H2kConfig config;
        List<Cell> cells;
        RowKeySlice rowKey;
        Path schemaDir;
        PayloadBuilder builder;

        @Setup(Level.Trial)
        public void setUpTrial() {
            try {
                schemaDir = Files.createTempDirectory("avro-bench-cold");
                writeSchema(schemaDir.resolve(SCHEMA_FILE_NAME));
                Configuration cfg = baseConfig(schemaDir);
                config = H2kConfig.from(cfg, BENCH_BOOTSTRAP);
                cells = Collections.unmodifiableList(sampleCells());
                rowKey = new RowKeySlice.Mutable(ROW_KEY, 0, ROW_KEY.length);
            } catch (IOException e) {
                throw new UncheckedIOException("Не удалось подготовить бенчмарк", e);
            }
        }

        @Setup(Level.Invocation)
        public void setUpInvocation() {
            builder = new PayloadBuilder(SimpleDecoder.INSTANCE, config, mockFactory());
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            deleteDirectoryQuietly(schemaDir);
        }
    }

    /** Горячий путь: схема уже зарегистрирована, измеряем чистую стоимость сериализации. */
    @Benchmark
    public void serializeHot(HotState state, Blackhole bh) {
        bh.consume(state.builder.buildRowPayloadBytes(TABLE, state.cells, state.rowKey, WAL_SEQ, WAL_WRITE_TIME));
    }

    /** Холодный путь: включает регистрацию схемы в Mock SR и создание сериализатора. */
    @Benchmark
    public void serializeWithRegistration(ColdState state, Blackhole bh) {
        bh.consume(state.builder.buildRowPayloadBytes(TABLE, state.cells, state.rowKey, WAL_SEQ, WAL_WRITE_TIME));
    }

    // --- helpers ---

    private static Configuration baseConfig(Path schemaDir) {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", BENCH_BOOTSTRAP);
        cfg.set("h2k.topic.pattern", "${table}");
        cfg.set("h2k.payload.format", "avro_binary");
        cfg.set("h2k.avro.mode", "confluent");
        cfg.set("h2k.avro.sr.urls", "mock://schema-registry");
        cfg.set("h2k.avro.schema.dir", schemaDir.toString());
        cfg.set("h2k.capacity.hints", "bench:orders=6");
        return cfg;
    }

    private static List<Cell> sampleCells() {
        long baseTs = 1690000000000L;
        Cell status = new KeyValue(ROW_KEY, CF, Bytes.toBytes("status"), baseTs, Bytes.toBytes("NEW"));
        Cell amount = new KeyValue(ROW_KEY, CF, Bytes.toBytes("amount"), baseTs + 5, Bytes.toBytes("100"));
        return Arrays.asList(status, amount);
    }

    private static void writeSchema(Path schemaPath) throws IOException {
        Files.createDirectories(schemaPath.getParent());
        Files.write(schemaPath, SCHEMA_JSON.getBytes(StandardCharsets.UTF_8));
        // Для таблиц с namespace создаём копию с именем "bench:orders.avsc", чтобы совпало с ожиданием AvroSchemaRegistry
    }

    private static void deleteDirectoryQuietly(Path dir) {
        if (dir == null) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                            // best effort cleanup
                        }
                    });
        } catch (IOException ignored) {
            // best effort cleanup
        }
    }
}
