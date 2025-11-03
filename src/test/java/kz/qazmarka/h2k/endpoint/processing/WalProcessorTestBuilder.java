package kz.qazmarka.h2k.endpoint.processing;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.kafka.serializer.RowKeySliceSerializer;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Билдер тестовых данных для создания WalRowProcessor и связанных компонентов.
 */
class WalProcessorTestBuilder {

    private static final Path SCHEMA_DIR = Paths.get("src", "test", "resources", "avro").toAbsolutePath();
    
    private final TableName tableName = TableName.valueOf("INT_TEST_TABLE");
    private final String topic = "test-topic";
    private final int batchSize = 10;
    private final long flushIntervalMs = 5_000;
    private WalMeta walMeta = WalMeta.EMPTY;
    private WalCfFilterCache cfFilter = WalCfFilterCache.EMPTY;
    private final int capacityHint = 4;
    private final String columnFamilies = "d";
    private final String primaryKeyColumns = "id";
    
    /**
     * Устанавливает WalMeta.
     */
    WalProcessorTestBuilder withWalMeta(long writeTime, long sequenceId) {
        this.walMeta = new WalMeta(writeTime, sequenceId);
        return this;
    }
    
    /**
     * Устанавливает CF фильтр через массив разрешённых CF.
     */
    WalProcessorTestBuilder withAllowedCfs(byte[][] cfs) {
        this.cfFilter = WalCfFilterCache.build(cfs);
        return this;
    }
    
    /**
     * Устанавливает CF фильтр через строковый список разрешённых CF.
     */
    WalProcessorTestBuilder withAllowedCfs(String... cfs) {
        byte[][] bytesCfs = new byte[cfs.length][];
        for (int i = 0; i < cfs.length; i++) {
            bytesCfs[i] = Bytes.toBytes(cfs[i]);
        }
        return withAllowedCfs(bytesCfs);
    }
    
    /**
     * Создаёт H2kConfig с заданными параметрами.
     */
    H2kConfig buildConfig() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.kafka.bootstrap.servers", "mock:9092");
        cfg.set("h2k.avro.schema.dir", SCHEMA_DIR.toString());
        cfg.set("h2k.avro.sr.urls", "http://mock-sr");
        return H2kConfig.from(cfg, "mock:9092", buildMetadata());
    }
    
    /**
     * Создаёт PhoenixTableMetadataProvider.
     */
    PhoenixTableMetadataProvider buildMetadata() {
        return PhoenixTableMetadataProvider.builder()
                .table(tableName)
                .capacityHint(capacityHint)
                .columnFamilies(columnFamilies)
                .primaryKeyColumns(primaryKeyColumns)
                .done()
                .build();
    }
    
    /**
     * Создаёт MockProducer для Kafka.
     */
    MockProducer<RowKeySlice, byte[]> buildProducer() {
        return new MockProducer<>(
                true,
                new RowKeySliceSerializer(),
                new ByteArraySerializer());
    }
    
    /**
     * Создаёт декодер для тестов.
     */
    Decoder buildDecoder() {
        return new Decoder() {
            @Override
            public Object decode(TableName table, String qualifier, byte[] value) {
                if (value == null) {
                    return null;
                }
                if ("value_long".equalsIgnoreCase(qualifier)) {
                    return Bytes.toLong(value);
                }
                return new String(value, StandardCharsets.UTF_8);
            }

            @Override
            public void decodeRowKey(TableName table, RowKeySlice rowKey, int saltBytes, java.util.Map<String, Object> out) {
                out.put("id", new String(rowKey.toByteArray(), StandardCharsets.UTF_8));
            }
        };
    }
    
    /**
     * Создаёт WalRowProcessor с заданным producer.
     */
    WalRowProcessor buildProcessor(H2kConfig config, MockProducer<RowKeySlice, byte[]> producer) {
        PayloadBuilder builder = new PayloadBuilder(buildDecoder(), config, new MockSchemaRegistryClient());
        WalRowDispatcher dispatcher = new WalRowDispatcher(builder, producer);
        WalObserverHub observers = WalObserverHub.create(config);
        return new WalRowProcessor(dispatcher, observers);
    }
    
    /**
     * Создаёт BatchSender.
     */
    BatchSender buildBatchSender() {
        return new BatchSender(batchSize, (int) flushIntervalMs);
    }
    
    /**
     * Создаёт WalCounterService.EntryCounters.
     */
    WalCounterService.EntryCounters buildCounters() {
        return new WalCounterService().newEntryCounters();
    }
    
    /**
     * Создаёт RowContext с заданными параметрами.
     */
    WalRowProcessor.RowContext buildContext(
            H2kConfig config,
            BatchSender sender,
            WalCounterService.EntryCounters counters) {
        return new WalRowProcessor.RowContext(
                topic,
                tableName,
                walMeta,
                sender,
                config.describeTableOptions(tableName),
                cfFilter,
                counters);
    }
    
    /**
     * Создаёт список с одной ячейкой.
     */
    List<Cell> buildSingleCell(byte[] row, byte[] family) {
        ArrayList<Cell> cells = new ArrayList<>(1);
        cells.add(new KeyValue(row, family, Bytes.toBytes("value_long"), 1L, Bytes.toBytes(1L)));
        return cells;
    }
    
    /**
     * Создаёт RowKeySlice.Mutable из байтового массива.
     */
    RowKeySlice.Mutable buildRowKey(byte[] row) {
        return new RowKeySlice.Mutable(row, 0, row.length);
    }
}
