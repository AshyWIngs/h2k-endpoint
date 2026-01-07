package kz.qazmarka.h2k.payload.serializer.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Простой локальный микробенч для проверки пропускной способности сериализатора.
 * Не является тестом и запускается вручную.
 */
public final class ConfluentAvroPayloadSerializerMicrobench {
    private static final int DEFAULT_WARMUP = 10_000;
    private static final int DEFAULT_ITERATIONS = 100_000;

    public static void main(String[] args) {
        int warmup = readIntArg(args, 0, DEFAULT_WARMUP);
        int iterations = readIntArg(args, 1, DEFAULT_ITERATIONS);

        SerializerTestBuilder builder = new SerializerTestBuilder();
        AvroSchemaRegistry localRegistry = builder.buildLocalRegistry();
        builder.withLocalRegistry(localRegistry);
        RecordingSchemaRegistryClient client = builder.buildMockClient();
        TableName table = builder.buildTableName();
        Schema schema = localRegistry.getByTable(table.getNameAsString());
        GenericData.Record avroRecord = builder.buildAvroRecord(schema);

        try (ConfluentAvroPayloadSerializer serializer = builder.buildSerializer(client)) {
            for (int i = 0; i < warmup; i++) {
                serializer.serialize(table, avroRecord);
            }
            long start = System.nanoTime();
            long bytes = 0;
            for (int i = 0; i < iterations; i++) {
                bytes += serializer.serialize(table, avroRecord).length;
            }
            long elapsedNs = System.nanoTime() - start;
            double seconds = elapsedNs / 1_000_000_000.0d;
            double ops = iterations / seconds;
            double mb = bytes / (1024.0d * 1024.0d);
            System.out.println("итерации=" + iterations
                    + " время_с=" + String.format(java.util.Locale.ROOT, "%.3f", seconds)
                    + " операций_в_сек=" + String.format(java.util.Locale.ROOT, "%.1f", ops)
                    + " мб=" + String.format(java.util.Locale.ROOT, "%.2f", mb));
        }
    }

    private static int readIntArg(String[] args, int index, int def) {
        if (args == null || args.length <= index) {
            return def;
        }
        try {
            return Integer.parseInt(args[index]);
        } catch (NumberFormatException ex) {
            return def;
        }
    }

    private ConfluentAvroPayloadSerializerMicrobench() {}
}
