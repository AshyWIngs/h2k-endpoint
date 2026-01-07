package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Потокобезопасный писатель Confluent Avro: формирует заголовок (magic byte + schemaId)
 * и сериализует запись без лишних аллокаций. Буферы кэшируются в ThreadLocal.
 */
final class ConfluentRecordWriter {

    private final Schema schema;
    private final byte magicByte;
    private final int magicHeaderLength;
    private final int maxThreadlocalBuffer;
    private final ThreadLocal<HeaderByteArrayOutputStream> localBaos;
    private final ThreadLocal<BinaryEncoder> localEncoder = new ThreadLocal<>();
    private final ThreadLocal<ByteOptimizedDatumWriter> localWriter;

    ConfluentRecordWriter(Schema schema, byte magicByte, int magicHeaderLength, int maxThreadlocalBuffer) {
        this.schema = schema;
        this.magicByte = magicByte;
        this.magicHeaderLength = magicHeaderLength;
        this.maxThreadlocalBuffer = maxThreadlocalBuffer;
        this.localBaos = ThreadLocal.withInitial(() -> new HeaderByteArrayOutputStream(512 + magicHeaderLength));
        this.localWriter = ThreadLocal.withInitial(() -> new ByteOptimizedDatumWriter(schema));
    }

    byte[] write(GenericData.Record avroRecord, int schemaId) {
        HeaderByteArrayOutputStream baos = localBaos.get();
        baos.resetWithHeader(magicHeaderLength);
        byte[] buffer = baos.buffer();
        buffer[0] = magicByte;
        buffer[1] = (byte) ((schemaId >>> 24) & 0xFF);
        buffer[2] = (byte) ((schemaId >>> 16) & 0xFF);
        buffer[3] = (byte) ((schemaId >>> 8) & 0xFF);
        buffer[4] = (byte) (schemaId & 0xFF);
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, localEncoder.get());
        localEncoder.set(encoder);
        ByteOptimizedDatumWriter writer = localWriter.get();
        writer.setSchema(schema);
        try {
            writer.write(avroRecord, encoder);
            encoder.flush();
        } catch (IOException | RuntimeException ex) {
            localEncoder.remove();
            localWriter.remove();
            localBaos.remove();
            throw new IllegalStateException("Avro: ошибка сериализации записи: " + ex.getMessage(), ex);
        }
        byte[] result = baos.toByteArray();
        if (result.length > maxThreadlocalBuffer) {
            localBaos.set(new HeaderByteArrayOutputStream(512 + magicHeaderLength));
            localEncoder.remove();
        }
        return result;
    }

    private static final class HeaderByteArrayOutputStream extends ByteArrayOutputStream {
        HeaderByteArrayOutputStream(int size) {
            super(size);
        }

        void resetWithHeader(int headerLength) {
            if (buf.length < headerLength) {
                buf = new byte[headerLength];
            }
            count = headerLength;
        }

        byte[] buffer() {
            return buf;
        }
    }

    private static final class ByteOptimizedDatumWriter extends org.apache.avro.generic.GenericDatumWriter<GenericData.Record> {
        ByteOptimizedDatumWriter(Schema schema) {
            super(schema);
        }

        @Override
        protected void writeBytes(Object datum, Encoder out) throws IOException {
            if (datum instanceof kz.qazmarka.h2k.payload.builder.BinarySlice) {
                kz.qazmarka.h2k.payload.builder.BinarySlice slice =
                        (kz.qazmarka.h2k.payload.builder.BinarySlice) datum;
                out.writeBytes(slice.array(), slice.offset(), slice.length());
                return;
            }
            super.writeBytes(datum, out);
        }

        @Override
        protected Iterable<Map.Entry<Object, Object>> getMapEntries(Object value) {
            return sortEntriesIfMap(value, this::fallbackMapEntries);
        }

        private Iterable<Map.Entry<Object, Object>> fallbackMapEntries(Object value) {
            return super.getMapEntries(value);
        }

        private static Iterable<Map.Entry<Object, Object>> sortEntriesIfMap(Object value, MapEntryFallback fallback) {
            if (!(value instanceof Map)) {
                return fallback.apply(value);
            }
            Map<?, ?> map = (Map<?, ?>) value;
            Map<String, Object> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = entry.getKey() == null ? "" : entry.getKey().toString();
                sorted.put(key, entry.getValue());
            }
            List<Map.Entry<Object, Object>> ordered = new ArrayList<>(sorted.size());
            for (Map.Entry<String, Object> entry : sorted.entrySet()) {
                ordered.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
            }
            return ordered;
        }

        @FunctionalInterface
        private interface MapEntryFallback {
            Iterable<Map.Entry<Object, Object>> apply(Object value);
        }

    }
}
