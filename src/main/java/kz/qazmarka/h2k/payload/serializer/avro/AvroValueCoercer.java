package kz.qazmarka.h2k.payload.serializer.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import kz.qazmarka.h2k.payload.builder.BinarySlice;

/**
 * Приведение Java-значений к типам Avro с поддержкой BinarySlice/union/массива и т.п.
 * Код извлечён из прежнего AvroSerializer, чтобы переиспользовать преобразование без промежуточных Map.
 */
public final class AvroValueCoercer {
    private static final Object NO_DEFAULT = new Object();
    private static final EnumMap<Schema.Type, Coercer> COERCERS = new EnumMap<>(Schema.Type.class);
    private static final String TYPE_BOOLEAN = "BOOLEAN";

    static {
        COERCERS.put(Schema.Type.NULL, (schema, v, path) -> null);
        COERCERS.put(Schema.Type.BOOLEAN, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Boolean) {
                return v;
            }
            if (v instanceof CharSequence) {
                String text = v.toString().trim();
                if (text.isEmpty()) {
                    throw typeError(path, TYPE_BOOLEAN, v);
                }
                if ("1".equals(text) || "true".equalsIgnoreCase(text)) {
                    return Boolean.TRUE;
                }
                if ("0".equals(text) || "false".equalsIgnoreCase(text)) {
                    return Boolean.FALSE;
                }
                throw typeError(path, TYPE_BOOLEAN, v);
            }
            if (v instanceof Number) {
                int numeric = ((Number) v).intValue();
                if (numeric == 0) return Boolean.FALSE;
                if (numeric == 1) return Boolean.TRUE;
                throw typeError(path, TYPE_BOOLEAN, v);
            }
            throw typeError(path, TYPE_BOOLEAN, v);
        });
        COERCERS.put(Schema.Type.INT, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Number) return ((Number) v).intValue();
            throw typeError(path, "INT", v);
        });
        COERCERS.put(Schema.Type.LONG, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Number) return ((Number) v).longValue();
            throw typeError(path, "LONG", v);
        });
        COERCERS.put(Schema.Type.DOUBLE, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Number) return ((Number) v).doubleValue();
            throw typeError(path, "DOUBLE", v);
        });
        COERCERS.put(Schema.Type.FLOAT, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof Number) return ((Number) v).floatValue();
            throw typeError(path, "FLOAT", v);
        });
        COERCERS.put(Schema.Type.STRING, (schema, v, path) -> {
            if (v == null) return null;
            if (v instanceof CharSequence) {
                return v.toString();
            }
            throw typeError(path, "STRING", v);
        });
        COERCERS.put(Schema.Type.BYTES, (schema, v, path) -> coerceBytes(v, path));
        COERCERS.put(Schema.Type.UNION, AvroValueCoercer::coerceUnion);
        COERCERS.put(Schema.Type.ARRAY, AvroValueCoercer::coerceArray);
        COERCERS.put(Schema.Type.MAP, AvroValueCoercer::coerceMap);
        COERCERS.put(Schema.Type.RECORD, AvroValueCoercer::coerceRecord);
        COERCERS.put(Schema.Type.ENUM, (schema, v, path) -> {
            if (v == null) return null;
            if (schema.getEnumSymbols().contains(String.valueOf(v))) {
                return new GenericData.EnumSymbol(schema, v.toString());
            }
            throw typeError(path, "ENUM", v);
        });
        COERCERS.put(Schema.Type.FIXED, (schema, v, path) -> {
            if (v == null) return null;
            byte[] bytes;
            if (v instanceof byte[]) {
                bytes = (byte[]) v;
            } else if (v instanceof BinarySlice) {
                BinarySlice slice = (BinarySlice) v;
                bytes = new byte[slice.length()];
                System.arraycopy(slice.array(), slice.offset(), bytes, 0, slice.length());
            } else {
                throw typeError(path, "FIXED", v);
            }
            if (bytes.length != schema.getFixedSize()) {
                throw new IllegalStateException("Avro: поле '" + path + "' ожидает FIXED " + schema.getFixedSize()
                        + ", получено " + bytes.length);
            }
            return new GenericData.Fixed(schema, bytes);
        });
    }

    interface Coercer {
        Object apply(Schema schema, Object value, String path);
    }

    public static Object coerceValue(Schema schema, Object value, String path) {
        Coercer coercer = COERCERS.get(schema.getType());
        if (coercer == null) {
            throw new IllegalStateException("Avro: неподдержанный тип поля '" + path + "': " + schema.getType());
        }
        return coercer.apply(schema, value, path);
    }

    private static Object coerceArray(Schema fieldSchema, Object v, String path) {
        if (v == null) {
            return null;
        }
        if (!(v instanceof List)) {
            throw typeError(path, "ARRAY", v);
        }
        Schema elem = fieldSchema.getElementType();
        List<?> in = (List<?>) v;
        List<Object> out = new ArrayList<>(in.size());
        for (int i = 0; i < in.size(); i++) {
            out.add(coerceValue(elem, in.get(i), path + "[" + i + "]"));
        }
        return out;
    }

    private static Map<String, Object> coerceMap(Schema fieldSchema, Object v, String path) {
        if (v == null) {
            throw new IllegalStateException("Avro: поле '" + path + "' обязательно и не может быть null");
        }
        Map<String, Object> in = toStringObjectMap(v, path);
        Schema vt = fieldSchema.getValueType();
        List<String> keys = new ArrayList<>(in.keySet());
        Collections.sort(keys);
        Map<String, Object> mapOut = new LinkedHashMap<>(in.size());
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            mapOut.put(key, coerceValue(vt, in.get(key), path + "." + key));
        }
        return mapOut;
    }

    private static GenericRecord coerceRecord(Schema fieldSchema, Object v, String path) {
        if (v == null) {
            return null;
        }
        Map<String, Object> m = toStringObjectMap(v, path);
        GenericData.Record out = new GenericData.Record(fieldSchema);
        for (Schema.Field f : fieldSchema.getFields()) {
            Object val = m.containsKey(f.name()) ? m.get(f.name()) : defaultValue(f);
            out.put(f.pos(), coerceValue(f.schema(), val, path + '.' + f.name()));
        }
        return out;
    }

    private static Object coerceUnion(Schema fieldSchema, Object v, String path) {
        boolean nullableAllowed = hasNullableBranch(fieldSchema);
        if (v == null) {
            if (nullableAllowed) {
                return null;
            }
            throw new IllegalStateException("Avro: поле обязательно: " + path);
        }

        IllegalStateException lastError = null;
        for (Schema option : fieldSchema.getTypes()) {
            if (option.getType() == Schema.Type.NULL) {
                continue;
            }
            try {
                return coerceValue(option, v, path);
            } catch (IllegalStateException ex) {
                lastError = ex;
            }
        }
        throw unionMismatch(path, lastError);
    }

    private static boolean hasNullableBranch(Schema unionSchema) {
        for (Schema option : unionSchema.getTypes()) {
            if (option.getType() == Schema.Type.NULL) {
                return true;
            }
        }
        return false;
    }

    private static IllegalStateException unionMismatch(String path, IllegalStateException lastError) {
        String message = "Avro: значение поля '" + path + "' не подходит ни к одной ветке union";
        return lastError == null ? new IllegalStateException(message) : new IllegalStateException(message, lastError);
    }

    private static Map<String, Object> toStringObjectMap(Object v, String path) {
        if (!(v instanceof Map<?, ?>)) {
            throw typeError(path, "MAP", v);
        }
        Map<?, ?> input = (Map<?, ?>) v;
        Map<String, Object> normalized = new HashMap<>(input.size());
        // Без лямбды: простой for-loop для избежания ненужных объектов на горячем пути
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            normalized.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return normalized;
    }

    private static Object coerceBytes(Object datum, String path) {
        if (datum == null) {
            return null;
        }
        if (datum instanceof ByteBuffer) {
            return datum;
        }
        if (datum instanceof BinarySlice) {
            BinarySlice slice = (BinarySlice) datum;
            return ByteBuffer.wrap(slice.array(), slice.offset(), slice.length());
        }
        if (datum instanceof byte[]) {
            byte[] arr = (byte[]) datum;
            return ByteBuffer.wrap(arr);
        }
        throw typeError(path, "BYTES", datum);
    }

    private static Object defaultValue(Schema.Field field) {
        Object def = field.defaultVal();
        return def == null ? NO_DEFAULT : def;
    }

    private static IllegalStateException typeError(String path, String expected, Object actual) {
        return new IllegalStateException(
                "Avro: поле '" + path + "' ожидает " + expected + ", получено: " +
                        (actual == null ? "null" : actual.getClass().getSimpleName()));
    }

    private AvroValueCoercer() {}
}
