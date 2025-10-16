package kz.qazmarka.h2k.schema.phoenix;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.types.PhoenixArray;

/**
 * Набор утилит для нормализации значений Phoenix: время → epoch millis, массивы → {@code List<Object>}.
 */
public final class PhoenixValueNormalizer {

    private static final ValueTransformer IDENTITY = (value, table, qualifier) -> value;
    private static final Map<Class<?>, ValueTransformer> BASE_TRANSFORMERS = buildBaseTransformers();
    private static final Map<Class<?>, ValueTransformer> TEMP_TRANSFORMERS = buildTemporalTransformers();
    private static final ConcurrentMap<Class<?>, ValueTransformer> TRANSFORMER_CACHE = new ConcurrentHashMap<>(8);
    private static final Map<Class<?>, PrimitiveArrayBoxer> PRIMITIVE_BOXERS = buildPrimitiveBoxers();

    private PhoenixValueNormalizer() {
    }

    /**
     * Приводит значение Phoenix к удобному виду: нормализует время и разворачивает массивы.
     *
     * @param value     исходное значение Phoenix
     * @param table     таблица (для сообщений об ошибках)
     * @param qualifier имя колонки (для сообщений об ошибках)
     * @return нормализованное значение (может быть {@code null})
     */
    public static Object normalizeValue(Object value, TableName table, String qualifier) {
        if (value == null) return null;
        ValueTransformer transformer = findTransformer(value.getClass());
        return transformer.apply(value, table, qualifier);
    }

    /** Преобразует временные типы Phoenix (Timestamp/Date/Time) в epoch millis. */
    static Object normalizeTemporal(Object valueObj) {
        if (valueObj == null) return null;
        ValueTransformer transformer = TEMP_TRANSFORMERS.get(valueObj.getClass());
        if (transformer == null) {
            return valueObj;
        }
        return transformer.apply(valueObj, null, null);
    }

    private static Map<Class<?>, ValueTransformer> buildBaseTransformers() {
        Map<Class<?>, ValueTransformer> map = new HashMap<>(8);
        map.put(Timestamp.class, (value, table, qualifier) -> ((Timestamp) value).getTime());
        map.put(Date.class, (value, table, qualifier) -> ((Date) value).getTime());
        map.put(Time.class, (value, table, qualifier) -> ((Time) value).getTime());
        map.put(PhoenixArray.class, (value, table, qualifier) -> toListFromPhoenixArray((PhoenixArray) value, table, qualifier));
        return Collections.unmodifiableMap(map);
    }

    private static Map<Class<?>, ValueTransformer> buildTemporalTransformers() {
        Map<Class<?>, ValueTransformer> map = new HashMap<>(4);
        map.put(Timestamp.class, BASE_TRANSFORMERS.get(Timestamp.class));
        map.put(Date.class, BASE_TRANSFORMERS.get(Date.class));
        map.put(Time.class, BASE_TRANSFORMERS.get(Time.class));
        return Collections.unmodifiableMap(map);
    }

    private static List<Object> toListFromPhoenixArray(PhoenixArray pa, TableName table, String qualifier) {
        try {
            Object raw = pa.getArray();
            return toListFromRawArray(raw);
        } catch (SQLException e) {
            throw new IllegalStateException("Ошибка декодирования PhoenixArray для " + table + "." + qualifier, e);
        }
    }

    /**
     * Преобразует массив Phoenix (объектный или примитивный) в список Java, минимизируя аллокации.
     */
    /** Преобразует массив Phoenix (объектный/примитивный) в список без лишних копий. */
    public static List<Object> toListFromRawArray(Object raw) {
        Objects.requireNonNull(raw, "Сырый массив Phoenix не может быть null");
        if (raw instanceof Object[]) {
            return Arrays.asList((Object[]) raw);
        }
        Class<?> componentType = raw.getClass().getComponentType();
        PrimitiveArrayBoxer boxer = PRIMITIVE_BOXERS.get(componentType);
        if (boxer != null) {
            return boxer.box(raw);
        }
        int n = Array.getLength(raw);
        if (n == 0) return Collections.emptyList();
        ArrayList<Object> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(Array.get(raw, i));
        }
        return list;
    }

    private static Map<Class<?>, PrimitiveArrayBoxer> buildPrimitiveBoxers() {
        Map<Class<?>, PrimitiveArrayBoxer> m = new HashMap<>(8);
        m.put(Integer.TYPE, boxer(a -> ((int[]) a).length, (a, i) -> ((int[]) a)[i]));
        m.put(Long.TYPE, boxer(a -> ((long[]) a).length, (a, i) -> ((long[]) a)[i]));
        m.put(Double.TYPE, boxer(a -> ((double[]) a).length, (a, i) -> ((double[]) a)[i]));
        m.put(Float.TYPE, boxer(a -> ((float[]) a).length, (a, i) -> ((float[]) a)[i]));
        m.put(Short.TYPE, boxer(a -> ((short[]) a).length, (a, i) -> ((short[]) a)[i]));
        m.put(Byte.TYPE, boxer(a -> ((byte[]) a).length, (a, i) -> ((byte[]) a)[i]));
        m.put(Boolean.TYPE, boxer(a -> ((boolean[]) a).length, (a, i) -> ((boolean[]) a)[i]));
        m.put(Character.TYPE, boxer(a -> ((char[]) a).length, (a, i) -> ((char[]) a)[i]));
        return Collections.unmodifiableMap(m);
    }

    private static PrimitiveArrayBoxer boxer(ToIntFunction<Object> lengthFn,
                                             BiFunction<Object, Integer, Object> elementFn) {
        return array -> {
            int length = lengthFn.applyAsInt(array);
            if (length == 0) {
                return Collections.emptyList();
            }
            ArrayList<Object> list = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                list.add(elementFn.apply(array, i));
            }
            return list;
        };
    }

    private static ValueTransformer findTransformer(Class<?> type) {
        return TRANSFORMER_CACHE.computeIfAbsent(type, PhoenixValueNormalizer::resolveTransformer);
    }

    private static ValueTransformer resolveTransformer(Class<?> type) {
        Class<?> current = type;
        while (current != null) {
            ValueTransformer direct = BASE_TRANSFORMERS.get(current);
            if (direct != null) {
                return direct;
            }
            ValueTransformer nested = searchInterfaces(current.getInterfaces());
            if (nested != null) {
                return nested;
            }
            current = current.getSuperclass();
        }
        return IDENTITY;
    }

    private static ValueTransformer searchInterfaces(Class<?>[] interfaces) {
        for (Class<?> iface : interfaces) {
            ValueTransformer direct = BASE_TRANSFORMERS.get(iface);
            if (direct != null) {
                return direct;
            }
            ValueTransformer nested = searchInterfaces(iface.getInterfaces());
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    @FunctionalInterface
    private interface PrimitiveArrayBoxer {
        List<Object> box(Object array);
    }

    @FunctionalInterface
    private interface ValueTransformer {
        Object apply(Object value, TableName table, String qualifier);
    }
}
