package kz.qazmarka.h2k.kafka.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Сериализатор rowkey без лишних аллокаций.
 * Если {@link RowKeySlice} покрывает весь исходный массив (offset=0 и длина совпадает с размером массива),
 * возвращается оригинальный массив без копирования. Во всех остальных случаях выполняется однократное
 * копирование только затрагиваемой части.
 */
public final class RowKeySliceSerializer implements Serializer<RowKeySlice> {

    private static final byte[] EMPTY = new byte[0];

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // конфигурация не требуется
    }

    @Override
    public byte[] serialize(String topic, RowKeySlice data) {
        if (data == null) {
            return EMPTY;
        }
        byte[] array = data.getArray();
        int offset = data.getOffset();
        int length = data.getLength();
        if (length == 0) {
            return EMPTY;
        }
        if (offset == 0 && array.length == length) {
            return array;
        }
        byte[] copy = new byte[length];
        System.arraycopy(array, offset, copy, 0, length);
        return copy;
    }

    @Override
    public void close() {
        // нет ресурсов для освобождения
    }
}
