package kz.qazmarka.h2k.payload.builder;

/**
 * Компактное представление среза байтов (массив + смещение + длина) без копирования.
 * Используется для передачи бинарных значений в Avro-конвейер без создания {@link java.nio.ByteBuffer}.
 */
public final class BinarySlice {
    private final byte[] array;
    private final int offset;
    private final int length;

    private BinarySlice(byte[] array, int offset, int length) {
        this.array = array;
        this.offset = offset;
        this.length = length;
    }

    public static BinarySlice of(byte[] array, int offset, int length) {
        if (array == null) {
            throw new IllegalArgumentException("Массив array не задан");
        }
        if (offset < 0 || length < 0 || offset > array.length - length) {
            throw new IndexOutOfBoundsException("Некорректные границы среза: off=" + offset + ", len=" + length + ", cap=" + array.length);
        }
        return new BinarySlice(array, offset, length);
    }

    public byte[] array() {
        return array;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }
}
