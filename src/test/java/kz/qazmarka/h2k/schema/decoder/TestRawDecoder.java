package kz.qazmarka.h2k.schema.decoder;

import java.util.Arrays;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;

/**
 * Тестовый raw-декодер: возвращает исходные байты без копий.
 * Используется в юнит-тестах вместо устаревшего {@code SimpleDecoder}.
 */
public final class TestRawDecoder implements Decoder {

    public static final TestRawDecoder INSTANCE = new TestRawDecoder();

    private TestRawDecoder() {
    }

    @Override
    public Object decode(TableName table, String qualifier, byte[] value) {
        Objects.requireNonNull(table, MSG_TABLE_REQUIRED);
        Objects.requireNonNull(qualifier, MSG_QUALIFIER_REQUIRED);
        return value;
    }

    @Override
    public Object decode(TableName table, byte[] qual, byte[] value) {
        Objects.requireNonNull(table, MSG_TABLE_REQUIRED);
        Objects.requireNonNull(qual, MSG_QUALIFIER_REQUIRED);
        return value;
    }

    @Override
    public Object decode(TableName table,
                         byte[] qual,
                         int qOff,
                         int qLen,
                         byte[] value,
                         int vOff,
                         int vLen) {
        Objects.requireNonNull(table, MSG_TABLE_REQUIRED);
        Objects.requireNonNull(qual, MSG_QUALIFIER_REQUIRED);
        if (value == null) {
            return null;
        }
        if (vOff < 0 || vLen < 0 || vOff > value.length || vOff + vLen > value.length) {
            throw new IllegalArgumentException("Некорректные границы среза значения: off=" + vOff +
                    ", len=" + vLen + ", value.length=" + value.length);
        }
        if (vOff == 0 && vLen == value.length) {
            return value;
        }
        return Arrays.copyOfRange(value, vOff, vOff + vLen);
    }
}
