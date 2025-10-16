package kz.qazmarka.h2k.util;

/**
 * Быстрые побитовые/байтовые утилиты без сторонних зависимостей.
 * Содержит только «горячие» методы, востребованные при построении payload.
 */
public final class Bytes {

    /** Общая таблица для быстрого вывода HEX. */
    private static final char[] HEX_TABLE = "0123456789abcdef".toCharArray();

    private Bytes() { /* no instances */ }

    /**
     * Конвертирует срез байтов в HEX-строку без промежуточных объектов.
     *
     * @throws IndexOutOfBoundsException если (off,len) выходят за границы массива
     */
    public static String toHex(byte[] a, int off, int len) {
        if (a == null || len == 0) return "";
        if (off < 0 || len < 0 || off > a.length - len) {
            throw new IndexOutOfBoundsException("срез hex вне границ: off=" + off + " len=" + len + " cap=" + a.length);
        }
        char[] out = new char[len * 2];
        int p = 0;
        int iEnd = off + len;
        for (int i = off; i < iEnd; i++) {
            int v = a[i] & 0xFF;
            out[p++] = HEX_TABLE[v >>> 4];
            out[p++] = HEX_TABLE[v & 0x0F];
        }
        return new String(out);
    }

}
