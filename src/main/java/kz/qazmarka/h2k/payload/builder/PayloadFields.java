package kz.qazmarka.h2k.payload.builder;

/**
 * Магазин названий служебных полей Avro-записи, которые дополняют данные Phoenix.
 * Централизация избавляет от «магических строк» в сборщиках payload и тестах.
 */
public final class PayloadFields {
    public static final String DELETE = "_delete";
    public static final String EVENT_TS = "_event_ts";
    public static final String WAL_SEQ = "_wal_seq";
    public static final String WAL_WRITE_TIME = "_wal_write_time";

    private PayloadFields() {}
}
