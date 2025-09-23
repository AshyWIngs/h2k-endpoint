package kz.qazmarka.h2k.endpoint.internal;

/**
 * Результат обработки одной записи WAL.
 */
public final class WalProcessingResult {
    public final org.apache.hadoop.hbase.TableName table;
    public final String topic;
    public final int rowsSent;
    public final int cellsSent;

    public WalProcessingResult(org.apache.hadoop.hbase.TableName table,
                               String topic,
                               int rowsSent,
                               int cellsSent) {
        this.table = table;
        this.topic = topic;
        this.rowsSent = rowsSent;
        this.cellsSent = cellsSent;
    }
}
