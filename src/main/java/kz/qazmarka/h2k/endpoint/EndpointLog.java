package kz.qazmarka.h2k.endpoint;

import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import kz.qazmarka.h2k.payload.builder.PayloadBuilder;

/**
 * Лёгкий помощник для формирования единых лог-сообщений репликации.
 */
final class EndpointLog {

    static final String LOG_UNKNOWN = "-";
    private static final String LOG_MESSAGE_FORMAT = "{} msg={}";

    private final Logger log;
    private final Supplier<String> peerIdSupplier;
    private final LongSupplier lastAckOffsetSupplier;
    private final IntSupplier lastAckPartitionSupplier;
    private final Supplier<PayloadBuilder> payloadBuilderSupplier;

    EndpointLog(Logger log,
                Supplier<String> peerIdSupplier,
                LongSupplier lastAckOffsetSupplier,
                IntSupplier lastAckPartitionSupplier,
                Supplier<PayloadBuilder> payloadBuilderSupplier) {
        this.log = log;
        this.peerIdSupplier = peerIdSupplier;
        this.lastAckOffsetSupplier = lastAckOffsetSupplier;
        this.lastAckPartitionSupplier = lastAckPartitionSupplier;
        this.payloadBuilderSupplier = payloadBuilderSupplier;
    }

    LogContext contextEmpty() {
        return new LogContext(safeValue(peerIdSupplier.get()),
                LOG_UNKNOWN,
                LOG_UNKNOWN,
                LOG_UNKNOWN,
                currentOffsets(),
                currentPartition(),
                currentSchemaId());
    }

    LogContext contextForEntries(List<WAL.Entry> entries, String walGroupId) {
        if (entries == null || entries.isEmpty()) {
            return contextEmpty();
        }
        return contextForEntry(entries.get(0), walGroupId);
    }

    LogContext contextForEntry(WAL.Entry entry, String walGroupId) {
        if (entry == null) {
            return contextEmpty();
        }
        WALKey key = entry.getKey();
        String table = LOG_UNKNOWN;
        String region = LOG_UNKNOWN;
        if (key != null) {
            table = formatTable(key.getTablename());
            region = formatRegion(key.getEncodedRegionName());
        }
        String wal = formatWal(walGroupId, key);
        return new LogContext(safeValue(peerIdSupplier.get()),
                table,
                region,
                wal,
                currentOffsets(),
                currentPartition(),
                currentSchemaId());
    }

    String prefix(String event, LogContext context) {
        StringBuilder sb = new StringBuilder(160);
        sb.append("событие=").append(safeValue(event));
        sb.append(" peerId=").append(context.peerId);
        sb.append(" table=").append(context.table);
        sb.append(" region=").append(context.region);
        sb.append(" wal=").append(context.wal);
        sb.append(" offsets=").append(context.offsets);
        sb.append(" partition=").append(context.partition);
        sb.append(" schemaId=").append(context.schemaId);
        return sb.toString();
    }

    void info(String event, LogContext context, String template, Object... args) {
        if (!log.isInfoEnabled()) {
            return;
        }
        log.info(LOG_MESSAGE_FORMAT, prefix(event, context), formatMessage(template, args));
    }

    void warn(String event, LogContext context, String template, Object... args) {
        if (!log.isWarnEnabled()) {
            return;
        }
        log.warn(LOG_MESSAGE_FORMAT, prefix(event, context), formatMessage(template, args));
    }

    void error(String event, LogContext context, String template, Object... args) {
        if (!log.isErrorEnabled()) {
            return;
        }
        log.error(LOG_MESSAGE_FORMAT, prefix(event, context), formatMessage(template, args));
    }

    void debug(String event, LogContext context, String template, Object... args) {
        if (!log.isDebugEnabled()) {
            return;
        }
        log.debug(LOG_MESSAGE_FORMAT, prefix(event, context), formatMessage(template, args));
    }

    static String safeExceptionMessage(Throwable ex) {
        if (ex == null) {
            return LOG_UNKNOWN;
        }
        String message = ex.getMessage();
        return message == null || message.isEmpty() ? LOG_UNKNOWN : message;
    }

    private String currentOffsets() {
        long offset = lastAckOffsetSupplier.getAsLong();
        if (offset < 0L) {
            return LOG_UNKNOWN;
        }
        return String.valueOf(offset);
    }

    private String currentPartition() {
        int partition = lastAckPartitionSupplier.getAsInt();
        if (partition < 0) {
            return LOG_UNKNOWN;
        }
        return String.valueOf(partition);
    }

    private String currentSchemaId() {
        PayloadBuilder builder = payloadBuilderSupplier.get();
        if (builder == null) {
            return LOG_UNKNOWN;
        }
        int schemaId = builder.lastSchemaId();
        if (schemaId <= 0) {
            return LOG_UNKNOWN;
        }
        return String.valueOf(schemaId);
    }

    private static String formatTable(TableName table) {
        if (table == null) {
            return LOG_UNKNOWN;
        }
        return table.getNameWithNamespaceInclAsString();
    }

    private static String formatRegion(byte[] region) {
        if (region == null || region.length == 0) {
            return LOG_UNKNOWN;
        }
        return Bytes.toStringBinary(region);
    }

    private static String formatWal(String walGroupId, WALKey key) {
        String group = safeValue(walGroupId);
        if (key == null) {
            return group;
        }
        long logSeq = key.getLogSeqNum();
        if (logSeq < 0L) {
            return group;
        }
        if (LOG_UNKNOWN.equals(group)) {
            return String.valueOf(logSeq);
        }
        return group + ':' + logSeq;
    }

    private static String safeValue(String value) {
        if (value == null || value.isEmpty()) {
            return LOG_UNKNOWN;
        }
        return value;
    }

    private static String formatMessage(String template, Object... args) {
        if (args == null || args.length == 0) {
            return template;
        }
        return MessageFormatter.arrayFormat(template, args).getMessage();
    }

    static final class LogContext {
        final String peerId;
        final String table;
        final String region;
        final String wal;
        final String offsets;
        final String partition;
        final String schemaId;

        LogContext(String peerId,
                   String table,
                   String region,
                   String wal,
                   String offsets,
                   String partition,
                   String schemaId) {
            this.peerId = peerId;
            this.table = table;
            this.region = region;
            this.wal = wal;
            this.offsets = offsets;
            this.partition = partition;
            this.schemaId = schemaId;
        }
    }
}
