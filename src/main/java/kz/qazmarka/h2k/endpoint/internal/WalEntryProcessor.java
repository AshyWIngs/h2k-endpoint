package kz.qazmarka.h2k.endpoint.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.kafka.BatchSender;
import kz.qazmarka.h2k.payload.PayloadBuilder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Обрабатывает записи WAL: группирует по rowkey, фильтрует и отправляет события в Kafka.
 */
public final class WalEntryProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(WalEntryProcessor.class);

    private final PayloadBuilder payloadBuilder;
    private final TopicManager topicManager;
    private final Producer<byte[], byte[]> producer;

    public WalEntryProcessor(PayloadBuilder payloadBuilder,
                             TopicManager topicManager,
                             Producer<byte[], byte[]> producer) {
        this.payloadBuilder = payloadBuilder;
        this.topicManager = topicManager;
        this.producer = producer;
    }

    public WalProcessingResult process(WAL.Entry entry,
                                       BatchSender sender,
                                       boolean includeWalMeta,
                                       boolean filterEnabled,
                                       byte[][] cfFamilies,
                                       long minTs) {
        TableName table = entry.getKey().getTablename();
        String topic = topicManager.resolveTopic(table);
        topicManager.ensureTopicIfNeeded(topic);

        WalMeta walMeta = includeWalMeta ? readWalMeta(entry) : WalMeta.EMPTY;

        int rowsSent = 0;
        int cellsSent = 0;
        for (Map.Entry<RowKeySlice, List<Cell>> rowEntry : filteredRows(entry, filterEnabled, cfFamilies, minTs)) {
            sendRow(topic, table, walMeta, rowEntry, sender);
            rowsSent++;
            cellsSent += rowEntry.getValue().size();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                    table, topic, rowsSent, cellsSent, filterEnabled, topicManager.ensureEnabled());
        }

        return new WalProcessingResult(table, topic, rowsSent, cellsSent);
    }

    private Iterable<Map.Entry<RowKeySlice, List<Cell>>> filteredRows(WAL.Entry entry,
                                                                      boolean doFilter,
                                                                      byte[][] cfFamilies,
                                                                      long minTs) {
        final Map<RowKeySlice, List<Cell>> byRow = groupByRow(entry);
        if (!doFilter) {
            return byRow.entrySet();
        }
        List<Map.Entry<RowKeySlice, List<Cell>>> out = null;
        for (Map.Entry<RowKeySlice, List<Cell>> e : byRow.entrySet()) {
            if (passWalTsFilter(e.getValue(), cfFamilies, minTs)) {
                if (out == null) {
                    out = new ArrayList<>(Math.min(byRow.size(), 16));
                }
                out.add(e);
            }
        }
        return (out != null) ? out : Collections.<Map.Entry<RowKeySlice, List<Cell>>>emptyList();
    }

    private Map<RowKeySlice, List<Cell>> groupByRow(WAL.Entry entry) {
        final List<Cell> cells = entry.getEdit().getCells();
        if (cells == null || cells.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<RowKeySlice, List<Cell>> byRow = new LinkedHashMap<>(initialCapacity(cells.size()));

        byte[] prevArr = null;
        int prevOff = -1;
        int prevLen = -1;
        List<Cell> currentList = null;

        for (Cell c : cells) {
            final byte[] arr = c.getRowArray();
            final int off = c.getRowOffset();
            final int len = c.getRowLength();

            if (currentList != null && arr == prevArr && off == prevOff && len == prevLen) {
                currentList.add(c);
            } else {
                RowKeySlice key = new RowKeySlice(arr, off, len);
                currentList = byRow.computeIfAbsent(key, k -> new ArrayList<>(8));
                currentList.add(c);
                prevArr = arr;
                prevOff = off;
                prevLen = len;
            }
        }
        return byRow;
    }

    private static boolean passWalTsFilter(List<Cell> cells, byte[][] cfFamilies, long minTs) {
        if (cfFamilies == null || cfFamilies.length == 0) {
            return true;
        }
        final int n = cfFamilies.length;
        if (n == 1) {
            return passWalTsFilter1(cells, cfFamilies[0], minTs);
        } else if (n == 2) {
            return passWalTsFilter2(cells, cfFamilies[0], cfFamilies[1], minTs);
        }
        return passWalTsFilterN(cells, cfFamilies, minTs);
    }

    private static boolean passWalTsFilter1(List<Cell> cells,
                                            byte[] cf0,
                                            long minTs) {
        for (Cell c : cells) {
            if (c.getTimestamp() >= minTs && CellUtil.matchingFamily(c, cf0)) {
                return true;
            }
        }
        return false;
    }

    private static boolean passWalTsFilter2(List<Cell> cells,
                                            byte[] cf0,
                                            byte[] cf1,
                                            long minTs) {
        for (Cell c : cells) {
            if (c.getTimestamp() >= minTs &&
                (CellUtil.matchingFamily(c, cf0) || CellUtil.matchingFamily(c, cf1))) {
                return true;
            }
        }
        return false;
    }

    private static boolean passWalTsFilterN(List<Cell> cells,
                                            byte[][] cfFamilies,
                                            long minTs) {
        for (Cell c : cells) {
            if (c.getTimestamp() < minTs) {
                continue;
            }
            for (byte[] cf : cfFamilies) {
                if (CellUtil.matchingFamily(c, cf)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void sendRow(String topic,
                         TableName table,
                         WalMeta wm,
                         Map.Entry<RowKeySlice, List<Cell>> rowEntry,
                         BatchSender sender) {
        final List<Cell> cells = rowEntry.getValue();
        final byte[] keyBytes = rowEntry.getKey().toByteArray();
        final byte[] valueBytes = payloadBuilder.buildRowPayloadBytes(table, cells, rowEntry.getKey(), wm.seq, wm.writeTime);
        sender.add(send(topic, keyBytes, valueBytes));
    }

    private static WalMeta readWalMeta(WAL.Entry entry) {
        long walSeq = -1L;
        try {
            walSeq = entry.getKey().getSequenceId();
        } catch (IOException e) {
            // допускаем отсутствие sequenceId без логирования
        }
        final long walWriteTime = entry.getKey().getWriteTime();
        return new WalMeta(walSeq, walWriteTime);
    }

    private java.util.concurrent.Future<RecordMetadata> send(String topic, byte[] key, byte[] value) {
        return producer.send(new ProducerRecord<>(topic, key, value));
    }

    private static int initialCapacity(int expected) {
        if (expected <= 0) {
            return 16;
        }
        long cap = 1L + (4L * expected + 2L) / 3L;
        return cap > (1L << 30) ? (1 << 30) : (int) cap;
    }
}
