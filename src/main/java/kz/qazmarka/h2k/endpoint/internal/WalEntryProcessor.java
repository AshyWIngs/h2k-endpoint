package kz.qazmarka.h2k.endpoint.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.kafka.producer.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
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

    /**
     * Конвертирует одну запись WAL в набор Kafka-сообщений: группирует по rowkey, опционально фильтрует
     * по CF, собирает payload и добавляет futures в переданный {@link BatchSender}.
     */
    /**
     * Конвертирует одну запись WAL в набор Kafka-сообщений: группирует по rowkey, фильтрует и отправляет в Kafka.
     */
    public void process(WAL.Entry entry,
                        BatchSender sender,
                        boolean includeWalMeta,
                        boolean filterEnabled,
                        byte[][] cfFamilies) {
        final List<Cell> cells = entry.getEdit().getCells();
        if (cells == null || cells.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Репликация: запись WAL без ячеек — таблица={}, фильтр={}, ensure-включён={}",
                        entry.getKey().getTablename(), filterEnabled, topicManager.ensureEnabled());
            }
            return;
        }

        final TableName table = entry.getKey().getTablename();
        final String topic = topicManager.resolveTopic(table);
        topicManager.ensureTopicIfNeeded(topic);

        final WalMeta walMeta = includeWalMeta ? readWalMeta(entry) : WalMeta.EMPTY;

        int rowsSent = 0;
        int cellsSent = 0;

        final List<Cell> rowBuffer = new ArrayList<>(8);
        RowKeySlice currentSlice = null;
        byte[] prevArr = null;
        int prevOff = -1;
        int prevLen = -1;

        for (Cell cell : cells) {
            final byte[] arr = cell.getRowArray();
            final int off = cell.getRowOffset();
            final int len = cell.getRowLength();

            if (currentSlice != null && arr == prevArr && off == prevOff && len == prevLen) {
                rowBuffer.add(cell);
                continue;
            }

            if (currentSlice != null) {
                if (!filterEnabled || passCfFilter(rowBuffer, cfFamilies)) {
                    sendRow(topic, table, walMeta, currentSlice, rowBuffer, sender);
                    rowsSent++;
                    cellsSent += rowBuffer.size();
                }
                rowBuffer.clear();
            }

            rowBuffer.add(cell);
            currentSlice = new RowKeySlice(arr, off, len);
            prevArr = arr;
            prevOff = off;
            prevLen = len;
        }

        if (currentSlice != null && (!filterEnabled || passCfFilter(rowBuffer, cfFamilies))) {
            sendRow(topic, table, walMeta, currentSlice, rowBuffer, sender);
            rowsSent++;
            cellsSent += rowBuffer.size();
            rowBuffer.clear();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                    table, topic, rowsSent, cellsSent, filterEnabled, topicManager.ensureEnabled());
        }
    }

    /** Проверяет, содержат ли ячейки допустимые CF. */
    private static boolean passCfFilter(List<Cell> cells, byte[][] cfFamilies) {
        if (cfFamilies == null || cfFamilies.length == 0) {
            return true; // фильтр выключен
        }
        final int n = cfFamilies.length;
        if (n == 1) {
            return passCfFilter1(cells, cfFamilies[0]);
        }
        if (n == 2) {
            return passCfFilter2(cells, cfFamilies[0], cfFamilies[1]);
        }
        return passCfFilterN(cells, cfFamilies);
    }

    /** Проверяет CF при одном допустимом семействе. */
    static boolean passCfFilter1(List<Cell> cells, byte[] cf0) {
        for (Cell c : cells) {
            if (CellUtil.matchingFamily(c, cf0)) {
                return true;
            }
        }
        return false;
    }

    /** Проверяет CF при двух допустимых семействах. */
    static boolean passCfFilter2(List<Cell> cells, byte[] cf0, byte[] cf1) {
        for (Cell c : cells) {
            if (CellUtil.matchingFamily(c, cf0)) {
                return true;
            }
            if (CellUtil.matchingFamily(c, cf1)) {
                return true;
            }
        }
        return false;
    }

    /** Проверяет CF при трех и более допустимых семействах. */
    static boolean passCfFilterN(List<Cell> cells, byte[][] cfFamilies) {
        for (Cell c : cells) {
            for (byte[] cf : cfFamilies) {
                if (CellUtil.matchingFamily(c, cf)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Формирует Kafka-запись для одной строки HBase и добавляет future в {@link BatchSender}. */
    private void sendRow(String topic,
                         TableName table,
                         WalMeta wm,
                         RowKeySlice rowKey,
                         List<Cell> cells,
                         BatchSender sender) {
        final byte[] keyBytes = rowKey.toByteArray();
        final byte[] valueBytes = payloadBuilder.buildRowPayloadBytes(table, new ArrayList<>(cells), rowKey, wm.seq, wm.writeTime);
        sender.add(send(topic, keyBytes, valueBytes));
    }

    /** Считывает sequenceId/writeTime из ключа WAL; ошибки приводят к -1. */
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
}
