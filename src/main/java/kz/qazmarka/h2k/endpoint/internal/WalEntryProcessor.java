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

        ProcessingCounters counters = new ProcessingCounters();
        RowProcessingContext context = new RowProcessingContext(
                filterEnabled,
                cfFamilies,
                topic,
                table,
                walMeta,
                sender,
                counters);

        final List<Cell> rowBuffer = new ArrayList<>(8);
        RowKeySlice currentSlice = null;
        byte[] prevArr = null;
        int prevOff = -1;
        int prevLen = -1;

        for (Cell cell : cells) {
            final byte[] arr = cell.getRowArray();
            final int off = cell.getRowOffset();
            final int len = cell.getRowLength();

            if (isSameRow(currentSlice, arr, off, len, prevArr, prevOff, prevLen)) {
                rowBuffer.add(cell);
                continue;
            }

            context.flushRow(rowBuffer, currentSlice);

            rowBuffer.add(cell);
            currentSlice = new RowKeySlice(arr, off, len);
            prevArr = arr;
            prevOff = off;
            prevLen = len;
        }

        context.flushRow(rowBuffer, currentSlice);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                    table, topic, counters.rowsSent, counters.cellsSent, filterEnabled, topicManager.ensureEnabled());
        }
    }

    /**
     * Быстрый тест: принадлежит ли текущая ячейка тому же rowkey, что и предыдущая.
     * Сравнение выполняется по ссылке и координатам, чтобы избежать побайтного сравнения на горячем пути.
     */
    private static boolean isSameRow(RowKeySlice currentSlice,
                                     byte[] arr,
                                     int off,
                                     int len,
                                     byte[] prevArr,
                                     int prevOff,
                                     int prevLen) {
        return currentSlice != null && arr == prevArr && off == prevOff && len == prevLen;
    }

    /**
     * Контекст обработки строки WAL: инкапсулирует настройки фильтрации и сборки payload,
     * чтобы передавать в helper-методы меньше параметров и снизить когнитивную сложность.
     */
    private final class RowProcessingContext {
        private final boolean filterEnabled;
        private final byte[][] cfFamilies;
        private final String topic;
        private final TableName table;
        private final WalMeta walMeta;
        private final BatchSender sender;
        private final ProcessingCounters counters;

        RowProcessingContext(boolean filterEnabled,
                             byte[][] cfFamilies,
                             String topic,
                             TableName table,
                             WalMeta walMeta,
                             BatchSender sender,
                             ProcessingCounters counters) {
            this.filterEnabled = filterEnabled;
            this.cfFamilies = cfFamilies;
            this.topic = topic;
            this.table = table;
            this.walMeta = walMeta;
            this.sender = sender;
            this.counters = counters;
        }

        /**
         * Отправляет накопленную по текущему rowkey партию ячеек, если она прошла фильтр CF.
         * Метод очищает буфер вне зависимости от результата, чтобы подготовиться к следующей строке.
         */
        void flushRow(List<Cell> rowBuffer, RowKeySlice currentSlice) {
            if (currentSlice == null || rowBuffer.isEmpty()) {
                return;
            }
            if (filterEnabled && !passCfFilter(rowBuffer, cfFamilies)) {
                rowBuffer.clear();
                return;
            }
            WalEntryProcessor.this.sendRow(topic, table, walMeta, currentSlice, rowBuffer, sender);
            counters.rowsSent++;
            counters.cellsSent += rowBuffer.size();
            rowBuffer.clear();
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

    /**
     * Лёгкие счётчики по записи WAL — количество отправленных строк и ячеек.
     * Выделены в отдельный объект, чтобы передавать по ссылке и избегать возврата pair-структур.
     */
    private static final class ProcessingCounters {
        int rowsSent;
        int cellsSent;
    }

    /**
     * Отправляет одну собранную строку в Kafka. Используется как горячим путём, так и в тестах
     * для проверки выбранного сериализатора без запуска полного цикла {@link #process}.
     */
    void sendRow(String topic,
                 TableName table,
                 WalMeta walMeta,
                 RowKeySlice rowKey,
                 List<Cell> cells,
                 BatchSender sender) {
        final byte[] keyBytes = rowKey.toByteArray();
        final byte[] valueBytes = payloadBuilder.buildRowPayloadBytes(table, new ArrayList<>(cells), rowKey, walMeta.seq, walMeta.writeTime);
        sender.add(producer.send(new ProducerRecord<>(topic, keyBytes, valueBytes)));
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

}
