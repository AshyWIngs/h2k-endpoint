package kz.qazmarka.h2k.endpoint.processing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Обрабатывает записи WAL: группирует по rowkey, фильтрует и отправляет события в Kafka.
 * Контракт использования:
 * - экземпляр создаётся и используется в одном потоке репликации HBase;
 * - горячий путь повторно использует {@link RowKeySlice.Mutable}, чтобы избегать лишних аллокаций;
 * - передаваемый {@link BatchSender} управляет жизненным циклом фьючерсов KafkaProducer и должен очищаться
 *   вызывающей стороной после каждого вызова;
 * - счётчики основаны на {@link java.util.concurrent.atomic.LongAdder} и потокобезопасны, их можно опрашивать
 *   из TopicManager или через JMX без дополнительной синхронизации.
 */
public final class WalEntryProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(WalEntryProcessor.class);
    private static final long THROUGHPUT_LOG_INTERVAL_NS = TimeUnit.SECONDS.toNanos(5);
    private static final int ROW_BUFFER_BASE_CAPACITY = 32;
    private static final int ROW_BUFFER_TRIM_THRESHOLD = 4_096;

    private final PayloadBuilder payloadBuilder;
    private final TopicManager topicManager;
    private final Producer<byte[], byte[]> producer;
    private final H2kConfig config;
    private final ArrayList<Cell> rowBuffer = new ArrayList<>(ROW_BUFFER_BASE_CAPACITY);
    private final LongAdder entriesProcessed = new LongAdder();
    private final LongAdder rowsProcessed = new LongAdder();
    private final LongAdder cellsProcessed = new LongAdder();
    private final LongAdder rowsFiltered = new LongAdder();
    private final LongAdder entriesWindow = new LongAdder();
    private final LongAdder rowsWindow = new LongAdder();
    private final LongAdder cellsWindow = new LongAdder();
    private final LongAdder filteredRowsWindow = new LongAdder();
    private final LongAdder rowBufferUpsize = new LongAdder();
    private final LongAdder rowBufferTrim = new LongAdder();
    private final TableCapacityObserver capacityObserver;
    private final CfFilterObserver cfFilterObserver;
    private final AtomicLong throughputWindowStart = new AtomicLong(System.nanoTime());
    private final java.util.concurrent.atomic.AtomicReference<CfFilterCache> cfFilterCache =
            new java.util.concurrent.atomic.AtomicReference<>(CfFilterCache.EMPTY);
    private int rowBufferCapacity = ROW_BUFFER_BASE_CAPACITY;
    private final RowProcessingContext rowContext = new RowProcessingContext();

    public WalEntryProcessor(PayloadBuilder payloadBuilder,
                             TopicManager topicManager,
                             Producer<byte[], byte[]> producer,
                             H2kConfig config) {
        this.payloadBuilder = payloadBuilder;
        this.topicManager = topicManager;
        this.producer = producer;
        this.config = config;
        this.capacityObserver = TableCapacityObserver.create(config);
        this.cfFilterObserver = CfFilterObserver.create();
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
                        boolean includeWalMeta) {
        final List<Cell> cells = entry.getEdit().getCells();
        final TableName table = entry.getKey().getTablename();
        final H2kConfig.CfFilterSnapshot cfSnapshot = config.describeCfFilter(table);
        final boolean filterConfigured = cfSnapshot.enabled();

        if (skipEmptyEntry(entry, cells, filterConfigured)) {
            return;
        }

        final String topic = topicManager.resolveTopic(table);
        topicManager.ensureTopicIfNeeded(topic);

        final WalMeta walMeta = includeWalMeta ? readWalMeta(entry) : WalMeta.EMPTY;

        ProcessingCounters counters = new ProcessingCounters();
        CfFilterCache cfCache = prepareCfCache(filterConfigured ? cfSnapshot.families() : null);
        boolean filterActive = filterConfigured && !cfCache.isEmpty();

        RowProcessingContext context = createRowProcessingContext(filterActive, cfCache, topic, table, walMeta, sender, counters);

        final ArrayList<Cell> localRowBuffer = prepareRowBuffer(cells.size());
        processWalCells(cells, context, localRowBuffer);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                    table, topic, counters.rowsSent, counters.cellsSent, filterActive, topicManager.ensureEnabled());
        }

        finalizeEntry(table, counters, filterActive, cfSnapshot);
        logThroughput(counters);
    }

    /**
     * Проверяет, переданы ли в записи ячейки, и логирует отладочную информацию при пустом WAL.
     *
     * @param entry          исходная запись WAL
     * @param cells          коллекция ячеек из WALEdit
     * @param filterEnabled  флаг включения фильтра CF
     * @return {@code true}, если запись пустая и обработку нужно завершить
     */
    private boolean skipEmptyEntry(WAL.Entry entry, List<Cell> cells, boolean filterConfigured) {
        if (cells != null && !cells.isEmpty()) {
            return false;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL без ячеек — таблица={}, фильтр_сконфигурирован={}, ensure-включён={}",
                    entry.getKey().getTablename(), filterConfigured, topicManager.ensureEnabled());
        }
        return true;
    }

    /**
     * Формирует контекст обработки строки с учётом настроек фильтрации и метаданных, уменьшая когнитивную
     * сложность основного метода {@link #process(WAL.Entry, BatchSender, boolean)}.
     * Контекст переиспользуется как поле класса: WalEntryProcessor работает в одном потоке репликации,
     * поэтому повторная конфигурация безопасна и устраняет лишние аллокации.
     */
    private RowProcessingContext createRowProcessingContext(boolean filterActive,
                                                            CfFilterCache cfCache,
                                                            String topic,
                                                            TableName table,
                                                            WalMeta walMeta,
                                                            BatchSender sender,
                                                            ProcessingCounters counters) {
        return rowContext.configure(filterActive, cfCache, topic, table, walMeta, sender, counters);
    }

    /**
     * Проходит по ячейкам записи WAL, группируя их по rowkey и отправляя в Kafka через контекст строки.
     */
    private void processWalCells(List<Cell> cells,
                                 RowProcessingContext context,
                                 ArrayList<Cell> rowBuffer) {
        RowKeySlice.Mutable currentSlice = null;
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
            if (currentSlice == null) {
                currentSlice = new RowKeySlice.Mutable(arr, off, len);
            } else {
                currentSlice.reset(arr, off, len);
            }
            prevArr = arr;
            prevOff = off;
            prevLen = len;
        }

        context.flushRow(rowBuffer, currentSlice);
    }

    /**
     * Подготавливает переиспользуемый буфер ячеек под новое сканирование строки: очищает прошлые данные,
     * гарантирует минимально необходимую ёмкость и ведёт счётчики увеличений для диагностики GC-нагрузки.
     *
     * @param expectedCells прогнозируемое количество ячеек в текущем rowkey
     * @return готовый буфер, принадлежащий текущему экземпляру процессора
     */
    private ArrayList<Cell> prepareRowBuffer(int expectedCells) {
        ArrayList<Cell> buffer = this.rowBuffer;
        buffer.clear();
        int capacity = Math.max(ROW_BUFFER_BASE_CAPACITY, expectedCells);
        if (capacity > rowBufferCapacity) {
            rowBufferUpsize.increment();
            rowBufferCapacity = capacity;
        }
        buffer.ensureCapacity(capacity);
        return buffer;
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
     * Контекст обработки строки WAL: хранит настройки фильтра/топика/метаданных для передачи в helper-методы.
     */
    private final class RowProcessingContext {
        private boolean filterActive;
        private CfFilterCache cfCache;
        private String topic;
        private TableName table;
        private WalMeta walMeta;
        private BatchSender sender;
        private ProcessingCounters counters;

        RowProcessingContext configure(boolean filterActive,
                                       CfFilterCache cfCache,
                                       String topic,
                                       TableName table,
                                       WalMeta walMeta,
                                       BatchSender sender,
                                       ProcessingCounters counters) {
            this.filterActive = filterActive;
            this.cfCache = cfCache;
            this.topic = topic;
            this.table = table;
            this.walMeta = walMeta;
            this.sender = sender;
            this.counters = counters;
            return this;
        }

        /**
         * Отправляет накопленную по текущему rowkey партию ячеек, если она прошла фильтр CF.
         * Метод очищает буфер вне зависимости от результата, чтобы подготовиться к следующей строке.
         */
        void flushRow(ArrayList<Cell> rowBuffer, RowKeySlice currentSlice) {
            if (currentSlice == null || rowBuffer.isEmpty()) {
                return;
            }
            final int processedCells = rowBuffer.size();
            counters.cellsSeen += processedCells;
            counters.maxRowCellsSeen = Math.max(counters.maxRowCellsSeen, processedCells);
            if (filterActive && !passCfFilter(rowBuffer, cfCache.families, cfCache.hashes)) {
                resetRowBuffer(rowBuffer, processedCells);
                counters.rowsFiltered++;
                return;
            }
            WalEntryProcessor.this.sendRow(topic, table, walMeta, currentSlice, rowBuffer, sender);
            counters.rowsSent++;
            counters.cellsSent += processedCells;
            counters.maxRowCellsSent = Math.max(counters.maxRowCellsSent, processedCells);
            resetRowBuffer(rowBuffer, processedCells);
        }

        /**
         * Сбрасывает состояние буфера строки после обработки: очищает список и при необходимости
         * усаживает ёмкость до базовой, фиксируя диагностические счётчики внешнего класса.
         */
        private void resetRowBuffer(ArrayList<Cell> rowBuffer, int processedCells) {
            rowBuffer.clear();
            if (processedCells >= ROW_BUFFER_TRIM_THRESHOLD) {
                rowBuffer.trimToSize();
                rowBuffer.ensureCapacity(ROW_BUFFER_BASE_CAPACITY);
                WalEntryProcessor.this.rowBufferTrim.increment();
                WalEntryProcessor.this.rowBufferCapacity = ROW_BUFFER_BASE_CAPACITY;
            }
        }
    }

    /**
     * Сколько раз буфер rowBuffer расширялся сверх базовой ёмкости.
     */
    public long rowBufferUpsizeCount() {
        return rowBufferUpsize.sum();
    }

    /**
     * Сколько раз буфер rowBuffer принудительно усаживался после крупных строк.
     */
    public long rowBufferTrimCount() {
        return rowBufferTrim.sum();
    }

    private CfFilterCache prepareCfCache(byte[][] cfFamilies) {
        if (cfFamilies == null || cfFamilies.length == 0) {
            return CfFilterCache.EMPTY;
        }
        CfFilterCache current = cfFilterCache.get();
        if (current.matches(cfFamilies)) {
            return current;
        }
        CfFilterCache computed = CfFilterCache.build(cfFamilies);
        while (true) {
            if (cfFilterCache.compareAndSet(current, computed)) {
                return computed;
            }
            current = cfFilterCache.get();
            if (current.matches(cfFamilies)) {
                return current;
            }
        }
    }

    private static boolean passCfFilter(List<Cell> cells, byte[][] cfFamilies, int[] cfHashes) {
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
        return passCfFilterN(cells, cfFamilies, cfHashes);
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
    static boolean passCfFilterN(List<Cell> cells, byte[][] cfFamilies, int[] cfHashes) {
        for (Cell c : cells) {
            int cellHash = Bytes.hashCode(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
            for (int i = 0; i < cfFamilies.length; i++) {
                if (cellHash == cfHashes[i]
                        && Bytes.equals(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength(),
                        cfFamilies[i], 0, cfFamilies[i].length)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Кеширует отфильтрованный список CF и их хеши для ускорения повторных проверок. */
    private static final class CfFilterCache {
        private static final CfFilterCache EMPTY = new CfFilterCache(null, null, null);

        final byte[][] sourceRef;
        final byte[][] families;
        final int[] hashes;

        private CfFilterCache(byte[][] sourceRef, byte[][] families, int[] hashes) {
            this.sourceRef = sourceRef;
            this.families = families;
            this.hashes = hashes;
        }

        boolean isEmpty() {
            return families == null || families.length == 0;
        }

        boolean matches(byte[][] candidate) {
            if (candidate == sourceRef) {
                return true;
            }
            return isEmpty() && (candidate == null || candidate.length == 0);
        }

        static CfFilterCache build(byte[][] source) {
            ArrayList<byte[]> sanitized = new ArrayList<>(source.length);
            for (byte[] cf : source) {
                if (cf == null || cf.length == 0) {
                    continue;
                }
                sanitized.add(cf);
            }
            if (sanitized.isEmpty()) {
                return EMPTY;
            }
            byte[][] copy = sanitized.toArray(new byte[0][]);
            Arrays.sort(copy, Bytes.BYTES_COMPARATOR);
            int uniqueCount = 1;
            for (int i = 1; i < copy.length; i++) {
                if (!Bytes.equals(copy[i], copy[i - 1])) {
                    uniqueCount++;
                }
            }
            byte[][] unique = new byte[uniqueCount][];
            int[] hashes = new int[uniqueCount];
            int idx = 0;
            unique[idx] = copy[0];
            hashes[idx] = Bytes.hashCode(copy[0], 0, copy[0].length);
            for (int i = 1; i < copy.length; i++) {
                if (!Bytes.equals(copy[i], copy[i - 1])) {
                    idx++;
                    unique[idx] = copy[i];
                    hashes[idx] = Bytes.hashCode(copy[i], 0, copy[i].length);
                }
            }
            return new CfFilterCache(source, unique, hashes);
        }
    }

    /**
     * Лёгкие счётчики по записи WAL — количество отправленных строк и ячеек.
     * Выделены в отдельный объект, чтобы передавать по ссылке и избегать возврата pair-структур.
     */
    
    /** Счётчики по текущей записи WAL (строки/ячейки/фильтр). */
    private static final class ProcessingCounters {
        int rowsSent;
        int cellsSent;
        int rowsFiltered;
        int cellsSeen;
        int maxRowCellsSeen;
        int maxRowCellsSent;
    }

    /**
     * Обновляет глобальные счётчики и диагностические наблюдатели по завершённой записи WAL.
     */
    private void finalizeEntry(TableName table,
                               ProcessingCounters counters,
                               boolean filterActive,
                               H2kConfig.CfFilterSnapshot cfSnapshot) {
        entriesProcessed.increment();
        long rowsSeen = (long) counters.rowsSent + (long) counters.rowsFiltered;
        if (rowsSeen > 0L) {
            rowsProcessed.add(rowsSeen);
        }
        cellsProcessed.add(counters.cellsSeen);
        if (counters.rowsFiltered > 0) {
            rowsFiltered.add(counters.rowsFiltered);
        }

        if (rowsSeen <= 0L) {
            return;
        }

        int capacityCandidate = (counters.maxRowCellsSent > 0)
                ? counters.maxRowCellsSent
                : counters.maxRowCellsSeen;
        long rowsForCapacity = (counters.rowsSent > 0)
                ? counters.rowsSent
                : rowsSeen;
        if (capacityCandidate > 0) {
            capacityObserver.observe(table, capacityCandidate, rowsForCapacity);
        }
        cfFilterObserver.observe(table, rowsSeen, counters.rowsFiltered, filterActive, cfSnapshot);
    }

    /** Возвращает агрегированные счётчики обработанных записей WAL. */
    public WalMetrics metrics() {
        return new WalMetrics(
                entriesProcessed.sum(),
                rowsProcessed.sum(),
                cellsProcessed.sum(),
                rowsFiltered.sum());
    }

    /** @return суммарное количество обработанных WAL-записей. */
    public long entriesTotal() {
        return entriesProcessed.sum();
    }

    /** @return суммарное количество строк (включая отфильтрованные) с момента старта. */
    public long rowsTotal() {
        return rowsProcessed.sum();
    }

    /** @return суммарное количество ячеек, увиденных на горячем пути. */
    public long cellsTotal() {
        return cellsProcessed.sum();
    }

    /** @return суммарное количество строк, отфильтрованных по CF. */
    public long rowsFilteredTotal() {
        return rowsFiltered.sum();
    }

    /**
     * Агрегирует статистику за период и выводит DEBUG-лог с throughput (строки/ячейки/фильтрация).
     */
    private void logThroughput(ProcessingCounters counters) {
        entriesWindow.increment();
        if (counters.rowsSent > 0) {
            rowsWindow.add(counters.rowsSent);
        }
        if (counters.cellsSent > 0) {
            cellsWindow.add(counters.cellsSent);
        }
        if (counters.rowsFiltered > 0) {
            filteredRowsWindow.add(counters.rowsFiltered);
        }

        long now = System.nanoTime();
        long windowStart = throughputWindowStart.get();
        long elapsed = now - windowStart;
        if (elapsed < THROUGHPUT_LOG_INTERVAL_NS) {
            return;
        }
        if (!throughputWindowStart.compareAndSet(windowStart, now)) {
            return;
        }

        long rowsPreview = rowsWindow.sum();
        long cellsPreview = cellsWindow.sum();
        long filteredPreview = filteredRowsWindow.sum();
        if (rowsPreview == 0L && cellsPreview == 0L && filteredPreview == 0L) {
            return;
        }

        long entries = entriesWindow.sumThenReset();
        long rows = rowsWindow.sumThenReset();
        long cells = cellsWindow.sumThenReset();
        long filteredRows = filteredRowsWindow.sumThenReset();
        if (elapsed <= 0L) {
            return;
        }

        double intervalSeconds = elapsed / 1_000_000_000.0;
        double rowsPerSec = rows / intervalSeconds;
        double cellsPerSec = cells / intervalSeconds;

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Скорость WAL: записей={}, строк={}, строк/с={}, ячеек={}, ячеек/с={}, отфильтровано_строк={}, интервал_мс={}",
                    entries,
                    rows,
                    formatDecimal(rowsPerSec),
                    cells,
                    formatDecimal(cellsPerSec),
                    filteredRows,
                    TimeUnit.NANOSECONDS.toMillis(elapsed));
        }
    }

    private static String formatDecimal(double value) {
        return String.format(Locale.ROOT, "%.1f", value);
    }

    public static final class WalMetrics {
        private final long entries;
        private final long rows;
        private final long cells;
        private final long filteredRows;

        WalMetrics(long entries, long rows, long cells, long filteredRows) {
            this.entries = entries;
            this.rows = rows;
            this.cells = cells;
            this.filteredRows = filteredRows;
        }

        public long entries() { return entries; }

        public long rows() { return rows; }

        public long cells() { return cells; }

        public long filteredRows() { return filteredRows; }
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
        final byte[] valueBytes = payloadBuilder.buildRowPayloadBytes(table, cells, rowKey, walMeta.seq, walMeta.writeTime);
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
