package kz.qazmarka.h2k.endpoint.processing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private final TableOptionsObserver tableOptionsObserver;
    private final SaltUsageObserver saltObserver;
    private final AtomicLong throughputWindowStart = new AtomicLong(System.nanoTime());
    private final java.util.concurrent.atomic.AtomicReference<CfFilterCache> cfFilterCache =
            new java.util.concurrent.atomic.AtomicReference<>(CfFilterCache.EMPTY);
    private int rowBufferCapacity = ROW_BUFFER_BASE_CAPACITY;

    public WalEntryProcessor(PayloadBuilder payloadBuilder,
                             TopicManager topicManager,
                             Producer<byte[], byte[]> producer,
                             H2kConfig config) {
        this.payloadBuilder = payloadBuilder;
        this.topicManager = topicManager;
        this.producer = producer;
        this.config = config;
        if (config.isObserversEnabled()) {
            this.capacityObserver = TableCapacityObserver.create(config);
            this.cfFilterObserver = CfFilterObserver.create();
            this.tableOptionsObserver = TableOptionsObserver.create();
            this.saltObserver = SaltUsageObserver.create();
        } else {
            this.capacityObserver = TableCapacityObserver.disabled();
            this.cfFilterObserver = CfFilterObserver.disabled();
            this.tableOptionsObserver = TableOptionsObserver.disabled();
            this.saltObserver = SaltUsageObserver.disabled();
        }
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
                        boolean includeWalMeta)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<Cell> cells = entry.getEdit().getCells();
        TableName table = entry.getKey().getTablename();
        H2kConfig.TableOptionsSnapshot tableOptions = config.describeTableOptions(table);
        H2kConfig.CfFilterSnapshot cfSnapshot = config.describeCfFilter(table);
        boolean filterConfigured = cfSnapshot.enabled();

        if (skipEmptyEntry(entry, cells, filterConfigured)) {
            return;
        }

        String topic = topicManager.resolveTopic(table);
        topicManager.ensureTopicIfNeeded(topic);

        WalMeta walMeta = includeWalMeta ? readWalMeta(entry) : WalMeta.EMPTY;

        ProcessingCounters counters = new ProcessingCounters();
        CfFilterCache cfCache = prepareCfCache(filterConfigured ? cfSnapshot.families() : null);
        boolean filterActive = filterConfigured && !cfCache.isEmpty();

    RowTask rowTask = new RowTask(topic, table, walMeta, sender, counters, tableOptions, cfCache);
    ArrayList<Cell> rowBuf = prepareRowBuffer(cells.size());
        RowKeySlice.Mutable rowKey = null;
        byte[] currentArr = null;
        int currentOff = -1;
        int currentLen = -1;

        for (Cell cell : cells) {
            byte[] rowArray = cell.getRowArray();
            int rowOffset = cell.getRowOffset();
            int rowLength = cell.getRowLength();
        boolean sameRow = rowKey != null
            && Objects.equals(rowArray, currentArr)
                    && rowOffset == currentOff
                    && rowLength == currentLen;

            if (!sameRow) {
        flushRow(rowKey, rowBuf, rowTask);
                rowBuf.add(cell);
                if (rowKey == null) {
                    rowKey = new RowKeySlice.Mutable(rowArray, rowOffset, rowLength);
                } else {
                    rowKey.reset(rowArray, rowOffset, rowLength);
                }
                currentArr = rowArray;
                currentOff = rowOffset;
                currentLen = rowLength;
                continue;
            }

            rowBuf.add(cell);
        }

    flushRow(rowKey, rowBuf, rowTask);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                    table, topic, counters.rowsSent, counters.cellsSent, filterActive, topicManager.ensureEnabled());
        }

        finalizeEntry(table, counters, filterActive, cfSnapshot, tableOptions);
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
     * Проходит по ячейкам записи WAL, группируя их по rowkey и отправляя в Kafka через контекст строки.
     */
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

    private void flushRow(RowKeySlice.Mutable rowKey,
                          ArrayList<Cell> rowBuffer,
                          RowTask rowTask)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (rowKey == null || rowBuffer.isEmpty()) {
            return;
        }
        rowTask.flush(rowKey, rowBuffer);
    }

    /**
     * Очищает буфер строки, при необходимости усаживает ёмкость и обновляет диагностические счётчики.
     * Используется в обработке и тестах для проверки поведения trim.
     */
    void resetRowBufferForTest(ArrayList<Cell> rowBuffer, int processedCells) {
        resetRowBuffer(rowBuffer, processedCells);
    }

    /** Возвращает порог усадки rowBuffer (используется в модульных тестах). */
    static int rowBufferTrimThresholdForTest() {
        return ROW_BUFFER_TRIM_THRESHOLD;
    }

    private void resetRowBuffer(ArrayList<Cell> rowBuffer, int processedCells) {
        rowBuffer.clear();
        if (processedCells >= ROW_BUFFER_TRIM_THRESHOLD) {
            rowBuffer.trimToSize();
            rowBuffer.ensureCapacity(ROW_BUFFER_BASE_CAPACITY);
            rowBufferTrim.increment();
            rowBufferCapacity = ROW_BUFFER_BASE_CAPACITY;
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
            return Objects.equals(candidate, sourceRef)
                    || (isEmpty() && (candidate == null || candidate.length == 0));
        }

        boolean allows(List<Cell> cells) {
            if (isEmpty()) {
                return true;
            }
            if (cells.isEmpty()) {
                return false;
            }
            int familyCount = families.length;
            if (familyCount == 1) {
                return containsFamily(cells, families[0]);
            }
            if (familyCount == 2) {
                return containsFamily(cells, families[0]) || containsFamily(cells, families[1]);
            }
            return containsAnyFamily(cells);
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

        private static boolean containsFamily(List<Cell> cells, byte[] family) {
            for (Cell cell : cells) {
                if (CellUtil.matchingFamily(cell, family)) {
                    return true;
                }
            }
            return false;
        }

        private boolean containsAnyFamily(List<Cell> cells) {
            for (Cell cell : cells) {
                int cellHash = Bytes.hashCode(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                for (int i = 0; i < families.length; i++) {
                    if (cellHash == hashes[i]
                            && Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                            families[i], 0, families[i].length)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /** Упрощённые проверки CF используются в модульных тестах. */
    static boolean passCfFilter1(List<Cell> cells, byte[] cf0) {
        for (Cell cell : cells) {
            if (CellUtil.matchingFamily(cell, cf0)) {
                return true;
            }
        }
        return false;
    }

    static boolean passCfFilter2(List<Cell> cells, byte[] cf0, byte[] cf1) {
        for (Cell cell : cells) {
            if (CellUtil.matchingFamily(cell, cf0) || CellUtil.matchingFamily(cell, cf1)) {
                return true;
            }
        }
        return false;
    }

    static boolean passCfFilterN(List<Cell> cells, byte[][] cfFamilies, int[] cfHashes) {
        if (cfFamilies == null || cfFamilies.length == 0) {
            return true;
        }
        if (cells.isEmpty()) {
            return false;
        }
        for (Cell cell : cells) {
            int cellHash = Bytes.hashCode(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            for (int i = 0; i < cfFamilies.length; i++) {
                if (cellHash == cfHashes[i]
                        && Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                        cfFamilies[i], 0, cfFamilies[i].length)) {
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
    
    /** Счётчики по текущей записи WAL (строки/ячейки/фильтр). */
    private static final class ProcessingCounters {
        int rowsSent;
        int cellsSent;
        int rowsFiltered;
        int cellsSeen;
        int maxRowCellsSeen;
        int maxRowCellsSent;
    }

    private final class RowTask {
        private final String topic;
        private final TableName table;
        private final WalMeta walMeta;
        private final BatchSender sender;
        private final ProcessingCounters counters;
        private final H2kConfig.TableOptionsSnapshot tableOptions;
        private final CfFilterCache cfFilter;

        RowTask(String topic,
                TableName table,
                WalMeta walMeta,
                BatchSender sender,
                ProcessingCounters counters,
                H2kConfig.TableOptionsSnapshot tableOptions,
                CfFilterCache cfFilter) {
            this.topic = topic;
            this.table = table;
            this.walMeta = walMeta;
            this.sender = sender;
            this.counters = counters;
            this.tableOptions = tableOptions;
            this.cfFilter = cfFilter;
        }

        void flush(RowKeySlice.Mutable rowKey, ArrayList<Cell> rowBuffer)
                throws InterruptedException, ExecutionException, TimeoutException {
            if (tableOptions != null) {
                saltObserver.observeRow(table, tableOptions, rowKey.getLength());
            }
            int cellsInRow = rowBuffer.size();
            counters.cellsSeen += cellsInRow;
            counters.maxRowCellsSeen = Math.max(counters.maxRowCellsSeen, cellsInRow);
            if (!cfFilter.isEmpty() && !cfFilter.allows(rowBuffer)) {
                counters.rowsFiltered++;
                resetRowBuffer(rowBuffer, cellsInRow);
                return;
            }
            sendRow(topic, table, walMeta, rowKey, rowBuffer, sender);
            counters.rowsSent++;
            counters.cellsSent += cellsInRow;
            counters.maxRowCellsSent = Math.max(counters.maxRowCellsSent, cellsInRow);
            resetRowBuffer(rowBuffer, cellsInRow);
        }
    }

    /**
     * Обновляет глобальные счётчики и диагностические наблюдатели по завершённой записи WAL.
     */
    private void finalizeEntry(TableName table,
                               ProcessingCounters counters,
                               boolean filterActive,
                               H2kConfig.CfFilterSnapshot cfSnapshot,
                               H2kConfig.TableOptionsSnapshot tableOptions) {
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
        tableOptionsObserver.observe(table, tableOptions, cfSnapshot);
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
        addIfPositive(rowsWindow, counters.rowsSent);
        addIfPositive(cellsWindow, counters.cellsSent);
        addIfPositive(filteredRowsWindow, counters.rowsFiltered);

        long now = System.nanoTime();
        long windowStart = throughputWindowStart.get();
        long elapsed = now - windowStart;
        if (!windowReady(elapsed, windowStart, now)) {
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

        emitThroughput(elapsed, entries, rows, cells, filteredRows);
    }

    private void emitThroughput(long elapsedNs,
                                long entries,
                                long rows,
                                long cells,
                                long filteredRows) {
        if (elapsedNs <= 0L) {
            return;
        }
        double intervalSeconds = elapsedNs / 1_000_000_000.0;
        if (intervalSeconds <= 0D) {
            return;
        }
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
                    TimeUnit.NANOSECONDS.toMillis(elapsedNs));
        }
    }

    private static void addIfPositive(LongAdder target, int delta) {
        if (delta > 0) {
            target.add(delta);
        }
    }

    private boolean windowReady(long elapsed, long windowStart, long now) {
        return elapsed >= THROUGHPUT_LOG_INTERVAL_NS
                && throughputWindowStart.compareAndSet(windowStart, now);
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
                 BatchSender sender)
            throws InterruptedException, ExecutionException, TimeoutException {
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
