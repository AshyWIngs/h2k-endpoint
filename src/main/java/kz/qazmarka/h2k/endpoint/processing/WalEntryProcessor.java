package kz.qazmarka.h2k.endpoint.processing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.TableMetadataView;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
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
    private static final int ROW_BUFFER_BASE_CAPACITY = 32;
    private static final int ROW_BUFFER_TRIM_THRESHOLD = 4_096;

    private final TopicManager topicManager;
    private final TableMetadataView metadata;
    private final ArrayList<Cell> rowBuffer = new ArrayList<>(ROW_BUFFER_BASE_CAPACITY);
    private final LongAdder rowBufferUpsize = new LongAdder();
    private final LongAdder rowBufferTrim = new LongAdder();
    private final WalObserverHub observers;
    private final WalCounterService counterService = new WalCounterService();
    private final WalRowDispatcher rowDispatcher;
    private final WalRowProcessor rowProcessor;
    private final java.util.concurrent.atomic.AtomicReference<WalCfFilterCache> cfFilterCache =
            new java.util.concurrent.atomic.AtomicReference<>(WalCfFilterCache.EMPTY);
    private int rowBufferCapacity = ROW_BUFFER_BASE_CAPACITY;

    public WalEntryProcessor(PayloadBuilder payloadBuilder,
                             TopicManager topicManager,
                             Producer<byte[], byte[]> producer,
                             TableMetadataView metadata) {
        this.topicManager = topicManager;
        this.metadata = Objects.requireNonNull(metadata, "метаданные таблиц");
        this.rowDispatcher = new WalRowDispatcher(payloadBuilder, producer);
        this.observers = WalObserverHub.create(metadata);
        this.rowProcessor = new WalRowProcessor(rowDispatcher, observers);
    }

    /**
     * Конвертирует одну запись WAL в набор Kafka-сообщений: группирует по rowkey, опционально фильтрует
     * по CF, собирает payload и добавляет futures в переданный {@link BatchSender}.
     */
    public void process(WAL.Entry entry,
                        BatchSender sender,
                        boolean includeWalMeta)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<Cell> cells = entry.getEdit().getCells();
        TableName table = entry.getKey().getTablename();
        TableOptionsSnapshot tableOptions = metadata.describeTableOptions(table);
        CfFilterSnapshot cfSnapshot = metadata.describeCfFilter(table);
        boolean filterConfigured = cfSnapshot.enabled();

        if (skipEmptyEntry(entry, cells, filterConfigured)) {
            return;
        }

        String topic = topicManager.resolveTopic(table);
        topicManager.ensureTopicIfNeeded(topic);

        WalMeta walMeta = includeWalMeta ? readWalMeta(entry) : WalMeta.EMPTY;

        WalCounterService.EntryCounters counters = counterService.newEntryCounters();
        WalCfFilterCache cfCache = prepareCfCache(filterConfigured ? cfSnapshot.families() : null);
        boolean filterActive = filterConfigured && !cfCache.isEmpty();

        WalRowProcessor.RowContext rowContext = new WalRowProcessor.RowContext(
                topic,
                table,
                walMeta,
                sender,
                tableOptions,
                cfCache,
                counters);
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
                flushRow(rowKey, rowBuf, rowContext);
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

        flushRow(rowKey, rowBuf, rowContext);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                    table, topic, counters.rowsSent, counters.cellsSent, filterActive, topicManager.ensureEnabled());
        }

        finalizeEntry(table, counters, filterActive, cfSnapshot, tableOptions);
        counterService.logThroughput(counters, LOG);
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
                          WalRowProcessor.RowContext rowContext)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (rowKey == null || rowBuffer.isEmpty()) {
            return;
        }
        int processedCells = rowBuffer.size();
        rowProcessor.processRow(rowKey, rowBuffer, rowContext);
        resetRowBuffer(rowBuffer, processedCells);
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

    private WalCfFilterCache prepareCfCache(byte[][] cfFamilies) {
        if (cfFamilies == null || cfFamilies.length == 0) {
            return WalCfFilterCache.EMPTY;
        }
        WalCfFilterCache current = cfFilterCache.get();
        if (current.matches(cfFamilies)) {
            return current;
        }
        WalCfFilterCache computed = WalCfFilterCache.build(cfFamilies);
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

    /**
     * Обновляет глобальные счётчики и диагностические наблюдатели по завершённой записи WAL.
     */
    private void finalizeEntry(TableName table,
                               WalCounterService.EntryCounters counters,
                               boolean filterActive,
                               CfFilterSnapshot cfSnapshot,
                               TableOptionsSnapshot tableOptions) {
        WalCounterService.EntrySummary summary = counterService.completeEntry(counters);
        observers.finalizeEntry(table, summary, filterActive, cfSnapshot, tableOptions);
    }

    /** Возвращает агрегированные счётчики обработанных записей WAL. */
    public WalMetrics metrics() {
        WalCounterService.MetricsSnapshot snapshot = counterService.snapshot();
        return new WalMetrics(
                snapshot.entries,
                snapshot.rows,
                snapshot.cells,
                snapshot.filteredRows);
    }

    /** @return суммарное количество обработанных WAL-записей. */
    public long entriesTotal() {
        return counterService.entriesTotal();
    }

    /** @return суммарное количество строк (включая отфильтрованные) с момента старта. */
    public long rowsTotal() {
        return counterService.rowsTotal();
    }

    /** @return суммарное количество ячеек, увиденных на горячем пути. */
    public long cellsTotal() {
        return counterService.cellsTotal();
    }

    /** @return суммарное количество строк, отфильтрованных по CF. */
    public long rowsFilteredTotal() {
        return counterService.rowsFilteredTotal();
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
        rowDispatcher.dispatch(topic, table, walMeta, rowKey, cells, sender);
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
