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
import org.apache.hadoop.hbase.util.Bytes;
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
public final class WalEntryProcessor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WalEntryProcessor.class);
    /**
     * Базовая ёмкость буфера строк: соответствует типичному числу ячеек для компактных WAL-записей.
     */
    private static final int ROW_BUFFER_BASE_CAPACITY = 32;
    /**
     * Порог усадки буфера. Если за одну строку обработано столько или больше ячеек,
     * буфер сбрасывается до базовой ёмкости, чтобы не удерживать крупные массивы в горячем пути.
     */
    private static final int ROW_BUFFER_TRIM_THRESHOLD = 4_096;

    private final TopicManager topicManager;
    private final PayloadBuilder payloadBuilder;
    private final TableMetadataView metadata;
    private final ArrayList<Cell> rowBuffer = new ArrayList<>(ROW_BUFFER_BASE_CAPACITY);
    private final LongAdder rowBufferUpsize = new LongAdder();
    private final LongAdder rowBufferTrim = new LongAdder();
    private final WalObserverHub observers;
    private final WalCounterService counterService = new WalCounterService();
    private final WalRowDispatcher rowDispatcher;
    private final WalRowProcessor rowProcessor;
    private final RowAggregator rowAggregator = new RowAggregator();
    private final java.util.concurrent.atomic.AtomicReference<WalCfFilterCache> cfFilterCache =
            new java.util.concurrent.atomic.AtomicReference<>(WalCfFilterCache.EMPTY);
    private int rowBufferCapacity = ROW_BUFFER_BASE_CAPACITY;

    public WalEntryProcessor(PayloadBuilder payloadBuilder,
                             TopicManager topicManager,
                             Producer<RowKeySlice, byte[]> producer,
                             TableMetadataView metadata) {
        this.topicManager = topicManager;
        this.payloadBuilder = Objects.requireNonNull(payloadBuilder, "payloadBuilder");
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
                        BatchSender sender)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<Cell> cells = entry.getEdit().getCells();
        TableName table = entry.getKey().getTablename();
        TableOptionsSnapshot tableOptions = metadata.describeTableOptions(table);
        CfFilterSnapshot cfSnapshot = metadata.describeCfFilter(table);
        boolean filterConfigured = cfSnapshot.enabled();

        if (skipEmptyEntry(entry, cells, filterConfigured)) {
            return;
        }

        EntryContext context = prepareEntryContext(entry, sender, table, tableOptions, cfSnapshot, filterConfigured);
        ArrayList<Cell> localRowBuffer = prepareRowBuffer(cells.size(), context.tableOptions.capacityHint());
        processEntryCells(cells, context, localRowBuffer);
        logEntrySummary(context);

        finalizeEntry(context.table, context.counters, context.filterActive, context.cfSnapshot, context.tableOptions);
        counterService.logThroughput(context.counters, LOG);
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
    private ArrayList<Cell> prepareRowBuffer(int totalCells, int capacityHint) {
        ArrayList<Cell> buffer = this.rowBuffer;
        buffer.clear();
        int preferredUpperBound = capacityHint > 0
                ? Math.max(capacityHint, ROW_BUFFER_BASE_CAPACITY)
                : ROW_BUFFER_TRIM_THRESHOLD;
        int target = Math.min(totalCells, preferredUpperBound);
        int capacity = Math.max(ROW_BUFFER_BASE_CAPACITY, target);
        if (capacity > rowBufferCapacity) {
            rowBufferUpsize.increment();
            rowBufferCapacity = capacity;
        }
        buffer.ensureCapacity(capacity);
        return buffer;
    }

    private EntryContext prepareEntryContext(WAL.Entry entry,
                                             BatchSender sender,
                                             TableName table,
                                             TableOptionsSnapshot tableOptions,
                                             CfFilterSnapshot cfSnapshot,
                                             boolean filterConfigured) {
        String topic = topicManager.resolveTopic(table);
        topicManager.ensureTopicIfNeeded(topic);
        WalMeta walMeta = readWalMeta(entry);
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

        return new EntryContext(
                table,
                topic,
                tableOptions,
                cfSnapshot,
                counters,
                filterActive,
                rowContext);
    }

    private void processEntryCells(List<Cell> cells, EntryContext context, ArrayList<Cell> rowBuffer)
            throws InterruptedException, ExecutionException, TimeoutException {
        rowAggregator.aggregate(cells, context, rowBuffer);
    }

    private void logEntrySummary(EntryContext context) {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        WalCounterService.EntryCounters counters = context.counters;
        LOG.debug("Репликация: запись WAL обработана: таблица={}, топик={}, строк отправлено={}, ячеек отправлено={}, фильтр={}, ensure-включён={}",
                context.table, context.topic, counters.rowsSent, counters.cellsSent, context.filterActive, topicManager.ensureEnabled());
    }

    private static final class EntryContext {
        final TableName table;
        final String topic;
        final TableOptionsSnapshot tableOptions;
        final CfFilterSnapshot cfSnapshot;
        final WalCounterService.EntryCounters counters;
        final boolean filterActive;
        final WalRowProcessor.RowContext rowContext;

        EntryContext(TableName table,
                     String topic,
                     TableOptionsSnapshot tableOptions,
                     CfFilterSnapshot cfSnapshot,
                     WalCounterService.EntryCounters counters,
                     boolean filterActive,
                     WalRowProcessor.RowContext rowContext) {
            this.table = table;
            this.topic = topic;
            this.tableOptions = tableOptions;
            this.cfSnapshot = cfSnapshot;
            this.counters = counters;
            this.filterActive = filterActive;
            this.rowContext = rowContext;
        }
    }

    private static final class RowKeyState {
        private RowKeySlice.Mutable rowKey;
        private byte[] currentArray;
        private int currentOffset = -1;
        private int currentLength = -1;

        RowKeySlice.Mutable rowKey() {
            return rowKey;
        }

        boolean sameRow(Cell cell) {
            if (rowKey == null) {
                return false;
            }
            if (currentArray == cell.getRowArray()
                    && currentOffset == cell.getRowOffset()
                    && currentLength == cell.getRowLength()) {
                return true;
            }
            if (currentLength != cell.getRowLength()) {
                return false;
            }
            if (Bytes.equals(rowKey.getArray(), rowKey.getOffset(), currentLength,
                    cell.getRowArray(), cell.getRowOffset(), currentLength)) {
                currentArray = cell.getRowArray();
                currentOffset = cell.getRowOffset();
                return true;
            }
            return false;
        }

        void startRow(Cell cell, ArrayList<Cell> buffer) {
            buffer.add(cell);
            byte[] rowArray = cell.getRowArray();
            int rowOffset = cell.getRowOffset();
            int rowLength = cell.getRowLength();
            if (rowKey == null) {
                rowKey = new RowKeySlice.Mutable(rowArray, rowOffset, rowLength);
            } else {
                rowKey.reset(rowArray, rowOffset, rowLength);
            }
            currentArray = rowArray;
            currentOffset = rowOffset;
            currentLength = rowLength;
        }

        void clear() {
            currentArray = null;
            currentOffset = -1;
            currentLength = -1;
        }
    }

    private final class RowAggregator {
        private final RowKeyState keyState = new RowKeyState();

        void aggregate(List<Cell> cells,
                       EntryContext context,
                       ArrayList<Cell> rowBuffer)
                throws InterruptedException, ExecutionException, TimeoutException {
            if (cells == null || cells.isEmpty()) {
                keyState.clear();
                rowBuffer.clear();
                return;
            }
            for (Cell cell : cells) {
                if (!keyState.sameRow(cell)) {
                    flushCurrentRow(context, rowBuffer);
                    keyState.startRow(cell, rowBuffer);
                } else {
                    rowBuffer.add(cell);
                }
            }
            flushCurrentRow(context, rowBuffer);
        }

        private void flushCurrentRow(EntryContext context,
                                     ArrayList<Cell> rowBuffer)
                throws InterruptedException, ExecutionException, TimeoutException {
            RowKeySlice.Mutable currentKey = keyState.rowKey();
            if (currentKey == null || rowBuffer.isEmpty()) {
                return;
            }
            int processedCells = rowBuffer.size();
            rowProcessor.processRow(currentKey, rowBuffer, context.rowContext);
            resetBuffer(rowBuffer, processedCells);
            keyState.clear();
        }

        private void resetBuffer(ArrayList<Cell> buffer, int processedCells) {
            buffer.clear();
            if (processedCells >= ROW_BUFFER_TRIM_THRESHOLD) {
                buffer.trimToSize();
                buffer.ensureCapacity(ROW_BUFFER_BASE_CAPACITY);
                WalEntryProcessor.this.rowBufferTrim.increment();
                WalEntryProcessor.this.rowBufferCapacity = ROW_BUFFER_BASE_CAPACITY;
            }
        }
    }

    /**
     * @return сколько раз буфер rowBuffer расширялся сверх базовой ёмкости.
     */
    public long rowBufferUpsizeCount() {
        return rowBufferUpsize.sum();
    }

    /**
     * @return сколько раз буфер rowBuffer принудительно усаживался после строк, превышающих порог
     *         {@value #ROW_BUFFER_TRIM_THRESHOLD}.
     */
    public long rowBufferTrimCount() {
        return rowBufferTrim.sum();
    }

    /**
     * Освобождает ресурсы, связанные с построением payload (в частности, фоновые ретраи Schema Registry).
     * Метод безопасно вызывается при остановке {@link KafkaReplicationEndpoint}.
     */
    @Override
    public void close() {
        try {
            payloadBuilder.close();
        } catch (Exception ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("WalEntryProcessor: ошибка при закрытии PayloadBuilder (игнорируется)", ex);
            }
        }
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
