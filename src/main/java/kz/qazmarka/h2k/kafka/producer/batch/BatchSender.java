package kz.qazmarka.h2k.kafka.producer.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Минимальный буфер отправок Kafka: накапливает {@link Future} и блокирующе
 * дожидается подтверждений, когда число накопленных элементов достигает порога.
 *
 * Безопасность потоков: экземпляр не потокобезопасен.
 */
public final class BatchSender implements AutoCloseable {

    private final int batchSize;
    private final int timeoutMs;
    private final ArrayList<Future<RecordMetadata>> pending;

    public BatchSender(int batchSize, int timeoutMs) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize должен быть > 0");
        }
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs должен быть > 0");
        }
        this.batchSize = batchSize;
        this.timeoutMs = timeoutMs;
        this.pending = new ArrayList<>(batchSize);
    }

    /**
     * Добавляет отправку в буфер. Возвращает {@code true}, если после добавления
     * достигнут или превышен порог и вызывающему коду имеет смысл вызвать {@link #flush()}.
     */
    public void add(Future<RecordMetadata> future)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (future == null) {
            return;
        }
        pending.add(future);
        if (pending.size() >= batchSize) {
            flush();
        }
    }

    /**
     * Добавляет набор отправок, вызывая {@link #flush()} сразу после достижения порога.
     */
    public void addAll(Collection<? extends Future<RecordMetadata>> futures)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (futures == null || futures.isEmpty()) {
            return;
        }
        for (Future<RecordMetadata> f : futures) {
            add(f);
        }
    }

    /**
     * Блокирующе дожидается подтверждений всех накопленных отправок.
     */
    public void flush() throws InterruptedException, ExecutionException, TimeoutException {
        if (pending.isEmpty()) {
            return;
        }
        waitAll(pending, timeoutMs);
        pending.clear();
    }

    public boolean hasPending() {
        return !pending.isEmpty();
    }

    public int pendingCount() {
        return pending.size();
    }

    public int batchSize() {
        return batchSize;
    }

    public int timeoutMs() {
        return timeoutMs;
    }

    private static void waitAll(List<Future<RecordMetadata>> futures, int timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        final long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        for (Future<RecordMetadata> future : futures) {
            if (future != null) {
                awaitOne(future, deadlineNs);
            }
        }
    }

    private static void awaitOne(Future<RecordMetadata> future, long deadlineNs)
            throws InterruptedException, ExecutionException, TimeoutException {
        long remaining = deadlineNs - System.nanoTime();
        if (remaining <= 0L) {
            throw new TimeoutException("Таймаут ожидания подтверждений от Kafka");
        }
        future.get(remaining, TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() throws InterruptedException, ExecutionException, TimeoutException {
        flush();
    }
}
