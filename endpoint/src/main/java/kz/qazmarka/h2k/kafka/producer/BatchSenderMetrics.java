package kz.qazmarka.h2k.kafka.producer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Потокобезопасный сборщик метрик для {@link BatchSender}: считает успехи/ошибки "тихих" сбросов,
 * подтверждённые отправки и усредняет задержку flush.
 */
public final class BatchSenderMetrics implements BatchSender.Listener {

    private final LongAdder flushSuccess = new LongAdder();
    private final LongAdder flushFailures = new LongAdder();
    private final LongAdder recordsConfirmed = new LongAdder();
    private final LongAdder latencyTotalMs = new LongAdder();
    private final AtomicLong lastFlushLatencyMs = new AtomicLong();
    private final AtomicLong maxFlushLatencyMs = new AtomicLong();
    private final AtomicLong currentAwaitEvery = new AtomicLong();
    private final AtomicLong configuredAwaitEvery = new AtomicLong();

    /** Обновляет базовый (конфигурационный) awaitEvery для последующего экспорта. */
    public void updateConfiguredAwaitEvery(int awaitEvery) {
        configuredAwaitEvery.set(awaitEvery);
        currentAwaitEvery.compareAndSet(0L, awaitEvery);
    }

    @Override
    public void onFlushSuccess(int processed, long latencyMs, int adaptiveAwaitEvery) {
        flushSuccess.increment();
        recordsConfirmed.add(processed);
        latencyTotalMs.add(latencyMs);
        lastFlushLatencyMs.set(latencyMs);
        maxFlushLatencyMs.accumulateAndGet(latencyMs, Math::max);
        currentAwaitEvery.set(adaptiveAwaitEvery);
    }

    @Override
    public void onFlushFailure() {
        flushFailures.increment();
    }

    public long flushSuccessTotal() {
        return flushSuccess.sum();
    }

    public long flushFailuresTotal() {
        return flushFailures.sum();
    }

    public long recordsConfirmedTotal() {
        return recordsConfirmed.sum();
    }

    public long lastFlushLatencyMs() {
        return lastFlushLatencyMs.get();
    }

    public long maxFlushLatencyMs() {
        return maxFlushLatencyMs.get();
    }

    public long avgFlushLatencyMs() {
        long success = flushSuccess.sum();
        if (success == 0L) {
            return 0L;
        }
        long total = latencyTotalMs.sum();
        if (total <= 0L) {
            return 0L;
        }
        return Math.round((double) total / (double) success);
    }

    public long currentAwaitEvery() {
        long current = currentAwaitEvery.get();
        if (current == 0L) {
            return configuredAwaitEvery.get();
        }
        return current;
    }

    public long configuredAwaitEvery() {
        return configuredAwaitEvery.get();
    }
}

