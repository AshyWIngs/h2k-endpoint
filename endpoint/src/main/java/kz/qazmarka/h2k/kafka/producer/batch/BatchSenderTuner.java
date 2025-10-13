package kz.qazmarka.h2k.kafka.producer.batch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Управляет базовым {@code awaitEvery} между репликациями, опираясь на метрики {@link BatchSenderMetrics}.
 * Логика проста: после каждого успешного {@link BatchSender} мы измеряем задержку сброса.
 * Если задержка стабильно растёт — аккуратно снижаем порог, чтобы чаще ждать подтверждений.
 * Если задержка мала — даём себе право постепенно увеличивать порог.
 * Решения принимаются не чаще заданного интервала «охлаждения», чтобы избежать дрожания.
 */
public final class BatchSenderTuner {

    private static final Logger LOG = LoggerFactory.getLogger(BatchSenderTuner.class);

    private final boolean enabled;
    private final int minAwaitEvery;
    private final int maxAwaitEvery;
    private final long highLatencyMs;
    private final long lowLatencyMs;
    private final long cooldownNs;

    private final AtomicInteger recommendedAwait = new AtomicInteger();
    private final AtomicLong lastDecisionAtNs = new AtomicLong();
    private final AtomicLong decisionsTotal = new AtomicLong();

    private volatile long lastObservedLatencyMs;
    private volatile int lastObservedAwait;
    private volatile boolean lastDecisionIncrease;

    public BatchSenderTuner(boolean enabled,
                            int minAwaitEvery,
                            int maxAwaitEvery,
                            long highLatencyMs,
                            long lowLatencyMs,
                            long cooldownMs) {
        this.enabled = enabled && minAwaitEvery > 0 && maxAwaitEvery >= minAwaitEvery;
        this.minAwaitEvery = Math.max(1, minAwaitEvery);
        this.maxAwaitEvery = Math.max(this.minAwaitEvery, maxAwaitEvery);
        this.highLatencyMs = Math.max(1L, highLatencyMs);
        long low = Math.max(1L, Math.min(lowLatencyMs, this.highLatencyMs - 1L));
        this.lowLatencyMs = low;
        long cooldown = Math.max(1_000L, cooldownMs);
        this.cooldownNs = TimeUnit.MILLISECONDS.toNanos(cooldown);
    }

    /**
     * Возвращает базовый {@code awaitEvery}, который следует использовать для следующей репликации.
     * При первом вызове возвращает исходное значение (без изменений).
     */
    public int selectAwaitEvery(int configuredAwaitEvery, BatchSenderMetrics metrics) {
        if (!enabled) {
            return configuredAwaitEvery;
        }
        int effectiveConfigured = clamp(configuredAwaitEvery);
        int observed = clamp((int) metrics.currentAwaitEvery());
        if (observed <= 0) {
            observed = effectiveConfigured;
        }
        lastObservedAwait = observed;
        recommendedAwait.compareAndSet(0, observed);
        int current = recommendedAwait.get();
        if (current <= 0) {
            current = observed;
        }
        return clamp(current);
    }

    /**
     * Обновляет рекомендации после завершения репликации.
     * @param metrics метрики текущего {@link BatchSender}
     * @param success {@code true}, если репликация завершилась без ошибок ожидания
     */
    public void afterBatch(BatchSenderMetrics metrics, boolean success) {
        if (!enabled) {
            return;
        }
        if (!success) {
            handleFailure(metrics);
            return;
        }
        int observed = clamp((int) metrics.currentAwaitEvery());
        if (observed <= 0) {
            observed = clamp((int) metrics.configuredAwaitEvery());
        }
        lastObservedAwait = observed;
        long latency = Math.max(metrics.avgFlushLatencyMs(), metrics.lastFlushLatencyMs());
        lastObservedLatencyMs = latency;
        if (latency <= 0L) {
            recommendedAwait.compareAndSet(0, observed);
            return;
        }
        if (!cooldownElapsed()) {
            recommendedAwait.compareAndSet(0, observed);
            return;
        }
        int candidate = observed;
        String reason = null;
        if (latency > highLatencyMs && observed > minAwaitEvery) {
            int decrease = Math.max(1, observed / 4);
            candidate = Math.max(minAwaitEvery, observed - decrease);
            reason = "latency-high";
        } else if (latency < lowLatencyMs && observed < maxAwaitEvery) {
            int increase = Math.max(1, Math.max(1, observed / 5));
            candidate = Math.min(maxAwaitEvery, observed + increase);
            reason = "latency-low";
        }
        if (reason != null && candidate != observed) {
            recommendedAwait.set(candidate);
            lastDecisionAtNs.set(System.nanoTime());
            decisionsTotal.incrementAndGet();
            lastDecisionIncrease = candidate > observed;
            if (LOG.isInfoEnabled()) {
                LOG.info("BatchSenderAutoTune: адаптирую awaitEvery {} → {} (latency={} мс, причина={})",
                        observed, candidate, latency, reason);
            }
        } else {
            recommendedAwait.compareAndSet(0, observed);
        }
    }

    public long recommendedAwaitEvery() {
        if (!enabled) {
            return 0L;
        }
        int current = recommendedAwait.get();
        if (current <= 0) {
            current = lastObservedAwait > 0 ? clamp(lastObservedAwait) : minAwaitEvery;
        }
        return clamp(current);
    }

    public long decisionsTotal() {
        return decisionsTotal.get();
    }

    public long cooldownRemainingMs() {
        if (!enabled) {
            return 0L;
        }
        long last = lastDecisionAtNs.get();
        if (last == 0L) {
            return 0L;
        }
        long elapsed = System.nanoTime() - last;
        long remainingNs = cooldownNs - elapsed;
        if (remainingNs <= 0L) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(remainingNs);
    }

    public long lastObservedLatencyMs() {
        return lastObservedLatencyMs;
    }

    public long lastObservedAwaitEvery() {
        return lastObservedAwait;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int lastObservedAwait() {
        return lastObservedAwait;
    }

    public boolean lastDecisionWasIncrease() {
        return lastDecisionIncrease;
    }

    private boolean cooldownElapsed() {
        long last = lastDecisionAtNs.get();
        if (last == 0L) {
            return true;
        }
        long elapsed = System.nanoTime() - last;
        return elapsed >= cooldownNs;
    }

    /**
     * Обработка неуспешного ожидания подтверждений: мгновенно снижает порог до минимального,
     * увеличивает счётчики решений и фиксирует информацию для дальнейшего анализа.
     *
     * @param metrics агрегированные метрики текущего {@link BatchSender}
     */
    private void handleFailure(BatchSenderMetrics metrics) {
        int observed = clamp((int) metrics.currentAwaitEvery());
        if (observed <= 0) {
            observed = clamp((int) metrics.configuredAwaitEvery());
        }
        lastObservedAwait = observed;
        lastObservedLatencyMs = metrics.lastFlushLatencyMs();
        recommendedAwait.set(minAwaitEvery);
        lastDecisionAtNs.set(System.nanoTime());
        decisionsTotal.incrementAndGet();
        lastDecisionIncrease = false;
        logFailure(metrics.failureStreak(), observed);
    }

    /**
     * Выводит диагностические WARN‑сообщения, помогая оператору увидеть затяжную серию неуспехов.
     * Сообщения появляются на первой ошибке и далее на степенях двойки, чтобы не «шуметь» в логах.
     */
    private void logFailure(long streak, int observed) {
        if (!LOG.isWarnEnabled()) {
            return;
        }
        if (streak <= 0) {
            LOG.warn("BatchSenderAutoTune: ошибка ожидания подтверждений Kafka, awaitEvery удерживается на минимуме {}", minAwaitEvery);
            return;
        }
        if (streak == 1) {
            LOG.warn("BatchSenderAutoTune: ошибка ожидания подтверждений Kafka, понижаю awaitEvery {} → {}", observed, minAwaitEvery);
            return;
        }
        if ((streak & (streak - 1)) == 0) { // log на степенях двойки
            LOG.warn("BatchSenderAutoTune: продолжаются ошибки ожидания (streak={}), awaitEvery={} (min)", streak, minAwaitEvery);
        }
    }

    private int clamp(int value) {
        if (value <= 0) {
            return minAwaitEvery;
        }
        if (value < minAwaitEvery) {
            return minAwaitEvery;
        }
        if (value > maxAwaitEvery) {
            return maxAwaitEvery;
        }
        return value;
    }
}
