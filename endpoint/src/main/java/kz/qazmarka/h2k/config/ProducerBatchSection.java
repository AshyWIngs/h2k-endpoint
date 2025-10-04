package kz.qazmarka.h2k.config;

import kz.qazmarka.h2k.util.Parsers;

import org.apache.hadoop.conf.Configuration;

/**
 * Настройки батч-отправки продьюсера (awaitEvery/timeout и диагностические флаги).
 */
public final class ProducerBatchSection {
    final int awaitEvery;
    final int awaitTimeoutMs;
    final boolean countersEnabled;
    final boolean debugOnFailure;
    final AutotuneSettings autotune;

    private ProducerBatchSection(int awaitEvery,
                                 int awaitTimeoutMs,
                                 boolean countersEnabled,
                                 boolean debugOnFailure,
                                 AutotuneSettings autotune) {
        this.awaitEvery = awaitEvery;
        this.awaitTimeoutMs = awaitTimeoutMs;
        this.countersEnabled = countersEnabled;
        this.debugOnFailure = debugOnFailure;
        this.autotune = autotune;
    }

    /**
     * Формирует секцию по ключам {@code h2k.producer.*}, гарантируя минимальные значения.
     *
     * @param cfg конфигурация Hadoop с параметрами продьюсера
     * @return секция настроек BatchSender
     */
    static ProducerBatchSection from(Configuration cfg) {
        int awaitEvery = Parsers.readIntMin(cfg, H2kConfig.K_PRODUCER_AWAIT_EVERY, H2kConfig.DEFAULT_AWAIT_EVERY, 1);
        int awaitTimeoutMs = Parsers.readIntMin(cfg, H2kConfig.K_PRODUCER_AWAIT_TIMEOUT_MS, H2kConfig.DEFAULT_AWAIT_TIMEOUT_MS, 1);
        boolean countersEnabled = cfg.getBoolean(H2kConfig.Keys.PRODUCER_BATCH_COUNTERS_ENABLED, H2kConfig.DEFAULT_PRODUCER_BATCH_COUNTERS_ENABLED);
        boolean debugOnFailure = cfg.getBoolean(H2kConfig.Keys.PRODUCER_BATCH_DEBUG_ON_FAILURE, H2kConfig.DEFAULT_PRODUCER_BATCH_DEBUG_ON_FAILURE);
        boolean autotuneEnabled = Parsers.readBoolean(cfg, H2kConfig.Keys.PRODUCER_BATCH_AUTOTUNE_ENABLED, H2kConfig.DEFAULT_PRODUCER_BATCH_AUTOTUNE_ENABLED);
        int autotuneMinAwait = cfg.getInt(H2kConfig.Keys.PRODUCER_BATCH_AUTOTUNE_MIN, H2kConfig.DEFAULT_PRODUCER_BATCH_AUTOTUNE_MIN);
        if (autotuneMinAwait < 0) {
            autotuneMinAwait = 0;
        }
        int autotuneMaxAwait = cfg.getInt(H2kConfig.Keys.PRODUCER_BATCH_AUTOTUNE_MAX, H2kConfig.DEFAULT_PRODUCER_BATCH_AUTOTUNE_MAX);
        if (autotuneMaxAwait < 0) {
            autotuneMaxAwait = 0;
        }
        int autotuneLatencyHighMs = cfg.getInt(H2kConfig.Keys.PRODUCER_BATCH_AUTOTUNE_LATENCY_HIGH_MS, H2kConfig.DEFAULT_PRODUCER_BATCH_AUTOTUNE_LATENCY_HIGH_MS);
        if (autotuneLatencyHighMs < 0) {
            autotuneLatencyHighMs = 0;
        }
        int autotuneLatencyLowMs = cfg.getInt(H2kConfig.Keys.PRODUCER_BATCH_AUTOTUNE_LATENCY_LOW_MS, H2kConfig.DEFAULT_PRODUCER_BATCH_AUTOTUNE_LATENCY_LOW_MS);
        if (autotuneLatencyLowMs < 0) {
            autotuneLatencyLowMs = 0;
        }
        int autotuneCooldownMs = Parsers.readIntMin(cfg, H2kConfig.Keys.PRODUCER_BATCH_AUTOTUNE_COOLDOWN_MS, H2kConfig.DEFAULT_PRODUCER_BATCH_AUTOTUNE_COOLDOWN_MS, 1000);

        AutotuneSettings autotune = new AutotuneSettings(autotuneEnabled, autotuneMinAwait, autotuneMaxAwait,
                autotuneLatencyHighMs, autotuneLatencyLowMs, autotuneCooldownMs);
        return new ProducerBatchSection(awaitEvery, awaitTimeoutMs, countersEnabled, debugOnFailure, autotune);
    }

    boolean autotuneEnabled() {
        return autotune.enabled;
    }

    int autotuneMinAwait() {
        return autotune.minAwait;
    }

    int autotuneMaxAwait() {
        return autotune.maxAwait;
    }

    int autotuneLatencyHighMs() {
        return autotune.latencyHighMs;
    }

    int autotuneLatencyLowMs() {
        return autotune.latencyLowMs;
    }

    int autotuneCooldownMs() {
        return autotune.cooldownMs;
    }

    static final class AutotuneSettings {
        final boolean enabled;
        final int minAwait;
        final int maxAwait;
        final int latencyHighMs;
        final int latencyLowMs;
        final int cooldownMs;

        AutotuneSettings(boolean enabled,
                          int minAwait,
                          int maxAwait,
                          int latencyHighMs,
                          int latencyLowMs,
                          int cooldownMs) {
            this.enabled = enabled;
            this.minAwait = minAwait;
            this.maxAwait = maxAwait;
            this.latencyHighMs = latencyHighMs;
            this.latencyLowMs = latencyLowMs;
            this.cooldownMs = cooldownMs;
        }
    }
}
