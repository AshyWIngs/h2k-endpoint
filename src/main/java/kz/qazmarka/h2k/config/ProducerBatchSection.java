package kz.qazmarka.h2k.config;

import org.apache.hadoop.conf.Configuration;

import kz.qazmarka.h2k.util.Parsers;

/**
 * Секция настроек батч-отправки продьюсера: только размер порога и таймаут ожидания.
 */
public final class ProducerBatchSection {
    final int awaitEvery;
    final int awaitTimeoutMs;

    private ProducerBatchSection(int awaitEvery, int awaitTimeoutMs) {
        this.awaitEvery = awaitEvery;
        this.awaitTimeoutMs = awaitTimeoutMs;
    }

    static ProducerBatchSection from(Configuration cfg) {
        int awaitEvery = Parsers.readIntMin(cfg, H2kConfig.K_PRODUCER_AWAIT_EVERY, H2kConfig.DEFAULT_AWAIT_EVERY, 1);
        int awaitTimeoutMs = Parsers.readIntMin(cfg, H2kConfig.K_PRODUCER_AWAIT_TIMEOUT_MS, H2kConfig.DEFAULT_AWAIT_TIMEOUT_MS, 1);
        return new ProducerBatchSection(awaitEvery, awaitTimeoutMs);
    }
}
