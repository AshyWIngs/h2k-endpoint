package kz.qazmarka.h2k.config;

/**
 * DTO с настройками ожидания подтверждений Kafka Producer.
 */
public final class ProducerAwaitSettings {

    private final int awaitEvery;
    private final int awaitTimeoutMs;

    ProducerAwaitSettings(int awaitEvery, int awaitTimeoutMs) {
        this.awaitEvery = awaitEvery < 1 ? 1 : awaitEvery;
        this.awaitTimeoutMs = awaitTimeoutMs < 1 ? 1 : awaitTimeoutMs;
    }

    public int getAwaitEvery() {
        return awaitEvery;
    }

    public int getAwaitTimeoutMs() {
        return awaitTimeoutMs;
    }
}
