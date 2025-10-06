package kz.qazmarka.h2k.kafka.producer.batch.bench;

import java.util.concurrent.TimeoutException;

/**
 * Специализированное исключение для симуляции таймаута в тестовых Future.
 */
public final class SimulatedTimeoutException extends TimeoutException {
    public SimulatedTimeoutException(String message) {
        super(message);
    }
}
