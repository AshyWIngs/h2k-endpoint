package kz.qazmarka.h2k.kafka.producer.batch.bench;

/**
 * Неконтролируемое исключение для ситуаций, когда симулятор таймаутов
 * сработал в условиях, где таймаут невозможен (например, get() без лимита).
 */
public final class UnexpectedSimulatedTimeoutException extends RuntimeException {
    public UnexpectedSimulatedTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
