package kz.qazmarka.h2k.kafka.producer.batch.bench;

/**
 * Неконтролируемое исключение для ситуаций, когда симулятор таймаутов
 * сработал в условиях, где таймаут невозможен (например, get() без лимита).
 */
public final class UnexpectedSimulatedTimeoutException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public UnexpectedSimulatedTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
