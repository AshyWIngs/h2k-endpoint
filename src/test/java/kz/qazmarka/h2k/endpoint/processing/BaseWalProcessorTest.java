package kz.qazmarka.h2k.endpoint.processing;

/**
 * Базовый класс для тестов WalRowProcessor с общими утилитами.
 */
abstract class BaseWalProcessorTest {

    /**
     * Создаёт новый builder для тестов.
     */
    protected WalProcessorTestBuilder builder() {
        return new WalProcessorTestBuilder();
    }
}
