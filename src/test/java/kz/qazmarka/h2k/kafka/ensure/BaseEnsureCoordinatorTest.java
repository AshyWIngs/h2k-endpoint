package kz.qazmarka.h2k.kafka.ensure;

/**
 * Базовый класс для тестов EnsureCoordinator с фабричным методом builder().
 */
abstract class BaseEnsureCoordinatorTest {
    
    /**
     * Фабричный метод для создания билдера с настройками по умолчанию.
     */
    protected EnsureCoordinatorTestBuilder builder() {
        return new EnsureCoordinatorTestBuilder();
    }
}
