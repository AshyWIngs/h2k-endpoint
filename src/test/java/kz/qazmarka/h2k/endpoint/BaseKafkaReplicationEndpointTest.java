package kz.qazmarka.h2k.endpoint;

/**
 * Базовый класс для интеграционных тестов KafkaReplicationEndpoint с фабричным методом builder().
 */
abstract class BaseKafkaReplicationEndpointTest {
    
    /**
     * Фабричный метод для создания билдера с настройками по умолчанию.
     */
    protected KafkaReplicationEndpointTestBuilder builder() {
        return new KafkaReplicationEndpointTestBuilder();
    }
}
