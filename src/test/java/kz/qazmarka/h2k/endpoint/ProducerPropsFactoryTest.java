package kz.qazmarka.h2k.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Тесты строгой увязки идемпотентности и порядка в настройках продьюсера.
 */
class ProducerPropsFactoryTest {

    @Test
    @DisplayName("Идемпотентный профиль с max.in.flight выше лимита принудительно зажимается до 5")
    void idempotentSettingsAreClampedToOrderingSafeValues() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.producer.enable.idempotence", "true");
        cfg.set("h2k.producer.acks", "1");
        cfg.set("h2k.producer.max.in.flight", "10");
        cfg.set("h2k.producer.retries", "5");

        Properties props = ProducerPropsFactory.build(cfg, "broker:9092");

        assertEquals("true", props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", props.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals(String.valueOf(Integer.MAX_VALUE), props.getProperty(ProducerConfig.RETRIES_CONFIG));
        assertEquals("5", props.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
    }

    @Test
    @DisplayName("Неидемпотентный профиль не трогаем: max.in.flight и acks остаются из конфига")
    void nonIdempotentSettingsRemainUntouched() {
        Configuration cfg = new Configuration(false);
        cfg.set("h2k.producer.enable.idempotence", "false");
        cfg.set("h2k.producer.acks", "1");
        cfg.set("h2k.producer.max.in.flight", "7");
        cfg.set("h2k.producer.retries", "3");

        Properties props = ProducerPropsFactory.build(cfg, "broker:9092");

        assertEquals("false", props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("1", props.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals("7", props.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        assertTrue(Integer.parseInt(props.getProperty(ProducerConfig.RETRIES_CONFIG)) > 0);
    }
}
