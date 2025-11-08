package kz.qazmarka.h2k.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;

/**
 * Юнит‑тесты билдера {@link H2kConfigBuilder} и связанных DTO: проверяем иммутабельность коллекций,
 * нормализацию граничных значений и корректное применение флагов observers.
 */
class H2kConfigBuilderTest {

    @Test
    @DisplayName("build(): коллекции копируются и не зависят от исходных ссылок")
    void buildCopiesCollections() {
        H2kConfigBuilder builder = new H2kConfigBuilder("kafka:9092");

    Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("cleanup.policy", "compact");
        builder.topic()
                .pattern("${table}")
                .maxLength(200)
                .configs(topicConfigs)
                .done();

    List<String> urls = new ArrayList<>();
        urls.add("http://sr1:8081");
    Map<String, String> auth = new HashMap<>();
        auth.put("basic.username", "user");
    Map<String, String> props = new HashMap<>();
        props.put("mode", "avro");

        builder.avro()
                .schemaDir("/tmp/avro")
                .schemaRegistryUrls(urls)
                .schemaRegistryAuth(auth)
                .properties(props)
                .done();

        builder.ensure()
                .enabled(true)
                .allowIncreasePartitions(true)
                .allowDiffConfigs(false)
                .partitions(3)
                .replication((short) 2)
                .adminTimeoutMs(7000L)
                .adminClientId("suite-client")
                .unknownBackoffMs(1500L)
                .done();

        builder.producer()
                .awaitEvery(50)
                .awaitTimeoutMs(2000)
                .done();

        builder.observersEnabled(true);
        builder.jmxEnabled(false);

        H2kConfig config = builder.build();

        // мутируем исходные коллекции — DTO не должно измениться
        topicConfigs.put("retention.ms", "1000");
        urls.add("http://sr2:8081");
        auth.put("basic.password", "secret");
        props.put("mode", "json");

        assertEquals("compact", config.getTopicSettings().getTopicConfigs().get("cleanup.policy"));
        assertEquals(1, config.getTopicSettings().getTopicConfigs().size());
        assertEquals("http://sr1:8081", config.getAvroSettings().getRegistryUrls().get(0));
        assertEquals(1, config.getAvroSettings().getRegistryUrls().size());
        assertEquals("user", config.getAvroSettings().getRegistryAuth().get("basic.username"));
        assertFalse(config.getAvroSettings().getRegistryAuth().containsKey("basic.password"));
        assertEquals("avro", config.getAvroSettings().getProperties().get("mode"));
        assertTrue(config.isObserversEnabled());
        assertFalse(config.isJmxEnabled());

        Map<String, String> configsView = config.getTopicSettings().getTopicConfigs();
        UnsupportedOperationException topicConfigsError = assertThrows(
                UnsupportedOperationException.class,
                () -> configsView.put("new", "value"));
        assertNotNull(topicConfigsError);

        List<String> immutableUrls = config.getAvroSettings().getRegistryUrls();
        UnsupportedOperationException urlsError = assertThrows(
                UnsupportedOperationException.class,
                () -> immutableUrls.add("http://invalid:8081"));
        assertNotNull(urlsError);

        Map<String, String> immutableAuth = config.getAvroSettings().getRegistryAuth();
        UnsupportedOperationException authError = assertThrows(
                UnsupportedOperationException.class,
                () -> immutableAuth.put("basic.realm", "demo"));
        assertNotNull(authError);
    }

    @Test
    @DisplayName("build(): нормализация граничных значений и safe defaults")
    void buildNormalizesBounds() {
        H2kConfigBuilder builder = new H2kConfigBuilder("kafka:9092");

        builder.topic()
                .pattern("${table}")
                .maxLength(100)
                .configs(null)
                .done();

        builder.avro()
                .schemaDir(null)
                .schemaRegistryUrls(null)
                .schemaRegistryAuth(null)
                .properties(null)
                .done();

        builder.ensure()
                .enabled(true)
                .allowIncreasePartitions(false)
                .allowDiffConfigs(false)
                .partitions(0)
                .replication((short) 0)
                .adminTimeoutMs(0)
                .adminClientId("client-id")
                .unknownBackoffMs(0)
                .done();

        builder.producer()
                .awaitEvery(0)
                .awaitTimeoutMs(0)
                .done();

        H2kConfig config = builder.build();

        assertTrue(config.getTopicSettings().getTopicConfigs().isEmpty());
        assertTrue(config.getAvroSettings().getRegistryUrls().isEmpty());
        assertTrue(config.getAvroSettings().getRegistryAuth().isEmpty());
        assertTrue(config.getAvroSettings().getProperties().isEmpty());

        EnsureSettings.TopicSpec topicSpec = config.getEnsureSettings().getTopicSpec();
        assertEquals(1, topicSpec.getPartitions());
        assertEquals(1, topicSpec.getReplication());

        EnsureSettings.AdminSpec adminSpec = config.getEnsureSettings().getAdminSpec();
        assertEquals(1L, adminSpec.getTimeoutMs());
        assertEquals("client-id", adminSpec.getClientId());
        assertEquals(1L, adminSpec.getUnknownBackoffMs());

        assertEquals(1, config.getProducerSettings().getAwaitEvery());
        assertEquals(1, config.getProducerSettings().getAwaitTimeoutMs());
        assertSame(PhoenixTableMetadataProvider.NOOP, config.getTableMetadataProvider());
        assertFalse(config.isObserversEnabled());
        assertTrue(config.isJmxEnabled());
    }
}
