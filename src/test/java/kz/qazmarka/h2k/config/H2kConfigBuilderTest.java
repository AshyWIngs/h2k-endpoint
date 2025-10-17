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
    @DisplayName("buildData: коллекции копируются и не зависят от исходных ссылок")
    void buildDataCopiesCollections() {
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

        H2kConfigData data = builder.buildData();

        // мутируем исходные коллекции — DTO не должно измениться
        topicConfigs.put("retention.ms", "1000");
        urls.add("http://sr2:8081");
        auth.put("basic.password", "secret");
        props.put("mode", "json");

        assertEquals("compact", data.topic.getTopicConfigs().get("cleanup.policy"));
        assertEquals(1, data.topic.getTopicConfigs().size());
        assertEquals("http://sr1:8081", data.avro.getRegistryUrls().get(0));
        assertEquals(1, data.avro.getRegistryUrls().size());
        assertEquals("user", data.avro.getRegistryAuth().get("basic.username"));
        assertFalse(data.avro.getRegistryAuth().containsKey("basic.password"));
        assertEquals("avro", data.avro.getProperties().get("mode"));
        assertTrue(data.observersEnabled);

        Map<String, String> configsView = data.topic.getTopicConfigs();
        UnsupportedOperationException topicConfigsError = assertThrows(
                UnsupportedOperationException.class,
                () -> configsView.put("new", "value"));
        assertNotNull(topicConfigsError);

        List<String> immutableUrls = data.avro.getRegistryUrls();
        UnsupportedOperationException urlsError = assertThrows(
                UnsupportedOperationException.class,
                () -> immutableUrls.add("http://invalid:8081"));
        assertNotNull(urlsError);

        Map<String, String> immutableAuth = data.avro.getRegistryAuth();
        UnsupportedOperationException authError = assertThrows(
                UnsupportedOperationException.class,
                () -> immutableAuth.put("basic.realm", "demo"));
        assertNotNull(authError);
    }

    @Test
    @DisplayName("buildData: нормализация граничных значений и safe defaults")
    void buildDataNormalizesBounds() {
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

        H2kConfigData data = builder.buildData();

        assertTrue(data.topic.getTopicConfigs().isEmpty());
        assertTrue(data.avro.getRegistryUrls().isEmpty());
        assertTrue(data.avro.getRegistryAuth().isEmpty());
        assertTrue(data.avro.getProperties().isEmpty());

        EnsureSettings.TopicSpec topicSpec = data.ensure.getTopicSpec();
        assertEquals(1, topicSpec.getPartitions());
        assertEquals(1, topicSpec.getReplication());

        EnsureSettings.AdminSpec adminSpec = data.ensure.getAdminSpec();
        assertEquals(1L, adminSpec.getTimeoutMs());
        assertEquals("client-id", adminSpec.getClientId());
        assertEquals(1L, adminSpec.getUnknownBackoffMs());

        assertEquals(1, data.producer.getAwaitEvery());
        assertEquals(1, data.producer.getAwaitTimeoutMs());
        assertSame(PhoenixTableMetadataProvider.NOOP, data.metadataProvider);
        assertFalse(data.observersEnabled);
    }
}
