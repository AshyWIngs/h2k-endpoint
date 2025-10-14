package kz.qazmarka.h2k.endpoint.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;

/**
 * Проверяет накопление статистики TableCapacityObserver и фиксацию рекомендаций.
 */
class TableCapacityObserverTest {

    /**
     * Проверяет, что после накопления достаточного числа строк наблюдатель увеличивает максимум и
     * фиксирует рекомендацию по обновлению `h2k.capacityHint` в Avro-схеме.
     */
    @Test
    @DisplayName("Рекомендуемая подсказка появляется после достижения порога строк")
    void recommendationAppearsAfterThreshold() {
        Configuration configuration = new Configuration(false);
        configuration.set("h2k.kafka.bootstrap.servers", "mock:9092");
        configuration.set("h2k.topic.pattern", "${namespace}.${qualifier}");

        PhoenixTableMetadataProvider provider = new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) { return null; }

            @Override
            public Integer capacityHint(TableName table) {
                return "NS:CAP".equalsIgnoreCase(table.getNameAsString()) ? 2 : null;
            }

            @Override
            public String[] columnFamilies(TableName table) { return SchemaRegistry.EMPTY; }
        };

        H2kConfig h2kConfig = H2kConfig.from(configuration, "mock:9092", provider);
        TableCapacityObserver observer = TableCapacityObserver.create(h2kConfig);
        TableName table = TableName.valueOf("ns", "cap");

        for (int i = 0; i < 20; i++) {
            observer.observe(table, 4, 10);
        }
        observer.observe(table, 6, 5);

        Map<TableName, TableCapacityObserver.StatsSnapshot> snapshot = observer.snapshot();
        TableCapacityObserver.StatsSnapshot stats = snapshot.get(table);
        assertNotNull(stats, "Ожидаем статистику по таблице");
        assertEquals(6, stats.maxFields(), "Максимум полей должен обновиться до 6");
        assertEquals(205, stats.rowsObserved(), "Общее число строк должно учитывать все наблюдения");
        assertEquals(6, stats.lastRecommendation(), "Рекомендованное значение совпадает с максимумом");
        assertEquals(6, observer.totalObservedMax(), "Агрегированный максимум совпадает с наблюдением");
        assertTrue(stats.lastRecommendation() > 0, "Должно быть зафиксировано хотя бы одно предупреждение");
    }

    @Test
    @DisplayName("Отключённый наблюдатель игнорирует события и возвращает пустую статистику")
    void disabledObserverNoops() {
        TableCapacityObserver observer = TableCapacityObserver.disabled();
        TableName table = TableName.valueOf("ns", "disabled");

        observer.observe(table, 10, 100);
        observer.observe(table, 20, 200);

        assertEquals(0L, observer.totalObservedMax(), "Агрегированный максимум должен быть 0 при отключении");
        assertTrue(observer.snapshot().isEmpty(), "Снимок статистики должен быть пустым");
    }
}
