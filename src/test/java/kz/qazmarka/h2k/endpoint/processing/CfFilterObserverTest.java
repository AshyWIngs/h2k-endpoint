package kz.qazmarka.h2k.endpoint.processing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;

/**
 * Тесты для CfFilterObserver: убеждаемся, что счётчики копятся и выявляется неэффективный фильтр.
 */
class CfFilterObserverTest {

    /**
     * Убеждаемся, что наблюдатель логирует предупреждение при неэффективном фильтре и
     * не делает этого, когда доля отфильтрованных строк достаточна.
     */
    @Test
    @DisplayName("Наблюдатель фиксирует неэффективный фильтр после порога строк")
    void detectsIneffectiveFilter() {
        Configuration configuration = new Configuration(false);
        configuration.set("h2k.kafka.bootstrap.servers", "mock:9092");
        configuration.set("h2k.topic.pattern", "${namespace}.${qualifier}");
        PhoenixTableMetadataProvider provider = new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) { return null; }

            @Override
            public Integer capacityHint(TableName table) { return null; }

            @Override
            public String[] columnFamilies(TableName table) { return new String[]{"cf1", "cf2"}; }
        };

        H2kConfig h2kConfig = H2kConfig.from(configuration, "mock:9092", provider);
        CfFilterObserver observer = CfFilterObserver.create();

    TableName effective = TableName.valueOf("ns", "effective");
    CfFilterSnapshot effectiveSnapshot = h2kConfig.describeCfFilter(effective);
        for (int i = 0; i < 3; i++) {
            observer.observe(effective, 200, 100, true, effectiveSnapshot);
        }
        assertEquals(0, observer.ineffectiveTables(), "При хорошем соотношении предупреждений быть не должно");
        CfFilterObserver.Stats effectiveStats = observer.snapshot().get(effective);
        assertNotNull(effectiveStats, "Ожидаем статистику по таблице");
        assertEquals(600L, effectiveStats.rowsTotal.sum(), "Накопленное количество строк должно совпадать");

    TableName ineffective = TableName.valueOf("ns", "ineffective");
    CfFilterSnapshot ineffectiveSnapshot = h2kConfig.describeCfFilter(ineffective);
        for (int i = 0; i < 5; i++) {
            observer.observe(ineffective, 120, 0, true, ineffectiveSnapshot);
        }
        assertEquals(1, observer.ineffectiveTables(), "Должно появиться предупреждение о неэффективном фильтре");
        CfFilterObserver.Stats ineffectiveStats = observer.snapshot().get(ineffective);
        assertNotNull(ineffectiveStats, "Статистика по второй таблице должна существовать");
        assertEquals(600L, ineffectiveStats.rowsTotal.sum(), "Суммарное число строк корректно");
        assertEquals(0L, ineffectiveStats.rowsFiltered.sum(), "Отфильтрованных строк нет");
    }

    @Test
    @DisplayName("Наблюдатель фиксирует слишком агрессивный фильтр (95%+)")
    void detectsTooEffectiveFilter() {
        Configuration configuration = new Configuration(false);
        configuration.set("h2k.kafka.bootstrap.servers", "mock:9092");
        configuration.set("h2k.topic.pattern", "${namespace}.${qualifier}");
        PhoenixTableMetadataProvider provider = new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) { return null; }

            @Override
            public Integer capacityHint(TableName table) { return null; }

            @Override
            public String[] columnFamilies(TableName table) { return new String[]{"cf1","cf2"}; }
        };
        H2kConfig h2kConfig = H2kConfig.from(configuration, "mock:9092", provider);

        CfFilterObserver observer = CfFilterObserver.create();
        TableName table = TableName.valueOf("ns", "too_effective");
        CfFilterSnapshot snapshot = h2kConfig.describeCfFilter(table);

        // 96% отфильтровано: 480 из 500, пять раз — чтобы гарантировать накопление выше порога
        for (int i = 0; i < 5; i++) {
            observer.observe(table, 500, 480, true, snapshot);
        }
        assertTrue(observer.tooEffectiveTables() >= 1, "Ожидаем предупреждение о слишком агрессивном фильтре");
        // При этом не должно считаться как 'неэффективный' (низкая доля)
        assertEquals(0L, observer.ineffectiveTables(), "Неэффективный счётчик не должен увеличиваться");
    }

    @Test
    @DisplayName("Отключённый CfFilterObserver не накапливает статистику")
    void disabledObserverSkipsAccumulation() {
        CfFilterObserver observer = CfFilterObserver.disabled();
        TableName table = TableName.valueOf("ns", "disabled");

        observer.observe(table, 500, 500, true, null);
        observer.observe(table, 500, 0, true, null);

        assertEquals(0L, observer.ineffectiveTables(), "Отключённый наблюдатель не должен считать предупреждения");
        assertTrue(observer.snapshot().isEmpty(), "Статистика должна оставаться пустой при отключении");
    }
}
