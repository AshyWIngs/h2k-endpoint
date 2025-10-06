package kz.qazmarka.h2k.endpoint.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;

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
        configuration.set("h2k.cf.list", "cf1,cf2");

        H2kConfig h2kConfig = H2kConfig.from(configuration, "mock:9092");
        CfFilterObserver observer = CfFilterObserver.create(h2kConfig);

        TableName effective = TableName.valueOf("ns", "effective");
        for (int i = 0; i < 3; i++) {
            observer.observe(effective, 200, 100, true);
        }
        assertEquals(0, observer.ineffectiveTables(), "При хорошем соотношении предупреждений быть не должно");
        CfFilterObserver.Stats effectiveStats = observer.snapshot().get(effective);
        assertNotNull(effectiveStats, "Ожидаем статистику по таблице");
        assertEquals(600L, effectiveStats.rowsTotal.sum(), "Накопленное количество строк должно совпадать");

        TableName ineffective = TableName.valueOf("ns", "ineffective");
        for (int i = 0; i < 5; i++) {
            observer.observe(ineffective, 120, 0, true);
        }
        assertEquals(1, observer.ineffectiveTables(), "Должно появиться предупреждение о неэффективном фильтре");
        CfFilterObserver.Stats ineffectiveStats = observer.snapshot().get(ineffective);
        assertNotNull(ineffectiveStats, "Статистика по второй таблице должна существовать");
        assertEquals(600L, ineffectiveStats.rowsTotal.sum(), "Суммарное число строк корректно");
        assertEquals(0L, ineffectiveStats.rowsFiltered.sum(), "Отфильтрованных строк нет");
    }
}
