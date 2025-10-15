package kz.qazmarka.h2k.endpoint.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

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

    @Test
    @DisplayName("Без превышения подсказки предупреждение не логируется")
    void noWarningWhenWithinCapacityHint() {
        PhoenixTableMetadataProvider provider = new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) { return 0; }

            @Override
            public Integer capacityHint(TableName table) {
                return "NS:HINT".equalsIgnoreCase(table.getNameAsString()) ? 5 : null;
            }

            @Override
            public String[] columnFamilies(TableName table) { return SchemaRegistry.EMPTY; }
        };
        H2kConfig config = H2kConfig.from(new Configuration(false), "mock:9092", provider);
        TableCapacityObserver observer = TableCapacityObserver.create(config);
        TableName table = TableName.valueOf("ns", "hint");

        try (WarnCapture capture = new WarnCapture()) {
            for (int i = 0; i < 5; i++) {
                observer.observe(table, 5, 50);
            }
            TableCapacityObserver.StatsSnapshot stats = observer.snapshot().get(table);
            assertNotNull(stats, "Статистика должна создаться даже без предупреждения");
            assertEquals(5, stats.maxFields());
            assertEquals(250, stats.rowsObserved());
            assertEquals(0, stats.lastRecommendation(), "Рекомендаций быть не должно");
            assertEquals(0, capture.warnCount(), "WARN не должен логироваться, когда максимум не превышает подсказку");
        }
    }

    @Test
    @DisplayName("Превышение подсказки ведёт к предупреждению с рекомендацией")
    void warningIncludesRecommendationWhenCapacityExceeded() {
        PhoenixTableMetadataProvider provider = new PhoenixTableMetadataProvider() {
            @Override
            public Integer saltBytes(TableName table) { return 0; }

            @Override
            public Integer capacityHint(TableName table) {
                return "NS:WARN".equalsIgnoreCase(table.getNameAsString()) ? 2 : null;
            }

            @Override
            public String[] columnFamilies(TableName table) { return SchemaRegistry.EMPTY; }
        };
        H2kConfig config = H2kConfig.from(new Configuration(false), "mock:9092", provider);
        TableCapacityObserver observer = TableCapacityObserver.create(config);
        TableName table = TableName.valueOf("ns", "warn");

        try (WarnCapture capture = new WarnCapture()) {
            for (int i = 0; i < 5; i++) {
                observer.observe(table, 6, 50);
            }
            TableCapacityObserver.StatsSnapshot stats = observer.snapshot().get(table);
            assertNotNull(stats, "Ожидаем накопленную статистику");
            assertEquals(6, stats.maxFields());
            assertEquals(250, stats.rowsObserved());
            assertEquals(6, stats.lastRecommendation(), "Последняя рекомендация должна совпадать с наблюдаемым максимумом");
            assertTrue(capture.warnCount() > 0, "WARN должен логироваться при превышении подсказки");
            String[] fragments = {
                    table.getNameWithNamespaceInclAsString(),
                    "h2k.capacityHint",
                    "Рекомендуется обновить"
            };
            for (String fragment : fragments) {
                assertTrue(capture.contains(fragment),
                        "WARN должен содержать фрагмент: " + fragment + "\nСообщения: " + Arrays.toString(capture.messages()));
            }
        }
    }

    /**
     * Хелпер для перехвата WARN-логгирования TableCapacityObserver.
     */
    private static final class WarnCapture implements AutoCloseable {
        private final Field logField;
        private final Logger original;
        private final Logger proxy;
        private final AtomicInteger warns = new AtomicInteger(0);
        private final List<String> messages = new ArrayList<>();

        WarnCapture() {
            try {
                logField = TableCapacityObserver.class.getDeclaredField("LOG");
                logField.setAccessible(true);
                Field modifiers = Field.class.getDeclaredField("modifiers");
                modifiers.setAccessible(true);
                modifiers.setInt(logField, logField.getModifiers() & ~Modifier.FINAL);

                original = (Logger) logField.get(null);
                proxy = (Logger) Proxy.newProxyInstance(
                        original.getClass().getClassLoader(),
                        new Class<?>[]{Logger.class},
                        new WarnInterceptor(original, warns, messages));
                logField.set(null, proxy);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Не удалось перехватить логгер TableCapacityObserver", e);
            }
        }

        int warnCount() {
            return warns.get();
        }

        boolean contains(String fragment) {
            for (String message : messages) {
                if (message != null && message.contains(fragment)) {
                    return true;
                }
            }
            return false;
        }

        String[] messages() {
            return messages.toArray(new String[0]);
        }

        @Override
        public void close() {
            try {
                logField.set(null, original);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Не удалось восстановить исходный логгер TableCapacityObserver", e);
            }
        }
    }

    private static final class WarnInterceptor implements InvocationHandler {
        private final Logger delegate;
        private final AtomicInteger warns;
        private final List<String> messages;

        WarnInterceptor(Logger delegate, AtomicInteger warns, List<String> messages) {
            this.delegate = delegate;
            this.warns = warns;
            this.messages = messages;
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
            if ("warn".equals(method.getName())) {
                warns.incrementAndGet();
                messages.add(resolveMessage(args));
            }
            try {
                return method.invoke(delegate, args);
            } catch (ReflectiveOperationException e) {
                Throwable cause = e.getCause();
                throw cause != null ? cause : e;
            }
        }

        private String resolveMessage(Object[] args) {
            if (args == null || args.length == 0) {
                return null;
            }
            int offset = 0;
            if (args[0] instanceof org.slf4j.Marker) {
                offset = 1;
            }
            if (offset >= args.length) {
                return null;
            }
            Object first = args[offset];
            if (!(first instanceof String)) {
                return first == null ? null : first.toString();
            }
            Object[] fmtArgs = (args.length - offset - 1) <= 0
                    ? new Object[0]
                    : Arrays.copyOfRange(args, offset + 1, args.length);
            FormattingTuple tuple = MessageFormatter.arrayFormat((String) first, fmtArgs);
            return tuple.getMessage();
        }
    }
}
