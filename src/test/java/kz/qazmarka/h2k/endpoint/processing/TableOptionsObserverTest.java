package kz.qazmarka.h2k.endpoint.processing;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.CfFilterSnapshot;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfigBuilder;
import kz.qazmarka.h2k.config.TableOptionsSnapshot;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;

class TableOptionsObserverTest {

    @Test
    void shouldLogOnFirstObservationAndOnChange() {
        TableOptionsObserver observer = TableOptionsObserver.create();
        TableName table = TableName.valueOf("ns", "tbl");
        Snapshot initial = snapshot(table, 1, 16, new String[]{"b", "d"});
        Snapshot updated = snapshot(table, 2, 32, new String[]{"b"});

        try (InfoCapture capture = new InfoCapture()) {
            observer.observe(table, initial.options, initial.cf);
            assertEquals(1, capture.infoCount(), "Должен быть зафиксирован INFO при первой записи");

            observer.observe(table, initial.options, initial.cf);
            assertEquals(1, capture.infoCount(), "Повтор того же состояния не должен логироваться");

            observer.observe(table, updated.options, updated.cf);
            assertEquals(2, capture.infoCount(), "Изменение параметров должно логироваться");

            List<String> logged = capture.messages();
            assertEquals(2, logged.size(), "Ожидаем по сообщению на каждое изменение");
            assertTrue(logged.get(1).contains("saltBytes=2"),
                    "Лог должен отражать обновлённые значения");
        }
    }

    private Snapshot snapshot(TableName table, final int salt, final int capacity, final String[] cfNames) {
    H2kConfigBuilder builder = new H2kConfigBuilder("mock:9092");
        builder.tableMetadataProvider(new PhoenixTableMetadataProvider() {
            private final String[] pk = new String[]{"ID"};

            @Override
            public Integer saltBytes(TableName tbl) {
                return table.equals(tbl) ? salt : null;
            }

            @Override
            public Integer capacityHint(TableName tbl) {
                return table.equals(tbl) ? capacity : null;
            }

            @Override
            public String[] columnFamilies(TableName tbl) {
                return table.equals(tbl) ? cfNames : SchemaRegistry.EMPTY;
            }

            @Override
            public String[] primaryKeyColumns(TableName tbl) {
                return pk;
            }
        });
        H2kConfig config = builder.build();
        TableOptionsSnapshot options = config.describeTableOptions(table);
        return new Snapshot(options, options.cfFilter());
    }

    private static final class Snapshot {
        final TableOptionsSnapshot options;
        final CfFilterSnapshot cf;

        Snapshot(TableOptionsSnapshot options, CfFilterSnapshot cf) {
            this.options = options;
            this.cf = cf;
        }
    }

    private static final class InfoCapture implements AutoCloseable {
        private final AutoCloseable restore;
        private final AtomicInteger info = new AtomicInteger();
        private final List<String> messages = new ArrayList<>();

        InfoCapture() {
            Logger delegate = LoggerFactory.getLogger(TableOptionsObserver.class);
            Logger proxy = (Logger) Proxy.newProxyInstance(
                    delegate.getClass().getClassLoader(),
                    new Class<?>[]{Logger.class},
                    new InfoInterceptor(delegate, info, messages));
            restore = TableOptionsObserver.withLoggerForTest(proxy);
        }

        int infoCount() {
            return info.get();
        }

        List<String> messages() {
            return new ArrayList<>(messages);
        }

        @Override
        public void close() {
            try {
                restore.close();
            } catch (Exception e) {
                throw new IllegalStateException("Не удалось восстановить логгер TableOptionsObserver", e);
            }
        }
    }

    private static final class InfoInterceptor implements InvocationHandler {
        private final Logger delegate;
        private final AtomicInteger info;
        private final List<String> messages;

        InfoInterceptor(Logger delegate, AtomicInteger info, List<String> messages) {
            this.delegate = delegate;
            this.info = info;
            this.messages = messages;
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
            String name = method.getName();
            switch (name) {
                case "info":
                    info.incrementAndGet();
                    messages.add(resolveMessage(args));
                    return null;
                case "isInfoEnabled":
                    return Boolean.TRUE;
                default:
                    try {
                        return method.invoke(delegate, args);
                    } catch (ReflectiveOperationException e) {
                        Throwable cause = e.getCause();
                        throw cause != null ? cause : e;
                    }
            }
        }

        private String resolveMessage(Object[] args) {
            if (args == null || args.length == 0) {
                return null;
            }
            Object first = args[0];
            if (!(first instanceof String)) {
                return first == null ? null : first.toString();
            }
            Object[] fmtArgs;
            if (args.length == 2 && args[1] instanceof Object[]) {
                fmtArgs = (Object[]) args[1];
            } else if (args.length == 1) {
                fmtArgs = new Object[0];
            } else {
                fmtArgs = new Object[args.length - 1];
                System.arraycopy(args, 1, fmtArgs, 0, fmtArgs.length);
            }
            return org.slf4j.helpers.MessageFormatter.arrayFormat((String) first, fmtArgs).getMessage();
        }
    }
}
