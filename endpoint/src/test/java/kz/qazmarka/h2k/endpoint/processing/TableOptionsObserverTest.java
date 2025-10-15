package kz.qazmarka.h2k.endpoint.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import kz.qazmarka.h2k.config.H2kConfig;
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
        H2kConfig.Builder builder = new H2kConfig.Builder("mock:9092");
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
        H2kConfig.TableOptionsSnapshot options = config.describeTableOptions(table);
        return new Snapshot(options, options.cfFilter());
    }

    private static final class Snapshot {
        final H2kConfig.TableOptionsSnapshot options;
        final H2kConfig.CfFilterSnapshot cf;

        Snapshot(H2kConfig.TableOptionsSnapshot options, H2kConfig.CfFilterSnapshot cf) {
            this.options = options;
            this.cf = cf;
        }
    }

    private static final class InfoCapture implements AutoCloseable {
        private final Field logField;
        private final Logger original;
        private final Logger proxy;
        private final AtomicInteger info = new AtomicInteger();
        private final List<String> messages = new ArrayList<>();

        InfoCapture() {
            try {
                logField = TableOptionsObserver.class.getDeclaredField("LOG");
                logField.setAccessible(true);
                Field modifiers = Field.class.getDeclaredField("modifiers");
                modifiers.setAccessible(true);
                modifiers.setInt(logField, logField.getModifiers() & ~Modifier.FINAL);

                original = (Logger) logField.get(null);
                proxy = (Logger) Proxy.newProxyInstance(
                        original.getClass().getClassLoader(),
                        new Class<?>[]{Logger.class},
                        new InfoInterceptor(original, info, messages));
                logField.set(null, proxy);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Не удалось перехватить логгер TableOptionsObserver", e);
            }
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
                logField.set(null, original);
            } catch (IllegalAccessException e) {
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
