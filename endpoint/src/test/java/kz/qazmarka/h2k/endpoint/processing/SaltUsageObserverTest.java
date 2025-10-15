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

class SaltUsageObserverTest {

    @Test
    void shouldLogAverageAndWarnAboutShortRow() {
        SaltUsageObserver observer = SaltUsageObserver.create();
        TableName table = TableName.valueOf("ns", "salted");
        H2kConfig.TableOptionsSnapshot options = snapshot(table, 1, 0);

        try (SaltCapture capture = new SaltCapture()) {
            for (int i = 0; i < 220; i++) {
                observer.observeRow(table, options, 10);
            }
            assertTrue(capture.infoCount() > 0, "После порогового числа строк должен появиться INFO");
            assertTrue(capture.contains("средний PK"), "INFO должен содержать расчёт средней длины PK");

            observer.observeRow(table, options, 1);
            assertEquals(1, capture.warnCount(), "Первый короткий rowkey должен привести к WARN");
            observer.observeRow(table, options, 1);
            assertEquals(1, capture.warnCount(), "Повторный короткий rowkey не должен дублировать WARN");
        }
    }

    private H2kConfig.TableOptionsSnapshot snapshot(TableName table, final int saltBytes, final int capacityHint) {
        H2kConfig.Builder builder = new H2kConfig.Builder("mock:9092");
        builder.tableMetadataProvider(new PhoenixTableMetadataProvider() {
            private final String[] pk = new String[]{"ID"};

            @Override
            public Integer saltBytes(TableName tbl) {
                return table.equals(tbl) ? saltBytes : null;
            }

            @Override
            public Integer capacityHint(TableName tbl) {
                return table.equals(tbl) ? capacityHint : null;
            }

            @Override
            public String[] columnFamilies(TableName tbl) {
                return SchemaRegistry.EMPTY;
            }

            @Override
            public String[] primaryKeyColumns(TableName tbl) {
                return pk;
            }
        });
        H2kConfig config = builder.build();
        return config.describeTableOptions(table);
    }

    private static final class SaltCapture implements AutoCloseable {
        private final Field logField;
        private final Logger original;
        private final Logger proxy;
        private final AtomicInteger info = new AtomicInteger();
        private final AtomicInteger warn = new AtomicInteger();
        private final List<String> messages = new ArrayList<>();

        SaltCapture() {
            try {
                logField = SaltUsageObserver.class.getDeclaredField("LOG");
                logField.setAccessible(true);
                Field modifiers = Field.class.getDeclaredField("modifiers");
                modifiers.setAccessible(true);
                modifiers.setInt(logField, logField.getModifiers() & ~Modifier.FINAL);

                original = (Logger) logField.get(null);
                proxy = (Logger) Proxy.newProxyInstance(
                        original.getClass().getClassLoader(),
                        new Class<?>[]{Logger.class},
                        new SaltInterceptor(original, info, warn, messages));
                logField.set(null, proxy);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Не удалось перехватить логгер SaltUsageObserver", e);
            }
        }

        int infoCount() {
            return info.get();
        }

        int warnCount() {
            return warn.get();
        }

        boolean contains(String fragment) {
            for (String msg : messages) {
                if (msg != null && msg.contains(fragment)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close() {
            try {
                logField.set(null, original);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Не удалось восстановить логгер SaltUsageObserver", e);
            }
        }
    }

    private static final class SaltInterceptor implements InvocationHandler {
        private final Logger delegate;
        private final AtomicInteger info;
        private final AtomicInteger warn;
        private final List<String> messages;

        SaltInterceptor(Logger delegate,
                        AtomicInteger info,
                        AtomicInteger warn,
                        List<String> messages) {
            this.delegate = delegate;
            this.info = info;
            this.warn = warn;
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
                case "warn":
                    warn.incrementAndGet();
                    messages.add(resolveMessage(args));
                    return null;
                case "isInfoEnabled":
                    return Boolean.TRUE;
                case "isWarnEnabled":
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
