package kz.qazmarka.h2k.payload.builder;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.util.Bytes;

// Лёгкий потокобезопасный кэш qualifier -> String без глобальной синхронизации.
final class QualifierCache {
    private final ConcurrentHashMap<AbstractKey, String> cache = new ConcurrentHashMap<>(64);
    private final ThreadLocal<LookupKey> lookup = ThreadLocal.withInitial(LookupKey::new);

    String intern(byte[] array, int offset, int length) {
        if (length == 0) {
            return "";
        }
        LookupKey key = lookup.get();
        key.set(array, offset, length);
        try {
            String cached = cache.get(key);
            if (cached != null) {
                return cached;
            }
            String created = new String(array, offset, length, StandardCharsets.UTF_8);
            String normalized = normalizeFieldKey(created);
            OwnedKey stored = key.toOwned();
            String race = cache.putIfAbsent(stored, normalized);
            return race != null ? race : normalized;
        } finally {
            key.clear();
        }
    }

    void cleanupThreadLocal() {
        lookup.remove();
    }

    static String normalizeFieldKey(String name) {
        if (name == null) {
            return "";
        }
        if (name.isEmpty()) {
            return "";
        }
        return normalizeNonEmpty(name);
    }

    private static String normalizeNonEmpty(String name) {
        int start = trimStart(name);
        int end = trimEnd(name, start);
        if (start >= end) {
            return "";
        }
        return normalizeRange(name, start, end);
    }

    private static String normalizeRange(String name, int start, int end) {
        boolean hasLower = hasAsciiLower(name, start, end);
        if (isFullRange(name, start, end)) {
            if (!hasLower) {
                return name;
            }
            return name.toUpperCase(Locale.ROOT);
        }
        String trimmed = name.substring(start, end);
        if (hasLower) {
            return trimmed.toUpperCase(Locale.ROOT);
        }
        return trimmed;
    }

    private static boolean isFullRange(String name, int start, int end) {
        return start == 0 && end == name.length();
    }

    private static int trimStart(String name) {
        int length = name.length();
        int start = 0;
        while (start < length && name.charAt(start) <= ' ') {
            start++;
        }
        return start;
    }

    private static int trimEnd(String name, int start) {
        int end = name.length();
        while (end > start && name.charAt(end - 1) <= ' ') {
            end--;
        }
        return end;
    }

    private static boolean hasAsciiLower(String name, int start, int end) {
        for (int i = start; i < end; i++) {
            char ch = name.charAt(i);
            if (ch >= 'a' && ch <= 'z') {
                return true;
            }
        }
        return false;
    }

    private abstract static class AbstractKey {
        byte[] bytes;
        int offset;
        int length;
        int hash;

        final void reset(byte[] array, int off, int len) {
            this.bytes = array;
            this.offset = off;
            this.length = len;
            this.hash = Bytes.hashCode(array, off, len);
        }

        final void clear() {
            this.bytes = null;
            this.offset = 0;
            this.length = 0;
            this.hash = 0;
        }

        @Override
        public final int hashCode() {
            return hash;
        }

        @Override
        public final boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof AbstractKey)) {
                return false;
            }
            AbstractKey that = (AbstractKey) other;
            return this.length == that.length
                    && Bytes.equals(
                    this.bytes, this.offset, this.length,
                    that.bytes, that.offset, that.length);
        }
    }

    private static final class LookupKey extends AbstractKey {
        void set(byte[] array, int off, int len) {
            reset(array, off, len);
        }

        OwnedKey toOwned() {
            byte[] copy = new byte[length];
            System.arraycopy(bytes, offset, copy, 0, length);
            return new OwnedKey(copy);
        }
    }

    private static final class OwnedKey extends AbstractKey {
        OwnedKey(byte[] copy) {
            reset(copy, 0, copy.length);
        }
    }
}
