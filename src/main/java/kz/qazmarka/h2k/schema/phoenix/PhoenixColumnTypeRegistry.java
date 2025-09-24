package kz.qazmarka.h2k.schema.phoenix;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.schema.registry.SchemaRegistry;

/**
 * Кэш разрешения Phoenix {@link PDataType} по колонкам с нормализацией строковых типов из реестра.
 */
public final class PhoenixColumnTypeRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixColumnTypeRegistry.class);
    private static final String T_VARCHAR = "VARCHAR";

    private static final Map<String, PDataType<?>> TYPE_MAP;
    static {
        Map<String, PDataType<?>> m = new HashMap<>(32);
        m.put("VARCHAR", PVarchar.INSTANCE);
        m.put("CHAR", PChar.INSTANCE);
        m.put("CHARACTER", PChar.INSTANCE);
        m.put("UNSIGNED TINYINT", PUnsignedTinyint.INSTANCE);
        m.put("UNSIGNED SMALLINT", PUnsignedSmallint.INSTANCE);
        m.put("UNSIGNED INT", PUnsignedInt.INSTANCE);
        m.put("UNSIGNED LONG", PUnsignedLong.INSTANCE);
        m.put("TINYINT", PTinyint.INSTANCE);
        m.put("SMALLINT", PSmallint.INSTANCE);
        m.put("INTEGER", PInteger.INSTANCE);
        m.put("INT", PInteger.INSTANCE);
        m.put("BIGINT", PLong.INSTANCE);
        m.put("FLOAT", PFloat.INSTANCE);
        m.put("DOUBLE", PDouble.INSTANCE);
        m.put("DECIMAL", PDecimal.INSTANCE);
        m.put("BOOLEAN", PBoolean.INSTANCE);
        m.put("TIMESTAMP", PTimestamp.INSTANCE);
        m.put("TIME", PTime.INSTANCE);
        m.put("DATE", PDate.INSTANCE);
        m.put("VARCHAR ARRAY", PVarcharArray.INSTANCE);
        m.put("CHARACTER VARYING ARRAY", PVarcharArray.INSTANCE);
        m.put("STRING ARRAY", PVarcharArray.INSTANCE);
        m.put("VARBINARY", PVarbinary.INSTANCE);
        m.put("BINARY", PBinary.INSTANCE);
        m.put("NUMERIC", PDecimal.INSTANCE);
        m.put("NUMBER", PDecimal.INSTANCE);
        m.put("STRING", PVarchar.INSTANCE);
        m.put("CHARACTER VARYING", PVarchar.INSTANCE);
        m.put("BINARY VARYING", PVarbinary.INSTANCE);
        m.put("LONG", PLong.INSTANCE);
        m.put("BOOL", PBoolean.INSTANCE);
        TYPE_MAP = Collections.unmodifiableMap(m);
    }

    private final SchemaRegistry registry;
    private final ConcurrentMap<TableName, ConcurrentMap<String, PDataType<?>>> cache = new ConcurrentHashMap<>();
    private final Set<ColKey> unknownTypeWarned =
            Collections.newSetFromMap(new ConcurrentHashMap<ColKey, Boolean>());

    public PhoenixColumnTypeRegistry(SchemaRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    /** Возвращает {@link PDataType} колонки, нормализуя строку типа и кэшируя результат. */
    public PDataType<?> resolve(TableName table, String qualifier) {
        final ConcurrentMap<String, PDataType<?>> byQualifier =
                cache.computeIfAbsent(table, t -> new ConcurrentHashMap<>());

        return byQualifier.computeIfAbsent(qualifier, q -> {
            final String raw = registry.columnType(table, q);
            final String norm = normalizeTypeName(raw == null ? T_VARCHAR : raw);

            final PDataType<?> pd = TYPE_MAP.get(norm);
            if (pd != null) {
                return pd;
            }

            final ColKey warnKey = new ColKey(table, q);
            if (unknownTypeWarned.add(warnKey)) {
                LOG.warn("Неизвестный тип Phoenix в реестре для колонки {}.{} -> '{}' (нормализовано '{}'). Будет использован VARCHAR по умолчанию.",
                        table.getNameAsString(), q, raw, norm);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Повтор неизвестного типа Phoenix: {}.{} -> '{}' (нормализовано '{}')",
                        table.getNameAsString(), q, raw, norm);
            }
            return PVarchar.INSTANCE;
        });
    }

    private static String normalizeTypeName(String typeName) {
        String t = typeName == null ? "" : typeName.trim().toUpperCase(Locale.ROOT);
        if (t.isEmpty()) return T_VARCHAR;

        t = stripParenParams(t);
        t = normalizeArraySyntax(t);
        t = t.replace('_', ' ');
        return collapseSpaces(t);
    }

    private static String stripParenParams(String t) {
        int p = t.indexOf('(');
        if (p < 0) return t;
        int q = t.indexOf(')', p + 1);
        if (q > p) {
            return (t.substring(0, p) + t.substring(q + 1)).trim();
        }
        return t.substring(0, p).trim();
    }

    private static String normalizeArraySyntax(String t) {
        if (t.endsWith("[]")) {
            String base = t.substring(0, t.length() - 2).trim();
            base = stripParenParams(base);
            return base + " ARRAY";
        }
        if (t.startsWith("ARRAY<") && t.endsWith(">")) {
            String inner = t.substring(6, t.length() - 1).trim();
            inner = stripParenParams(inner);
            return inner + " ARRAY";
        }
        return t;
    }

    private static String collapseSpaces(String t) {
        StringBuilder sb = new StringBuilder(t.length());
        boolean space = false;
        for (int i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f') {
                if (!space) {
                    sb.append(' ');
                    space = true;
                }
            } else {
                sb.append(c);
                space = false;
            }
        }
        return sb.toString();
    }

    private static final class ColKey {
        final String ns;
        final String name;
        final String qual;
        final int hash;

        ColKey(TableName t, String qual) {
            this.ns = t.getNamespaceAsString();
            this.name = t.getNameAsString();
            this.qual = qual;
            this.hash = 31 * (31 * ns.hashCode() + name.hashCode()) + qual.hashCode();
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || o.getClass() != ColKey.class) return false;
            ColKey other = (ColKey) o;
            return this.hash == other.hash
                    && this.ns.equals(other.ns)
                    && this.name.equals(other.name)
                    && this.qual.equals(other.qual);
        }
    }
}
