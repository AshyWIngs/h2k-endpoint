package kz.qazmarka.h2k.config;

import java.nio.charset.StandardCharsets;

/**
 * Иммутабельный снимок конфигурации CF-фильтра для конкретной таблицы.
 * Хранит исходный список column family и источник данных (Avro или дефолтные значения).
 */
public final class CfFilterSnapshot {
    private static final byte[][] EMPTY_FAMILIES = new byte[0][];
    private static final CfFilterSnapshot DISABLED = new CfFilterSnapshot(false, EMPTY_FAMILIES, "", TableValueSource.DEFAULT);

    private final boolean enabled;
    private final byte[][] families;
    private final String csv;
    private final TableValueSource source;

    private CfFilterSnapshot(boolean enabled, byte[][] families, String csv, TableValueSource source) {
        this.enabled = enabled;
        this.families = families;
        this.csv = csv;
        this.source = source;
    }

    static CfFilterSnapshot disabled() {
        return DISABLED;
    }

    static CfFilterSnapshot from(String[] names, TableValueSource source) {
        if (names == null || names.length == 0) {
            return DISABLED;
        }
        int len = names.length;
        byte[][] immutableFamilies = new byte[len][];
        for (int i = 0; i < len; i++) {
            String name = names[i];
            immutableFamilies[i] = (name == null) ? new byte[0] : name.getBytes(StandardCharsets.UTF_8);
        }
        String csv = String.join(",", names);
        TableValueSource resolvedSource = (source == null) ? TableValueSource.AVRO : source;
        return new CfFilterSnapshot(true, immutableFamilies, csv, resolvedSource);
    }

    public boolean enabled() {
        return enabled;
    }

    public byte[][] families() {
        return families;
    }

    public String csv() {
        return csv;
    }

    public TableValueSource source() {
        return source;
    }
}
