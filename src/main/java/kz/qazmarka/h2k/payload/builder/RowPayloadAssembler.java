package kz.qazmarka.h2k.payload.builder;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.util.Maps;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Собирает корневую карту payload для строки WAL, добавляя метаданные, PK и служебные поля.
 * Логику построения вынесено из {@link PayloadBuilder}, чтобы разгрузить горячий путь и облегчить тестирование.
 */
final class RowPayloadAssembler {

    private final Decoder decoder;
    private final H2kConfig cfg;
    private final QualifierCache qualifierCache = new QualifierCache();

    RowPayloadAssembler(Decoder decoder, H2kConfig cfg) {
        this.decoder = decoder;
        this.cfg = cfg;
    }

    Map<String, Object> assemble(TableName table,
                                 List<Cell> cells,
                                 RowKeySlice rowKey,
                                 long walSeq,
                                 long walWriteTime) {
        final boolean includeMeta = cfg.isIncludeMeta();
        final boolean includeWalMeta = includeMeta && cfg.isIncludeMetaWal();
        final boolean includeRowKey = cfg.isIncludeRowKey();
        final boolean rowkeyB64 = cfg.isRowkeyBase64();

        final int cellsCount = (cells == null ? 0 : cells.size());
        final boolean includeRowKeyPresent = includeRowKey && rowKey != null;
        int cap = 1 // _event_ts
                + cellsCount
                + (includeMeta ? 5 : 0)
                + (includeRowKeyPresent ? 1 : 0)
                + (includeWalMeta ? 2 : 0);

        final Map<String, Object> obj = newRootMap(table, cap);
        MetaWriter.addMetaIfEnabled(includeMeta, obj, table, cellsCount, cfg.getCfNamesCsv());
        decodePkFromRowKey(table, rowKey, obj);

        CellStats stats = decodeCells(table, cells, obj);
        MetaWriter.addCellsCfIfMeta(obj, includeMeta, stats.cfCells);
        if (stats.cfCells > 0) {
            obj.put(PayloadFields.EVENT_TS, stats.maxTs);
        }
        MetaWriter.addDeleteFlagIfNeeded(obj, stats.hasDelete);

        final int saltBytes = cfg.getSaltBytesFor(table);
        RowKeyWriter.addRowKeyIfPresent(includeRowKeyPresent, obj, rowKey, rowkeyB64, saltBytes);
        MetaWriter.addWalMeta(includeWalMeta, obj, walSeq, walWriteTime);
        return obj;
    }

    private Map<String, Object> newRootMap(TableName table, int estimatedKeysCount) {
        final int hint = cfg.getCapacityHintFor(table);
        final int target = Math.max(estimatedKeysCount, hint > 0 ? hint : 0);
        final int initialCapacity = Maps.initialCapacity(target);
        return new LinkedHashMap<>(initialCapacity, 0.75f);
    }

    private void decodePkFromRowKey(TableName table, RowKeySlice rk, Map<String, Object> out) {
        if (rk == null) {
            return;
        }
        final int saltBytes = cfg.getSaltBytesFor(table);
        decoder.decodeRowKey(table, rk, saltBytes, out);
    }

    private CellStats decodeCells(TableName table, List<Cell> cells, Map<String, Object> obj) {
        CellStats s = new CellStats();
        if (cells == null || cells.isEmpty()) {
            return s;
        }

        final boolean serializeNulls = cfg.isJsonSerializeNulls();
        for (Cell cell : cells) {
            processCell(table, cell, obj, s, serializeNulls);
        }
        return s;
    }

    private void processCell(TableName table,
                             Cell cell,
                             Map<String, Object> obj,
                             CellStats s,
                             boolean serializeNulls) {
        s.cfCells++;
        long ts = cell.getTimestamp();
        if (ts > s.maxTs) s.maxTs = ts;
        if (CellUtil.isDelete(cell)) {
            s.hasDelete = true;
            return;
        }

        final byte[] qa = cell.getQualifierArray();
        final int qo = cell.getQualifierOffset();
        final int ql = cell.getQualifierLength();
        final String columnName = qualifierCache.intern(qa, qo, ql);

        final byte[] va = cell.getValueArray();
        final int vo = cell.getValueOffset();
        final int vl = cell.getValueLength();

        Object decoded = decoder.decode(table, qa, qo, ql, va, vo, vl);
        if (decoded != null) {
            obj.put(columnName, decoded);
            return;
        }

        if (va != null && vl > 0) {
            obj.put(columnName, new String(va, vo, vl, StandardCharsets.UTF_8));
            return;
        }

        if (serializeNulls && (!obj.containsKey(columnName) || obj.get(columnName) == null)) {
            obj.put(columnName, null);
        }
    }

    /** Минимальный потокобезопасный кэш qualifier → String, чтобы исключить повторные аллокации. */
    private static final class QualifierCache {
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
                OwnedKey stored = key.toOwned();
                String race = cache.putIfAbsent(stored, created);
                return race != null ? race : created;
            } finally {
                lookup.remove();
            }
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

            @Override
            public final int hashCode() {
                return hash;
            }

            @Override
            public final boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof AbstractKey)) return false;
                AbstractKey other = (AbstractKey) o;
                if (this.hash != other.hash || this.length != other.length) {
                    return false;
                }
                return Bytes.equals(this.bytes, this.offset, this.length,
                        other.bytes, other.offset, other.length);
            }
        }

        private static final class LookupKey extends AbstractKey {
            void set(byte[] array, int off, int len) {
                reset(array, off, len);
            }

            OwnedKey toOwned() {
                return new OwnedKey(bytes, offset, length);
            }
        }

        private static final class OwnedKey extends AbstractKey {
            OwnedKey(byte[] source, int off, int len) {
                byte[] copy = new byte[len];
                System.arraycopy(source, off, copy, 0, len);
                reset(copy, 0, len);
            }
        }
    }

    /** Утилиты для метаданных и служебных полей. */
    private static final class MetaWriter {
        private MetaWriter() { }

        static void addMetaIfEnabled(boolean includeMeta,
                                     Map<String, Object> obj,
                                     TableName table,
                                     int totalCells,
                                     String cfList) {
            if (!includeMeta) {
                return;
            }
            obj.put(PayloadFields.TABLE, table.getNameAsString());
            obj.put(PayloadFields.NAMESPACE, table.getNamespaceAsString());
            obj.put(PayloadFields.QUALIFIER, table.getQualifierAsString());
            obj.put(PayloadFields.CF, cfList);
            obj.put(PayloadFields.CELLS_TOTAL, totalCells);
        }

        static void addCellsCfIfMeta(Map<String, Object> obj, boolean includeMeta, int cfCells) {
            if (includeMeta) {
                obj.put(PayloadFields.CELLS_CF, cfCells);
            }
        }

        static void addDeleteFlagIfNeeded(Map<String, Object> obj, boolean hasDelete) {
            if (hasDelete) {
                obj.put(PayloadFields.DELETE, 1);
            }
        }

        static void addWalMeta(boolean includeWalMeta,
                               Map<String, Object> obj,
                               long walSeq,
                               long walWriteTime) {
            if (!includeWalMeta) {
                return;
            }
            if (walSeq >= 0L) {
                obj.put(PayloadFields.WAL_SEQ, walSeq);
            }
            if (walWriteTime >= 0L) {
                obj.put(PayloadFields.WAL_WRITE_TIME, walWriteTime);
            }
        }
    }

    /** Утилита записи rowkey в payload. */
    private static final class RowKeyWriter {
        private RowKeyWriter() { }

        static void addRowKeyIfPresent(boolean includeRowKeyPresent,
                                       Map<String, Object> obj,
                                       RowKeySlice rk,
                                       boolean base64,
                                       int salt) {
            if (!includeRowKeyPresent || rk == null || rk.isEmpty()) {
                return;
            }
            final int len = rk.getLength();
            int off = rk.getOffset() + salt;
            int effLen = len - salt;
            if (effLen <= 0) {
                return;
            }
            if (base64) {
                obj.put(PayloadFields.ROWKEY, kz.qazmarka.h2k.util.Bytes.base64(rk.getArray(), off, effLen));
            } else {
                obj.put(PayloadFields.ROWKEY, kz.qazmarka.h2k.util.Bytes.toHex(rk.getArray(), off, effLen));
            }
        }
    }

    private static final class CellStats {
        long maxTs;
        boolean hasDelete;
        int cfCells;
    }
}
