package kz.qazmarka.h2k.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.schema.types.PDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Парсер Phoenix rowkey: извлекает значения PK, учитывая соль, фиксированные и варьируемые сегменты.
 */
final class PhoenixPkParser {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixPkParser.class);

    private final SchemaRegistry registry;
    private final PhoenixColumnTypeRegistry types;

    private static final Set<String> PK_WARNED =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    PhoenixPkParser(SchemaRegistry registry, PhoenixColumnTypeRegistry types) {
        this.registry = registry;
        this.types = types;
    }

    void decodeRowKey(TableName table,
                      RowKeySlice rk,
                      int saltBytes,
                      Map<String, Object> out) {
        if (table == null || rk == null || out == null) return;

        final String[] pk = registry.primaryKeyColumns(table);
        if (pk == null || pk.length == 0) return;

        final byte[] a = rk.getArray();
        int pos = rk.getOffset() + Math.max(0, saltBytes);
        final int end = rk.getOffset() + rk.getLength();
        if (pos > end) return;

        int added = 0;
        for (int i = 0; i < pk.length; i++) {
            final String col = pk[i];
            if (col == null) return;

            final PDataType<?> t = types.resolve(table, col);
            final boolean isLast = (i == pk.length - 1);
            final ValSeg vs = parsePkValue(t, a, pos, end, isLast);
            if (!vs.ok) return;
            pos = vs.nextPos;
            out.put(col, vs.val);
            added++;
        }
        if (pk.length > 0 && added == 0) {
            warnPkNotDecodedOnce(table, pk);
        }
    }

    private ValSeg parsePkValue(PDataType<?> t,
                                byte[] a, int pos, int end, boolean isLast) {
        final Integer fixed = t.getByteSize();
        final Seg seg = (fixed != null)
                ? readFixedSegment(pos, end, fixed)
                : readVarSegment(a, pos, end, isLast);
        if (!seg.ok) return ValSeg.bad();
        final Object valueObj = convertBytesToObject(t, a, seg.off, seg.len, fixed == null);
        if (valueObj == null) return ValSeg.bad();
        final Object normalized = PhoenixValueNormalizer.normalizeTemporal(valueObj);
        return ValSeg.of(normalized, seg.nextPos);
    }

    private Object convertBytesToObject(PDataType<?> t,
                                        byte[] a, int off, int len, boolean needUnescape) {
        try {
            if (needUnescape) {
                byte[] de = unescapePhoenix(a, off, len);
                return t.toObject(de, 0, de.length);
            }
            return t.toObject(a, off, len);
        } catch (RuntimeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("PK: ошибка преобразования сегмента через Phoenix (type={})", t, e);
            }
            return null;
        }
    }

    private static Seg readFixedSegment(int pos, int end, int fixed) {
        if (pos + fixed > end) return Seg.bad();
        return Seg.of(pos, fixed, pos + fixed);
    }

    private static Seg readVarSegment(byte[] a, int pos, int end, boolean isLast) {
        if (isLast) {
            int len = Math.max(0, end - pos);
            return Seg.of(pos, len, end);
        }
        int sep = findUnescapedSeparator(a, pos, end);
        if (sep < 0) return Seg.bad();
        int len = sep - pos;
        return Seg.of(pos, len, sep + 1);
    }

    private static int findUnescapedSeparator(byte[] a, int from, int end) {
        int i = from;
        while (i < end) {
            int b = a[i] & 0xFF;
            if (b == 0x00) {
                if (i == from) return i;
                if ((a[i - 1] & 0xFF) != 0xFF) return i;
            }
            i++;
        }
        return -1;
    }

    private static byte[] unescapePhoenix(byte[] a, int off, int len) {
        if (len <= 0) return new byte[0];
        byte[] r = new byte[len];
        int w = 0;
        int i = off;
        int end = off + len;
        while (i < end) {
            int b = a[i] & 0xFF;
            if (b == 0xFF && i + 1 < end) {
                int n = a[i + 1] & 0xFF;
                switch (n) {
                    case 0x00:
                        r[w++] = 0x00;
                        i += 2;
                        break;
                    case 0xFF:
                        r[w++] = (byte) 0xFF;
                        i += 2;
                        break;
                    default:
                        r[w++] = a[i];
                        i += 1;
                        break;
                }
            } else {
                r[w++] = a[i];
                i += 1;
            }
        }
        if (w == r.length) return r;
        byte[] shrunk = new byte[w];
        System.arraycopy(r, 0, shrunk, 0, w);
        return shrunk;
    }

    private static void warnPkNotDecodedOnce(TableName table, String[] pk) {
        final String t = table.getNameWithNamespaceInclAsString();
        if (PK_WARNED.add(t) && LOG.isWarnEnabled()) {
            LOG.warn("PK из rowkey не декодированы для таблицы {}. Объявлены в schema.json: {}. " +
                    "Проверьте корректность rowkey и настроек соли/типов.", t, Arrays.toString(pk));
        }
    }

    private static final class Seg {
        final int off;
        final int len;
        final int nextPos;
        final boolean ok;
        private Seg(int off, int len, int nextPos, boolean ok) {
            this.off = off;
            this.len = len;
            this.nextPos = nextPos;
            this.ok = ok;
        }
        static Seg bad() { return new Seg(0, 0, 0, false); }
        static Seg of(int off, int len, int nextPos) { return new Seg(off, len, nextPos, true); }
    }

    private static final class ValSeg {
        final Object val;
        final int nextPos;
        final boolean ok;
        private ValSeg(Object val, int nextPos, boolean ok) {
            this.val = val;
            this.nextPos = nextPos;
            this.ok = ok;
        }
        static ValSeg bad() { return new ValSeg(null, 0, false); }
        static ValSeg of(Object v, int p) { return new ValSeg(v, p, true); }
    }
}
