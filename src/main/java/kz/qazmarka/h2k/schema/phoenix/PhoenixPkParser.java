package kz.qazmarka.h2k.schema.phoenix;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.schema.phoenix.PhoenixColumnTypeRegistry.PhoenixType;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Парсер Phoenix rowkey: извлекает значения PK, учитывая соль, фиксированные и варьируемые сегменты.
 */
public final class PhoenixPkParser {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixPkParser.class);

    private final SchemaRegistry registry;
    private final PhoenixColumnTypeRegistry types;

    private static final Set<String> PK_WARNED =
            Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Set<TableColumnKey> PK_COLUMN_WARNED =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * @param registry реестр колонок/PK (Avro, JSON и т.п.)
     * @param types    реестр типов Phoenix с нормализацией
     */
    public PhoenixPkParser(SchemaRegistry registry, PhoenixColumnTypeRegistry types) {
        this.registry = registry;
        this.types = types;
    }

    /**
     * Расшифровывает составной rowkey Phoenix в именованные поля, учитывая соль и разделители.
     *
     * @param table     таблица Phoenix (для логов и разрешения типов)
     * @param rk        исходный rowkey (может быть {@code null})
     * @param saltBytes длина соли в байтах
     * @param out       карта, в которую записываются значения PK
     */
    public void decodeRowKey(TableName table,
                             RowKeySlice rk,
                             int saltBytes,
                             Map<String, Object> out) {
        if (!isDecodable(table, rk, out)) {
            return;
        }

        final String[] pk = registry.primaryKeyColumns(table);
        if (!hasPrimaryKey(pk)) {
            return;
        }

        int start = computeRowKeyStart(rk, saltBytes);
        if (start == Integer.MIN_VALUE) {
            return;
        }

        final int end = rk.getOffset() + rk.getLength();
        final PkCtx ctx = new PkCtx(table, rk.getArray(), end);
        int added = decodePkColumns(pk, ctx, start, out);
        if (added == 0) {
            warnPkNotDecodedOnce(table, pk);
        }
    }

    private boolean isDecodable(TableName table, RowKeySlice rk, Map<String, Object> out) {
        return table != null && rk != null && out != null;
    }

    private boolean hasPrimaryKey(String[] pk) {
        return pk != null && pk.length > 0;
    }

    private int computeRowKeyStart(RowKeySlice rk, int saltBytes) {
        int offset = rk.getOffset();
        int end = offset + rk.getLength();
        int start = offset + Math.max(0, saltBytes);
        return start > end ? Integer.MIN_VALUE : start;
    }

    private int decodePkColumns(String[] pk,
                                PkCtx ctx,
                                int pos,
                                Map<String, Object> out) {
        int added = 0;
        int cursor = pos;
        for (int i = 0; i < pk.length; i++) {
            String column = pk[i];
            if (column == null) {
                onMissingPkColumn(ctx.table, i, added, pk);
                return added;
            }
            PhoenixType type = types.resolve(ctx.table, column);
            boolean isLast = (i == pk.length - 1);
            int before = out.size();
            int newPos = applyPkSegment(ctx, column, type, cursor, isLast, out);
            if (newPos == Integer.MIN_VALUE) {
                break;
            }
            added += (out.size() - before);
            cursor = newPos;
        }
        return added;
    }

    /**
     * Обрабатывает ситуацию, когда имя PK-колонки отсутствует в реестре: пишет предупреждение
     * и фиксирует факт пустого результата (если ничего не распознано).
     */
    private void onMissingPkColumn(TableName table, int index, int added, String[] pk) {
        warnPkColumnDecodeFailure(table, "#" + index, "Имя PK-колонки отсутствует в реестре");
        if (added == 0) {
            warnPkNotDecodedOnce(table, pk);
        }
    }

    /**
     * Парсит один сегмент PK и при успехе помещает значение в {@code out}.
     * Возвращает новый {@code pos}, либо {@link Integer#MIN_VALUE} если разбор невозможно продолжить.
     */
    private int applyPkSegment(PkCtx ctx,
                               String column,
                               PhoenixType t,
                               int pos,
                               boolean isLast,
                               Map<String, Object> out) {
        final ValSeg vs = parsePkValue(ctx.table, column, t, ctx.a, pos, ctx.end, isLast);
        if (vs.ok) {
            out.put(column, vs.val);
            return vs.nextPos;
        }
        warnPkColumnDecodeFailure(ctx.table, column, vs.error);
        if (vs.nextPos <= pos) {
            return Integer.MIN_VALUE;
        }
        return vs.nextPos;
    }

    private ValSeg parsePkValue(TableName table,
                                String column,
                                PhoenixType t,
                                byte[] a,
                                int pos,
                                int end,
                                boolean isLast) {
        final Integer fixed = t.byteSize();
        final Seg seg = (fixed != null)
                ? readFixedSegment(pos, end, fixed)
                : readVarSegment(a, pos, end, isLast);
        if (!seg.ok) {
            return ValSeg.bad(pos, "не удалось выделить сегмент PK для колонки '" + column + "'");
        }
        final Object valueObj = convertBytesToObject(table, column, t, a, seg.off, seg.len, fixed == null);
        if (valueObj == null) {
            return ValSeg.bad(seg.nextPos, "Phoenix не смог преобразовать сегмент PK для колонки '" + column + "'");
        }
        final Object normalized = PhoenixValueNormalizer.normalizeTemporal(valueObj);
        return ValSeg.of(normalized, seg.nextPos);
    }

    private Object convertBytesToObject(TableName table,
                                        String column,
                                        PhoenixType t,
                                        byte[] a,
                                        int off,
                                        int len,
                                        boolean needUnescape) {
        try {
            if (needUnescape) {
                byte[] de = unescapePhoenix(a, off, len);
                return t.toObject(de, 0, de.length);
            }
            return t.toObject(a, off, len);
        } catch (RuntimeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("PK: ошибка преобразования сегмента через Phoenix ({}.{}, type={})", table, column, t, e);
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
        byte[] buffer = new byte[len];
        int readIndex = off;
        int writeIndex = 0;
        int end = off + len;
        while (readIndex < end) {
            int current = a[readIndex] & 0xFF;
            if (isEscapeSequence(current, readIndex, end)) {
                writeIndex = appendEscapedByte(a, buffer, writeIndex, readIndex + 1);
                readIndex += 2;
            } else {
                buffer[writeIndex++] = a[readIndex++];
            }
        }
        return writeIndex == buffer.length ? buffer : trim(buffer, writeIndex);
    }

    private static boolean isEscapeSequence(int currentByte, int index, int end) {
        return currentByte == 0xFF && index + 1 < end;
    }

    /**
     * Копирует байт после escape-последовательности в результирующий буфер.
     *
     * @param source     исходный массив Phoenix rowkey
     * @param target     буфер назначения
     * @param writeIndex текущая позиция записи (не мутируется — метод возвращает смещённый индекс)
     * @param escapeIndex позиция байта после 0xFF-эскейпа
     * @return следующая позиция записи после добавления байтов
     */
    private static int appendEscapedByte(byte[] source, byte[] target, int writeIndex, int escapeIndex) {
        int nextIndex = writeIndex;
        int escapeValue = source[escapeIndex] & 0xFF;
        switch (escapeValue) {
            case 0x00:
                target[nextIndex++] = 0x00;
                break;
            case 0xFF:
                target[nextIndex++] = (byte) 0xFF;
                break;
            default:
                target[nextIndex++] = source[escapeIndex - 1];
                target[nextIndex++] = source[escapeIndex];
                break;
        }
        return nextIndex;
    }

    private static byte[] trim(byte[] buffer, int size) {
        byte[] trimmed = new byte[size];
        System.arraycopy(buffer, 0, trimmed, 0, size);
        return trimmed;
    }

    private static void warnPkNotDecodedOnce(TableName table, String[] pk) {
        final String t = table.getNameWithNamespaceInclAsString();
        if (PK_WARNED.add(t) && LOG.isWarnEnabled()) {
            LOG.warn("PK из rowkey не декодированы для таблицы {}. Определены в Avro-схеме: {}. " +
                    "Проверьте корректность rowkey и настроек соли/типов.", t, Arrays.toString(pk));
        }
    }

    private void warnPkColumnDecodeFailure(TableName table, String column, String message) {
        TableColumnKey key = new TableColumnKey(table.getNameWithNamespaceInclAsString(), column);
        String text = (message == null || message.isEmpty()) ? "неизвестная ошибка" : message;
        if (PK_COLUMN_WARNED.add(key)) {
            LOG.warn("PK: пропускаю колонку {}.{} — {}", table.getNameWithNamespaceInclAsString(), column, text);
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("PK: повтор ошибки декодирования для {}.{} — {}", table.getNameWithNamespaceInclAsString(), column, text);
        }
    }

    static void clearWarnCachesForTest() {
        PK_WARNED.clear();
        PK_COLUMN_WARNED.clear();
    }

    static Set<String> pkWarnedSnapshotForTest() {
        return new HashSet<>(PK_WARNED);
    }

    static Set<WarnedColumn> pkColumnWarnedSnapshotForTest() {
        Set<WarnedColumn> snapshot = new HashSet<>(PK_COLUMN_WARNED.size());
        for (TableColumnKey key : PK_COLUMN_WARNED) {
            snapshot.add(new WarnedColumn(key));
        }
        return snapshot;
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
        private final String error;

        private ValSeg(Object val, int nextPos, boolean ok, String error) {
            this.val = val;
            this.nextPos = nextPos;
            this.ok = ok;
            this.error = error;
        }
        static ValSeg bad(int nextPos, String error) { return new ValSeg(null, nextPos, false, error); }
        static ValSeg of(Object v, int p) { return new ValSeg(v, p, true, null); }
    }

    static final class WarnedColumn {
        private final TableColumnKey key;

        WarnedColumn(TableColumnKey key) {
            this.key = key;
        }

        String table() {
            return key.table();
        }

        String column() {
            return key.column();
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof WarnedColumn)) {
                return false;
            }
            WarnedColumn other = (WarnedColumn) o;
            return key.equals(other.key);
        }
    }

    /** Контекст разбора PK: неизменяемые атрибуты текущего rowkey. */
    private static final class PkCtx {
        final TableName table;
        final byte[] a;
        final int end;
        PkCtx(TableName table, byte[] a, int end) {
            this.table = table;
            this.a = a;
            this.end = end;
        }
    }
}
