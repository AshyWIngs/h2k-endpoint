package kz.qazmarka.h2k.endpoint.processing;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Кэш разрешённых CF для фильтрации строк WAL на горячем пути.
 * Содержит предвычисленные хеши семейств и позволяет быстро проверять, разрешена ли строка.
 */
final class WalCfFilterCache {
    static final WalCfFilterCache EMPTY = new WalCfFilterCache(null, null, null);

    private final byte[][] sourceRef;
    private final byte[][] families;
    private final int[] hashes;

    private WalCfFilterCache(byte[][] sourceRef, byte[][] families, int[] hashes) {
        this.sourceRef = sourceRef;
        this.families = families;
        this.hashes = hashes;
    }

    boolean isEmpty() {
        return families == null || families.length == 0;
    }

    boolean matches(byte[][] candidate) {
        return Objects.equals(candidate, sourceRef)
                || (isEmpty() && (candidate == null || candidate.length == 0));
    }

    boolean allows(List<Cell> cells) {
        if (isEmpty()) {
            return true;
        }
        if (cells.isEmpty()) {
            return false;
        }
        int count = families.length;
        if (count == 1) {
            return containsFamily(cells, families[0]);
        }
        if (count == 2) {
            return containsFamily(cells, families[0]) || containsFamily(cells, families[1]);
        }
        return containsAnyFamily(cells, families, hashes);
    }

    static WalCfFilterCache build(byte[][] source) {
        if (source == null || source.length == 0) {
            return EMPTY;
        }
        byte[][] unique = sanitizeFamilies(source);
        if (unique.length == 0) {
            return EMPTY;
        }
        int[] hashes = computeHashes(unique);
        return new WalCfFilterCache(source, unique, hashes);
    }

    private static byte[][] sanitizeFamilies(byte[][] source) {
        ArrayList<byte[]> sanitized = new ArrayList<>(source.length);
        for (byte[] cf : source) {
            if (cf != null && cf.length > 0) {
                sanitized.add(cf);
            }
        }
        if (sanitized.isEmpty()) {
            return new byte[0][];
        }
        byte[][] copy = sanitized.toArray(new byte[0][]);
        java.util.Arrays.sort(copy, Bytes.BYTES_COMPARATOR);
        return deduplicate(copy);
    }

    private static byte[][] deduplicate(byte[][] sorted) {
        int uniqueCount = 1;
        for (int i = 1; i < sorted.length; i++) {
            if (!Bytes.equals(sorted[i], sorted[i - 1])) {
                uniqueCount++;
            }
        }
        byte[][] unique = new byte[uniqueCount][];
        int idx = 0;
        unique[idx++] = sorted[0];
        for (int i = 1; i < sorted.length; i++) {
            if (!Bytes.equals(sorted[i], sorted[i - 1])) {
                unique[idx++] = sorted[i];
            }
        }
        return unique;
    }

    static boolean containsFamily(List<Cell> cells, byte[] family) {
        for (Cell cell : cells) {
            if (CellUtil.matchingFamily(cell, family)) {
                return true;
            }
        }
        return false;
    }

    static boolean containsAnyFamily(List<Cell> cells, byte[][] families, int[] hashes) {
        for (Cell cell : cells) {
            int cellHash = Bytes.hashCode(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            for (int i = 0; i < families.length; i++) {
                if (cellHash == hashes[i]
                        && Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                        families[i], 0, families[i].length)) {
                    return true;
                }
            }
        }
        return false;
    }

    static int[] computeHashes(byte[][] families) {
        int[] hashes = new int[families.length];
        for (int i = 0; i < families.length; i++) {
            byte[] cf = families[i];
            hashes[i] = cf == null ? 0 : Bytes.hashCode(cf, 0, cf.length);
        }
        return hashes;
    }
}
