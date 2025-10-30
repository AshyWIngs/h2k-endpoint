package kz.qazmarka.h2k.endpoint.processing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
    static final WalCfFilterCache EMPTY = new WalCfFilterCache(null, true, new int[0], new byte[0][][]);

    private final byte[][] sourceRef;
    private final boolean noFamilies;
    private final int[] hashBuckets;
    private final byte[][][] familiesByHash;

    private WalCfFilterCache(byte[][] sourceRef,
                             boolean empty,
                             int[] hashBuckets,
                             byte[][][] familiesByHash) {
        this.sourceRef = sourceRef;
        this.noFamilies = empty;
        this.hashBuckets = hashBuckets;
        this.familiesByHash = familiesByHash;
    }

    boolean isEmpty() {
        return noFamilies;
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
        if (hashBuckets.length == 1 && familiesByHash[0].length == 1) {
            return containsFamily(cells, familiesByHash[0][0]);
        }
        return containsAnyFamily(cells, hashBuckets, familiesByHash);
    }

    static WalCfFilterCache build(byte[][] source) {
        if (source == null || source.length == 0) {
            return EMPTY;
        }
        byte[][] unique = sanitizeFamilies(source);
        if (unique.length == 0) {
            return EMPTY;
        }
        LinkedHashMap<Integer, List<byte[]>> grouped = groupByHash(unique);
        int size = grouped.size();
        int[] hashes = new int[size];
        byte[][][] familiesByHash = new byte[size][][];
        int idx = 0;
        for (java.util.Map.Entry<Integer, List<byte[]>> entry : grouped.entrySet()) {
            hashes[idx] = entry.getKey();
            List<byte[]> families = entry.getValue();
            byte[][] bucket = new byte[families.size()][];
            for (int j = 0; j < families.size(); j++) {
                byte[] fam = families.get(j);
                byte[] copy = new byte[fam.length];
                System.arraycopy(fam, 0, copy, 0, fam.length);
                bucket[j] = copy;
            }
            familiesByHash[idx] = bucket;
            idx++;
        }
        return new WalCfFilterCache(source, false, hashes, familiesByHash);
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

    private static LinkedHashMap<Integer, List<byte[]>> groupByHash(byte[][] families) {
        LinkedHashMap<Integer, List<byte[]>> grouped = new LinkedHashMap<>(families.length);
        for (byte[] family : families) {
            int hash = Bytes.hashCode(family, 0, family.length);
            grouped.computeIfAbsent(hash, h -> new ArrayList<>()).add(family);
        }
        return grouped;
    }

    private static boolean containsFamily(List<Cell> cells, byte[] family) {
        for (Cell cell : cells) {
            if (CellUtil.matchingFamily(cell, family)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAnyFamily(List<Cell> cells,
                                             int[] hashBuckets,
                                             byte[][][] familiesByHash) {
        for (Cell cell : cells) {
            int cellHash = Bytes.hashCode(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            for (int i = 0; i < hashBuckets.length; i++) {
                if (cellHash == hashBuckets[i]) {
                    byte[][] candidates = familiesByHash[i];
                    for (byte[] family : candidates) {
                        if (Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                                family, 0, family.length)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
