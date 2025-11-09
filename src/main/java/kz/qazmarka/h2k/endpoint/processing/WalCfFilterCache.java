package kz.qazmarka.h2k.endpoint.processing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Кэш разрешённых CF для фильтрации строк WAL на горячем пути.
 * Содержит предвычисленные хеши семейств и позволяет быстро проверять, разрешена ли строка.
 * 
 * Использует интернирование byte[] массивов для часто используемых CF имён,
 * что сокращает allocations при повторных вызовах build() с одинаковыми CF.
 */
final class WalCfFilterCache {
    static final WalCfFilterCache EMPTY = new WalCfFilterCache(null, true, new int[0], new byte[0][][]);
    
    /**
     * Интернированные CF byte[] массивы для переиспользования.
     * Ключ - обёртка с hashCode и equals, значение - канонический byte[].
     */
    private static final ConcurrentHashMap<ByteArrayWrapper, byte[]> INTERNED_CF = new ConcurrentHashMap<>(16);

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
                // Используем интернирование для переиспользования одинаковых CF
                ByteArrayWrapper wrapper = new ByteArrayWrapper(fam);
                byte[] interned = INTERNED_CF.computeIfAbsent(wrapper, k -> {
                    byte[] copy = new byte[k.bytes.length];
                    System.arraycopy(k.bytes, 0, copy, 0, k.bytes.length);
                    return copy;
                });
                bucket[j] = interned;
            }
            familiesByHash[idx] = bucket;
            idx++;
        }
        sortBuckets(hashes, familiesByHash);
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
        Arrays.sort(copy, Bytes.BYTES_COMPARATOR);
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
            int bucketIndex = Arrays.binarySearch(hashBuckets, cellHash);
            if (bucketIndex < 0) {
                continue;
            }
            byte[][] candidates = familiesByHash[bucketIndex];
            for (byte[] family : candidates) {
                if (Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                        family, 0, family.length)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void sortBuckets(int[] hashes, byte[][][] familiesByHash) {
        for (int i = 1; i < hashes.length; i++) {
            int key = hashes[i];
            byte[][] bucket = familiesByHash[i];
            int j = i - 1;
            while (j >= 0 && hashes[j] > key) {
                hashes[j + 1] = hashes[j];
                familiesByHash[j + 1] = familiesByHash[j];
                j--;
            }
            hashes[j + 1] = key;
            familiesByHash[j + 1] = bucket;
        }
    }

    /**
     * Обёртка для byte[] массива с предвычисленным hashCode для эффективного интернирования.
     * Используется как ключ в INTERNED_CF для переиспользования одинаковых CF имён.
     */
    private static final class ByteArrayWrapper {
        final byte[] bytes;
        final int hash;

        ByteArrayWrapper(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Bytes.hashCode(bytes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ByteArrayWrapper)) return false;
            ByteArrayWrapper that = (ByteArrayWrapper) o;
            return hash == that.hash && Bytes.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
