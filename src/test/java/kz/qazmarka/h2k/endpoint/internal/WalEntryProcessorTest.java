package kz.qazmarka.h2k.endpoint.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Юнит‑тесты для внутренних помощников {@link WalEntryProcessor} (initialCapacity и фильтры WAL).
 */
class WalEntryProcessorTest {

    private static Cell cell(String row, String cf, long ts) {
        return new KeyValue(bytes(row), bytes(cf), bytes("q"), ts, bytes("v"));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static int expectedCapacity(int n) {
        if (n <= 0) return 16;
        long t = 1L + (4L * n + 2L) / 3L;
        return (t > (1L << 30)) ? (1 << 30) : (int) t;
    }

    @Test
    @DisplayName("initialCapacity(): граничные и типовые значения")
    void initialCapacity_basic() {
        assertEquals(expectedCapacity(0),  WalEntryProcessor.initialCapacity(0));
        assertEquals(expectedCapacity(1),  WalEntryProcessor.initialCapacity(1));
        assertEquals(expectedCapacity(3),  WalEntryProcessor.initialCapacity(3));
        assertEquals(expectedCapacity(100), WalEntryProcessor.initialCapacity(100));
    }

    @Test
    @DisplayName("initialCapacity(): отрицательные/нулевые и экстремальные значения")
    void initialCapacity_edges() {
        assertEquals(16, WalEntryProcessor.initialCapacity(0));
        assertEquals(16, WalEntryProcessor.initialCapacity(-1));
        assertEquals(16, WalEntryProcessor.initialCapacity(Integer.MIN_VALUE));
        assertEquals(1 << 30, WalEntryProcessor.initialCapacity(Integer.MAX_VALUE));
    }

    @Test
    @DisplayName("passWalTsFilter1(): один CF — true/false по порогу и семейству")
    void filter_oneCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 50L),
                cell("r", "a", 100L)
        );
        byte[] cfA = bytes("a");
        assertTrue(WalEntryProcessor.passWalTsFilter1(cells, cfA, 90L));
        assertTrue(WalEntryProcessor.passWalTsFilter1(cells, cfA, 100L));
        assertFalse(WalEntryProcessor.passWalTsFilter1(cells, cfA, 110L));
        assertFalse(WalEntryProcessor.passWalTsFilter1(cells, bytes("b"), 10L));
        assertTrue(WalEntryProcessor.passWalTsFilter1(cells, cfA, 0L));
    }

    @Test
    @DisplayName("passWalTsFilter2(): комбинация двух CF")
    void filter_twoCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 100L),
                cell("r", "b", 90L),
                cell("r", "c", 120L)
        );
        assertTrue(WalEntryProcessor.passWalTsFilter2(cells, bytes("a"), bytes("b"), 95L));
        assertTrue(WalEntryProcessor.passWalTsFilter2(cells, bytes("a"), bytes("b"), 100L));
        assertFalse(WalEntryProcessor.passWalTsFilter2(cells, bytes("a"), bytes("b"), 130L));
    }

    @Test
    @DisplayName("passWalTsFilterN(): множественный набор CF")
    void filter_manyCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 10L),
                cell("r", "b", 20L),
                cell("r", "c", 30L)
        );
        assertTrue(WalEntryProcessor.passWalTsFilterN(cells, new byte[][]{bytes("a"), bytes("b"), bytes("c")}, 25L));
        assertFalse(WalEntryProcessor.passWalTsFilterN(cells, new byte[][]{bytes("a"), bytes("b"), bytes("c")}, 31L));
        assertTrue(WalEntryProcessor.passWalTsFilterN(cells, new byte[][]{bytes("a"), bytes("c"), bytes("c")}, 5L));
        assertFalse(WalEntryProcessor.passWalTsFilterN(Collections.<Cell>emptyList(), new byte[][]{bytes("a")}, 0L));
    }
}
