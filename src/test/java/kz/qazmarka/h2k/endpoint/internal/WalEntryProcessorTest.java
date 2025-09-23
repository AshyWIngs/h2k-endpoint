package kz.qazmarka.h2k.endpoint.internal;

import java.lang.reflect.Method;
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

    private static Method privateMethod(String name, Class<?>... types) throws Exception {
        Method m = WalEntryProcessor.class.getDeclaredMethod(name, types);
        m.setAccessible(true);
        return m;
    }

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
    void initialCapacity_basic() throws Exception {
        Method m = privateMethod("initialCapacity", int.class);
        assertEquals(expectedCapacity(0),  (Integer) m.invoke(null, 0));
        assertEquals(expectedCapacity(1),  (Integer) m.invoke(null, 1));
        assertEquals(expectedCapacity(3),  (Integer) m.invoke(null, 3));
        assertEquals(expectedCapacity(100),(Integer) m.invoke(null, 100));
    }

    @Test
    @DisplayName("initialCapacity(): отрицательные/нулевые и экстремальные значения")
    void initialCapacity_edges() throws Exception {
        Method m = privateMethod("initialCapacity", int.class);
        assertEquals(16, (Integer) m.invoke(null, 0));
        assertEquals(16, (Integer) m.invoke(null, -1));
        assertEquals(16, (Integer) m.invoke(null, Integer.MIN_VALUE));
        assertEquals(1 << 30, (Integer) m.invoke(null, Integer.MAX_VALUE));
    }

    @Test
    @DisplayName("passWalTsFilter1(): один CF — true/false по порогу и семейству")
    void filter_oneCf() throws Exception {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 50L),
                cell("r", "a", 100L)
        );
        byte[] cfA = bytes("a");
        Method m = privateMethod("passWalTsFilter1", List.class, byte[].class, long.class);
        assertTrue((Boolean) m.invoke(null, cells, cfA, 90L));
        assertTrue((Boolean) m.invoke(null, cells, cfA, 100L));
        assertFalse((Boolean) m.invoke(null, cells, cfA, 110L));
        assertFalse((Boolean) m.invoke(null, cells, bytes("b"), 10L));
        assertTrue((Boolean) m.invoke(null, cells, cfA, 0L));
    }

    @Test
    @DisplayName("passWalTsFilter2(): комбинация двух CF")
    void filter_twoCf() throws Exception {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 100L),
                cell("r", "b", 90L),
                cell("r", "c", 120L)
        );
        Method m = privateMethod("passWalTsFilter2", List.class, byte[].class, byte[].class, long.class);
        assertTrue((Boolean) m.invoke(null, cells, bytes("a"), bytes("b"), 95L));
        assertTrue((Boolean) m.invoke(null, cells, bytes("a"), bytes("b"), 100L));
        assertFalse((Boolean) m.invoke(null, cells, bytes("a"), bytes("b"), 130L));
    }

    @Test
    @DisplayName("passWalTsFilterN(): множественный набор CF")
    void filter_manyCf() throws Exception {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 10L),
                cell("r", "b", 20L),
                cell("r", "c", 30L)
        );
        Method m = privateMethod("passWalTsFilterN", List.class, byte[][].class, long.class);
        assertTrue((Boolean) m.invoke(null, cells, new byte[][]{bytes("a"), bytes("b"), bytes("c")}, 25L));
        assertFalse((Boolean) m.invoke(null, cells, new byte[][]{bytes("a"), bytes("b"), bytes("c")}, 31L));
        assertTrue((Boolean) m.invoke(null, cells, new byte[][]{bytes("a"), bytes("c"), bytes("c")}, 5L));
        assertFalse((Boolean) m.invoke(null, Collections.<Cell>emptyList(), new byte[][]{bytes("a")}, 0L));
    }
}
