package kz.qazmarka.h2k.endpoint.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
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

    @Test
    @DisplayName("passCfFilter1(): один CF — true, если встречается целевое семейство")
    void filter_oneCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 50L),
                cell("r", "a", 100L)
        );
        byte[] cfA = bytes("a");
        assertTrue(WalEntryProcessor.passCfFilter1(cells, cfA));
        assertFalse(WalEntryProcessor.passCfFilter1(cells, bytes("b")));
        assertFalse(WalEntryProcessor.passCfFilter1(Collections.<Cell>emptyList(), cfA));
    }

    @Test
    @DisplayName("passCfFilter2(): true при совпадении с любым из двух семейств")
    void filter_twoCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 100L),
                cell("r", "b", 90L),
                cell("r", "c", 120L)
        );
        assertTrue(WalEntryProcessor.passCfFilter2(cells, bytes("a"), bytes("b")), "совпадение по первому CF");
        assertTrue(WalEntryProcessor.passCfFilter2(cells, bytes("x"), bytes("b")), "совпадение по второму CF");
        assertFalse(WalEntryProcessor.passCfFilter2(cells, bytes("x"), bytes("y")), "нет совпадений");
    }

    @Test
    @DisplayName("passCfFilterN(): три и более CF")
    void filter_manyCf() {
        List<Cell> cells = Arrays.asList(
                cell("r", "a", 10L),
                cell("r", "b", 20L),
                cell("r", "c", 30L)
        );
        assertTrue(WalEntryProcessor.passCfFilterN(cells, new byte[][]{bytes("a"), bytes("b"), bytes("c")}));
        assertTrue(WalEntryProcessor.passCfFilterN(cells, new byte[][]{bytes("z"), bytes("b"), bytes("y")}));
        assertFalse(WalEntryProcessor.passCfFilterN(cells, new byte[][]{bytes("x"), bytes("y"), bytes("z")}));
        assertFalse(WalEntryProcessor.passCfFilterN(Collections.<Cell>emptyList(), new byte[][]{bytes("a")}));
    }
}
