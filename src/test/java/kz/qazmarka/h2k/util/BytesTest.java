package kz.qazmarka.h2k.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BytesTest {

    @Test
    @DisplayName("toHex(): корректно кодирует произвольный срез")
    void toHexSlice() {
        byte[] data = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        String hex = Bytes.toHex(data, 1, 2); // AD BE
        assertEquals("adbe", hex);
    }

    @Test
    @DisplayName("toHex(): возвращает пустую строку для пустого диапазона")
    void toHexEmptySlice() {
        byte[] data = {(byte) 0x00};
        assertEquals("", Bytes.toHex(data, 0, 0));
    }

    @Test
    @DisplayName("toHex(): выбрасывает IndexOutOfBounds при некорректном срезе")
    void toHexOutOfBounds() {
        byte[] data = {0x00, 0x01};
        IndexOutOfBoundsException ex =
                assertThrows(IndexOutOfBoundsException.class, () -> Bytes.toHex(data, 1, 5));
        assertNotNull(ex);
    }
}
