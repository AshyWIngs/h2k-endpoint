package kz.qazmarka.h2k.payload.serializer.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AvroValueCoercerTest {

    private static final Schema BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
    private static final Schema NULLABLE_BOOLEAN_SCHEMA =
            Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)));

    @Test
    @DisplayName("Строковые представления boolean мапятся в корректные значения")
    void coerceBooleanFromStrings() {
        assertEquals(Boolean.TRUE, AvroValueCoercer.coerceValue(BOOLEAN_SCHEMA, "true", "flag"));
        assertEquals(Boolean.TRUE, AvroValueCoercer.coerceValue(BOOLEAN_SCHEMA, "TRUE", "flag"));
        assertEquals(Boolean.TRUE, AvroValueCoercer.coerceValue(BOOLEAN_SCHEMA, "1", "flag"));

        assertEquals(Boolean.FALSE, AvroValueCoercer.coerceValue(BOOLEAN_SCHEMA, "false", "flag"));
        assertEquals(Boolean.FALSE, AvroValueCoercer.coerceValue(BOOLEAN_SCHEMA, "0", "flag"));
    }

    @Test
    @DisplayName("Числовые значения 0/1 приводятся к boolean")
    void coerceBooleanFromNumbers() {
        assertEquals(Boolean.TRUE, AvroValueCoercer.coerceValue(BOOLEAN_SCHEMA, 1, "flag"));
        assertEquals(Boolean.FALSE, AvroValueCoercer.coerceValue(BOOLEAN_SCHEMA, 0, "flag"));
    }

    @Test
    @DisplayName("Union с boolean принимает строковые значения")
    void coerceNullableBooleanFromStrings() {
        assertEquals(Boolean.TRUE, AvroValueCoercer.coerceValue(NULLABLE_BOOLEAN_SCHEMA, "true", "flag"));
        assertEquals(Boolean.FALSE, AvroValueCoercer.coerceValue(NULLABLE_BOOLEAN_SCHEMA, "0", "flag"));
        assertNull(AvroValueCoercer.coerceValue(NULLABLE_BOOLEAN_SCHEMA, null, "flag"));
    }

    @Test
    @DisplayName("Некорректные строки приводят к IllegalStateException")
    void coerceBooleanRejectsInvalidStrings() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> AvroValueCoercer.coerceValue(BOOLEAN_SCHEMA, "not-a-bool", "flag"));
        assertTrue(ex.getMessage().contains("BOOLEAN"));
    }
}
