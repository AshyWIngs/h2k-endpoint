package kz.qazmarka.h2k.payload.serializer.avro;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
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

    @Test
    @DisplayName("Map значения сериализуются детерминированно по отсортированным ключам")
    void coerceMapDeterministicOrder() throws Exception {
        Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.LONG));
        Schema recordSchema = Schema.createRecord("T_MAP", null, null, false);
        List<Schema.Field> fields = new ArrayList<>(1);
        fields.add(new Schema.Field("metrics", mapSchema, null, (Object) null));
        recordSchema.setFields(fields);

        Map<String, Object> metrics1 = new HashMap<>();
        metrics1.put("b", 2L);
        metrics1.put("a", 1L);
        Map<String, Object> metrics2 = new HashMap<>();
        metrics2.put("a", 1L);
        metrics2.put("b", 2L);

        byte[] payload1 = serializeMapRecord(recordSchema, metrics1);
        byte[] payload2 = serializeMapRecord(recordSchema, metrics2);

        assertArrayEquals(payload1, payload2, "порядок ключей не должен влиять на байты Avro");
        Map<String, Object> coerced = castMap(AvroValueCoercer.coerceValue(mapSchema, metrics1, "metrics"));
        assertEquals(Arrays.asList("a", "b"), new ArrayList<>(coerced.keySet()));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    private static byte[] serializeMapRecord(Schema schema, Map<String, Object> metrics) throws IOException {
        GenericData.Record avroRecord = new GenericData.Record(schema);
        Object coerced = AvroValueCoercer.coerceValue(schema.getField("metrics").schema(), metrics, "metrics");
        avroRecord.put("metrics", coerced);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);
        writer.write(avroRecord, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
