package kz.qazmarka.h2k.payload.serializer.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ConfluentRecordWriterTest {

    @Test
    @DisplayName("Запись map детерминирована по ключам")
    void mapSerializationIsDeterministic() {
        String schemaJson = "{" +
                "\"type\":\"record\"," +
                "\"name\":\"MapRecord\"," +
                "\"namespace\":\"kz.qazmarka.h2k.test\"," +
                "\"fields\":[{" +
                    "\"name\":\"id\",\"type\":\"int\"},{" +
                    "\"name\":\"tags\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}" +
                "]" +
            "}";
        Schema schema = new Schema.Parser().parse(schemaJson);
        ConfluentRecordWriter writer = new ConfluentRecordWriter(schema, (byte) 0x0, 5, 1 << 20);

        GenericData.Record r1 = new GenericData.Record(schema);
        r1.put("id", 1);
        Map<String, String> tags1 = new HashMap<>();
        tags1.put("b", "2");
        tags1.put("a", "1");
        r1.put("tags", tags1);

        GenericData.Record r2 = new GenericData.Record(schema);
        r2.put("id", 1);
        Map<String, String> tags2 = new HashMap<>();
        tags2.put("a", "1");
        tags2.put("b", "2");
        r2.put("tags", tags2);

        byte[] s1 = writer.write(r1, 10);
        byte[] s2 = writer.write(r2, 10);

        assertArrayEquals(s1, s2, "Сериализация должна быть стабильной независимо от порядка вставки ключей map");
    }
}
