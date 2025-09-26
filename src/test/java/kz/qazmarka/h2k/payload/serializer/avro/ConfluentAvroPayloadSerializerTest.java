package kz.qazmarka.h2k.payload.serializer.avro;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.registry.AvroSchemaRegistry;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Тесты режима Confluent Schema Registry (avro-binary).
 */
class ConfluentAvroPayloadSerializerTest {

    private static final TableName TABLE = TableName.valueOf("T_AVRO");
    private static final TableName TABLE_ESCAPE = TableName.valueOf("T_AVRO_ESCAPE");
    private static final Decoder STRING_DECODER = (table, qualifier, value) ->
            value == null ? null : new String(value, StandardCharsets.UTF_8);

    @Test
    @DisplayName("Confluent: бинарный вывод с заголовком magic/id")
    void confluentBinaryProducesHeader() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("{\"id\":7}"));
            server.start();
            String baseUrl = baseUrl(server);

            H2kConfig cfg = buildConfig("avro-binary", baseUrl);
            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            long ts = 555L;
            byte[] rowKey = Bytes.toBytes("rk1");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), ts, Bytes.toBytes("OK"));
            List<Cell> cells = Collections.singletonList(cell);

            byte[] out = builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), ts, ts);

            assertEquals(0x0, out[0]);
            int schemaId = ((out[1] & 0xFF) << 24) | ((out[2] & 0xFF) << 16) | ((out[3] & 0xFF) << 8) | (out[4] & 0xFF);
            assertEquals(7, schemaId);

            byte[] payload = new byte[out.length - 5];
            System.arraycopy(out, 5, payload, 0, payload.length);

            AvroSchemaRegistry registry = new AvroSchemaRegistry(Paths.get("src", "test", "resources", "avro"));
            Schema schema = registry.getByTable(TABLE.getNameAsString());
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            GenericRecord avroRecord = reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
            assertEquals("OK", avroRecord.get("value").toString());

            RecordedRequest request = takeRequest(server, 5, TimeUnit.SECONDS);
            assertNotNull(request, "Schema Registry должен получить запрос на регистрацию");
            assertEquals("/subjects/T_AVRO/versions", request.getPath());
            assertTrue(request.getBody().readUtf8().contains("\"schema\""), "Запрос должен содержать поле schema");

            builder.buildRowPayloadBytes(TABLE, cells, RowKeySlice.whole(rowKey), ts, ts);
            assertNull(takeRequest(server, 200, TimeUnit.MILLISECONDS), "Схема должна регистрироваться один раз");
        }
    }

    @Test
    @DisplayName("Confluent: JSON payload к SR экранирует управляющие символы")
    void confluentSchemaPayloadEscaping() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("{\"id\":9}"));
            server.start();
            String baseUrl = baseUrl(server);

            H2kConfig cfg = buildConfig("avro-binary", baseUrl);
            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            byte[] rowKey = Bytes.toBytes("rk_escape");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("ESC"));
            List<Cell> cells = Collections.singletonList(cell);

            builder.buildRowPayloadBytes(TABLE_ESCAPE, cells, RowKeySlice.whole(rowKey), 1L, 1L);

            RecordedRequest request = takeRequest(server, 5, TimeUnit.SECONDS);
            assertNotNull(request, "Schema Registry должен получить запрос");
            String body = request.getBody().readUtf8();
            int start = body.indexOf("\"schema\":\"");
            assertTrue(start >= 0, "В запросе должно присутствовать поле schema");
            int end = body.indexOf("\"}", start);
            assertTrue(end > start, "Некорректный JSON при регистрации схемы: " + body);
            String schemaEscaped = body.substring(start + "\"schema\":\"".length(), end);
            assertTrue(schemaEscaped.contains("\\n"), "Перевод строки должен быть экранирован");
            assertTrue(schemaEscaped.contains("\\u0001"), "Управляющие символы должны кодироваться через \\uXXXX");
            String schemaRaw = jsonUnescape(schemaEscaped);
            String expectedDoc = "line \"q\"\nnext" + (char) 0x0001;
            Schema parsed = new Schema.Parser().parse(schemaRaw);
            assertEquals(expectedDoc, parsed.getDoc(), "Doc должен восстанавливаться после обратного экранирования");
        }
    }

    @Test
    @DisplayName("Confluent: ошибки Schema Registry пробрасываются")
    void confluentErrorPropagates() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse()
                .setResponseCode(500)
                .setHeader("Content-Type", "application/json")
                .setBody("{\"error_code\":500,\"message\":\"boom\"}"));
            server.start();
            String baseUrl = baseUrl(server);

            H2kConfig cfg = buildConfig("avro-binary", baseUrl);
            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            byte[] rowKey = Bytes.toBytes("rk_fail");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("fail"));
            List<Cell> cells = Collections.singletonList(cell);
            RowKeySlice rowKeySlice = RowKeySlice.whole(rowKey);

            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                    builder.buildRowPayloadBytes(TABLE, cells, rowKeySlice, 1L, 1L)
            );
            assertTrue(ex.getMessage().contains("Schema Registry"));

            RecordedRequest request = takeRequest(server, 5, TimeUnit.SECONDS);
            assertNotNull(request, "Регистрация схемы должна запрашиваться");
            assertEquals("/subjects/T_AVRO/versions", request.getPath());
        }
    }

    @Test
    @DisplayName("Confluent: формат avro-json запрещён")
    void confluentJsonNotAllowed() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.start();
            String baseUrl = baseUrl(server);

            H2kConfig cfg = buildConfig("avro-json", baseUrl);
            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            byte[] rowKey = Bytes.toBytes("rk");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("x"));
            List<Cell> cells = Collections.singletonList(cell);
            RowKeySlice rowKeySlice = RowKeySlice.whole(rowKey);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                    builder.buildRowPayloadBytes(TABLE, cells, rowKeySlice, 1L, 1L)
            );
            assertTrue(ex.getMessage().contains("confluent"));
            assertEquals(0, server.getRequestCount());
        }
    }

    @Test
    @DisplayName("Confluent: стратегия table-upper + suffix формирует ожидаемый subject")
    void confluentSubjectStrategyUpper() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("{\"id\":11}"));
            server.start();
            String baseUrl = baseUrl(server);

            Configuration c = new Configuration(false);
            c.set("h2k.cf.list", "d");
            c.set("h2k.payload.format", "avro-binary");
            c.set("h2k.avro.mode", "confluent");
            c.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
            c.set("h2k.avro.sr.urls", baseUrl);
            c.set("h2k.avro.subject.strategy", "table-upper");
            c.set("h2k.avro.subject.suffix", "-value");

            H2kConfig cfg = H2kConfig.from(c, "dummy:9092");
            PayloadBuilder builder = new PayloadBuilder(STRING_DECODER, cfg);
            byte[] rowKey = Bytes.toBytes("rk_subject");
            Cell cell = new KeyValue(rowKey, Bytes.toBytes("d"), Bytes.toBytes("value"), 1L, Bytes.toBytes("x"));
            builder.buildRowPayloadBytes(TABLE, Collections.singletonList(cell), RowKeySlice.whole(rowKey), 1L, 1L);

            RecordedRequest request = takeRequest(server, 5, TimeUnit.SECONDS);
            assertNotNull(request, "Schema Registry должен получить запрос на регистрацию");
            assertEquals("/subjects/T_AVRO-value/versions", request.getPath());
        }
    }

    private static H2kConfig buildConfig(String format, String srUrl) {
        Configuration c = new Configuration(false);
        c.set("h2k.cf.list", "d");
        c.set("h2k.payload.format", format);
        c.set("h2k.avro.mode", "confluent");
        c.set("h2k.avro.schema.dir", Paths.get("src", "test", "resources", "avro").toAbsolutePath().toString());
        c.set("h2k.avro.sr.urls", srUrl);
        return H2kConfig.from(c, "dummy:9092");
    }

    private static String baseUrl(MockWebServer server) {
        String url = server.url("/").toString();
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static RecordedRequest takeRequest(MockWebServer server, long timeout, TimeUnit unit) {
        try {
            return server.takeRequest(timeout, unit);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            fail("Ожидание запроса прервано", ie);
            return null; // unreachable
        }
    }

    private static String jsonUnescape(String input) {
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (i + 1 >= input.length()) {
                sb.append('\\');
                break;
            }
            char next = input.charAt(++i);
            switch (next) {
                case '"': sb.append('"'); break;
                case '\\': sb.append('\\'); break;
                case '/': sb.append('/'); break;
                case 'b': sb.append('\b'); break;
                case 'f': sb.append('\f'); break;
                case 'n': sb.append('\n'); break;
                case 'r': sb.append('\r'); break;
                case 't': sb.append('\t'); break;
                case 'u':
                    if (i + 4 >= input.length()) {
                        throw new IllegalArgumentException("Неверная escape-последовательность \\u в: " + input);
                    }
                    int code = Integer.parseInt(input.substring(i + 1, i + 5), 16);
                    sb.append((char) code);
                    i += 4;
                    break;
                default:
                    sb.append(next);
            }
        }
        return sb.toString();
    }
}
