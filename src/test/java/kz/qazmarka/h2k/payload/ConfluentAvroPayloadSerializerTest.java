package kz.qazmarka.h2k.payload;

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
import kz.qazmarka.h2k.schema.AvroSchemaRegistry;
import kz.qazmarka.h2k.schema.Decoder;
import kz.qazmarka.h2k.util.RowKeySlice;

/**
 * Тесты режима Confluent Schema Registry (avro-binary).
 */
class ConfluentAvroPayloadSerializerTest {

    private static final TableName TABLE = TableName.valueOf("T_AVRO");
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
}
