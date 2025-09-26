package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.payload.serializer.TableAwarePayloadSerializer;
import kz.qazmarka.h2k.schema.registry.AvroSchemaRegistry;

/**
 * Сериализация Avro через Confluent Schema Registry 5.3.x.
 *
 * Порядок байт: magic byte (0) + int32 schemaId (big-endian) + Avro binary payload.
 */
public final class ConfluentAvroPayloadSerializer implements TableAwarePayloadSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentAvroPayloadSerializer.class);
    private static final byte MAGIC_BYTE = 0x0;
    private static final int MAGIC_HEADER_LENGTH = 5;
    private static final int HTTP_TIMEOUT_MS = 10_000;
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private final AvroSchemaRegistry localRegistry;
    private final List<String> registryUrls;
    private final String basicAuthHeader;
    private final String subjectStrategy;
    private final String subjectPrefix;
    private final String subjectSuffix;

    /** subject -> schema info. */
    private final ConcurrentHashMap<String, SchemaInfo> cache = new ConcurrentHashMap<>();

    public ConfluentAvroPayloadSerializer(H2kConfig cfg) {
        Objects.requireNonNull(cfg, "cfg");
        if (cfg.getAvroMode() != H2kConfig.AvroMode.CONFLUENT) {
            throw new IllegalStateException("Avro: режим '" + cfg.getAvroMode() + "' не является Confluent");
        }

        Path baseDir;
        String dir = cfg.getAvroSchemaDir();
        if (dir == null || dir.trim().isEmpty()) {
            baseDir = Paths.get("conf", "avro");
        } else {
            baseDir = Paths.get(dir.trim());
        }
        this.localRegistry = new AvroSchemaRegistry(baseDir);

        List<String> urls = cfg.getAvroSchemaRegistryUrls();
        if (urls == null || urls.isEmpty()) {
            throw new IllegalStateException("Avro: не заданы адреса Schema Registry (h2k.avro.sr.urls)");
        }
        List<String> normalized = new ArrayList<>(urls.size());
        for (String u : urls) {
            if (u == null || u.trim().isEmpty()) continue;
            String trimmed = u.trim();
            if (trimmed.endsWith("/")) {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
            }
            normalized.add(trimmed);
        }
        if (normalized.isEmpty()) {
            throw new IllegalStateException("Avro: список Schema Registry пуст после нормализации");
        }
        this.registryUrls = Collections.unmodifiableList(normalized);

        Map<String, String> auth = cfg.getAvroSrAuth();
        this.basicAuthHeader = buildBasicAuth(auth);

        Map<String, String> avroProps = cfg.getAvroProps();
        this.subjectStrategy = prop(avroProps, "subject.strategy", "qualifier");
        this.subjectPrefix = prop(avroProps, "subject.prefix", "");
        this.subjectSuffix = prop(avroProps, "subject.suffix", "");
    }

    @Override
    /**
     * Сериализует payload в формат Confluent (magic byte + schema id + бинарный Avro).
     */
    public byte[] serialize(TableName table, Map<String, ?> obj) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(obj, "payload");

        SchemaInfo info = cache.get(table.getNameAsString());
        if (info == null) {
            info = register(table);
            SchemaInfo existing = cache.putIfAbsent(table.getNameAsString(), info);
            if (existing != null) {
                info = existing;
            }
        }

        byte[] payload = info.serializer.serialize(obj);
        byte[] out = new byte[MAGIC_HEADER_LENGTH + payload.length];
        out[0] = MAGIC_BYTE;
        int id = info.schemaId;
        out[1] = (byte) ((id >>> 24) & 0xFF);
        out[2] = (byte) ((id >>> 16) & 0xFF);
        out[3] = (byte) ((id >>> 8) & 0xFF);
        out[4] = (byte) (id & 0xFF);
        System.arraycopy(payload, 0, out, MAGIC_HEADER_LENGTH, payload.length);
        return out;
    }

    @Override
    public String format() {
        return "avro-binary";
    }

    @Override
    public String contentType() {
        return "application/avro-binary";
    }

    /** Регистрирует схему в Confluent SR и возвращает информацию (schemaId + сериализатор). */
    private SchemaInfo register(TableName table) {
        String tableKey = table.getNameAsString();
        Schema schema = loadSchema(tableKey);
        String subject = buildSubject(table);
        int schemaId = registerSchema(subject, schema.toString());
        LOG.debug("Avro Confluent: схема зарегистрирована: subject={}, id={}.", subject, schemaId);
        return new SchemaInfo(schemaId, new AvroSerializer(schema));
    }

    private Schema loadSchema(String tableKey) {
        try {
            return localRegistry.getByTable(tableKey);
        } catch (IllegalStateException ex) {
            throw new IllegalStateException(
                    "Avro: не удалось прочитать локальную схему для таблицы '" + tableKey + "': " + ex.getMessage(), ex);
        }
    }

    private String buildSubject(TableName table) {
        String base;
        String strategy = subjectStrategy.toLowerCase(Locale.ROOT);
        switch (strategy) {
            case "table":
                base = tableNameForSubject(table, CaseMode.ORIGINAL);
                break;
            case "table-upper":
                base = tableNameForSubject(table, CaseMode.UPPER);
                break;
            case "table-lower":
                base = tableNameForSubject(table, CaseMode.LOWER);
                break;
            case "qualifier":
                base = table.getQualifierAsString();
                break;
            default:
                base = table.getQualifierAsString();
        }
        String sanitized = sanitizeSubject(base);
        return subjectPrefix + sanitized + subjectSuffix;
    }

    private enum CaseMode { ORIGINAL, UPPER, LOWER }

    private static String tableNameForSubject(TableName table, CaseMode mode) {
        String ns = table.getNamespaceAsString();
        String qualifier = table.getQualifierAsString();
        String base;
        if (ns != null && !ns.isEmpty() && !"default".equalsIgnoreCase(ns)) {
            base = ns + ":" + qualifier;
        } else {
            base = qualifier;
        }
        switch (mode) {
            case UPPER:
                return base.toUpperCase(Locale.ROOT);
            case LOWER:
                return base.toLowerCase(Locale.ROOT);
            default:
                return base;
        }
    }

    private static String sanitizeSubject(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "subject";
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '-' || c == '_' || c == '.') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        return sb.toString();
    }

    private int registerSchema(String subject, String schemaJson) {
        String payload = buildRegisterPayload(schemaJson);
        IOException last = null;
        for (String baseUrl : registryUrls) {
            try {
                return doRegister(baseUrl, subject, payload);
            } catch (IOException ex) {
                last = ex;
                LOG.warn("Avro Confluent: не удалось зарегистрировать схему в {}: {}", baseUrl, ex.getMessage());
            }
        }
        throw new IllegalStateException("Avro: регистрация схемы в Schema Registry не удалась: " +
                (last == null ? "нет доступных адресов" : last.getMessage()), last);
    }

    private int doRegister(String baseUrl, String subject, String payload) throws IOException {
        String path = baseUrl + "/subjects/" + urlEncode(subject) + "/versions";
        HttpURLConnection conn = openConnection(path);
        try {
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
            if (basicAuthHeader != null) {
                conn.setRequestProperty("Authorization", basicAuthHeader);
            }
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            conn.setFixedLengthStreamingMode(bytes.length);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(bytes);
            }
            int code = conn.getResponseCode();
            String body = readBody(code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream());
            if (code == HttpURLConnection.HTTP_OK || code == HttpURLConnection.HTTP_CREATED) {
                return parseId(body, subject);
            }
            throw new IOException("Ответ " + code + " от Schema Registry: " + body);
        } finally {
            conn.disconnect();
        }
    }

    private static String buildRegisterPayload(String schemaJson) {
        String escaped = escapeForJson(schemaJson);
        StringBuilder sb = new StringBuilder(escaped.length() + 64);
        sb.append('{')
          .append("\"schema\":\"")
          .append(escaped)
          .append("\"}");
        return sb.toString();
    }

    private static String escapeForJson(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 32);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                case '\\':
                    sb.append('\\').append(c);
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20 || c == 0x2028 || c == 0x2029) {
                        sb.append('\\').append('u');
                        appendHex(sb, c);
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    private static void appendHex(StringBuilder sb, int ch) {
        sb.append(HEX[(ch >>> 12) & 0x0F])
          .append(HEX[(ch >>> 8) & 0x0F])
          .append(HEX[(ch >>> 4) & 0x0F])
          .append(HEX[ch & 0x0F]);
    }

    private static int parseId(String body, String subject) {
        if (body == null) {
            throw new IllegalStateException("Avro: пустой ответ Schema Registry при регистрации subject=" + subject);
        }
        int idx = body.indexOf("\"id\"");
        if (idx < 0) {
            throw new IllegalStateException("Avro: ответ Schema Registry не содержит поля id: " + body);
        }
        int colon = body.indexOf(':', idx);
        if (colon < 0) {
            throw new IllegalStateException("Avro: ответ Schema Registry некорректен: " + body);
        }
        int start = colon + 1;
        while (start < body.length() && Character.isWhitespace(body.charAt(start))) start++;
        int end = start;
        while (end < body.length() && Character.isDigit(body.charAt(end))) end++;
        if (start == end) {
            throw new IllegalStateException("Avro: идентификатор схемы не распознан: " + body);
        }
        return Integer.parseInt(body.substring(start, end));
    }

    private static HttpURLConnection openConnection(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(HTTP_TIMEOUT_MS);
        conn.setReadTimeout(HTTP_TIMEOUT_MS);
        return conn;
    }

    private static String readBody(InputStream is) throws IOException {
        if (is == null) {
            return "";
        }
        try (InputStream in = is;
             ByteArrayOutputStream baos = new ByteArrayOutputStream(256)) {
            byte[] buf = new byte[256];
            int r;
            while ((r = in.read(buf)) != -1) {
                baos.write(buf, 0, r);
            }
            return new String(baos.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (java.io.UnsupportedEncodingException ex) {
            throw new IllegalStateException("Avro: не удалось закодировать subject: " + s, ex);
        }
    }

    private static String buildBasicAuth(Map<String, String> props) {
        if (props == null || props.isEmpty()) {
            return null;
        }
        String user = prop(props, "basic.username", null);
        String pass = prop(props, "basic.password", null);
        if (user == null || pass == null) {
            return null;
        }
        String token = user + ":" + pass;
        byte[] encoded = java.util.Base64.getEncoder().encode(token.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded, StandardCharsets.US_ASCII);
    }

    private static String prop(Map<String, String> props, String key, String def) {
        if (props == null) return def;
        String exact = props.get(key);
        if (exact != null) return exact;
        String lowerKey = key.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, String> e : props.entrySet()) {
            if (e.getKey() != null && e.getKey().toLowerCase(Locale.ROOT).equals(lowerKey)) {
                return e.getValue();
            }
        }
        return def;
    }

    /** Простая структура для хранения schemaId и готового сериализатора. */
    private static final class SchemaInfo {
        final int schemaId;
        final AvroSerializer serializer;

        SchemaInfo(int schemaId, AvroSerializer serializer) {
            this.schemaId = schemaId;
            this.serializer = serializer;
        }
    }
}
