package kz.qazmarka.h2k.payload.serializer.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Сериализует Avro {@link GenericData.Record} в формат Confluent (magic byte + schema id + payload).
 * Регистрация схем выполняется лениво; локальные схемы читаются через {@link AvroSchemaRegistry}.
 */
public final class ConfluentAvroPayloadSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentAvroPayloadSerializer.class);
    private static final byte MAGIC_BYTE = 0x0;
    private static final int MAGIC_HEADER_LENGTH = 5;
    private static final String STRATEGY_TABLE = "table";
    private static final String SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";

    private final AvroSchemaRegistry localRegistry;
    private final List<String> registryUrls;
    private final SchemaRegistryClient schemaRegistryClient;
    private final LongAdder schemaRegisterSuccess = new LongAdder();
    private final LongAdder schemaRegisterFailure = new LongAdder();
    private final String subjectStrategy;
    private final String subjectPrefix;
    private final String subjectSuffix;

    private final AtomicBoolean firstSuccessLogged = new AtomicBoolean();
    private final AtomicBoolean firstFailureLogged = new AtomicBoolean();

    private final ConcurrentHashMap<String, SchemaInfo> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RemoteSchemaFingerprint> remoteFingerprints = new ConcurrentHashMap<>();

    public ConfluentAvroPayloadSerializer(H2kConfig cfg,
                                          SchemaRegistryClientFactory factory,
                                          AvroSchemaRegistry localRegistry) {
        this.localRegistry = Objects.requireNonNull(localRegistry, "localRegistry");
        Objects.requireNonNull(cfg, "cfg");
        Objects.requireNonNull(factory, "schemaRegistryClientFactory");

        List<String> urls = cfg.getAvroSchemaRegistryUrls();
        if (urls == null || urls.isEmpty()) {
            throw new IllegalStateException("Avro: не заданы адреса Schema Registry (h2k.avro.sr.urls)");
        }
        List<String> normalized = new ArrayList<>(urls.size());
        for (String u : urls) {
            if (u == null || u.trim().isEmpty()) {
                continue;
            }
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
        Map<String, String> avroProps = cfg.getAvroProps();
        this.subjectStrategy = prop(avroProps, "subject.strategy", STRATEGY_TABLE);
        this.subjectPrefix = prop(avroProps, "subject.prefix", "");
        this.subjectSuffix = prop(avroProps, "subject.suffix", "");
        this.schemaRegistryClient = createClient(factory, auth, avroProps);
    }

    public byte[] serialize(TableName table, GenericData.Record avroRecord) {
        Objects.requireNonNull(table, "не передано имя таблицы");
        Objects.requireNonNull(avroRecord, "record == null");

        final String subject = buildSubject(table);
        SchemaInfo info = cache.get(subject);
        if (info == null) {
            info = register(table, subject);
            SchemaInfo existing = cache.putIfAbsent(subject, info);
            if (existing != null) {
                info = existing;
            }
        }

        if (avroRecord.getSchema() != info.schema) {
            throw new IllegalStateException("Avro: запись для subject '" + subject + "' имеет неожиданную схему: "
                    + avroRecord.getSchema().getFullName());
        }

        byte[] payload = info.writer.write(avroRecord);
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

    public SchemaRegistryMetrics metrics() {
        return new SchemaRegistryMetrics(schemaRegisterSuccess.sum(), schemaRegisterFailure.sum());
    }

    private SchemaInfo register(TableName table, String subject) {
        String tableKey = table.getNameAsString();
        Schema schema = loadSchema(tableKey);
        long localFingerprint = SchemaNormalization.parsingFingerprint64(schema);
        maybeLogSchemaComparison(subject, localFingerprint);
        int schemaId = registerSchema(subject, schema);
        LOG.debug("Avro Confluent: схема зарегистрирована: subject={}, id={}.", subject, schemaId);
        if (firstSuccessLogged.compareAndSet(false, true)) {
            LOG.debug("Avro Confluent: первая успешная регистрация схемы — subject={}, id={}, urls={}",
                    subject, schemaId, registryUrls);
        }
        remoteFingerprints.put(subject, new RemoteSchemaFingerprint(schemaId, -1, localFingerprint));
        return new SchemaInfo(schemaId, schema);
    }

    private Schema loadSchema(String tableKey) {
        try {
            return localRegistry.getByTable(tableKey);
        } catch (IllegalStateException ex) {
            throw new IllegalStateException(
                    "Avro: не удалось прочитать локальную схему для таблицы '" + tableKey + "': " + ex.getMessage(), ex);
        }
    }

    private int registerSchema(String subject, Schema schema) {
        try {
            return schemaRegistryClient.register(subject, schema);
        } catch (RestClientException | IOException e) {
            schemaRegisterFailure.increment();
            if (firstFailureLogged.compareAndSet(false, true)) {
                LOG.warn("Avro Confluent: первая ошибка регистрации схемы — subject={}, urls={}, error={}",
                        subject, registryUrls, e.toString());
            }
            throw new IllegalStateException("Avro: не удалось зарегистрировать схему '" + subject + "'", e);
        } catch (RuntimeException e) {
            schemaRegisterFailure.increment();
            throw new IllegalStateException("Avro: не удалось зарегистрировать схему '" + subject + "'", e);
        }
    }

    private void maybeLogSchemaComparison(String subject, long localFingerprint) {
        try {
            SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
            if (metadata == null) {
                schemaRegisterSuccess.increment();
                return;
            }
            long remoteFingerprint = SchemaNormalization.parsingFingerprint64(new Schema.Parser().parse(metadata.getSchema()));
            RemoteSchemaFingerprint prev = remoteFingerprints.put(subject,
                    new RemoteSchemaFingerprint(metadata.getId(), metadata.getVersion(), remoteFingerprint));
            schemaRegisterSuccess.increment();
            if (remoteFingerprint != localFingerprint) {
                LOG.warn("Avro Confluent: локальная схема subject={} имеет fingerprint {}, тогда как удалённая {} (version={})",
                        subject, localFingerprint, remoteFingerprint, metadata.getVersion());
            }
            if (prev != null && prev.fingerprint != remoteFingerprint) {
                LOG.warn("Avro Confluent: fingerprint схемы subject={} изменился: remote={} → {} (id {} → {}, version {} → {})",
                        subject,
                        prev.fingerprint,
                        remoteFingerprint,
                        prev.schemaId,
                        metadata.getId(),
                        prev.version,
                        metadata.getVersion());
            }
        } catch (RestClientException | IOException | RuntimeException ex) {
            logFingerprintComparisonFailure(subject, ex);
        }
    }

    private String buildSubject(TableName table) {
        String base;
        String strategy = subjectStrategy.toLowerCase(Locale.ROOT);
        switch (strategy) {
            case STRATEGY_TABLE:
                base = tableNameForSubject(table, CaseMode.ORIGINAL);
                break;
            case "table-upper":
                base = tableNameForSubject(table, CaseMode.UPPER);
                break;
            case "table-lower":
                base = tableNameForSubject(table, CaseMode.LOWER);
                break;
            default:
                LOG.warn("Avro Confluent: неизвестная стратегия subject '{}', использую table", subjectStrategy);
                base = tableNameForSubject(table, CaseMode.ORIGINAL);
        }
        return subjectPrefix + base + subjectSuffix;
    }

    private String tableNameForSubject(TableName table, CaseMode mode) {
        String name = table.getNameWithNamespaceInclAsString();
        switch (mode) {
            case UPPER: return name.toUpperCase(Locale.ROOT);
            case LOWER: return name.toLowerCase(Locale.ROOT);
            default: return name;
        }
    }

    private void logFingerprintComparisonFailure(String subject, Exception ex) {
        if (LOG.isWarnEnabled()) {
            LOG.warn("Avro Confluent: не удалось сравнить fingerprint subject={}: {}", subject, ex.toString());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Трассировка ошибки сравнения fingerprint для subject={}", subject, ex);
        }
    }

    private SchemaRegistryClient createClient(SchemaRegistryClientFactory factory,
                                              Map<String, String> auth,
                                              Map<String, String> avroProps) {
        Map<String, Object> clientConfig = new HashMap<>();
        String user = prop(auth, "basic.username", null);
        String pass = prop(auth, "basic.password", null);
        if (user != null && pass != null) {
            String credentials = user + ':' + pass;
            clientConfig.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            clientConfig.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG, credentials);
            clientConfig.put(SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO, credentials);
        }

        int identityMapCapacity = parsePositiveInt(
                prop(avroProps, "client.cache.capacity", null),
                AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);

        return factory.create(registryUrls, clientConfig, identityMapCapacity);
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

    private static int parsePositiveInt(String value, int def) {
        if (value == null) {
            return def;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : def;
        } catch (NumberFormatException ex) {
            return def;
        }
    }

    private enum CaseMode { ORIGINAL, UPPER, LOWER }

    private static final class SchemaInfo {
        final int schemaId;
        final Schema schema;
        final RecordWriter writer;

        SchemaInfo(int schemaId, Schema schema) {
            this.schemaId = schemaId;
            this.schema = schema;
            this.writer = new RecordWriter(schema);
        }
    }

    private static final class RecordWriter {
        private final Schema schema;
        private final ThreadLocal<ByteArrayOutputStream> localBaos =
                ThreadLocal.withInitial(() -> new ByteArrayOutputStream(512));
        private final ThreadLocal<BinaryEncoder> localEncoder = new ThreadLocal<>();
        private final ThreadLocal<ByteOptimizedDatumWriter> localWriter;

        RecordWriter(Schema schema) {
            this.schema = schema;
            this.localWriter = ThreadLocal.withInitial(() -> new ByteOptimizedDatumWriter(schema));
        }

        byte[] write(GenericData.Record avroRecord) {
            try {
                ByteArrayOutputStream baos = localBaos.get();
                baos.reset();
                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, localEncoder.get());
                localEncoder.set(encoder);
                ByteOptimizedDatumWriter writer = localWriter.get();
                writer.setSchema(schema);
                writer.write(avroRecord, encoder);
                encoder.flush();
                return baos.toByteArray();
            } catch (IOException | RuntimeException ex) {
                throw new IllegalStateException("Avro: ошибка сериализации записи: " + ex.getMessage(), ex);
            } finally {
                localBaos.remove();
                localEncoder.remove();
                localWriter.remove();
            }
        }
    }

    private static final class ByteOptimizedDatumWriter extends org.apache.avro.generic.GenericDatumWriter<GenericData.Record> {
        ByteOptimizedDatumWriter(Schema schema) {
            super(schema);
        }

        @Override
        protected void writeBytes(Object datum, Encoder out) throws java.io.IOException {
            if (datum instanceof kz.qazmarka.h2k.payload.builder.BinarySlice) {
                kz.qazmarka.h2k.payload.builder.BinarySlice slice =
                        (kz.qazmarka.h2k.payload.builder.BinarySlice) datum;
                out.writeBytes(slice.array(), slice.offset(), slice.length());
                return;
            }
            super.writeBytes(datum, out);
        }
    }

    private static final class RemoteSchemaFingerprint {
        final int schemaId;
        final int version;
        final long fingerprint;

        RemoteSchemaFingerprint(int schemaId, int version, long fingerprint) {
            this.schemaId = schemaId;
            this.version = version;
            this.fingerprint = fingerprint;
        }
    }

    public static final class SchemaRegistryMetrics {
        private final long registered;
        private final long failures;

        SchemaRegistryMetrics(long registered, long failures) {
            this.registered = registered;
            this.failures = failures;
        }

        public long registeredSchemas() { return registered; }
        public long registrationFailures() { return failures; }
    }
}
