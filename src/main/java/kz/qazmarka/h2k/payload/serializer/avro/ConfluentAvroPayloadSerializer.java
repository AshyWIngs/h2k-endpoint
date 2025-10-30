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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import kz.qazmarka.h2k.config.AvroSettings;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;

/**
 * Сериализует Avro {@link GenericData.Record} в формат Confluent (magic byte + schema id + payload).
 * Регистрация схем выполняется лениво; локальные схемы читаются через {@link AvroSchemaRegistry}.
 * Если Schema Registry временно недоступен, сериализатор использует последний зарегистрированный идентификатор
 * схемы и размещает задачу повторной регистрации в фоновом потоке (с экспоненциальным бэкоффом).
 * Это позволяет не блокировать горячий путь репликации и повышает устойчивость при кратковременных сбоях SR.
 */
public final class ConfluentAvroPayloadSerializer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentAvroPayloadSerializer.class);
    private static final byte MAGIC_BYTE = 0x0;
    private static final int MAGIC_HEADER_LENGTH = 5;
    private static final String STRATEGY_TABLE = "table";
    private static final String SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";
    /** Максимальный размер буфера, который удерживаем в ThreadLocal (байты). */
    private static final int MAX_THREADLOCAL_BUFFER = 1 << 20;
    /** Начальная задержка (мс) между повторными попытками регистрации схемы. */
    private static final long INITIAL_RETRY_DELAY_MS = 1_000L;
    /** Максимальная задержка (мс) между повторными попытками регистрации схемы. */
    private static final long MAX_RETRY_DELAY_MS = 30_000L;
    /** Верхняя граница числа повторных попыток регистрации одной схемы. */
    private static final int MAX_RETRY_ATTEMPTS = 8;
    private static final RetrySettings DEFAULT_RETRY_SETTINGS =
            RetrySettings.defaultSettings(INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS, MAX_RETRY_ATTEMPTS);

    private final AvroSchemaRegistry localRegistry;
    private final List<String> registryUrls;
    private final SchemaRegistryClient schemaRegistryClient;
    private final Map<String, Object> schemaRegistryClientConfig;
    private final int identityMapCapacity;
    private final LongAdder schemaRegisterSuccess = new LongAdder();
    private final LongAdder schemaRegisterFailure = new LongAdder();
    private final String subjectStrategy;
    private final String subjectPrefix;
    private final String subjectSuffix;

    private final AtomicBoolean firstSuccessLogged = new AtomicBoolean();
    private final AtomicBoolean firstFailureLogged = new AtomicBoolean();

    private final ConcurrentHashMap<String, SchemaInfo> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RemoteSchemaFingerprint> remoteFingerprints = new ConcurrentHashMap<>();
    private final RetrySettings retrySettings;
    /**
     * Отдельный исполнитель для повторных попыток регистрации схем. Поток демонический и не блокирует остановку приложения.
     */
    private final ScheduledExecutorService retryExecutor;

    /**
     * @param avroSettings неизменяемые настройки Avro и Schema Registry
     * @param localRegistry локальный реестр Avro-схем
     */
    public ConfluentAvroPayloadSerializer(AvroSettings avroSettings,
                                          AvroSchemaRegistry localRegistry) {
        this(avroSettings, localRegistry, null, DEFAULT_RETRY_SETTINGS);
    }

    /**
     * Расширенный конструктор, позволяющий тестам подменять клиент Schema Registry.
     */
    public ConfluentAvroPayloadSerializer(AvroSettings avroSettings,
                                          AvroSchemaRegistry localRegistry,
                                          SchemaRegistryClient clientOverride) {
        this(avroSettings, localRegistry, clientOverride, DEFAULT_RETRY_SETTINGS);
    }

    ConfluentAvroPayloadSerializer(AvroSettings avroSettings,
                                   AvroSchemaRegistry localRegistry,
                                   SchemaRegistryClient clientOverride,
                                   RetrySettings retrySettings) {
        this.localRegistry = Objects.requireNonNull(localRegistry, "localRegistry");
        Objects.requireNonNull(avroSettings, "avroSettings");
        this.retrySettings = Objects.requireNonNull(retrySettings, "retrySettings");

        this.registryUrls = normalizeRegistryUrls(avroSettings.getRegistryUrls());
        Map<String, String> auth = avroSettings.getRegistryAuth();
        Map<String, String> avroProps = avroSettings.getProperties();

        SubjectSettings subjectSettings = resolveSubjectSettings(avroProps);
        this.subjectStrategy = subjectSettings.strategy;
        this.subjectPrefix = subjectSettings.prefix;
        this.subjectSuffix = subjectSettings.suffix;
        this.identityMapCapacity = resolveIdentityMapCapacity(avroProps);
        this.schemaRegistryClientConfig = buildClientConfig(auth);
        this.schemaRegistryClient = clientOverride != null
                ? clientOverride
                : new CachedSchemaRegistryClient(this.registryUrls, identityMapCapacity, schemaRegistryClientConfig);
        this.retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "h2k-schema-registry-retry");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Сериализует запись в формат Confluent Avro (magic byte + schemaId + payload).
     * Если Schema Registry недоступен, метод пытается использовать локально закешированный идентификатор
     * или последний успешно зарегистрированный fingerprint и параллельно запускает повторную регистрацию
     * во внутреннем планировщике.
     *
     * @param table таблица HBase, по которой вычисляется subject
     * @param avroRecord готовая Avro-запись с ожидаемой схемой
     * @return бинарное представление формата Confluent Avro
     * @throws IllegalStateException если схема неожиданная либо Schema Registry недоступен и нет подходящего кэша
     */
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

    /**
     * Возвращает счётчики регистрации схем: сколько попыток завершилось успехом и сколько — ошибкой.
     * Метод потоко-безопасен и не обнуляет счётчики; значения пригодны для экспонирования в метриках.
     */
    public SchemaRegistryMetrics metrics() {
        return new SchemaRegistryMetrics(schemaRegisterSuccess.sum(), schemaRegisterFailure.sum());
    }

    private SchemaInfo register(TableName table, String subject) {
        String tableKey = table.getNameAsString();
        Schema schema = loadSchema(tableKey);
        long localFingerprint = SchemaNormalization.parsingFingerprint64(schema);
        maybeLogSchemaComparison(subject, localFingerprint);
        SchemaInfo cached = cache.get(subject);
        try {
            int schemaId = schemaRegistryClient.register(subject, schema);
            LOG.debug("Avro Confluent: схема зарегистрирована: subject={}, id={}.", subject, schemaId);
            if (firstSuccessLogged.compareAndSet(false, true)) {
                LOG.debug("Avro Confluent: первая успешная регистрация схемы — subject={}, id={}, urls={}",
                        subject, schemaId, registryUrls);
            }
            remoteFingerprints.put(subject, new RemoteSchemaFingerprint(schemaId, -1, localFingerprint));
            schemaRegisterSuccess.increment();
            return new SchemaInfo(schemaId, schema);
        } catch (RestClientException | IOException ex) {
            schemaRegisterFailure.increment();
            handleRegistrationFailure(subject, schema, localFingerprint, ex);
            RemoteSchemaFingerprint fingerprint = remoteFingerprints.get(subject);
            if (fingerprint != null && fingerprint.fingerprint == localFingerprint) {
                SchemaInfo fallback = new SchemaInfo(fingerprint.schemaId, schema);
                cache.put(subject, fallback);
                LOG.warn("Avro Confluent: использую локально закешированный schemaId={} для subject={} (Schema Registry недоступен)", fingerprint.schemaId, subject);
                return fallback;
            }
            if (cached != null) {
                LOG.warn("Avro Confluent: Schema Registry недоступен, использую ранее зарегистрированный id={} для subject={}, регистрация выполнится в фоне", cached.schemaId, subject);
                return cached;
            }
            throw new IllegalStateException("Avro: не удалось зарегистрировать схему '" + subject + "' — подключение к Schema Registry недоступно", ex);
        } catch (RuntimeException ex) {
            schemaRegisterFailure.increment();
            throw new IllegalStateException("Avro: не удалось зарегистрировать схему '" + subject + "'", ex);
        }
    }

    private Schema loadSchema(String tableKey) {
        try {
            return localRegistry.getByTable(tableKey);
        } catch (IllegalStateException ex) {
            throw new IllegalStateException(
                    "Avro: не удалось прочитать локальную схему для таблицы '" + tableKey + "': " + ex.getMessage(), ex);
        }
    }

    private void maybeLogSchemaComparison(String subject, long localFingerprint) {
        try {
            SchemaMetadata metadata = fetchLatestSchemaMetadata(subject);
            if (metadata == null) {
                schemaRegisterSuccess.increment();
                return;
            }
            long remoteFingerprint = computeRemoteFingerprint(metadata);
            RemoteSchemaFingerprint previous = remoteFingerprints.put(subject,
                    new RemoteSchemaFingerprint(metadata.getId(), metadata.getVersion(), remoteFingerprint));
            schemaRegisterSuccess.increment();
            logFingerprintMismatch(subject, localFingerprint, remoteFingerprint, metadata.getVersion());
            logRemoteFingerprintChange(subject, previous, remoteFingerprint, metadata);
        } catch (RestClientException | IOException | RuntimeException ex) {
            logComparisonFailure(subject, ex);
        }
    }

    private SchemaMetadata fetchLatestSchemaMetadata(String subject) throws RestClientException, IOException {
        return schemaRegistryClient.getLatestSchemaMetadata(subject);
    }

    private long computeRemoteFingerprint(SchemaMetadata metadata) {
        Schema parsed = new Schema.Parser().parse(metadata.getSchema());
        return SchemaNormalization.parsingFingerprint64(parsed);
    }

    private void logFingerprintMismatch(String subject,
                                        long localFingerprint,
                                        long remoteFingerprint,
                                        int remoteVersion) {
        if (remoteFingerprint == localFingerprint) {
            return;
        }
        LOG.warn("Avro Confluent: локальная схема subject={} имеет fingerprint {}, тогда как удалённая {} (version={})",
                subject, localFingerprint, remoteFingerprint, remoteVersion);
    }

    private void logRemoteFingerprintChange(String subject,
                                            RemoteSchemaFingerprint previous,
                                            long remoteFingerprint,
                                            SchemaMetadata metadata) {
        if (previous == null || previous.fingerprint == remoteFingerprint) {
            return;
        }
        LOG.warn("Avro Confluent: fingerprint схемы subject={} изменился: remote={} → {} (id {} → {}, version {} → {})",
                subject,
                previous.fingerprint,
                remoteFingerprint,
                previous.schemaId,
                metadata.getId(),
                previous.version,
                metadata.getVersion());
    }

    private void logComparisonFailure(String subject, Exception ex) {
        if (LOG.isWarnEnabled()) {
            LOG.warn("Avro Confluent: не удалось сравнить fingerprint subject={}: {}", subject, ex.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Трассировка ошибки сравнения fingerprint для subject={}", subject, ex);
        }
    }

    private void handleRegistrationFailure(String subject,
                                            Schema schema,
                                            long localFingerprint,
                                            Exception error) {
        if (firstFailureLogged.compareAndSet(false, true)) {
            LOG.warn("Avro Confluent: первая ошибка регистрации схемы — subject={}, urls={}, error={}",
                    subject, registryUrls, error.getMessage());
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Avro Confluent: повторная ошибка регистрации subject={}: {}", subject, error.getMessage());
        }
        if (!retrySettings.retryEnabled()) {
            LOG.warn("Avro Confluent: повторные попытки регистрации отключены (maxAttempts={}): subject={}",
                    retrySettings.maxAttempts, subject);
            return;
        }
        scheduleRetry(subject, schema, localFingerprint, 1);
    }

    private void scheduleRetry(String subject, Schema schema, long fingerprint, int attempt) {
        long delay = retrySettings.delayMsForAttempt(attempt);
        retryExecutor.schedule(() -> retryRegistration(subject, schema, fingerprint, attempt), delay, TimeUnit.MILLISECONDS);
    }

    private void retryRegistration(String subject, Schema schema, long fingerprint, int attempt) {
        try {
            int schemaId = schemaRegistryClient.register(subject, schema);
            schemaRegisterSuccess.increment();
            cache.compute(subject, (key, existing) -> existing != null ? existing : new SchemaInfo(schemaId, schema));
            remoteFingerprints.put(subject, new RemoteSchemaFingerprint(schemaId, -1, fingerprint));
            LOG.info("Avro Confluent: схема subject={} успешно зарегистрирована после {} попыток", subject, attempt);
        } catch (RestClientException | IOException ex) {
            schemaRegisterFailure.increment();
            if (attempt >= retrySettings.maxAttempts) {
                LOG.error("Avro Confluent: регистрация схемы subject={} окончательно провалилась после {} попыток: {}",
                        subject, attempt, ex.getMessage());
                return;
            }
            LOG.warn("Avro Confluent: повторная регистрация схемы subject={} не удалась (попытка {}): {}",
                    subject, attempt, ex.getMessage());
            scheduleRetry(subject, schema, fingerprint, attempt + 1);
        } catch (RuntimeException ex) {
            schemaRegisterFailure.increment();
            LOG.error("Avro Confluent: критическая ошибка при повторной регистрации схемы subject={}", subject, ex);
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

    private Map<String, Object> buildClientConfig(Map<String, String> auth) {
        Map<String, Object> config = new HashMap<>();
        String user = prop(auth, "basic.username", null);
        String pass = prop(auth, "basic.password", null);
        if (user != null && pass != null) {
            String credentials = user + ':' + pass;
            config.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            config.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG, credentials);
            config.put(SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO, credentials);
        }
        return Collections.unmodifiableMap(config);
    }

    /**
     * Нормализует список адресов Schema Registry: удаляет пустые значения, обрезает пробелы и завершающий слэш.
     * Контракт: хотя бы один валидный URL обязателен; при нарушении выбрасывается {@link IllegalStateException}.
     */
    private static List<String> normalizeRegistryUrls(List<String> urls) {
        if (urls == null || urls.isEmpty()) {
            throw new IllegalStateException("Avro: не заданы адреса Schema Registry (h2k.avro.sr.urls)");
        }
        List<String> normalized = new ArrayList<>(urls.size());
        for (String url : urls) {
            String candidate = normalizeUrlEntry(url);
            if (candidate != null) {
                normalized.add(candidate);
            }
        }
        if (normalized.isEmpty()) {
            throw new IllegalStateException("Avro: список Schema Registry пуст после нормализации");
        }
        return Collections.unmodifiableList(normalized);
    }

    private static String normalizeUrlEntry(String url) {
        if (url == null) {
            return null;
        }
        String trimmed = url.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if (trimmed.endsWith("/")) {
            return trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static SubjectSettings resolveSubjectSettings(Map<String, String> props) {
        String strategy = prop(props, "subject.strategy", STRATEGY_TABLE);
        String prefix = prop(props, "subject.prefix", "");
        String suffix = prop(props, "subject.suffix", "");
        return new SubjectSettings(strategy, prefix, suffix);
    }

    private static int resolveIdentityMapCapacity(Map<String, String> props) {
        return parsePositiveInt(
                prop(props, "client.cache.capacity", null),
                AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
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

    Map<String, Object> clientConfigForTest() {
        return schemaRegistryClientConfig;
    }

    int identityMapCapacityForTest() {
        return identityMapCapacity;
    }

    List<String> registryUrlsForTest() {
        return registryUrls;
    }

    private enum CaseMode { ORIGINAL, UPPER, LOWER }

    private static final class SubjectSettings {
        final String strategy;
        final String prefix;
        final String suffix;

        SubjectSettings(String strategy, String prefix, String suffix) {
            this.strategy = strategy;
            this.prefix = prefix;
            this.suffix = suffix;
        }
    }

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
            ByteArrayOutputStream baos = localBaos.get();
            baos.reset();
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, localEncoder.get());
            localEncoder.set(encoder);
            ByteOptimizedDatumWriter writer = localWriter.get();
            writer.setSchema(schema);
            try {
                writer.write(avroRecord, encoder);
                encoder.flush();
            } catch (IOException | RuntimeException ex) {
                localEncoder.remove();
                localWriter.remove();
                localBaos.remove();
                throw new IllegalStateException("Avro: ошибка сериализации записи: " + ex.getMessage(), ex);
            }
            byte[] result = baos.toByteArray();
            if (result.length > MAX_THREADLOCAL_BUFFER) {
                localBaos.set(new ByteArrayOutputStream(512));
                localEncoder.remove();
            }
            return result;
        }
    }

    private static final class ByteOptimizedDatumWriter extends org.apache.avro.generic.GenericDatumWriter<GenericData.Record> {
        ByteOptimizedDatumWriter(Schema schema) {
            super(schema);
        }

        @Override
        protected void writeBytes(Object datum, Encoder out) throws IOException {
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

    @Override
    public void close() {
        retryExecutor.shutdownNow();
    }

    static final class RetrySettings {
        private final long initialDelayMs;
        private final long maxDelayMs;
        private final int maxAttempts;

        private RetrySettings(long initialDelayMs, long maxDelayMs, int maxAttempts) {
            if (initialDelayMs <= 0) {
                throw new IllegalArgumentException("initialDelayMs должен быть > 0");
            }
            if (maxDelayMs < initialDelayMs) {
                throw new IllegalArgumentException("maxDelayMs не может быть меньше initialDelayMs");
            }
            if (maxAttempts < 0) {
                throw new IllegalArgumentException("maxAttempts должен быть >= 0");
            }
            this.initialDelayMs = initialDelayMs;
            this.maxDelayMs = maxDelayMs;
            this.maxAttempts = maxAttempts;
        }

        static RetrySettings defaultSettings(long initialDelayMs, long maxDelayMs, int maxAttempts) {
            return new RetrySettings(initialDelayMs, maxDelayMs, maxAttempts);
        }

        static RetrySettings forTests(long initialDelayMs, long maxDelayMs, int maxAttempts) {
            return new RetrySettings(initialDelayMs, maxDelayMs, maxAttempts);
        }

        long delayMsForAttempt(int attempt) {
            if (attempt <= 0) {
                return initialDelayMs;
            }
            long exp = initialDelayMs << Math.min(Math.max(attempt - 1, 0), 8);
            return Math.min(maxDelayMs, exp);
        }

        boolean retryEnabled() {
            return maxAttempts > 0;
        }

        int maxAttempts() {
            return maxAttempts;
        }
    }
}
