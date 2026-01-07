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
import java.util.concurrent.atomic.AtomicLong;
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
    private final SchemaRegistryAccess schemaRegistry;
    private final Map<String, Object> schemaRegistryClientConfig;
    private final int identityMapCapacity;
    private final int fallbackSchemaId;
    private final LongAdder schemaRegisterSuccess = new LongAdder();
    private final LongAdder schemaRegisterFailure = new LongAdder();
    private final SubjectNamer subjectNamer;
    private final ConcurrentHashMap<String, String> subjectCache = new ConcurrentHashMap<>(16);

    private final AtomicLong lastFailureLoggedAtNano = new AtomicLong(0);
    private static final long FAILURE_LOG_INTERVAL_NANOS = 60_000_000_000L;

    private final ConcurrentHashMap<String, SchemaInfo> cache = new ConcurrentHashMap<>();
    private final RetrySettings retrySettings;
    private final SchemaFingerprintMonitor fingerprintMonitor;
    private final SchemaRegistrationRetrier retrier;
    private volatile int lastSchemaId = -1;

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
        this.fallbackSchemaId = avroSettings.getFallbackSchemaId();
        this.retrySettings = Objects.requireNonNull(retrySettings, "retrySettings");

        this.registryUrls = normalizeRegistryUrls(avroSettings.getRegistryUrls());
        Map<String, String> auth = avroSettings.getRegistryAuth();
        Map<String, String> avroProps = avroSettings.getProperties();

        this.identityMapCapacity = resolveIdentityMapCapacity(avroProps);
        this.schemaRegistryClientConfig = buildClientConfig(auth);
        SchemaRegistryClient resolvedClient = clientOverride != null
                ? clientOverride
                : new CachedSchemaRegistryClient(this.registryUrls, identityMapCapacity, schemaRegistryClientConfig);
        this.schemaRegistry = new SchemaRegistryAccess(resolvedClient);
        this.fingerprintMonitor = new SchemaFingerprintMonitor(LOG, this.schemaRegistry, schemaRegisterSuccess);
        this.retrier = new SchemaRegistrationRetrier(LOG, this.schemaRegistry, fingerprintMonitor, schemaRegisterSuccess, schemaRegisterFailure, this.retrySettings, avroSettings.getMaxPendingRetries());
        SubjectSettings subjectSettings = resolveSubjectSettings(avroProps);
        this.subjectNamer = new SubjectNamer(LOG, subjectSettings.strategy, subjectSettings.prefix, subjectSettings.suffix);
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
        Objects.requireNonNull(table, "Не передано имя таблицы");
        Objects.requireNonNull(avroRecord, "Avro-запись не может быть null");

        final String subject = buildSubject(table);
        SchemaInfo info = cache.get(subject);
        if (info == null) {
            info = register(table, subject);
            SchemaInfo existing = cache.putIfAbsent(subject, info);
            if (existing != null) {
                info = existing;
            }
        }

        if (!isSchemaParsesCompatible(avroRecord.getSchema(), info.schema)) {
            String fieldMismatch = describeFieldMismatch(avroRecord.getSchema(), info.schema);
            LOG.warn("Avro: запись для subject='{}' имеет несовместимую схему: {}.",
                    subject, fieldMismatch);
        }

        lastSchemaId = info.schemaId;
        return info.writer.write(avroRecord, info.schemaId);
    }

    /**
     * Возвращает последний schemaId, использованный при сериализации, или -1 если данных пока нет.
     */
    public int lastSchemaId() {
        return lastSchemaId;
    }

    /**
     * Возвращает последний успешно зарегистрированный fingerprint локальной схемы.
     * Используется для мониторинга и отладки версирования схем.
     */
    public long lastRecordedFingerprint() {
        return fingerprintMonitor.lastRecordedFingerprint();
    }

    /**
     * Возвращает текущий размер кэша схем.
     * Используется для мониторинга состояния кэша через JMX.
     */
    public int schemaCacheSize() {
        return cache.size();
    }

    /**
     * Возвращает счётчики регистрации схем: сколько попыток завершилось успехом и сколько — ошибкой.
     * Метод потоко-безопасен и не обнуляет счётчики; значения пригодны для экспонирования в метриках.
     */
    public SchemaRegistryMetrics metrics() {
        return new SchemaRegistryMetrics(schemaRegisterSuccess.sum(), schemaRegisterFailure.sum());
    }

    /**
     * Принудительно загружает локальные Avro-схемы в кэш {@link AvroSchemaRegistry}.
     *
     * @return число новых схем, добавленных в кэш локального реестра
     */
    public int preloadLocalSchemas() {
        return localRegistry.preloadAll();
    }

    private SchemaInfo register(TableName table, String subject) {
        String tableKey = table.getNameAsString();
        Schema schema = loadSchema(tableKey);
        long localFingerprint = SchemaNormalization.parsingFingerprint64(schema);
        fingerprintMonitor.observeRemoteFingerprint(subject, localFingerprint);
        SchemaInfo cached = cache.get(subject);
        try {
            int schemaId = schemaRegistry.register(subject, schema);
            LOG.debug("Avro Confluent: схема зарегистрирована: subject={}, id={}.", subject, schemaId);
            LOG.debug("Avro Confluent: первая успешная регистрация схемы — subject={}, id={}, urls={}",
                    subject, schemaId, registryUrls);
            fingerprintMonitor.recordSuccessfulRegistration(subject, localFingerprint, schemaId);
            schemaRegisterSuccess.increment();
            return new SchemaInfo(schemaId, schema);
        } catch (RestClientException ex) {
            return handleRestException(subject, schema, localFingerprint, cached, ex);
        } catch (IOException ex) {
            return handleIoException(subject, schema, localFingerprint, cached, ex);
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

    // Удалены вспомогательные приватные методы (логика инлайнена выше) для устранения предупреждений PMD.
    /**
     * После успешной регистрации в фоне обновляет локальный кэш идентификаторов схем.
     */
    private void handleRetrySuccess(String subject, Schema schema, int schemaId) {
        cache.compute(subject, (key, existing) -> existing != null ? existing : new SchemaInfo(schemaId, schema));
    }

    private String buildSubject(TableName table) {
        String key = table.getNameWithNamespaceInclAsString();
        String cached = subjectCache.get(key);
        if (cached != null) {
            return cached;
        }
        String created = subjectNamer.subjectFor(table);
        String race = subjectCache.putIfAbsent(key, created);
        return race != null ? race : created;
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
        String strategy = prop(props, "subject.strategy", SubjectNamer.DEFAULT_STRATEGY);
        String prefix = prop(props, "subject.prefix", "");
        String suffix = prop(props, "subject.suffix", "");
        return new SubjectSettings(strategy, prefix, suffix);
    }

    private SchemaInfo tryFallback(String subject,
                                   Schema schema,
                                   long localFingerprint,
                                   SchemaInfo cached,
                                   String reason) {
        SchemaFingerprintMonitor.RemoteSchemaFingerprint knownFingerprint = fingerprintMonitor.knownFingerprint(subject);
        if (knownFingerprint != null && knownFingerprint.fingerprint == localFingerprint) {
            SchemaInfo fallback = new SchemaInfo(knownFingerprint.schemaId, schema);
            cache.put(subject, fallback);
            LOG.warn("Avro Confluent: использую локально закешированный schemaId={} для subject={} ({}).",
                    knownFingerprint.schemaId, subject, reason);
            return fallback;
        }
        if (cached != null && cached.schema == schema) {
            LOG.warn("Avro Confluent: Schema Registry недоступен, использую ранее зарегистрированный id={} для subject={} ({}).",
                    cached.schemaId, subject, reason);
            return cached;
        }
        return null;
    }

    private void logRegisterFailure(String subject, String error) {
        long now = System.nanoTime();
        long last = lastFailureLoggedAtNano.get();
        if (now >= last + FAILURE_LOG_INTERVAL_NANOS && lastFailureLoggedAtNano.compareAndSet(last, now)) {
            LOG.warn("Avro Confluent: ошибка регистрации схемы — subject={}, urls={}, {}",
                    subject, registryUrls, error);
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Avro Confluent: ошибка регистрации subject={}: {}", subject, error);
        }
    }

    private SchemaInfo handleRestException(String subject,
                                           Schema schema,
                                           long localFingerprint,
                                           SchemaInfo cached,
                                           RestClientException ex) {
        schemaRegisterFailure.increment();
        String error = SchemaRegistryErrors.summary(ex);
        logRegisterFailure(subject, error);
        if (SchemaRegistryErrors.isAuthError(ex)) {
            throw new IllegalStateException("Avro: отказ в доступе к Schema Registry при регистрации '" + subject
                    + "' (" + error + ")", ex);
        }
        if (SchemaRegistryErrors.isIncompatible(ex)) {
            throw new IllegalStateException("Avro: схема '" + subject
                    + "' несовместима с настройками Schema Registry (" + error + ")", ex);
        }
        if (SchemaRegistryErrors.isInvalidSchema(ex)) {
            throw new IllegalStateException("Avro: схема '" + subject
                    + "' не проходит проверку Schema Registry (" + error + ")", ex);
        }
        if (!retrySettings.retryEnabled()) {
            throw new IllegalStateException("Avro: не удалось зарегистрировать схему '" + subject
                    + "' — повторные попытки отключены", ex);
        }
        if (!SchemaRegistryErrors.isRetryable(ex)) {
            throw new IllegalStateException("Avro: не удалось зарегистрировать схему '" + subject
                    + "' — ошибка Schema Registry (" + error + ")", ex);
        }
        SchemaInfo fallback = tryFallback(subject, schema, localFingerprint, cached, error);
        if (fallback != null) {
            retrier.schedule(subject, schema, localFingerprint, this::handleRetrySuccess);
            return fallback;
        }
        LOG.warn("Avro Confluent: Schema Registry недоступен и нет кэша; использую fallbackSchemaId={} для subject='{}'",
                fallbackSchemaId, subject);
        retrier.schedule(subject, schema, localFingerprint, this::handleRetrySuccess);
        SchemaInfo fallbackInfo = new SchemaInfo(fallbackSchemaId, schema);
        cache.putIfAbsent(subject, fallbackInfo);
        return fallbackInfo;
    }

    private SchemaInfo handleIoException(String subject,
                                         Schema schema,
                                         long localFingerprint,
                                         SchemaInfo cached,
                                         IOException ex) {
        schemaRegisterFailure.increment();
        logRegisterFailure(subject, ex.getMessage());
        if (!retrySettings.retryEnabled()) {
            throw new IllegalStateException("Avro: не удалось зарегистрировать схему '" + subject
                    + "' — повторные попытки отключены", ex);
        }
        SchemaInfo fallback = tryFallback(subject, schema, localFingerprint, cached, ex.getMessage());
        if (fallback != null) {
            retrier.schedule(subject, schema, localFingerprint, this::handleRetrySuccess);
            return fallback;
        }
        LOG.warn("Avro Confluent: ошибка подключения к Schema Registry; использую fallbackSchemaId={} для subject='{}'",
                fallbackSchemaId, subject);
        retrier.schedule(subject, schema, localFingerprint, this::handleRetrySuccess);
        SchemaInfo fallbackInfo = new SchemaInfo(fallbackSchemaId, schema);
        cache.putIfAbsent(subject, fallbackInfo);
        return fallbackInfo;
    }

    private static int resolveIdentityMapCapacity(Map<String, String> props) {
        return parsePositiveInt(
                prop(props, "client.cache.capacity", null),
                AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
    }

    /**
     * Проверяет совместимость двух Avro схем (вместо строгого == сравнения).
     */
    private static boolean isSchemaParsesCompatible(Schema recordSchema, Schema serializerSchema) {
        return recordSchema == serializerSchema ||
               (recordSchema != null && serializerSchema != null &&
                recordSchema.getFullName().equals(serializerSchema.getFullName()) &&
                recordSchema.getNamespace().equals(serializerSchema.getNamespace()));
    }

    /**
     * Описывает расхождения между двумя схемами для логирования.
     */
    private static String describeFieldMismatch(Schema recordSchema, Schema serializerSchema) {
        if (recordSchema == null || serializerSchema == null) {
            return "одна из схем=null";
        }
        if (!recordSchema.getFullName().equals(serializerSchema.getFullName())) {
            return "fullName: '" + recordSchema.getFullName() + "' vs '" + serializerSchema.getFullName() + "'";
        }
        List<Schema.Field> recordFields = recordSchema.getFields();
        List<Schema.Field> serializerFields = serializerSchema.getFields();
        if (recordFields.size() != serializerFields.size()) {
            return "количество полей: " + recordFields.size() + " vs " + serializerFields.size();
        }
        String fieldDiff = compareFields(recordFields, serializerFields);
        return fieldDiff != null ? fieldDiff : "несовместимые типы полей";
    }

    /**
     * Сравнивает списки полей двух схем; возвращает описание расхождений или null если поля идентичны.
     */
    private static String compareFields(List<Schema.Field> recordFields, List<Schema.Field> serializerFields) {
        StringBuilder diff = new StringBuilder();
        for (int i = 0; i < recordFields.size(); i++) {
            Schema.Field rf = recordFields.get(i);
            Schema.Field sf = serializerFields.get(i);
            if (!rf.name().equals(sf.name()) || !rf.schema().equals(sf.schema())) {
                if (diff.length() > 0) diff.append("; ");
                diff.append("[").append(i).append("]='").append(rf.name()).append("' vs '").append(sf.name()).append("'");
            }
        }
        return diff.length() > 0 ? "поля отличаются: " + diff.toString() : null;
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
        private final ThreadLocal<HeaderByteArrayOutputStream> localBaos =
                ThreadLocal.withInitial(() -> new HeaderByteArrayOutputStream(512 + MAGIC_HEADER_LENGTH));
        private final ThreadLocal<BinaryEncoder> localEncoder = new ThreadLocal<>();
        private final ThreadLocal<ByteOptimizedDatumWriter> localWriter;

        RecordWriter(Schema schema) {
            this.schema = schema;
            this.localWriter = ThreadLocal.withInitial(() -> new ByteOptimizedDatumWriter(schema));
        }

        byte[] write(GenericData.Record avroRecord, int schemaId) {
            HeaderByteArrayOutputStream baos = localBaos.get();
            baos.resetWithHeader(MAGIC_HEADER_LENGTH);
            byte[] buffer = baos.buffer();
            buffer[0] = MAGIC_BYTE;
            buffer[1] = (byte) ((schemaId >>> 24) & 0xFF);
            buffer[2] = (byte) ((schemaId >>> 16) & 0xFF);
            buffer[3] = (byte) ((schemaId >>> 8) & 0xFF);
            buffer[4] = (byte) (schemaId & 0xFF);
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
                localBaos.set(new HeaderByteArrayOutputStream(512 + MAGIC_HEADER_LENGTH));
                localEncoder.remove();
            }
            return result;
        }
    }

    private static final class HeaderByteArrayOutputStream extends ByteArrayOutputStream {
        HeaderByteArrayOutputStream(int size) {
            super(size);
        }

        void resetWithHeader(int headerLength) {
            if (buf.length < headerLength) {
                buf = new byte[headerLength];
            }
            count = headerLength;
        }

        byte[] buffer() {
            return buf;
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

    /**
     * Возвращает текущее количество ожидающих задач повторной регистрации схемы.
     * Используется для gauge-метрики мониторинга состояния Schema Registry.
     *
     * @return количество активных/ожидающих повторных попыток регистрации
     */
    public long getPendingSchemaRetriesCount() {
        return retrier.getPendingRetriesCount();
    }

    @Override
    public void close() {
        retrier.close();
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
