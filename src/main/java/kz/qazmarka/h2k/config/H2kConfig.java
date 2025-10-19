package kz.qazmarka.h2k.config;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;
import kz.qazmarka.h2k.util.Parsers;

/**
 * Иммутабельная конфигурация эндпоинта, прочитанная один раз из HBase {@link Configuration}.
 *
 * Содержит:
 *  - Базовые параметры Kafka и ограничение длины имени топика;
 *  - Параметры Avro/Schema Registry для сериализации в формат Confluent;
 *  - Параметры ожидания подтверждений отправок (awaitEvery/awaitTimeoutMs);
 *  - Параметры автосоздания топиков (партиции/репликация/таймаут/backoff), client.id для AdminClient и произвольные topic-level конфиги;
 *  - Табличные переопределения «соли» rowkey: параметр {@code h2k.salt.map} (TABLE[:BYTES]) и подсказки ёмкости {@code h2k.capacity.hint.*}.
 *
 * Все поля неизменяемые (иммутабельные).
 */
public final class H2kConfig implements TableMetadataView {
    /**
     * Дефолтный лимит длины имени Kafka‑топика (символов).
     * Значение 249 совместимо со старыми версиями брокеров Kafka.
     */
    static final int DEFAULT_TOPIC_MAX_LENGTH = 249;
    /** Плейсхолдер в шаблоне топика: будет заменён на "<namespace>_<qualifier>". */
    static final String PLACEHOLDER_TABLE = "${table}";
    /** Плейсхолдер в шаблоне топика: будет заменён на имя namespace таблицы. */
    static final String PLACEHOLDER_NAMESPACE = "${namespace}";
    /** Плейсхолдер в шаблоне топика: будет заменён на qualifier (имя таблицы без namespace). */
    static final String PLACEHOLDER_QUALIFIER = "${qualifier}";
    /** Имя namespace HBase по умолчанию. */
    static final String HBASE_DEFAULT_NS = "default";

    // ==== Дополнительные ключи конфигурации (для формата и AVRO) ====
    // Индивидуальные AVRO-ключи публикуются через внутренний класс Keys; здесь оставляем только общий префикс.
    /** Префикс для всех AVRO-настроек */
    static final String K_AVRO_PREFIX              = "h2k.avro.";
    /** Ключ каталога локальных Avro-схем. */
    static final String K_AVRO_SCHEMA_DIR          = "h2k.avro.schema.dir";
    /** Основной ключ списка URL Schema Registry (через запятую). */
    static final String K_AVRO_SR_URLS             = "h2k.avro.sr.urls";
    /** Алиас для совместимости: конфигурация могла использовать schema.registry без .urls. */
    static final String K_AVRO_SR_URLS_LEGACY      = "h2k.avro.schema.registry";
    /** Алиас c единственным URL. */
    static final String K_AVRO_SR_URL_LEGACY       = "h2k.avro.schema.registry.url";
    /** Префикс авторизационных параметров Schema Registry. */
    static final String K_AVRO_SR_AUTH_PREFIX      = "h2k.avro.sr.auth.";

    // ==== Ключи конфигурации (собраны в одном месте для устранения "хардкода") ====
    /**
     * Шаблон имени Kafka‑топика (поддерживаются плейсхолдеры ${table}, ${namespace}, ${qualifier}).
     * Используется в {@link #topicFor(TableName)}.
     */
    static final String K_TOPIC_PATTERN = "h2k.topic.pattern";
    /** Максимально допустимая длина имени Kafka‑топика. */
    static final String K_TOPIC_MAX_LENGTH = "h2k.topic.max.length";
    /** Флаг автосоздания недостающих топиков. */
    static final String K_ENSURE_TOPICS = "h2k.ensure.topics";
    /** Разрешать ли автоматическое увеличение числа партиций при ensureTopics. По умолчанию выключено. */
    static final String K_ENSURE_INCREASE_PARTITIONS = "h2k.ensure.increase.partitions";
    /** Разрешать ли дифф‑применение topic‑конфигов (incrementalAlterConfigs) при ensureTopics. По умолчанию выключено. */
    static final String K_ENSURE_DIFF_CONFIGS = "h2k.ensure.diff.configs";
    /** Целевое число партиций создаваемого топика. */
    static final String K_TOPIC_PARTITIONS = "h2k.topic.partitions";
    /** Целевой фактор репликации создаваемого топика. */
    static final String K_TOPIC_REPLICATION_FACTOR = "h2k.topic.replication";
    /** Таймаут операций Kafka AdminClient (мс) при ensureTopics. */
    static final String K_ADMIN_TIMEOUT_MS = "h2k.admin.timeout.ms";
    /** Явное значение client.id для Kafka AdminClient (для читаемых логов брокера). */
    static final String K_ADMIN_CLIENT_ID = "h2k.admin.client.id";
    /** Базовый backoff (мс) между повторами AdminClient при «неуверенных» ошибках. */
    static final String K_ENSURE_UNKNOWN_BACKOFF_MS = "h2k.ensure.unknown.backoff.ms";
    /** Каждые N отправок ожидать подтверждение (ограничение памяти/pressure). */
    static final String K_PRODUCER_AWAIT_EVERY = "h2k.producer.await.every";
    /** Таймаут ожидания подтверждения батча (мс). */
    static final String K_PRODUCER_AWAIT_TIMEOUT_MS = "h2k.producer.await.timeout.ms";
    /**
    // ==== Значения по умолчанию (в одном месте) ====
    /** Имя CF по умолчанию, если в конфигурации не задано явно. */
    /** Базовое значение client.id для AdminClient (к нему добавляется hostname, если доступен). */
    public static final String DEFAULT_ADMIN_CLIENT_ID = "h2k";
    /** По умолчанию автосоздание топиков включено. */
    static final boolean DEFAULT_ENSURE_TOPICS = true;
    /** По умолчанию увеличение партиций при ensureTopics отключено. */
    static final boolean DEFAULT_ENSURE_INCREASE_PARTITIONS = false;
    /** По умолчанию дифф‑применение конфигов при ensureTopics отключено. */
    static final boolean DEFAULT_ENSURE_DIFF_CONFIGS = false;
    /** Число партиций по умолчанию при создании топика. */
    static final int DEFAULT_TOPIC_PARTITIONS = 3;
    /** Фактор репликации по умолчанию при создании топика. */
    static final short DEFAULT_TOPIC_REPLICATION = 1;
    /** Таймаут операций AdminClient по умолчанию, мс. */
    static final long DEFAULT_ADMIN_TIMEOUT_MS = 60000L;
    /** Пауза между повторами при неопределённых ошибках AdminClient по умолчанию, мс. */
    static final long DEFAULT_UNKNOWN_BACKOFF_MS = 15000L;
    /** Количество отправок между ожиданиями по умолчанию. */
    static final int DEFAULT_AWAIT_EVERY = 500;
    /** Таймаут ожидания отправок по умолчанию, мс. */
    static final int DEFAULT_AWAIT_TIMEOUT_MS = 180000;
    /** Каталог локальных Avro-схем по умолчанию. */
    public static final String DEFAULT_AVRO_SCHEMA_DIR = "conf/avro";
    /** По умолчанию наблюдатели таблиц отключены. */
    static final boolean DEFAULT_OBSERVERS_ENABLED = false;

    // ==== Базовые ====
    private final String bootstrap;
    private final TopicNamingSettings topicSettings;
    private final AvroSettings avroSettings;

    // ==== Автосоздание топиков ====
    private final EnsureSettings ensureSettings;
    /** Настройки ожиданий Kafka Producer. */
    private final ProducerAwaitSettings producerSettings;
    /** Внешний поставщик табличных метаданных (например, Avro-схемы). */
    private final PhoenixTableMetadataProvider tableMetadataProvider;
    /** Включены ли наблюдатели TableCapacity/CfFilter. */
    private final boolean observersEnabled;
    /** Кэш вычисленных опций таблиц для повторного использования в горячем пути. */
    private final ConcurrentMap<String, TableOptionsSnapshot> tableOptionsCache = new ConcurrentHashMap<>(8);

    /**
     * Публичные ключи конфигурации h2k.* для использования в других пакетах проекта
     * (исключаем дубли строковых литералов). Значения синхронизированы с приватными K_* выше.
     */
    public static final class Keys {
        /** Адреса Kafka bootstrap.servers (обязательный параметр). Формат: host:port[,host2:port2]. */
        public static final String BOOTSTRAP = "h2k.kafka.bootstrap.servers";
        /** Префикс для переопределения любых свойств Kafka Producer (например, h2k.producer.acks). */
        public static final String PRODUCER_PREFIX = "h2k.producer.";
        /** Префикс для дополнительных конфигураций Kafka‑топика, собираемых в {@link #getTopicConfigs()}. */
        public static final String TOPIC_CONFIG_PREFIX = "h2k.topic.config.";
        /** Включение диагностической статистики (WalDiagnostics). */
        public static final String OBSERVERS_ENABLED = "h2k.observers.enabled";
        /** Каталог локальных Avro-схем (generic Avro и режим phoenix-avro для декодера). */
        public static final String AVRO_SCHEMA_DIR = "h2k.avro.schema.dir";
        public static final String AVRO_SCHEMA_REGISTRY_URL = "h2k.avro.schema.registry.url";
        public static final String AVRO_SUBJECT_STRATEGY    = "h2k.avro.subject.strategy";
        public static final String AVRO_COMPATIBILITY       = "h2k.avro.compatibility";
        public static final String AVRO_BINARY              = "h2k.avro.binary";

        private Keys() {}
    }

    /**
     * Приватный конструктор: используется коллекцией параметров {@link H2kConfigData},
     * которую формирует внешний билдер {@link H2kConfigBuilder}. Позволяет централизованно
     * инициализировать все final‑поля за один проход и сохранить иммутабельность без длинного
     * конструктора с десятками параметров.
     */
    private H2kConfig(H2kConfigData data) {
        if (data == null) {
            throw new IllegalArgumentException("Параметры конфигурации не могут быть null");
        }

    // Проверяем, что все секции заполнены загрузчиком, иначе выдаём понятные сообщения ещё до запуска endpoint.
    this.bootstrap = Objects.requireNonNull(data.bootstrap, "Адреса bootstrap не могут быть null");

    this.topicSettings = Objects.requireNonNull(data.topic, "Секция topic не может быть null");
    this.avroSettings = Objects.requireNonNull(data.avro, "Секция avro не может быть null");
    this.ensureSettings = Objects.requireNonNull(data.ensure, "Секция ensure не может быть null");
    this.producerSettings = Objects.requireNonNull(data.producer, "Секция producer не может быть null");
        PhoenixTableMetadataProvider provider = (data.metadataProvider == null)
                ? PhoenixTableMetadataProvider.NOOP
                : data.metadataProvider;
        this.tableMetadataProvider = provider;
        this.observersEnabled = data.observersEnabled;
    }

    /**
     * Пакетная фабрика: создаёт {@link H2kConfig} на основе заранее подготовленных данных.
     * Вынесена отдельно, чтобы внешний билдер не раскрывал детали конструктора.
     */
    static H2kConfig fromData(H2kConfigData data) {
        return new H2kConfig(data);
    }

    /**
     * Строит {@link H2kConfig} из HBase {@link Configuration} и явного списка bootstrap‑серверов Kafka.
     * Проверяет обязательные параметры, подставляет значения по умолчанию, предвычисляет быстрые флаги.
     * @param cfg HBase‑конфигурация с параметрами вида h2k.*
     * @param bootstrap значение для kafka bootstrap.servers (host:port[,host2:port2]) — обязательно
     * @return полностью инициализированная иммутабельная конфигурация
     * @throws IllegalArgumentException если bootstrap пустой или не указан
     */
    public static H2kConfig from(Configuration cfg, String bootstrap) {
        return new H2kConfigLoader().load(cfg, bootstrap);
    }

    public static H2kConfig from(Configuration cfg,
                                 String bootstrap,
                                 PhoenixTableMetadataProvider metadataProvider) {
        PhoenixTableMetadataProvider provider =
                (metadataProvider == null) ? PhoenixTableMetadataProvider.NOOP : metadataProvider;
        return new H2kConfigLoader().load(cfg, bootstrap, provider);
    }


    /**
     * Для namespace "default" префикс не добавляется (см. правила по h2k.topic.pattern).
     * Формирует имя Kafka‑топика по заданному шаблону {@link #topicPattern} с подстановкой
     * плейсхолдеров и санитизацией по правилам Kafka (замена недопустимых символов на "_",
     * обрезка до {@link #topicMaxLength}).
     * @param table таблица HBase (источник namespace и qualifier)
     * @return корректное имя Kafka‑топика
     */
    public String topicFor(TableName table) {
        return topicSettings.resolve(table);
    }

    /**
     * Санитизирует и нормализует произвольное «сырое» имя Kafka‑топика по тем же правилам,
     * что и {@link #topicFor(TableName)}: удаление ведущих/повторных разделителей, замена
     * недопустимых символов на '_', защита от "." и "..", обрезка до {@link #topicMaxLength}.
     *
     * Важно: эта функция — единая точка ответственности за правила формирования имени топика.
     * Используйте её при любых внешних/динамических именах, чтобы избежать расхождений.
     *
     * @implNote Делегирует нормализацию символов и разделителей вспомогательным методам {@code Parsers}; укорочение до {@code topicMaxLength} выполняется на финальном шаге.
     *
     * @param raw исходная строка (шаблон или произвольное имя)
     * @return корректное имя Kafka‑топика, соответствующее ограничениям брокера
     */
    public String sanitizeTopic(String raw) {
        return topicSettings.sanitize(raw);
    }


    // ===== Итоговые геттеры =====
    /** @return список Kafka bootstrap.servers */
    public String getBootstrap() { return bootstrap; }
    /** @return шаблон имени Kafka‑топика с плейсхолдерами */
    public String getTopicPattern() { return topicSettings.getPattern(); }
    /** @return максимальная допустимая длина имени топика */
    public int getTopicMaxLength() { return topicSettings.getMaxLength(); }
    /** @return каталог локальных Avro-схем */
    public String getAvroSchemaDir() { return avroSettings.getSchemaDir(); }
    /** @return неизменяемый список URL Schema Registry */
    public java.util.List<String> getAvroSchemaRegistryUrls() { return avroSettings.getRegistryUrls(); }
    /** @return карта авторизационных свойств для Schema Registry */
    public Map<String, String> getAvroSrAuth() { return avroSettings.getRegistryAuth(); }
    /** @return неизменяемая карта AVRO-свойств */
    public Map<String, String> getAvroProps() { return avroSettings.getProperties(); }
    /** @return имя Avro-свойства, помечающего поле для пропуска при построении payload */
    public String getPayloadSkipProperty() { return AvroSchemaProperties.PAYLOAD_SKIP; }

    /** @return создавать ли недостающие топики автоматически */
    public boolean isEnsureTopics() { return ensureSettings.isEnsureTopics(); }
    /**
     * Разрешено ли автоматическое увеличение числа партиций при ensureTopics.
     * По умолчанию false; управляется ключом h2k.ensure.increase.partitions.
     */
    public boolean isEnsureIncreasePartitions() { return ensureSettings.isAllowIncreasePartitions(); }
    /**
     * Разрешено ли дифф‑применение конфигов топика (incrementalAlterConfigs) при ensureTopics.
     * По умолчанию false; управляется ключом h2k.ensure.diff.configs.
     */
    public boolean isEnsureDiffConfigs() { return ensureSettings.isAllowDiffConfigs(); }
    /**
     * Число партиций для создаваемых Kafka-тем.
     * Значение нормализуется при построении конфигурации: минимум 1.
     */
    public int getTopicPartitions() { return ensureSettings.getTopicSpec().getPartitions(); }
    /**
     * Фактор репликации для создаваемых Kafka-тем.
     * Значение нормализуется при построении конфигурации: минимум 1.
     */
    public short getTopicReplication() { return ensureSettings.getTopicSpec().getReplication(); }
    /** @return таймаут операций AdminClient при ensureTopics, мс */
    public long getAdminTimeoutMs() { return ensureSettings.getAdminSpec().getTimeoutMs(); }

    /**
     * Таймаут как int для API, принимающих миллисекунды 32‑битным целым.
     * Возвращает {@code Integer.MAX_VALUE}, если значение выходит за пределы int.
     */
    public int getAdminTimeoutMsAsInt() {
        long v = getAdminTimeoutMs();
        return (v > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) v;
    }

    /** @return значение client.id для AdminClient */
    public String getAdminClientId() { return ensureSettings.getAdminSpec().getClientId(); }
    /**
     * Базовая задержка (в миллисекундах) для повторной попытки при «неуверенных» ошибках AdminClient.
     * Значение нормализуется при построении конфигурации: минимум 1 мс.
     */
    public long getUnknownBackoffMs() { return ensureSettings.getAdminSpec().getUnknownBackoffMs(); }
    /** @return размер батча отправок, после которого ожидаются подтверждения */
    public int getAwaitEvery() { return producerSettings.getAwaitEvery(); }
    /** @return таймаут ожидания подтверждений батча, мс */
    public int getAwaitTimeoutMs() { return producerSettings.getAwaitTimeoutMs(); }
    /** @return карта дополнительных конфигураций топика (h2k.topic.config.*) */
    public Map<String, String> getTopicConfigs() { return topicSettings.getTopicConfigs(); }

    /** @return плоская секция именования топиков (для фабрик и тестов) */
    public TopicNamingSettings getTopicSettings() { return topicSettings; }
    /** @return плоская секция Avro/Schema Registry */
    public AvroSettings getAvroSettings() { return avroSettings; }
    /** @return плоская секция ensure-топиков */
    public EnsureSettings getEnsureSettings() { return ensureSettings; }
    /** @return плоская секция ожиданий Kafka Producer */
    public ProducerAwaitSettings getProducerSettings() { return producerSettings; }

    /**
     * Возвращает количество байт соли для заданной таблицы.
     * Поиск выполняется по полному имени (ns:qualifier), затем по одному qualifier.
     * @param table имя таблицы HBase
     * @return 0 если соль не используется; {@code >0} — число байт соли
     */
    public int getSaltBytesFor(TableName table) {
        return resolveTableOptions(table).saltBytes();
    }

    /** Удобный булев геттер: используется ли соль для таблицы. */
    public boolean isSalted(TableName table) { return getSaltBytesFor(table) > 0; }

    /**
     * Возвращает массив имён PK-колонок для таблицы (может быть пустым).
     * @param table имя таблицы Phoenix
     * @return массив имён PK, никогда не {@code null}
     */
    public String[] primaryKeyColumns(TableName table) {
        if (table == null) {
            throw new IllegalArgumentException("Параметр table не задан");
        }
        String[] pk = tableMetadataProvider.primaryKeyColumns(table);
        if (pk == null || pk.length == 0) {
            return SchemaRegistry.EMPTY;
        }
        return pk.clone();
    }

    /** @return включены ли наблюдатели статистики таблиц. */
    @Override
    public boolean isObserversEnabled() { return observersEnabled; }

    /**
     * Возвращает подсказку ёмкости для заданной таблицы (если задана).
     * Поиск выполняется по полному имени (ns:qualifier), затем по одному qualifier.
     * @param table имя таблицы HBase
     * @return ожидаемое число полей в корневом JSON (0 — если подсказка не задана)
     */
    @Override
    public int getCapacityHintFor(TableName table) {
        return resolveTableOptions(table).capacityHint();
    }

    /**
     * Возвращает снимок табличных опций (соль/ёмкость) вместе с источниками данных.
     * Удобно для отладочного логирования и диагностических сценариев.
     */
    @Override
    public TableOptionsSnapshot describeTableOptions(TableName table) {
        return resolveTableOptions(table);
    }

    /**
     * Возвращает снимок конфигурации CF-фильтра для указанной таблицы.
     *
     * @param table таблица HBase/Phoenix
     * @return неизменяемый снимок фильтра CF
     */
    @Override
    public CfFilterSnapshot describeCfFilter(TableName table) {
        return resolveTableOptions(table).cfFilter();
    }

    private TableOptionsSnapshot resolveTableOptions(TableName table) {
        if (table == null) {
            throw new IllegalArgumentException("Параметр table не задан");
        }
        String fullName = Parsers.up(table.getNameAsString());
        return tableOptionsCache.computeIfAbsent(fullName, key -> computeTableOptions(table));
    }

    private TableOptionsSnapshot computeTableOptions(TableName table) {
        int saltBytes = 0;
        TableValueSource saltSource = TableValueSource.DEFAULT;

        Integer metaSalt = tableMetadataProvider.saltBytes(table);
        if (metaSalt != null) {
            saltBytes = clampSalt(metaSalt);
            saltSource = TableValueSource.AVRO;
        }

        int capacityHint = 0;
        TableValueSource capacitySource = TableValueSource.DEFAULT;

        Integer metaCapacity = tableMetadataProvider.capacityHint(table);
        if (metaCapacity != null && metaCapacity > 0) {
            capacityHint = metaCapacity;
            capacitySource = TableValueSource.AVRO;
        }

        String[] cfNames = tableMetadataProvider.columnFamilies(table);
        CfFilterSnapshot cfSnapshot;
        if (cfNames != null && cfNames.length > 0) {
            cfSnapshot = CfFilterSnapshot.from(cfNames, TableValueSource.AVRO);
        } else {
            cfSnapshot = CfFilterSnapshot.disabled();
        }

        return new TableOptionsSnapshot(saltBytes, saltSource, capacityHint, capacitySource, cfSnapshot);
    }

    private static int clampSalt(Integer raw) {
        if (raw == null) {
            return 0;
        }
        int v = raw;
        if (v < 0) {
            return 0;
        }
        return (v > 8) ? 8 : v;
    }

    /**
     * Строковое представление конфигурации с маскировкой bootstrap.servers.
     * Используется только для диагностических логов.
     */
    @Override
    public String toString() {
        final String maskedBootstrap = maskBootstrap(bootstrap);
    TopicNamingSettings ts = this.topicSettings;
    AvroSettings avro = this.avroSettings;
    EnsureSettings ensure = this.ensureSettings;
    EnsureSettings.TopicSpec topic = ensure.getTopicSpec();
    EnsureSettings.AdminSpec admin = ensure.getAdminSpec();
    ProducerAwaitSettings producer = this.producerSettings;
        return new StringBuilder(256)
                .append("H2kConfig{")
                .append("bootstrap=").append(maskedBootstrap)
        .append(", topicPattern='").append(ts.getPattern()).append('\'')
        .append(", topicMaxLength=").append(ts.getMaxLength())
        .append(", avroSchemaDir='").append(avro.getSchemaDir()).append('\'')
        .append(", avroSrUrls.size=").append(avro.getRegistryUrls().size())
        .append(", ensureTopics=").append(ensure.isEnsureTopics())
        .append(", ensureIncreasePartitions=").append(ensure.isAllowIncreasePartitions())
        .append(", ensureDiffConfigs=").append(ensure.isAllowDiffConfigs())
        .append(", topicPartitions=").append(topic.getPartitions())
        .append(", topicReplication=").append(topic.getReplication())
        .append(", adminTimeoutMs=").append(admin.getTimeoutMs())
        .append(", adminClientId='").append(admin.getClientId()).append('\'')
        .append(", unknownBackoffMs=").append(admin.getUnknownBackoffMs())
        .append(", awaitEvery=").append(producer.getAwaitEvery())
        .append(", awaitTimeoutMs=").append(producer.getAwaitTimeoutMs())
        .append(", topicConfigs.size=").append(ts.getTopicConfigs().size())
        .append(", avroSrAuth.size=").append(avro.getRegistryAuth().size())
        .append(", avroProps.size=").append(avro.getProperties().size())
                .append('}')
                .toString();
    }

    /** Маскирует bootstrap.servers для логов: показывает только первый host и признак продолжения. */
    private static String maskBootstrap(String s) {
        if (s == null || s.isEmpty()) return "";
        String[] parts = s.split(",", 2);
        String first = parts[0];
        int colon = first.indexOf(':');
        String host = (colon >= 0 ? first.substring(0, colon) : first);
        String suffix = (parts.length > 1 ? ",..." : "");
        return host + suffix;
    }

}
