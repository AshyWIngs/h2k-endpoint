package kz.qazmarka.h2k.config;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
public final class H2kConfig {
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
    /** CSV‑карта переопределений длины соли rowkey в байтах по таблицам. */
    static final String K_SALT_MAP = "h2k.salt.map";

    /**
     * Публичные ключи конфигурации h2k.* для использования в других пакетах проекта
     * (исключаем дубли строковых литералов). Значения синхронизированы с приватными K_* выше.
     */
    public static final class Keys {
        private Keys() {}
        /**
         * Список CF для экспорта в Kafka (CSV).
         * Пример: "d,b,0". Используется для фильтрации целевых семейств столбцов.
         */
        /**
         * Адреса Kafka bootstrap.servers (обязательный параметр).
         * Формат: host:port[,host2:port2].
         */
        public static final String BOOTSTRAP = "h2k.kafka.bootstrap.servers";
        /**
         * Префикс для переопределения любых свойств Kafka Producer (например,
         * h2k.producer.acks, h2k.producer.linger.ms, и т.п.).
         */
        public static final String PRODUCER_PREFIX = "h2k.producer.";
        /**
         * Префикс для дополнительных конфигураций Kafka‑топика, собираемых в {@link #getTopicConfigs()}.
         */
        public static final String TOPIC_CONFIG_PREFIX = "h2k.topic.config.";
        /**
         * Включить диагностические счётчики BatchSender по умолчанию (true/false).
         */
        public static final String PRODUCER_BATCH_COUNTERS_ENABLED = "h2k.producer.batch.counters.enabled";
        /**
         * Включить подробный DEBUG‑лог при неуспехе автоматического сброса батча (true/false).
         */
        public static final String PRODUCER_BATCH_DEBUG_ON_FAILURE = "h2k.producer.batch.debug.on.failure";
        /**
         * Включить расширенную автонастройку awaitEvery (true/false).
         */
        public static final String PRODUCER_BATCH_AUTOTUNE_ENABLED = "h2k.producer.batch.autotune.enabled";
        /** Минимальный awaitEvery, до которого может снижаться автонастройка. */
        public static final String PRODUCER_BATCH_AUTOTUNE_MIN = "h2k.producer.batch.autotune.min";
        /** Максимальный awaitEvery, до которого может расти автонастройка. */
        public static final String PRODUCER_BATCH_AUTOTUNE_MAX = "h2k.producer.batch.autotune.max";
        /** Высокий порог задержки (мс), при превышении которого awaitEvery уменьшается. */
        public static final String PRODUCER_BATCH_AUTOTUNE_LATENCY_HIGH_MS = "h2k.producer.batch.autotune.latency.high.ms";
        /** Низкий порог задержки (мс), при котором awaitEvery может увеличиваться. */
        public static final String PRODUCER_BATCH_AUTOTUNE_LATENCY_LOW_MS = "h2k.producer.batch.autotune.latency.low.ms";
        /** Минимальный интервал между решениями автонастройки, мс. */
        public static final String PRODUCER_BATCH_AUTOTUNE_COOLDOWN_MS = "h2k.producer.batch.autotune.cooldown.ms";
        /**
         * Табличные переопределения соли rowkey в байтах.
         * Формат CSV: TABLE[:BYTES] | TABLE=BYTES | NS:TABLE[:BYTES] | NS:TABLE=BYTES [, ...].
         * Если BYTES не указан — берётся 1. Значение клиппится в диапазон 0..8.
         * Допускается полное имя 'namespace:qualifier' или просто 'qualifier'. Поиск — case-insensitive.
         */
        public static final String SALT_MAP = "h2k.salt.map";
        /**
         * Префикс подсказок ёмкости корневого JSON по таблицам.
         * Формат ключа: {@code h2k.capacity.hint.<TABLE> = <int>}
         * где TABLE — "namespace:qualifier" или просто "qualifier".
         */
        public static final String CAPACITY_HINT_PREFIX = "h2k.capacity.hint.";
        /**
         * CSV с подсказками ёмкости корневого JSON по таблицам.
         * Формат: h2k.capacity.hints = "TABLE=keys[,NS:TABLE=keys2,...]".
         * Значение "keys" — ожидаемое число не-null полей (см. README).
         */
        public static final String CAPACITY_HINTS = "h2k.capacity.hints";

        /** Каталог локальных Avro-схем (generic Avro и режим phoenix-avro для декодера). */
        public static final String AVRO_SCHEMA_DIR = "h2k.avro.schema.dir";
        public static final String AVRO_SCHEMA_REGISTRY_URL = "h2k.avro.schema.registry.url";
        public static final String AVRO_SUBJECT_STRATEGY    = "h2k.avro.subject.strategy";
        public static final String AVRO_COMPATIBILITY       = "h2k.avro.compatibility";
        public static final String AVRO_BINARY              = "h2k.avro.binary";
    }

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
    /**
     * Размер батча отправок по умолчанию (каждые N отправок ожидаем подтверждения),
     * баланс скорости и потребления памяти.
     */
    static final int DEFAULT_AWAIT_EVERY = 500;
    /** Таймаут ожидания подтверждений батча по умолчанию, мс. */
    static final int DEFAULT_AWAIT_TIMEOUT_MS = 180000;

    /** По умолчанию диагностические счётчики BatchSender отключены. */
    static final boolean DEFAULT_PRODUCER_BATCH_COUNTERS_ENABLED = false;
    /** По умолчанию подробный DEBUG при неуспехе авто‑сброса отключён. */
    static final boolean DEFAULT_PRODUCER_BATCH_DEBUG_ON_FAILURE = false;
    /** По умолчанию расширенная автонастройка awaitEvery включена. */
    static final boolean DEFAULT_PRODUCER_BATCH_AUTOTUNE_ENABLED = true;
    /** По умолчанию минимальный порог автонастройки рассчитывается относительно базового awaitEvery. */
    static final int DEFAULT_PRODUCER_BATCH_AUTOTUNE_MIN = 0;
    /** По умолчанию максимальный порог автонастройки рассчитывается относительно базового awaitEvery. */
    static final int DEFAULT_PRODUCER_BATCH_AUTOTUNE_MAX = 0;
    /** По умолчанию высокий порог задержки (мс) рассчитывается от awaitTimeoutMs. */
    static final int DEFAULT_PRODUCER_BATCH_AUTOTUNE_LATENCY_HIGH_MS = 0;
    /** По умолчанию низкий порог задержки (мс) рассчитывается от awaitTimeoutMs. */
    static final int DEFAULT_PRODUCER_BATCH_AUTOTUNE_LATENCY_LOW_MS = 0;
    /** По умолчанию охлаждение между решениями автонастройки, мс. */
    static final int DEFAULT_PRODUCER_BATCH_AUTOTUNE_COOLDOWN_MS = 30000;
    /** Каталог локальных Avro-схем по умолчанию. */
    public static final String DEFAULT_AVRO_SCHEMA_DIR = "conf/avro";

    // ==== Базовые ====
    private final String bootstrap;
    private final String topicPattern;
    private final int topicMaxLength;

    private final String avroSchemaDir;
    private final java.util.List<String> avroSchemaRegistryUrls;
    private final Map<String, String> avroSrAuth;
    private final Map<String, String> avroProps;

    // ==== Автосоздание топиков ====
    private final boolean ensureTopics;
    /** Разрешено ли увеличение партиций при ensureTopics. */
    private final boolean ensureIncreasePartitions;
    /** Разрешено ли дифф‑применение topic‑конфигов при ensureTopics. */
    private final boolean ensureDiffConfigs;
    private final int topicPartitions;
    private final short topicReplication;
    private final long adminTimeoutMs;
    /** client.id для AdminClient (для фильтрации в логах брокеров). */
    private final String adminClientId;
    /** Backoff (мс) при неопределённом ответе от AdminClient (UNKNOWN/таймаут/сеть). */
    private final long unknownBackoffMs;
    /** Каждые N отправок ожидаем подтверждения (батчевое ожидание). */
    private final int awaitEvery;
    /** Таймаут ожидания подтверждений батча, мс. */
    private final int awaitTimeoutMs;
    /** Включать ли диагностические счётчики BatchSender по умолчанию. */
    private final boolean producerBatchCountersEnabled;
    /** Логировать ли подробные причины неуспеха авто-сброса в DEBUG. */
    private final boolean producerBatchDebugOnFailure;
    /** Автоматически ли адаптировать awaitEvery на основе метрик. */
    private final boolean producerBatchAutotuneEnabled;
    /** Минимально допустимый awaitEvery при автонастройке. */
    private final int producerBatchAutotuneMinAwait;
    /** Максимально допустимый awaitEvery при автонастройке. */
    private final int producerBatchAutotuneMaxAwait;
    /** Порог задержки, после которого awaitEvery снижается, мс. */
    private final int producerBatchAutotuneLatencyHighMs;
    /** Порог задержки, позволяющий увеличивать awaitEvery, мс. */
    private final int producerBatchAutotuneLatencyLowMs;
    /** Минимальный интервал между решениями автонастройки, мс. */
    private final int producerBatchAutotuneCooldownMs;
    /** Произвольные конфиги топика, собранные из h2k.topic.config.* */
    private final Map<String, String> topicConfigs;
    /** Переопределения длины соли rowkey в байтах по таблицам. 0 — соли нет. */
    private final Map<String, Integer> saltBytesByTable;
    /** Подсказки ёмкости корневого JSON по таблицам (ожидаемое число полей). */
    private final Map<String, Integer> capacityHintByTable;
    /** Внешний поставщик табличных метаданных (например, Avro-схемы). */
    private final PhoenixTableMetadataProvider tableMetadataProvider;
    /** Кэш вычисленных опций таблиц для повторного использования в горячем пути. */
    private final ConcurrentMap<String, TableOptionsSnapshot> tableOptionsCache = new ConcurrentHashMap<>(8);

    /**
     * Приватный конструктор: вызывается только билдером для инициализации
     * всех final‑полей за один проход. Сохраняет иммутабельность и избегает
     * длинного конструктора с множеством параметров.
     */
    private H2kConfig(Builder b) {
        this.bootstrap = b.bootstrap;
        this.topicPattern = b.topicPattern;
        this.topicMaxLength = b.topicMaxLength;
        this.avroSchemaDir = b.avroSchemaDir;
        this.avroSchemaRegistryUrls = Collections.unmodifiableList(new java.util.ArrayList<>(b.avroSchemaRegistryUrls));
        this.avroSrAuth = Collections.unmodifiableMap(new HashMap<>(b.avroSrAuth));
        this.avroProps = Collections.unmodifiableMap(new HashMap<>(b.avroProps));
        this.ensureTopics = b.ensureTopics;
        this.ensureIncreasePartitions = b.ensureIncreasePartitions;
        this.ensureDiffConfigs = b.ensureDiffConfigs;
        this.topicPartitions = b.topicPartitions;
        this.topicReplication = b.topicReplication;
        this.adminTimeoutMs = b.adminTimeoutMs;
        this.adminClientId = b.adminClientId;
        this.unknownBackoffMs = b.unknownBackoffMs;
        this.awaitEvery = b.awaitEvery;
        this.awaitTimeoutMs = b.awaitTimeoutMs;
        int computedAutotuneMinAwait = (b.producerBatchAutotuneMinAwait > 0)
                ? b.producerBatchAutotuneMinAwait
                : Math.max(16, Math.max(1, b.awaitEvery / 4));
        long baseAwait = b.awaitEvery;
        long maxCandidate = (b.producerBatchAutotuneMaxAwait > 0)
                ? b.producerBatchAutotuneMaxAwait
                : Math.max(baseAwait, baseAwait * 2L);
        int computedAutotuneMaxAwait = (int) Math.min(Integer.MAX_VALUE, Math.max(computedAutotuneMinAwait, maxCandidate));
        int computedAutotuneLatencyHighMs = (b.producerBatchAutotuneLatencyHighMs > 0)
                ? b.producerBatchAutotuneLatencyHighMs
                : Math.max(100, b.awaitTimeoutMs / 2);
        int computedAutotuneLatencyLowMs = (b.producerBatchAutotuneLatencyLowMs > 0)
                ? b.producerBatchAutotuneLatencyLowMs
                : Math.max(20, b.awaitTimeoutMs / 6);
        if (computedAutotuneLatencyLowMs >= computedAutotuneLatencyHighMs) {
            computedAutotuneLatencyLowMs = Math.max(10, Math.max(1, computedAutotuneLatencyHighMs / 2));
            if (computedAutotuneLatencyLowMs >= computedAutotuneLatencyHighMs) {
                computedAutotuneLatencyLowMs = Math.max(10, Math.max(1, computedAutotuneLatencyHighMs - 10));
            }
        }
        int computedAutotuneCooldownMs = Math.max(1000, b.producerBatchAutotuneCooldownMs);
        this.producerBatchCountersEnabled = b.producerBatchCountersEnabled;
        this.producerBatchDebugOnFailure = b.producerBatchDebugOnFailure;
        this.producerBatchAutotuneEnabled = b.producerBatchAutotuneEnabled;
        this.producerBatchAutotuneMinAwait = computedAutotuneMinAwait;
        this.producerBatchAutotuneMaxAwait = computedAutotuneMaxAwait;
        this.producerBatchAutotuneLatencyHighMs = computedAutotuneLatencyHighMs;
        this.producerBatchAutotuneLatencyLowMs = computedAutotuneLatencyLowMs;
        this.producerBatchAutotuneCooldownMs = computedAutotuneCooldownMs;
        this.topicConfigs = Collections.unmodifiableMap(new HashMap<>(b.topicConfigs));
        this.saltBytesByTable = Collections.unmodifiableMap(new HashMap<>(b.saltBytesByTable));
        this.capacityHintByTable = Collections.unmodifiableMap(new HashMap<>(b.capacityHintByTable));
        this.tableMetadataProvider = b.tableMetadataProvider;
    }

    /**
     * Билдер для пошаговой сборки иммутабельной конфигурации без громоздкого конструктора.
     * Удобнее читать, безопаснее изменять, удовлетворяет правилу Sonar S107 (ограничение числа параметров).
     * Все поля имеют разумные значения по умолчанию; сеттеры возвращают this для чейнинга.
     */
    public static final class Builder {
        /** Обязательный адрес(а) Kafka bootstrap.servers. */
        private String bootstrap;
        /** Шаблон имени топика (см. {@link H2kConfig#K_TOPIC_PATTERN}). */
        private String topicPattern = PLACEHOLDER_TABLE;
        /** Ограничение длины имени топика (символов). */
        private int topicMaxLength = DEFAULT_TOPIC_MAX_LENGTH;
        /** Каталог локальных Avro-схем. */
        private String avroSchemaDir = DEFAULT_AVRO_SCHEMA_DIR;
        /** Список URL Schema Registry. */
        private java.util.List<String> avroSchemaRegistryUrls = Collections.emptyList();
        /** Авторизационные параметры для Schema Registry. */
        private Map<String, String> avroSrAuth = Collections.emptyMap();
        /** Доп. AVRO-настройки (минимальный набор ключей, см. from()). */
        private Map<String, String> avroProps = Collections.emptyMap();
        /** Автоматически создавать недостающие топики. */
        private boolean ensureTopics = true;
        /** Разрешено ли автоматическое увеличение числа партиций. */
        private boolean ensureIncreasePartitions = DEFAULT_ENSURE_INCREASE_PARTITIONS;
        /** Разрешено ли дифф‑применение конфигов топика. */
        private boolean ensureDiffConfigs = DEFAULT_ENSURE_DIFF_CONFIGS;
        /** Целевое число партиций создаваемого топика. */
        private int topicPartitions = DEFAULT_TOPIC_PARTITIONS;
        /** Целевой фактор репликации создаваемого топика. */
        private short topicReplication = DEFAULT_TOPIC_REPLICATION;
        /** Таймаут операций AdminClient при ensureTopics (мс). */
        private long adminTimeoutMs = DEFAULT_ADMIN_TIMEOUT_MS;
        /** Значение client.id для AdminClient. */
        private String adminClientId = DEFAULT_ADMIN_CLIENT_ID;
        /** Базовый backoff между повторами при неопределённых ошибках (мс). */
        private long unknownBackoffMs = DEFAULT_UNKNOWN_BACKOFF_MS;

        /** Каждые N отправок ожидать подтверждение. */
        private int awaitEvery = DEFAULT_AWAIT_EVERY;
        /** Таймаут ожидания подтверждения батча (мс). */
        private int awaitTimeoutMs = DEFAULT_AWAIT_TIMEOUT_MS;

        /** Включены ли диагностические счётчики BatchSender по умолчанию. */
        boolean producerBatchCountersEnabled;
        /** Логировать ли подробности неуспешного авто‑сброса в DEBUG. */
        boolean producerBatchDebugOnFailure;
        /** Включена ли автонастройка awaitEvery. */
        boolean producerBatchAutotuneEnabled = DEFAULT_PRODUCER_BATCH_AUTOTUNE_ENABLED;
        /** Минимальный awaitEvery для автонастройки (0 — вычислить автоматически). */
        int producerBatchAutotuneMinAwait = DEFAULT_PRODUCER_BATCH_AUTOTUNE_MIN;
        /** Максимальный awaitEvery для автонастройки (0 — вычислить автоматически). */
        int producerBatchAutotuneMaxAwait = DEFAULT_PRODUCER_BATCH_AUTOTUNE_MAX;
        /** Порог высокой задержки для снижения awaitEvery (0 — вычислить автоматически). */
        int producerBatchAutotuneLatencyHighMs = DEFAULT_PRODUCER_BATCH_AUTOTUNE_LATENCY_HIGH_MS;
        /** Порог низкой задержки для увеличения awaitEvery (0 — вычислить автоматически). */
        int producerBatchAutotuneLatencyLowMs = DEFAULT_PRODUCER_BATCH_AUTOTUNE_LATENCY_LOW_MS;
        /** Минимальная пауза между решениями автонастройки, мс. */
        int producerBatchAutotuneCooldownMs = DEFAULT_PRODUCER_BATCH_AUTOTUNE_COOLDOWN_MS;

        /** Дополнительные конфиги топика, собранные из префикса h2k.topic.config.* */
        private Map<String, String> topicConfigs = Collections.emptyMap();
        /** Переопределения длины соли rowkey в байтах по таблицам. */
        private Map<String, Integer> saltBytesByTable = Collections.emptyMap();
        /** Подсказки ёмкости корневого JSON по таблицам. */
        private Map<String, Integer> capacityHintByTable = Collections.emptyMap();
        /** Внешний поставщик табличных метаданных (Avro и т.п.). */
        private PhoenixTableMetadataProvider tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
        /**
         * Устанавливает карту переопределений соли по таблицам: имя → байты (0 — без соли).
         * Ожидается уже готовая карта (например, результат {@link Parsers#readSaltMap(Configuration, String)}).
         * @param v неизменяемая или копируемая карта name→bytes
         * @return this
         */
        public Builder saltBytesByTable(Map<String, Integer> v) { this.saltBytesByTable = v; return this; }

        /**
         * Устанавливает подсказки ёмкости корневого JSON по таблицам.
         * Ожидается уже готовая карта (например, результат {@link Parsers#readCapacityHints(Configuration, String, String)}).
         * @param v неизменяемая или копируемая карта name→capacity
         * @return this
         */
        public Builder capacityHintByTable(Map<String, Integer> v) { this.capacityHintByTable = v; return this; }

        public Builder tableMetadataProvider(PhoenixTableMetadataProvider provider) {
            this.tableMetadataProvider = (provider == null) ? PhoenixTableMetadataProvider.NOOP : provider;
            return this;
        }

        /**
         * Создаёт билдер с обязательным адресом Kafka bootstrap.servers.
         * @param bootstrap список Kafka‑узлов в формате host:port[,host2:port2]
         */
        public Builder(String bootstrap) {
            this.bootstrap = bootstrap;
        }

        /**
         * Устанавливает шаблон имени Kafka‑топика. Поддерживаются плейсхолдеры
         * ${table}, ${namespace}, ${qualifier}.
         * @param v шаблон, например "${namespace}.${qualifier}"
         * @return this
         */
        public Builder topicPattern(String v) { this.topicPattern = v; return this; }
        /**
         * Ограничение длины имени топика (символов).
         * @param v максимальная длина (≥1)
         * @return this
         */
        public Builder topicMaxLength(int v) { this.topicMaxLength = v; return this; }
        /**
         * Автоматически создавать недостающие топики при старте.
         * @param v true — создавать при необходимости
         * @return this
         */
        public Builder ensureTopics(boolean v) { this.ensureTopics = v; return this; }

        /**
         * Каталог локальных Avro-схем.
         * @param v путь до каталога
         * @return this
         */
        public Builder avroSchemaDir(String v) {
            this.avroSchemaDir = (v == null || v.trim().isEmpty()) ? DEFAULT_AVRO_SCHEMA_DIR : v.trim();
            return this;
        }

        /**
         * Список URL Schema Registry.
         * @param v список URL
         * @return this
         */
        public Builder avroSchemaRegistryUrls(java.util.List<String> v) {
            this.avroSchemaRegistryUrls = (v == null) ? Collections.emptyList() : v;
            return this;
        }

        /**
         * Авторизационные параметры Schema Registry.
         * @param v карта ключей после префикса h2k.avro.sr.auth.
         * @return this
         */
        public Builder avroSrAuth(Map<String, String> v) { this.avroSrAuth = (v == null) ? Collections.emptyMap() : v; return this; }

        /**
         * AVRO-настройки (минимальный набор известных ключей).
         * @param v карта свойств
         * @return this
         */
        public Builder avroProps(Map<String, String> v) { this.avroProps = (v == null) ? Collections.emptyMap() : v; return this; }
        /**
         * Разрешить автоматическое увеличение числа партиций при ensureTopics.
         * @param v true — увеличивать партиции при необходимости
         * @return this
         */
        public Builder ensureIncreasePartitions(boolean v) { this.ensureIncreasePartitions = v; return this; }
        /**
         * Разрешить дифф‑применение конфигов топика (incrementalAlterConfigs) при ensureTopics.
         * @param v true — сравнивать и применять отличия конфигов
         * @return this
         */
        public Builder ensureDiffConfigs(boolean v) { this.ensureDiffConfigs = v; return this; }
        /**
         * Число партиций создаваемого топика (если ensureTopics=true).
         * @param v количество партиций (≥1)
         * @return this
         */
        public Builder topicPartitions(int v) { this.topicPartitions = v; return this; }
        /**
         * Фактор репликации создаваемого топика.
         * @param v фактор репликации (≥1)
         * @return this
         */
        public Builder topicReplication(short v) { this.topicReplication = v; return this; }
        /**
         * Таймаут операций AdminClient при ensureTopics, мс.
         * @param v таймаут в миллисекундах
         * @return this
         */
        public Builder adminTimeoutMs(long v) { this.adminTimeoutMs = v; return this; }
        /**
         * Значение client.id для AdminClient (удобно для фильтрации логов брокера).
         * @param v идентификатор клиента
         * @return this
         */
        public Builder adminClientId(String v) { this.adminClientId = v; return this; }
        /**
         * Backoff (мс) между повторами при неопределённом результате (UNKNOWN/timeout/сетевые ошибки).
         * @param v пауза между повторами в миллисекундах
         * @return this
         */
        public Builder unknownBackoffMs(long v) { this.unknownBackoffMs = v; return this; }

        /**
         * Каждые N отправок ждать подтверждения (батчевое ожидание).
         * @param v размер батча N (≥1)
         * @return this
         */
        public Builder awaitEvery(int v) { this.awaitEvery = v; return this; }
        /**
         * Таймаут ожидания подтверждений батча.
         * @param v таймаут в миллисекундах (≥1)
         * @return this
         */
        public Builder awaitTimeoutMs(int v) { this.awaitTimeoutMs = v; return this; }

        /**
         * Включить диагностические счётчики BatchSender по умолчанию.
         * @param v true — включить счётчики
         * @return this
         */
        public Builder producerBatchCountersEnabled(boolean v) { this.producerBatchCountersEnabled = v; return this; }
        /**
         * Включить DEBUG‑подробности ошибок авто‑сброса.
         * @param v true — включить подробный DEBUG
         * @return this
         */
        public Builder producerBatchDebugOnFailure(boolean v) { this.producerBatchDebugOnFailure = v; return this; }
        /**
         * Управление автонастройкой awaitEvery.
         * @param v включить/отключить автонастройку
         * @return this
         */
        public Builder producerBatchAutotuneEnabled(boolean v) { this.producerBatchAutotuneEnabled = v; return this; }
        /** Минимальный awaitEvery для автонастройки. */
        public Builder producerBatchAutotuneMinAwait(int v) { this.producerBatchAutotuneMinAwait = v; return this; }
        /** Максимальный awaitEvery для автонастройки. */
        public Builder producerBatchAutotuneMaxAwait(int v) { this.producerBatchAutotuneMaxAwait = v; return this; }
        /** Порог высокой задержки, мс. */
        public Builder producerBatchAutotuneLatencyHighMs(int v) { this.producerBatchAutotuneLatencyHighMs = v; return this; }
        /** Порог низкой задержки, мс. */
        public Builder producerBatchAutotuneLatencyLowMs(int v) { this.producerBatchAutotuneLatencyLowMs = v; return this; }
        /** Минимальная пауза между решениями автонастройки, мс. */
        public Builder producerBatchAutotuneCooldownMs(int v) { this.producerBatchAutotuneCooldownMs = v; return this; }

        /**
         * Произвольные конфиги топика из префикса h2k.topic.config.* (см. {@link Parsers#readTopicConfigs(Configuration, String)}).
         * @param v карта ключ‑значение конфигураций топика
         * @return this
         */
        public Builder topicConfigs(Map<String, String> v) { this.topicConfigs = v; return this; }

        /** @return сгруппированные настройки топика (pattern, CF, дополнительные конфиги). */
        public TopicOptions topic() { return new TopicOptions(); }

        /** @return сгруппированные настройки payload и Avro. */
        public AvroOptions avro() { return new AvroOptions(); }

        /** @return сгруппированные настройки ensure/topics. */
        public EnsureOptions ensure() { return new EnsureOptions(); }

        /** @return сгруппированные настройки ожиданий и batch‑поведения. */
        public ProducerOptions producer() { return new ProducerOptions(); }

        /** @return сгруппированные настройки подсказок по таблицам (соль/ёмкость). */
        public TableOptions tables() { return new TableOptions(); }

        /**
         * Собирает неизменяемый объект конфигурации с текущими значениями билдера.
         *
         * Возвращаемый экземпляр {@link H2kConfig} иммутабелен и фиксирует копии/снимки переданных карт и массивов там, где это требуется.
         * @return готовый {@link H2kConfig}
         */
        public H2kConfig build() { return new H2kConfig(this); }

        /** Опции, относящиеся к шаблону топика. */
        public final class TopicOptions {
            public TopicOptions pattern(String v) { Builder.this.topicPattern(v); return this; }
            public TopicOptions maxLength(int v) { Builder.this.topicMaxLength(v); return this; }
            public TopicOptions configs(Map<String, String> v) { Builder.this.topicConfigs(v); return this; }
            public Builder done() { return Builder.this; }
        }

        /** Опции payload/Avro. */
        public final class AvroOptions {
            public AvroOptions schemaDir(String dir) { Builder.this.avroSchemaDir(dir); return this; }
            public AvroOptions schemaRegistryUrls(java.util.List<String> urls) { Builder.this.avroSchemaRegistryUrls(urls); return this; }
            public AvroOptions schemaRegistryAuth(Map<String, String> auth) { Builder.this.avroSrAuth(auth); return this; }
            public AvroOptions properties(Map<String, String> props) { Builder.this.avroProps(props); return this; }
            public Builder done() { return Builder.this; }
        }

        /** Опции ensure/topics. */
        public final class EnsureOptions {
            public EnsureOptions enabled(boolean v) { Builder.this.ensureTopics(v); return this; }
            public EnsureOptions allowIncreasePartitions(boolean v) { Builder.this.ensureIncreasePartitions(v); return this; }
            public EnsureOptions allowDiffConfigs(boolean v) { Builder.this.ensureDiffConfigs(v); return this; }
            public EnsureOptions partitions(int v) { Builder.this.topicPartitions(v); return this; }
            public EnsureOptions replication(short v) { Builder.this.topicReplication(v); return this; }
            public EnsureOptions adminTimeoutMs(long v) { Builder.this.adminTimeoutMs(v); return this; }
            public EnsureOptions adminClientId(String v) { Builder.this.adminClientId(v); return this; }
            public EnsureOptions unknownBackoffMs(long v) { Builder.this.unknownBackoffMs(v); return this; }
            public Builder done() { return Builder.this; }
        }

        /** Опции ожиданий/продьюсера. */
        public final class ProducerOptions {
            public ProducerOptions awaitEvery(int v) { Builder.this.awaitEvery(v); return this; }
            public ProducerOptions awaitTimeoutMs(int v) { Builder.this.awaitTimeoutMs(v); return this; }
            public ProducerOptions batchCountersEnabled(boolean v) { Builder.this.producerBatchCountersEnabled(v); return this; }
            public ProducerOptions batchDebugOnFailure(boolean v) { Builder.this.producerBatchDebugOnFailure(v); return this; }
            public ProducerOptions autotuneEnabled(boolean v) { Builder.this.producerBatchAutotuneEnabled(v); return this; }
            public ProducerOptions autotuneMinAwait(int v) { Builder.this.producerBatchAutotuneMinAwait(v); return this; }
            public ProducerOptions autotuneMaxAwait(int v) { Builder.this.producerBatchAutotuneMaxAwait(v); return this; }
            public ProducerOptions autotuneLatencyHighMs(int v) { Builder.this.producerBatchAutotuneLatencyHighMs(v); return this; }
            public ProducerOptions autotuneLatencyLowMs(int v) { Builder.this.producerBatchAutotuneLatencyLowMs(v); return this; }
            public ProducerOptions autotuneCooldownMs(int v) { Builder.this.producerBatchAutotuneCooldownMs(v); return this; }
            public Builder done() { return Builder.this; }
        }

        /** Опции табличных подсказок. */
        public final class TableOptions {
            public TableOptions saltBytes(Map<String, Integer> v) { Builder.this.saltBytesByTable(v); return this; }
            public TableOptions capacityHints(Map<String, Integer> v) { Builder.this.capacityHintByTable(v); return this; }
            public TableOptions metadataProvider(PhoenixTableMetadataProvider provider) { Builder.this.tableMetadataProvider(provider); return this; }
            public Builder done() { return Builder.this; }
        }
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
        String ns = table.getNamespaceAsString();
        String qn = table.getQualifierAsString();
        // для namespace "default" префикс не пишем
        String tableAtom = HBASE_DEFAULT_NS.equals(ns) ? qn : (ns + "_" + qn);
        String nsAtom = HBASE_DEFAULT_NS.equals(ns) ? "" : ns;
        String base = topicPattern
                .replace(PLACEHOLDER_TABLE, tableAtom)
                .replace(PLACEHOLDER_NAMESPACE, nsAtom)
                .replace(PLACEHOLDER_QUALIFIER, qn);

        return sanitizeTopic(base);
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
        String base = (raw == null) ? "" : raw;
        // убрать ведущие и повторные разделители
        String s = Parsers.topicCollapseRepeatedDelimiters(
                Parsers.topicStripLeadingDelimiters(base));
        // санитизация под допустимые символы Kafka
        String sanitized = Parsers.topicSanitizeKafkaChars(s);

        // защита от ".", ".." и пустой строки — используем универсальный безопасный placeholder
        if (sanitized.equals(".") || sanitized.equals("..") || sanitized.isEmpty()) {
            sanitized = "topic";
        }

        // обрезка по максимальной длине
        return (sanitized.length() > topicMaxLength)
                ? sanitized.substring(0, topicMaxLength)
                : sanitized;
    }


    // ===== Итоговые геттеры =====
    /** @return список Kafka bootstrap.servers */
    public String getBootstrap() { return bootstrap; }
    /** @return шаблон имени Kafka‑топика с плейсхолдерами */
    public String getTopicPattern() { return topicPattern; }
    /** @return максимальная допустимая длина имени топика */
    public int getTopicMaxLength() { return topicMaxLength; }
    /** @return каталог локальных Avro-схем */
    public String getAvroSchemaDir() { return avroSchemaDir; }
    /** @return неизменяемый список URL Schema Registry */
    public java.util.List<String> getAvroSchemaRegistryUrls() { return avroSchemaRegistryUrls; }
    /** @return карта авторизационных свойств для Schema Registry */
    public Map<String, String> getAvroSrAuth() { return avroSrAuth; }
    /** @return неизменяемая карта AVRO-свойств */
    public Map<String, String> getAvroProps() { return avroProps; }

    /** @return создавать ли недостающие топики автоматически */
    public boolean isEnsureTopics() { return ensureTopics; }
    /**
     * Разрешено ли автоматическое увеличение числа партиций при ensureTopics.
     * По умолчанию false; управляется ключом h2k.ensure.increase.partitions.
     */
    public boolean isEnsureIncreasePartitions() { return ensureIncreasePartitions; }
    /**
     * Разрешено ли дифф‑применение конфигов топика (incrementalAlterConfigs) при ensureTopics.
     * По умолчанию false; управляется ключом h2k.ensure.diff.configs.
     */
    public boolean isEnsureDiffConfigs() { return ensureDiffConfigs; }
    /**
     * Число партиций для создаваемых Kafka-тем.
     * Значение нормализуется при построении конфигурации: минимум 1.
     */
    public int getTopicPartitions() { return topicPartitions; }
    /**
     * Фактор репликации для создаваемых Kafka-тем.
     * Значение нормализуется при построении конфигурации: минимум 1.
     */
    public short getTopicReplication() { return topicReplication; }
    /** @return таймаут операций AdminClient при ensureTopics, мс */
    public long getAdminTimeoutMs() { return adminTimeoutMs; }

    /**
     * Таймаут как int для API, принимающих миллисекунды 32‑битным целым.
     * Возвращает {@code Integer.MAX_VALUE}, если значение выходит за пределы int.
     */
    public int getAdminTimeoutMsAsInt() {
        long v = this.adminTimeoutMs;
        return (v > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) v;
    }

    /**
     * Готовые свойства для Kafka AdminClient.
     * Содержит как минимум bootstrap.servers и client.id.
     * Значения таймаутов намеренно не устанавливаются здесь и задаются на стороне TopicEnsurer.
     */
    public Properties kafkaAdminProps() {
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", this.bootstrap);
        p.setProperty("client.id", this.adminClientId);
        return p;
    }
    /** @return значение client.id для AdminClient */
    public String getAdminClientId() { return adminClientId; }
    /**
     * Базовая задержка (в миллисекундах) для повторной попытки при «неуверенных» ошибках AdminClient.
     * Значение нормализуется при построении конфигурации: минимум 1 мс.
     */
    public long getUnknownBackoffMs() { return unknownBackoffMs; }
    /** @return размер батча отправок, после которого ожидаются подтверждения */
    public int getAwaitEvery() { return awaitEvery; }
    /** @return таймаут ожидания подтверждений батча, мс */
    public int getAwaitTimeoutMs() { return awaitTimeoutMs; }
    /** @return включены ли счётчики BatchSender по умолчанию */
    public boolean isProducerBatchCountersEnabled() { return producerBatchCountersEnabled; }
    /** @return включён ли DEBUG‑лог подробностей ошибок авто‑сброса */
    public boolean isProducerBatchDebugOnFailure() { return producerBatchDebugOnFailure; }
    /** @return включена ли автонастройка awaitEvery */
    public boolean isProducerBatchAutotuneEnabled() { return producerBatchAutotuneEnabled; }
    /** @return минимальный awaitEvery, до которого может снижаться автонастройка */
    public int getProducerBatchAutotuneMinAwait() { return producerBatchAutotuneMinAwait; }
    /** @return максимальный awaitEvery, до которого может расти автонастройка */
    public int getProducerBatchAutotuneMaxAwait() { return producerBatchAutotuneMaxAwait; }
    /** @return порог высокой задержки, мс */
    public int getProducerBatchAutotuneLatencyHighMs() { return producerBatchAutotuneLatencyHighMs; }
    /** @return порог низкой задержки, мс */
    public int getProducerBatchAutotuneLatencyLowMs() { return producerBatchAutotuneLatencyLowMs; }
    /** @return минимальный интервал между решениями автонастройки, мс */
    public int getProducerBatchAutotuneCooldownMs() { return producerBatchAutotuneCooldownMs; }

    /** @return карта дополнительных конфигураций топика (h2k.topic.config.*) */
    public Map<String, String> getTopicConfigs() { return topicConfigs; }

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

    /** @return неизменяемая карта табличных переопределений соли (как задана в конфиге) */
    /** Карта переопределений соли rowkey в байтах. */
    public Map<String, Integer> getSaltBytesByTable() { return saltBytesByTable; }

    /**
     * Возвращает массив имён PK-колонок для таблицы (может быть пустым).
     * @param table имя таблицы Phoenix
     * @return массив имён PK, никогда не {@code null}
     */
    public String[] primaryKeyColumns(TableName table) {
        if (table == null) {
            throw new NullPointerException("table == null");
        }
        String[] pk = tableMetadataProvider.primaryKeyColumns(table);
        if (pk == null || pk.length == 0) {
            return SchemaRegistry.EMPTY;
        }
        return pk.clone();
    }

    /** @return неизменяемая карта подсказок ёмкости корневого JSON по таблицам */
    /** Подсказки начальной ёмкости JSON для конкретных таблиц. */
    public Map<String, Integer> getCapacityHintByTable() { return capacityHintByTable; }

    /**
     * Возвращает подсказку ёмкости для заданной таблицы (если задана).
     * Поиск выполняется по полному имени (ns:qualifier), затем по одному qualifier.
     * @param table имя таблицы HBase
     * @return ожидаемое число полей в корневом JSON (0 — если подсказка не задана)
     */
    public int getCapacityHintFor(TableName table) {
        return resolveTableOptions(table).capacityHint();
    }

    /**
     * Возвращает снимок табличных опций (соль/ёмкость) вместе с источниками данных.
     * Удобно для отладочного логирования и диагностических сценариев.
     */
    public TableOptionsSnapshot describeTableOptions(TableName table) {
        return resolveTableOptions(table);
    }

    /**
     * Возвращает снимок конфигурации CF-фильтра для указанной таблицы.
     *
     * @param table таблица HBase/Phoenix
     * @return неизменяемый снимок фильтра CF
     */
    public CfFilterSnapshot describeCfFilter(TableName table) {
        return resolveTableOptions(table).cfFilter();
    }

    private TableOptionsSnapshot resolveTableOptions(TableName table) {
        if (table == null) {
            throw new NullPointerException("table == null");
        }
        String fullName = Parsers.up(table.getNameAsString());
        return tableOptionsCache.computeIfAbsent(fullName, key -> computeTableOptions(table));
    }

    private TableOptionsSnapshot computeTableOptions(TableName table) {
        String full = Parsers.up(table.getNameAsString());
        String qualifier = Parsers.up(table.getQualifierAsString());

        int saltBytes = 0;
        ValueSource saltSource = ValueSource.DEFAULT;

        Integer explicitSalt = saltBytesByTable.get(full);
        if (explicitSalt == null) {
            explicitSalt = saltBytesByTable.get(qualifier);
        }
        if (explicitSalt != null) {
            saltBytes = clampSalt(explicitSalt);
            saltSource = ValueSource.EXPLICIT;
        } else {
            Integer metaSalt = tableMetadataProvider.saltBytes(table);
            if (metaSalt != null) {
                saltBytes = clampSalt(metaSalt);
                saltSource = ValueSource.AVRO;
            }
        }

        int capacityHint = 0;
        ValueSource capacitySource = ValueSource.DEFAULT;

        Integer explicitCapacity = capacityHintByTable.get(full);
        if (explicitCapacity == null) {
            explicitCapacity = capacityHintByTable.get(qualifier);
        }
        if (explicitCapacity != null) {
            capacityHint = Math.max(0, explicitCapacity);
            capacitySource = ValueSource.EXPLICIT;
        } else {
            Integer metaCapacity = tableMetadataProvider.capacityHint(table);
            if (metaCapacity != null && metaCapacity > 0) {
                capacityHint = metaCapacity;
                capacitySource = ValueSource.AVRO;
            }
        }

        String[] cfNames = tableMetadataProvider.columnFamilies(table);
        CfFilterSnapshot cfSnapshot;
        if (cfNames != null && cfNames.length > 0) {
            cfSnapshot = CfFilterSnapshot.from(cfNames, ValueSource.AVRO);
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

    /** Источники табличных параметров (соль/ёмкость). */
    public enum ValueSource {
        EXPLICIT("конфигурация h2k.*"),
        AVRO("Avro-схема"),
        DEFAULT("значение по умолчанию");

        private final String label;

        ValueSource(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    /** Иммутабельный снимок табличных опций. */
    public static final class TableOptionsSnapshot {
        private final int saltBytes;
        private final ValueSource saltSource;
        private final int capacityHint;
        private final ValueSource capacitySource;
        private final CfFilterSnapshot cfFilter;

        TableOptionsSnapshot(int saltBytes,
                             ValueSource saltSource,
                             int capacityHint,
                             ValueSource capacitySource,
                             CfFilterSnapshot cfFilter) {
            this.saltBytes = saltBytes;
            this.saltSource = saltSource;
            this.capacityHint = capacityHint;
            this.capacitySource = capacitySource;
            this.cfFilter = cfFilter == null ? CfFilterSnapshot.disabled() : cfFilter;
        }

        public int saltBytes() {
            return saltBytes;
        }

        public ValueSource saltSource() {
            return saltSource;
        }

        public int capacityHint() {
            return capacityHint;
        }

        public ValueSource capacitySource() {
            return capacitySource;
        }

        public CfFilterSnapshot cfFilter() {
            return cfFilter;
        }
    }

    /**
     * Иммутабельный снимок конфигурации фильтра CF для конкретной таблицы.
     * Снимок хранит актуальный список column family в виде CSV и UTF-8 байтов, а также источник данных
     * (Avro-схема или значения по умолчанию). Объект безопасен для публикации между потоками.
     */
    public static final class CfFilterSnapshot {
        private static final byte[][] EMPTY_FAMILIES = new byte[0][];
        private static final CfFilterSnapshot DISABLED = new CfFilterSnapshot(false, EMPTY_FAMILIES, "", ValueSource.DEFAULT);

        private final boolean enabled;
        private final byte[][] families;
        private final String csv;
        private final ValueSource source;

        private CfFilterSnapshot(boolean enabled, byte[][] families, String csv, ValueSource source) {
            this.enabled = enabled;
            this.families = families;
            this.csv = csv;
            this.source = source;
        }

        static CfFilterSnapshot disabled() {
            return DISABLED;
        }

        static CfFilterSnapshot from(String[] names, ValueSource source) {
            if (names == null || names.length == 0) {
                return DISABLED;
            }
            int len = names.length;
            byte[][] immutableFamilies = new byte[len][];
            for (int i = 0; i < len; i++) {
                String name = names[i];
                immutableFamilies[i] = name == null
                        ? new byte[0]
                        : name.getBytes(StandardCharsets.UTF_8);
            }
            String csv = String.join(",", names);
            return new CfFilterSnapshot(true, immutableFamilies, csv, source == null ? ValueSource.AVRO : source);
        }

        /**
         * @return {@code true}, если для таблицы настроена явная фильтрация по column family
         */
        public boolean enabled() {
            return enabled;
        }

        /**
         * @return массив UTF-8 байтов имен column family; не {@code null}
         */
        public byte[][] families() {
            return families;
        }

        /**
         * @return CSV-представление списка column family; пустая строка, если фильтр отключён
         */
        public String csv() {
            return csv;
        }

        /**
         * @return источник данных (Avro, конфигурация или значение по умолчанию)
         */
        public ValueSource source() {
            return source;
        }
    }

    /**
     * Строковое представление конфигурации с маскировкой bootstrap.servers.
     * Используется только для диагностических логов.
     */
    @Override
    public String toString() {
        final String maskedBootstrap = maskBootstrap(bootstrap);
        return new StringBuilder(256)
                .append("H2kConfig{")
                .append("bootstrap=").append(maskedBootstrap)
                .append(", topicPattern='").append(topicPattern).append('\'')
                .append(", topicMaxLength=").append(topicMaxLength)
                .append(", avroSchemaDir='").append(avroSchemaDir).append('\'')
                .append(", avroSrUrls.size=").append(avroSchemaRegistryUrls.size())
                .append(", ensureTopics=").append(ensureTopics)
                .append(", ensureIncreasePartitions=").append(ensureIncreasePartitions)
                .append(", ensureDiffConfigs=").append(ensureDiffConfigs)
                .append(", topicPartitions=").append(topicPartitions)
                .append(", topicReplication=").append(topicReplication)
                .append(", adminTimeoutMs=").append(adminTimeoutMs)
                .append(", adminClientId='").append(adminClientId).append('\'')
                .append(", unknownBackoffMs=").append(unknownBackoffMs)
                .append(", awaitEvery=").append(awaitEvery)
                .append(", awaitTimeoutMs=").append(awaitTimeoutMs)
                .append(", batchAutotuneEnabled=").append(producerBatchAutotuneEnabled)
                .append(", batchAutotuneMin=").append(producerBatchAutotuneMinAwait)
                .append(", batchAutotuneMax=").append(producerBatchAutotuneMaxAwait)
                .append(", batchAutotuneHighMs=").append(producerBatchAutotuneLatencyHighMs)
                .append(", batchAutotuneLowMs=").append(producerBatchAutotuneLatencyLowMs)
                .append(", batchAutotuneCooldownMs=").append(producerBatchAutotuneCooldownMs)
                .append(", topicConfigs.size=").append(topicConfigs.size())
                .append(", saltBytesByTable.size=").append(saltBytesByTable.size())
                .append(", capacityHintByTable.size=").append(capacityHintByTable.size())
                .append(", avroSrAuth.size=").append(avroSrAuth.size())
                .append(", avroProps.size=").append(avroProps.size())
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
