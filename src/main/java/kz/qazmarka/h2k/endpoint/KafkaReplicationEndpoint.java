package kz.qazmarka.h2k.endpoint;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.EnsureSettings;
import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfig.Keys;
import kz.qazmarka.h2k.config.ProducerAwaitSettings;
import kz.qazmarka.h2k.config.TopicNamingSettings;
import kz.qazmarka.h2k.endpoint.metrics.H2kMetricsJmx;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.payload.builder.PayloadBuilder;
import kz.qazmarka.h2k.schema.decoder.Decoder;
import kz.qazmarka.h2k.schema.decoder.ValueCodecPhoenix;
import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.avro.local.AvroSchemaRegistry;
import kz.qazmarka.h2k.schema.registry.avro.phoenix.AvroPhoenixSchemaRegistry;


/**
 * Репликация изменений из HBase 1.4.13 в Kafka (производитель 2.x).
 *
 * Назначение
 *  - Принимать батчи WAL-записей от фреймворка репликации HBase и надёжно публиковать их в Kafka.
 *  - Формировать Avro-запись (GenericRecord) по набору ячеек с учётом Phoenix-типов.
 *
 * Производительность и потокобезопасность
 *  - Горячий путь максимально прямолинейный: группировка по rowkey → сборка Avro {@code GenericRecord}
 *    → бинарная сериализация в формат Confluent → отправка в Kafka.
 *  - Все тяжёлые инициализации выполняются один раз в init()/doStart(): декодер, конфиг, KafkaProducer, PayloadBuilder.
 *  - Класс используется из потока репликации HBase, дополнительных потоков не создаёт.
 *
 * Логирование
 *  - INFO: только существенные ошибки и факты остановки/старта.
 *  - DEBUG: подробности инициализации, трассировка ошибок отправки, диагностические параметры.
 *  - Все сообщения на русском для удобства сопровождения.
 *
 * Конфигурация
 *  - Основные ключи h2k.* см. в {@link kz.qazmarka.h2k.config.H2kConfig H2kConfig}.
 *  - В частности: bootstrap серверов Kafka, выбор режима декодера, шаблон имён топиков.
 */
public final class KafkaReplicationEndpoint extends BaseReplicationEndpoint {

    /** Логгер класса. Все сообщения — на русском языке. */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReplicationEndpoint.class);

    // ядро
    /** Kafka Producer для отправки событий; ключ/значение сериализуются как байты. */
    private Producer<byte[], byte[]> producer;

    // вынесенная конфигурация и сервисы
    /** Плоские DTO-выкладки конфигурации для быстрого доступа без повторных геттеров. */
    private TopicNamingSettings topicSettings;
    private EnsureSettings ensureSettings;
    private ProducerAwaitSettings producerSettings;
    private TopicManager topicManager;
    private WalEntryProcessor walEntryProcessor;
    private PhoenixTableMetadataProvider tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
    /** Имя зарегистрированного JMX MBean с метриками H2K (для корректного снятия регистрации). */
    private javax.management.ObjectName h2kJmxName;

    /**
     * Счётчики отказов репликации и отметка времени последней ошибки.
     * Используются только для метрик/диагностики, не влияют на поведение.
     */
    private final LongAdder replicateFailures = new LongAdder();
    private final AtomicLong lastReplicateFailureAt = new AtomicLong(0L);

    /**
     * Инициализация эндпоинта: чтение конфигурации, подготовка продьюсера, декодера и сборщика payload.
     * Выполняется один раз на старте RegionServer.
     * @param context контекст от HBase
     * @throws IOException если отсутствует обязательный параметр h2k.kafka.bootstrap.servers
     */
    @Override
    public void init(ReplicationEndpoint.Context context) throws IOException {
        super.init(context);
        final Configuration cfg = context.getConfiguration();

        // обязательный параметр
        final String bootstrap = readBootstrapOrThrow(cfg);

        // producer
        setupProducer(cfg, bootstrap);

        // decoder
        Decoder decoder = chooseDecoder(cfg);

        // immut-конфиг, билдер и энсюрер
        H2kConfig runtimeConfig = H2kConfig.from(cfg, bootstrap, tableMetadataProvider);
        this.topicSettings = runtimeConfig.getTopicSettings();
        this.ensureSettings = runtimeConfig.getEnsureSettings();
        this.producerSettings = runtimeConfig.getProducerSettings();
        final PayloadBuilder payload = new PayloadBuilder(decoder, runtimeConfig);
        final TopicEnsurer topicEnsurer = TopicEnsurer.createIfEnabled(
                ensureSettings,
                topicSettings,
                bootstrap,
                null); // AdminClient конфигурация формируется внутри TopicEnsurer
        this.topicManager = new TopicManager(topicSettings, topicEnsurer);
        this.walEntryProcessor = new WalEntryProcessor(payload, topicManager, producer, runtimeConfig);
        registerMetrics(payload, walEntryProcessor);
        // JMX экспорт метрик через DynamicMBean: безопасно пытаемся зарегистрировать
        try {
            javax.management.ObjectName name = H2kMetricsJmx.register(this.topicManager);
            this.h2kJmxName = name;
            if (name != null && LOG.isInfoEnabled()) {
                LOG.info("JMX-метрики H2K зарегистрированы: {}", name);
            } else if (name == null) {
                LOG.warn("JMX-метрики H2K не были зарегистрированы (см. DEBUG для деталей)");
            }
        } catch (RuntimeException ex) {
            LOG.warn("Не удалось зарегистрировать JMX-метрики H2K: {}", ex.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки регистрации JMX-метрик H2K", ex);
            }
        }
        logPayloadSerializer(payload);
        logInitSummary();
    }

    /**
     * Итоговая сводка параметров инициализации в DEBUG.
     */
    private void logInitSummary() {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        final String cfSource;
        if (tableMetadataProvider == PhoenixTableMetadataProvider.NOOP) {
            cfSource = "disabled";
        } else {
            cfSource = "per-table";
        }
        LOG.debug("Инициализация завершена: шаблон_топика={}, cf_источник={}, ensure_topics={}",
                topicSettings.getPattern(),
                cfSource,
                ensureSettings.isEnsureTopics());
    }

    /**
     * Выводит в INFO активные параметры Avro/Schema Registry.
     * Видно даже при стандартном уровне логирования.
     */
    private static void logPayloadSerializer(PayloadBuilder payload) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        try {
            LOG.info("Параметры payload: {}", payload.describeSerializer());
        } catch (RuntimeException ex) {
            LOG.warn("Не удалось определить активный сериализатор payload: {}", ex.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки сериализатора", ex);
            }
        }
    }

    private void registerMetrics(PayloadBuilder payload, WalEntryProcessor walProcessor) {
        registerMetric("wal.entries.total", walProcessor::entriesTotal);
        registerMetric("wal.rows.total", walProcessor::rowsTotal);
        registerMetric("wal.cells.total", walProcessor::cellsTotal);
        registerMetric("wal.rows.filtered", walProcessor::rowsFilteredTotal);
        registerMetric("schema.registry.register.success", payload::schemaRegistryRegisteredCount);
        registerMetric("schema.registry.register.failures", payload::schemaRegistryFailedCount);
        registerMetric("wal.rowbuffer.upsizes", walProcessor::rowBufferUpsizeCount);
        registerMetric("wal.rowbuffer.trims", walProcessor::rowBufferTrimCount);
        // Метрики отказов репликации
        registerMetric("replicate.failures.total", replicateFailures::sum);
        registerMetric("replicate.last.failure.epoch.ms", lastReplicateFailureAt::get);
    }

    private void registerMetric(String name, LongSupplier supplier) {
        try {
            topicManager.registerMetric(name, supplier);
        } catch (RuntimeException ex) {
            LOG.warn("Не удалось зарегистрировать метрику '{}': {}", name, ex.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки при регистрации метрики '{}'", name, ex);
            }
        }
    }
    /**
     * Возвращает строку bootstrap‑серверов Kafka из конфигурации или бросает {@link IllegalStateException}, если параметр отсутствует.
     * Пустая строка после {@code trim()} считается отсутствующим параметром.
     * Видимость по умолчанию для модульных тестов.
     */
    static String readBootstrapOrThrow(Configuration cfg) {
        final String bootstrap = cfg.get(Keys.BOOTSTRAP, "").trim();
        if (bootstrap.isEmpty()) {
            throw new IllegalStateException("Отсутствует обязательный параметр конфигурации: " + Keys.BOOTSTRAP);
        }
        return bootstrap;
    }

    /** Выбирает и настраивает единственный поддерживаемый декодер Phoenix (Avro-схемы). */
    private Decoder chooseDecoder(Configuration cfg) {
        tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
        final String configuredDir = cfg.getTrimmed(H2kConfig.Keys.AVRO_SCHEMA_DIR);
        final String schemaDir;
        if (configuredDir == null || configuredDir.isEmpty()) {
            schemaDir = H2kConfig.DEFAULT_AVRO_SCHEMA_DIR;
        } else {
            schemaDir = configuredDir;
        }
        try {
            AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get(schemaDir));
            AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro);
            tableMetadataProvider = registry;
            LOG.debug("Режим декодирования: phoenix-avro, каталог={}", schemaDir);
            return new ValueCodecPhoenix(registry);
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "Не удалось инициализировать AvroPhoenixSchemaRegistry (" + schemaDir + "): " + e.getMessage(), e);
        }
    }

    /**
     * Конфигурация и создание {@link KafkaProducer}.
     * Заполняет обязательные параметры сериализации ключа/значения, включает идемпотентность и другие
     * безопасные дефолты, затем применяет любые переопределения из префикса {@code h2k.producer.*}.
     *
     * @param cfg HBase-конфигурация (источник префиксных параметров)
     * @param bootstrap список брокеров Kafka (формат host:port[,host2:port2])
     * @implNote Сборка properties вынесена в {@code ProducerPropsFactory.build(...)}. Конфигурация cfg уже включает peer-CONFIG.
     */
    private void setupProducer(Configuration cfg, String bootstrap) {
        Properties props = ProducerPropsFactory.build(cfg, bootstrap);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Kafka‑producer: client.id={}, брокеры={}, acks={}, компрессия={}, linger.ms={}, batch.size={}, идемпотентность={}",
                    props.get(ProducerConfig.CLIENT_ID_CONFIG),
                    props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    props.get(ProducerConfig.ACKS_CONFIG),
                    props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG),
                    props.get(ProducerConfig.LINGER_MS_CONFIG),
                    props.get(ProducerConfig.BATCH_SIZE_CONFIG),
                    props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        }

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Основной цикл обработки партии WAL‑записей: группировка по rowkey, сборка JSON и асинхронная отправка сообщений в Kafka
     * с последующим ожиданием подтверждений.
     *
     * @param ctx контекст с WAL‑записями
     * @return {@code true} — продолжать репликацию; {@code false} — попросить HBase повторить партию
     * @implNote Прерывание потока конвертируется в {@code false} с восстановлением флага прерывания.
     */
    @Override
    public boolean replicate(ReplicationEndpoint.ReplicateContext ctx) {
        final List<WAL.Entry> entries = ctx.getEntries();
        if (entries == null || entries.isEmpty()) {
            return true;
        }

        try {
            replicateOnce(entries);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            failReplicate("Репликация: поток прерван; запросим повтор партии", ie, true);
            return false;
        } catch (ExecutionException | TimeoutException ex) {
            failReplicate("Репликация: ошибка при ожидании подтверждений Kafka", ex, false);
            return false;
        } catch (org.apache.kafka.common.KafkaException ex) {
            failReplicate("Репликация: ошибка продьюсера Kafka", ex, false);
            return false;
        } catch (RuntimeException ex) {
            failReplicate("Репликация: непредвиденная ошибка", ex, false);
            return false;
        }
    }

    /**
     * Унифицированная обработка отказа replicate(): краткое сообщение на WARN/ERROR и стек на DEBUG.
     * @param prefix краткий текст события
     * @param ex исключение
     * @param warn если true — писать WARN; иначе ERROR
     * @return всегда false, чтобы попросить HBase повторить партию
     */
    private void failReplicate(String prefix, Throwable ex, boolean warn) {
        if (warn) {
            LOG.warn("{}: {}", prefix, ex.getMessage());
        } else {
            LOG.error("{}: {}", prefix, ex.getMessage());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Трассировка ошибки в replicate()", ex);
        }
        onReplicateFailure();
    }

    /**
     * Фиксирует отказ репликации для метрик и диагностики.
     * Вызывается из обработчиков исключений в replicate().
     */
    private void onReplicateFailure() {
        replicateFailures.increment();
        lastReplicateFailureAt.set(System.currentTimeMillis());
    }

    private void replicateOnce(List<WAL.Entry> entries)
            throws InterruptedException, ExecutionException, TimeoutException {
    // Контракт: отправляет события в Kafka, периодически ожидая подтверждения по awaitEvery/awaitTimeoutMs.
    // Исключения пробрасываются наружу и обрабатываются в replicate(). Никаких sleep/побочных потоков.
    final int awaitEvery = producerSettings.getAwaitEvery();
    final int awaitTimeoutMs = producerSettings.getAwaitTimeoutMs();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: записей={}, awaitEvery={}, awaitTimeoutMs={}",
                    entries.size(), awaitEvery, awaitTimeoutMs);
        }

        final boolean includeWalMeta = false;

        try (BatchSender sender = new BatchSender(
                awaitEvery,
                awaitTimeoutMs)) {
            for (WAL.Entry entry : entries) {
                processEntry(entry, sender, includeWalMeta);
            }
        }
    }

    /**
     * Обрабатывает один {@link WAL.Entry}: вычисляет имя топика, безопасно проверяет/создаёт тему,
     * формирует и отправляет сообщения по строкам.
     *
     * @param entry         запись WAL
     * @param sender        батч‑отправитель для ожидания ack по порогам
     * @param includeWalMeta включать ли метаданные WAL в payload
     */
    private void processEntry(WAL.Entry entry,
                              BatchSender sender,
                              boolean includeWalMeta)
            throws InterruptedException, ExecutionException, TimeoutException {
        walEntryProcessor.process(entry, sender, includeWalMeta);
    }

    /** В HBase 1.4 {@code Context} не предоставляет getPeerUUID(); сигнатура метода требуется API базового класса.
     *  Для совместимости с этой версией возвращаем {@code null} (допустимое значение для данного API).
     */
    @Override public UUID getPeerUUID() { return null; }
    /**
     * Сообщает фреймворку об успешном старте эндпоинта.
     */
    @Override
    protected void doStart() {
        notifyStarted();
    }
    /**
     * Корректное завершение работы: сброс и закрытие Kafka‑producer и TopicEnsurer, затем уведомление о стопе.
     * Исключения при закрытии не пробрасываются и логируются на уровне DEBUG.
     */
    @Override protected void doStop() {
        try {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        } catch (Exception e) {
            // При завершении работы проглатываем исключение, но выводим debug для диагностики
            if (LOG.isDebugEnabled()) {
                LOG.debug("doStop(): ошибка при закрытии Kafka producer (игнорируется при завершении работы)", e);
            }
        }
        if (topicManager != null) {
            topicManager.closeQuietly();
        }
        // Снятие регистрации JMX-метрик (если были зарегистрированы)
        try {
            if (h2kJmxName != null) {
                H2kMetricsJmx.unregisterQuietly(h2kJmxName);
                h2kJmxName = null;
            }
        } catch (RuntimeException ignore) {
            // игнорируем при остановке
        }
        notifyStopped();
    }
    /**
     * Построитель настроек {@link KafkaProducer}. Выполняется один раз при старте; не участвует в горячем пути.
     * Собирает безопасные дефолты и применяет переопределения из префикса {@code h2k.producer.*}
     * без перезаписи уже заданных ключей.
     */
    private static final class ProducerPropsFactory {

        /**
         * Формирует {@link Properties} для {@link KafkaProducer}:
         *  - базовые обязательные параметры (bootstrap, сериализаторы);
         *  - безопасные дефолты (acks=all, enable.idempotence=true, retries и т.д.);
         *  - pass-through: только валидные {@code h2k.producer.*} прокидываются как «родные» ключи продьюсера, если не заданы выше;
         *  - {@code client.id}: по умолчанию префикс + hostname + короткий случайный суффикс, фолбэк — UUID.
         *
         * @param cfg       HBase-конфигурация
         * @param bootstrap список брокеров Kafka (host:port[,host2:port2])
         * @return заполненный набор свойств для конструктора продьюсера
         */
        private static final String BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
        private static final Set<String> H2K_INTERNAL_KEYS = new HashSet<>(
                Arrays.asList(
                        "await.every",
                        "await.timeout.ms",
                        "batch.counters.enabled",
                        "batch.debug.on.failure"
                )
        );

        static Properties build(Configuration cfg, String bootstrap) {
            Properties props = createBaseProperties(bootstrap);
            applyDefaultProducerSettings(props, cfg);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, computeClientId(cfg));
            applyPassThroughSettings(props, cfg);
            return props;
        }

        private static Properties createBaseProperties(String bootstrap) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER);
            return props;
        }

        private static void applyDefaultProducerSettings(Properties props, Configuration cfg) {
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, cfg.get("h2k.producer.enable.idempotence", "true"));
            props.put(ProducerConfig.ACKS_CONFIG, cfg.get("h2k.producer.acks", "all"));
            props.put(ProducerConfig.RETRIES_CONFIG, cfg.get("h2k.producer.retries", String.valueOf(Integer.MAX_VALUE)));
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, cfg.get("h2k.producer.delivery.timeout.ms", "180000"));
            props.put(ProducerConfig.LINGER_MS_CONFIG, cfg.get("h2k.producer.linger.ms", "50"));
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, cfg.get("h2k.producer.batch.size", "65536"));
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cfg.get("h2k.producer.compression.type", "lz4"));
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                    cfg.get("h2k.producer.max.in.flight", "1"));
        }

        private static String computeClientId(Configuration cfg) {
            String defaultClientId = buildDefaultClientId();
            String configuredClientId = cfg.get("h2k.producer.client.id", defaultClientId);
            if (!configuredClientId.equals(defaultClientId)) {
                return configuredClientId;
            }
            String randomSuffix = UUID.randomUUID().toString().substring(0, 8);
            return configuredClientId + '-' + randomSuffix;
        }

        private static String buildDefaultClientId() {
            try {
                String host = InetAddress.getLocalHost().getHostName();
                if (host == null || host.isEmpty()) {
                    return H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + UUID.randomUUID();
                }
                return H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + host;
            } catch (UnknownHostException ignore) {
                return H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + UUID.randomUUID();
            }
        }

        private static void applyPassThroughSettings(Properties props, Configuration cfg) {
            Set<String> kafkaKeys = ProducerConfig.configNames();
            String prefix = Keys.PRODUCER_PREFIX;
            int prefixLen = prefix.length();
            for (Map.Entry<String, String> entry : cfg) {
                String key = entry.getKey();
                if (key.startsWith(prefix) && !"h2k.producer.max.in.flight".equals(key)) {
                    String realKey = key.substring(prefixLen);
                    if (!H2K_INTERNAL_KEYS.contains(realKey) && kafkaKeys.contains(realKey)) {
                        props.putIfAbsent(realKey, entry.getValue());
                    }
                }
            }
        }
    }
}
