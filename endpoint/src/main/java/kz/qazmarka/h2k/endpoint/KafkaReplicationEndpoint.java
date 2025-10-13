package kz.qazmarka.h2k.endpoint;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.H2kConfig;
import kz.qazmarka.h2k.config.H2kConfig.Keys;
import kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor;
import kz.qazmarka.h2k.endpoint.topic.TopicManager;
import kz.qazmarka.h2k.kafka.ensure.TopicEnsurer;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSenderMetrics;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSenderTuner;
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
    /** Иммутабельный снимок настроек h2k.* с предвычисленными флагами для горячего пути. */
    private H2kConfig h2k;
    private TopicManager topicManager;
    private WalEntryProcessor walEntryProcessor;
    private final BatchSenderMetrics batchMetrics = new BatchSenderMetrics();
    private BatchSenderTuner batchTuner;
    private PhoenixTableMetadataProvider tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;

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
        this.h2k = H2kConfig.from(cfg, bootstrap, tableMetadataProvider);
        if (h2k.isProducerBatchAutotuneEnabled()) {
            this.batchTuner = new BatchSenderTuner(
                    true,
                    h2k.getProducerBatchAutotuneMinAwait(),
                    h2k.getProducerBatchAutotuneMaxAwait(),
                    h2k.getProducerBatchAutotuneLatencyHighMs(),
                    h2k.getProducerBatchAutotuneLatencyLowMs(),
                    h2k.getProducerBatchAutotuneCooldownMs());
        } else {
            this.batchTuner = null;
        }
        batchMetrics.updateConfiguredAwaitEvery(this.h2k.getAwaitEvery());
        logSaltSummary();
        final PayloadBuilder payload = new PayloadBuilder(decoder, h2k);
        final TopicEnsurer topicEnsurer = TopicEnsurer.createIfEnabled(h2k);
        this.topicManager = new TopicManager(h2k, topicEnsurer);
        this.walEntryProcessor = new WalEntryProcessor(payload, topicManager, producer, h2k);
        registerMetrics(payload, walEntryProcessor);
        logPayloadSerializer(payload);
        logInitSummary();
    }

    /**
     * Печатает сводку по карте «соли» rowkey (h2k.salt.map) в DEBUG. Ошибки не критичны.
     */
    private void logSaltSummary() {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        try {
            final Map<String, Integer> saltMap = h2k.getSaltBytesByTable();
            if (saltMap == null || saltMap.isEmpty()) {
                LOG.debug("Соль rowkey не задана в h2k.salt.map; будут использованы значения из Avro-схем (см. DEBUG при обработке таблиц).");
            } else {
                LOG.debug("Соль rowkey (h2k.salt.map): {} ({} записей)", saltMap, saltMap.size());
            }
        } catch (Exception t) {
            LOG.debug("Не удалось вывести сводку h2k.salt.map (не критично)", t);
        }
    }

    /**
     * Итоговая сводка параметров инициализации в DEBUG.
     */
    private void logInitSummary() {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        final String cfSource = (tableMetadataProvider == PhoenixTableMetadataProvider.NOOP)
                ? "disabled"
                : "per-table";
        LOG.debug("Инициализация завершена: шаблон_топика={}, cf_источник={}, ensure_topics={}, batch_counters={}, autotune_enabled={}",
                h2k.getTopicPattern(),
                cfSource,
                h2k.isEnsureTopics(), h2k.isProducerBatchCountersEnabled(),
                batchTuner != null && batchTuner.isEnabled());
    }

    /**
     * Выводит в INFO активные параметры Avro/Schema Registry.
     * Видно даже при стандартном уровне логирования.
     */
    private void logPayloadSerializer(PayloadBuilder payload) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        try {
            LOG.info("Параметры payload: {}", payload.describeSerializer());
        } catch (RuntimeException ex) {
            LOG.warn("Не удалось определить активный сериализатор payload: {}", ex.toString());
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
        registerMetric("producer.batch.await.configured", batchMetrics::configuredAwaitEvery);
        registerMetric("producer.batch.await.current", batchMetrics::currentAwaitEvery);
        registerMetric("producer.batch.flush.success.total", batchMetrics::flushSuccessTotal);
        registerMetric("producer.batch.flush.failures.total", batchMetrics::flushFailuresTotal);
        registerMetric("producer.batch.records.confirmed.total", batchMetrics::recordsConfirmedTotal);
        registerMetric("producer.batch.flush.latency.last.ms", batchMetrics::lastFlushLatencyMs);
        registerMetric("producer.batch.flush.latency.max.ms", batchMetrics::maxFlushLatencyMs);
        registerMetric("producer.batch.flush.latency.avg.ms", batchMetrics::avgFlushLatencyMs);
        // Метрики деградации: помогают увидеть длительные серии «тихих» ошибок ожидания ACK.
        registerMetric("producer.batch.fail.streak.current", batchMetrics::failureStreak);
        registerMetric("producer.batch.fail.streak.max", batchMetrics::maxFailureStreak);
        registerMetric("producer.batch.fail.last.ms", batchMetrics::lastFailureAtMs);
        registerMetric("producer.batch.fail.last.await", batchMetrics::lastFailureAwaitEvery);
        registerAutotuneMetrics();
        registerMetric("wal.rowbuffer.upsizes", walProcessor::rowBufferUpsizeCount);
        registerMetric("wal.rowbuffer.trims", walProcessor::rowBufferTrimCount);
    }

    private void registerAutotuneMetrics() {
        if (batchTuner == null) {
            return;
        }
        registerMetric("producer.batch.autotune.enabled", () -> batchTuner.isEnabled() ? 1L : 0L);
        registerMetric("producer.batch.await.recommended", batchTuner::recommendedAwaitEvery);
        registerMetric("producer.batch.autotune.decisions.total", batchTuner::decisionsTotal);
        registerMetric("producer.batch.autotune.cooldown.remaining.ms", batchTuner::cooldownRemainingMs);
        registerMetric("producer.batch.autotune.last.latency.ms", batchTuner::lastObservedLatencyMs);
        registerMetric("producer.batch.autotune.last.await", batchTuner::lastObservedAwaitEvery);
    }

    private void registerMetric(String name, java.util.function.LongSupplier supplier) {
        try {
            topicManager.registerMetric(name, supplier);
        } catch (RuntimeException ex) {
            LOG.warn("Не удалось зарегистрировать метрику '{}': {}", name, ex.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Трассировка ошибки при регистрации метрики '{}'", name, ex);
            }
        }
    }
    /**
     * Возвращает строку bootstrap‑серверов Kafka из конфигурации или бросает {@link IOException}, если параметр отсутствует.
     * Пустая строка после {@code trim()} считается отсутствующим параметром.
     */
    private static String readBootstrapOrThrow(Configuration cfg) throws IOException {
        final String bootstrap = cfg.get(Keys.BOOTSTRAP, "").trim();
        if (bootstrap.isEmpty()) {
            throw new IOException("Отсутствует обязательный параметр конфигурации: " + Keys.BOOTSTRAP);
        }
        return bootstrap;
    }

    /** Выбирает и настраивает единственный поддерживаемый декодер Phoenix (Avro-схемы). */
    private Decoder chooseDecoder(Configuration cfg) {
        tableMetadataProvider = PhoenixTableMetadataProvider.NOOP;
        final String configuredDir = cfg.getTrimmed(H2kConfig.Keys.AVRO_SCHEMA_DIR);
        final String schemaDir = (configuredDir == null || configuredDir.isEmpty())
                ? H2kConfig.DEFAULT_AVRO_SCHEMA_DIR
                : configuredDir;
        try {
            AvroSchemaRegistry avro = new AvroSchemaRegistry(Paths.get(schemaDir));
            AvroPhoenixSchemaRegistry registry = new AvroPhoenixSchemaRegistry(avro);
            tableMetadataProvider = registry;
            LOG.debug("Режим декодирования: phoenix-avro, каталог={}", schemaDir);
            return new ValueCodecPhoenix(registry);
        } catch (Exception e) {
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
     * @implNote Сборка properties вынесена в {@code ProducerPropsFactory.build(...)}. Конфигурация cfg уже включает peer-CONFIG; приоритет peer над hbase-site.xml сохраняется.
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
            if (batchTuner != null) {
                batchTuner.afterBatch(batchMetrics, true);
            }
            return true;
        }

        boolean success = false;
        boolean result = false;
        try {
            replicateOnce(entries);
            success = true;
            result = true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Репликация: поток прерван; запросим повтор партии", ie);
            return false;
        } catch (java.util.concurrent.ExecutionException ee) {
            LOG.error("Репликация: ошибка при ожидании подтверждений Kafka", ee);
            return false;
        } catch (java.util.concurrent.TimeoutException te) {
            LOG.error("Репликация: таймаут ожидания подтверждений Kafka", te);
            return false;
        } catch (org.apache.kafka.common.KafkaException ke) {
            LOG.error("Репликация: ошибка продьюсера Kafka", ke);
            return false;
        } catch (Exception e) {
            LOG.error("Репликация: непредвиденная ошибка", e);
            return false;
        } finally {
            if (batchTuner != null) {
                batchTuner.afterBatch(batchMetrics, success);
            }
        }
        return result;
    }

    private void replicateOnce(List<WAL.Entry> entries)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        final int configuredAwaitEvery = h2k.getAwaitEvery();
        final int awaitEvery = batchTuner != null
                ? batchTuner.selectAwaitEvery(configuredAwaitEvery, batchMetrics)
                : configuredAwaitEvery;
        final int awaitTimeoutMs = h2k.getAwaitTimeoutMs();

        batchMetrics.updateConfiguredAwaitEvery(awaitEvery);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Репликация: записей={}, awaitEvery={}, awaitTimeoutMs={}, базовый awaitEvery={}",
                    entries.size(), awaitEvery, awaitTimeoutMs, configuredAwaitEvery);
        }

        final boolean includeWalMeta = false;

        try (BatchSender sender = new BatchSender(
                awaitEvery,
                awaitTimeoutMs,
                h2k.isProducerBatchCountersEnabled(),
                h2k.isProducerBatchDebugOnFailure(),
                batchMetrics)) {
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
                              boolean includeWalMeta) {
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
        notifyStopped();
    }
    /**
     * Построитель настроек {@link KafkaProducer}. Выполняется один раз при старте; не участвует в горячем пути.
     * Собирает безопасные дефолты и применяет переопределения из префикса {@code h2k.producer.*}
     * без перезаписи уже заданных ключей.
     */
    private static final class ProducerPropsFactory {
        private ProducerPropsFactory() {}

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
        static Properties build(Configuration cfg, String bootstrap) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

            // дефолты с возможностью переопределения через h2k.producer.*
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, cfg.get("h2k.producer.enable.idempotence", "true"));
            props.put(ProducerConfig.ACKS_CONFIG, cfg.get("h2k.producer.acks", "all"));
            props.put(ProducerConfig.RETRIES_CONFIG, cfg.get("h2k.producer.retries", String.valueOf(Integer.MAX_VALUE)));
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, cfg.get("h2k.producer.delivery.timeout.ms", "180000"));
            props.put(ProducerConfig.LINGER_MS_CONFIG, cfg.get("h2k.producer.linger.ms", "50"));
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, cfg.get("h2k.producer.batch.size", "65536"));
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cfg.get("h2k.producer.compression.type", "lz4"));
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, cfg.get("h2k.producer.max.in.flight", "1"));

            // Набор валидных ключей Kafka‑продьюсера (используем для фильтрации pass‑through)
            final java.util.Set<String> kafkaValidKeys = org.apache.kafka.clients.producer.ProducerConfig.configNames();
            // Наши «служебные» ключи, которые не должны попадать в конфиг Kafka‑продьюсера
            final java.util.Set<String> h2kInternalKeys = new java.util.HashSet<>(
                    java.util.Arrays.asList(
                            "await.every",
                            "await.timeout.ms",
                            "batch.counters.enabled",
                            "batch.debug.on.failure"
                    )
            );

            // client.id: базово per‑host; для дефолтного значения добавляем короткий случайный суффикс,
            // чтобы избежать коллизии MBean "kafka.producer:type=app-info,id=<client.id>" при нескольких пирах в одном JVM.
            String computedDefaultClientId;
            try {
                String host = java.net.InetAddress.getLocalHost().getHostName();
                computedDefaultClientId = (host == null || host.isEmpty())
                        ? (H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + java.util.UUID.randomUUID())
                        : (H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + host);
            } catch (java.net.UnknownHostException ignore) {
                computedDefaultClientId = H2kConfig.DEFAULT_ADMIN_CLIENT_ID + '-' + java.util.UUID.randomUUID();
            }
            // Пользователь может явно задать h2k.producer.client.id — тогда уважаем его как есть.
            String requestedClientId = cfg.get("h2k.producer.client.id", computedDefaultClientId);
            String clientIdToUse = requestedClientId;
            if (requestedClientId.equals(computedDefaultClientId)) {
                // Добавляем короткий суффикс только для дефолтного значения
                String rnd8 = java.util.UUID.randomUUID().toString().substring(0, 8);
                clientIdToUse = requestedClientId + '-' + rnd8;
            }
            props.put(org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG, clientIdToUse);

            // pass‑through: только «родные» ключи Kafka‑продьюсера; все прочие игнорируем
            final String prefix = Keys.PRODUCER_PREFIX;
            final int prefixLen = prefix.length();
            for (java.util.Map.Entry<String, String> e : cfg) {
                final String k = e.getKey();
                final boolean hasPrefix = k.startsWith(prefix);
                final boolean isAlias   = "h2k.producer.max.in.flight".equals(k);
                if (hasPrefix && !isAlias) {
                    final String real = k.substring(prefixLen);
                    final boolean isInternal = h2kInternalKeys.contains(real);
                    final boolean isKafkaKey = kafkaValidKeys.contains(real);
                    if (!isInternal && isKafkaKey) {
                        props.putIfAbsent(real, e.getValue());
                    }
                }
            }
            return props;
        }
    }
}
