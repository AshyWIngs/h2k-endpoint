package kz.qazmarka.h2k.kafka.ensure;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Внутренняя реализация ensure-логики Kafka-топиков.
 * Публичный API предоставляется обёрткой {@link TopicEnsurer}; данный класс держит бизнес-логику и состояние.
 */
final class TopicEnsureService implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(TopicEnsureService.class);
    // Сообщение о некорректном имени топика (чтобы не дублировать литерал)
    private static final String WARN_INVALID_TOPIC =
            "Некорректное имя Kafka-топика '{}': допускаются [a-zA-Z0-9._-], длина 1..{}, запрещены '.' и '..'";
    static final String CFG_RETENTION_MS = "retention.ms";
    static final String CFG_CLEANUP_POLICY = "cleanup.policy";
    static final String CFG_COMPRESSION_TYPE = "compression.type";
    static final String CFG_MIN_INSYNC_REPLICAS = "min.insync.replicas";
    /** Обёртка над AdminClient для unit‑тестируемости и стабильного API в классе. Может быть null, если ensureTopics=false. */
    private final KafkaTopicAdmin admin;
    /** Таймаут админских операций в миллисекундах (describe/create). */
    private final TopicEnsureConfig config;
    private final TopicEnsureState state;
    private final long adminTimeoutMs;
    /** Базовая величина backoff (мс) на «неуверенные» ошибки describe/create. */
    private final long unknownBackoffMs;
    /** Менеджер backoff-циклов (таймауты describe/create, повторные ensure). */
    private final TopicBackoffManager backoffManager;
    private final Map<String, String> topicConfigs;
    private final TopicDescribeSupport describeSupport;
    private final TopicCreationSupport creationSupport;

    /** Минимальный интерфейс для админских вызовов Kafka, удобный для unit-тестов. */
    /**
     * Помечает тему как подтверждённую (существует/успешно создана) и снимает возможный backoff.
     *
     * Вызов безопасен при повторении; кеш и карта backoff будут приведены к согласованному состоянию.
     *
     * @param t имя Kafka‑топика
     */
    private void markEnsured(String t) {
        state.ensured.add(t);
        backoffManager.markSuccess(t);
    }
    /**
     * Внутренний конструктор: принимает уже нормализованные значения из H2kConfig.
     * Не выполняет повторной валидации числовых параметров.
     *
     * @param admin обёртка над AdminClient или {@code null}, если ensureTopics=false
     * @param config неизменяемый набор параметров ensureTopics
     * @param state кеши и метрики ensureTopics
     */
    TopicEnsureService(KafkaTopicAdmin admin, TopicEnsureConfig config, TopicEnsureState state) {
        this.admin = admin;
        this.config = config;
        this.state = state;
        this.adminTimeoutMs = config.adminTimeoutMs();
        this.unknownBackoffMs = config.unknownBackoffMs();
        this.backoffManager = new TopicBackoffManager(state, config.unknownBackoffMs());
        this.topicConfigs = config.topicConfigs();
        TopicEnsureContext ctx = new TopicEnsureContext(state, backoffManager, adminTimeoutMs, this.topicConfigs, this::markEnsured, LOG);
        this.describeSupport = new TopicDescribeSupport(admin, ctx);
        this.creationSupport = new TopicCreationSupport(admin, config, ctx);
    }

    /**
     * Основной ensure-цикл для одной темы: нормализует имя, проверяет кеш и backoff, выполняет describe/create.
     */
    public void ensureTopic(String topic) {
        if (admin == null) {
            return;
        }
        state.ensureInvocations.increment();

        String t = normalizeTopicName(topic);
        if (t.isEmpty()) {
            LOG.warn("Пустое имя Kafka-топика — пропускаю ensure");
            return;
        }
        if (!isTopicNameAllowed(t)) {
            return;
        }
        if (fastCacheHit(t)) {
            return;       // уже успешно проверяли
        }
        if (backoffManager.shouldSkip(t)) {
            return; // действует backoff
        }

        TopicExistence ex = describeSupport.describeSingle(t);
        switch (ex) {
            case TRUE:
                creationSupport.ensureUpgrades(t);
                break;
            case UNKNOWN:
                // повторная попытка назначена в describeSupport
                break;
            case FALSE:
                creationSupport.ensureTopicCreated(t);
                break;
        }
    }

    /**
     * Быстрая проверка кеша подтверждённых тем.
     *
     * @param t имя Kafka‑топика
     * @return {@code true}, если тема уже была подтверждена ранее и повторная проверка не требуется
     */
    private boolean fastCacheHit(String t) {
        if (state.ensured.contains(t)) {
            state.ensureHitCache.increment();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kafka-топик '{}' уже проверен ранее — пропускаю ensure", t);
            }
            return true;
        }
        return false;
    }

    /**
     * Пытается создать тему, учитывая гонки и таймауты; обновляет метрики/кеш/логи.
     *
     * @param t имя Kafka‑топика
     */

    /** Быстрая проверка с ensure; возвращает {@code true}, только если тема подтверждена после вызова. */
    public boolean ensureTopicOk(String topic) {
        if (admin == null) {
            return false;               // ensureTopics=false
        }
        String t = normalizeTopicName(topic);
        if (t.isEmpty()) {
            return false;
        }
        if (!isTopicNameAllowed(t)) {
            return false;
        }
        // Быстрый путь: уже проверяли/создавали ранее — тема точно есть
        if (state.ensured.contains(t)) {
            return true;
        }
        // Иначе выполняем ensure и затем проверяем кеш
        ensureTopic(t);                                // ensureTopic сам валидирует имя и пр.
        return state.ensured.contains(t);
    }

    /** Batch-ensure нескольких тем с единичным describe и выборочным create. */
    public void ensureTopics(Collection<String> topics) {
        if (admin == null || topics == null || topics.isEmpty()) return;
        Set<String> toCheck = normalizeCandidates(topics);
        if (toCheck.isEmpty()) return;
        List<String> missing = describeSupport.describeMissing(toCheck);
        if (missing.isEmpty()) return;
        creationSupport.createMissingTopics(missing);
    }
    private LinkedHashSet<String> normalizeCandidates(Collection<String> topics) {
        LinkedHashSet<String> toCheck = new LinkedHashSet<>(topics.size());
        for (String raw : topics) {
            String t = normalizeTopicName(raw);
            boolean candidateOk = true;
            if (t.isEmpty()) {
                candidateOk = false; // пустые имена пропускаем молча
            }
            if (candidateOk && !isTopicNameAllowed(t)) {
                candidateOk = false;
            }
            if (candidateOk && fastCacheHit(t)) {
                candidateOk = false;
            }
            if (candidateOk && backoffManager.shouldSkip(t)) {
                candidateOk = false; // действует backoff — пропускаем до следующего окна
            }
            if (candidateOk) {
                toCheck.add(t);
            }
        }
        return toCheck;
    }

    /** Приводит имя Kafka-топика к нормализованному виду с trim и санитайзером из конфигурации. */
    private String normalizeTopicName(String topic) {
        String base = (topic == null) ? "" : topic.trim();
        return config.topicSanitizer().apply(base);
    }

    /** Проверяет соответствие имени ограничениям брокера и логирует предупреждение при нарушении. */
    private boolean isTopicNameAllowed(String topic) {
        if (TopicNameValidator.isValid(topic, config.topicNameMaxLen())) {
            return true;
        }
        LOG.warn(WARN_INVALID_TOPIC, topic, config.topicNameMaxLen());
        return false;
    }


    /**
     * Возвращает неизменяемый снимок внутренних счётчиков ensure-процесса.
     * Счётчики накопительные с момента создания инстанса; после перезапуска пира
     * значения обнуляются. Подсчёт размера очереди backoff выполняется без
     * лишних аллокаций через {@code state.unknownSize()}.
     *
     * @return неизменяемая карта «имя метрики → значение»
     */
    public Map<String, Long> getMetrics() {
        // 9 метрик → начальная ёмкость 13 (формула JDK для LinkedHashMap)
        Map<String, Long> m = new LinkedHashMap<>(13);
        m.put("ensure.invocations", state.ensureInvocations.longValue());
        m.put("ensure.cache.hit",   state.ensureHitCache.longValue());
        m.put("exists.true",        state.existsTrue.longValue());
        m.put("exists.false",       state.existsFalse.longValue());
        m.put("exists.unknown",     state.existsUnknown.longValue());
        m.put("create.ok",          state.createOk.longValue());
        m.put("create.race",        state.createRace.longValue());
        m.put("create.fail",        state.createFail.longValue());
        m.put("unknown.backoff.size", (long) state.unknownSize());
        return Collections.unmodifiableMap(m);
    }

    /**
     * Возвращает снимок очереди backoff: {@code topic → оставшееся время (мс)}.
     *
     * Контракт:
     *  - все значения считаются от единой точки времени {@code System.nanoTime()} (консистентность);
     *  - источник данных — немутируемая копия дедлайнов, см. {@link TopicEnsureState#snapshotUnknown()};
     *  - результат — неизменяемая карта; попытка модификации приводит к {@link UnsupportedOperationException};
     *  - сложность — {@code O(n)} по количеству элементов очереди; повторных снапшотов и лишних аллокаций нет.
     *
     * @return неизменяемая карта «topic → оставшиеся миллисекунды»
     */
    public Map<String, Long> getBackoffSnapshot() {
        final Map<String, Long> pending = state.snapshotUnknown();
        if (pending.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, Long> snapshot = new LinkedHashMap<>(pending.size());
        final long now = System.nanoTime();
        for (Map.Entry<String, Long> e : pending.entrySet()) {
            long remainingNs = e.getValue() - now;
            if (remainingNs < 0L) remainingNs = 0L;
            snapshot.put(e.getKey(), TimeUnit.NANOSECONDS.toMillis(remainingNs));
        }
        return Collections.unmodifiableMap(snapshot);
    }

    /** Закрывает AdminClient через обёртку KafkaTopicAdmin с таймаутом adminTimeoutMs. Безопасен к повторным вызовам. */
    @Override public void close() {
        if (admin != null) {
            if (LOG.isDebugEnabled()) LOG.debug("Закрываю Kafka AdminClient");
            admin.close(Duration.ofMillis(adminTimeoutMs));
        }
    }

    /**
     * Краткое диагностическое представление состояния TopicEnsurer.
     * Содержит только метаданные конфигурации и размеры кешей/метрик; без тяжёлых операций.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        sb.append("TopicEnsurer{")
          .append("partitions=").append(config.topicPartitions())
          .append(", replication=").append(config.topicReplication())
          .append(", adminTimeoutMs=").append(adminTimeoutMs)
          .append(", unknownBackoffMs=").append(unknownBackoffMs)
          .append(", ensured.size=").append(state.ensured.size())
          .append(", metrics={")
          .append("ensure=").append(state.ensureInvocations.longValue())
          .append(", hit=").append(state.ensureHitCache.longValue())
          .append(", existsT=").append(state.existsTrue.longValue())
          .append(", existsF=").append(state.existsFalse.longValue())
          .append(", existsU=").append(state.existsUnknown.longValue())
          .append(", createOk=").append(state.createOk.longValue())
          .append(", createRace=").append(state.createRace.longValue())
          .append(", createFail=").append(state.createFail.longValue())
          .append("}}");
        return sb.toString();
    }
}
