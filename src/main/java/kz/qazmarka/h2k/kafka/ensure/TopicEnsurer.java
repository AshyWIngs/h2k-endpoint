package kz.qazmarka.h2k.kafka.ensure;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kz.qazmarka.h2k.config.EnsureSettings;
import kz.qazmarka.h2k.config.TopicNamingSettings;
import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdmin;
import kz.qazmarka.h2k.kafka.ensure.admin.KafkaTopicAdminClient;
import kz.qazmarka.h2k.kafka.ensure.config.TopicEnsureConfig;

/**
 * Высокоуровневый фасад ensure-логики Kafka-топиков.
 * Инкапсулирует {@link EnsureCoordinator}, поставляя безопасный NOOP-экземпляр для сценариев,
 * когда ensure отключён конфигурацией или отсутствуют настройки bootstrap. Благодаря этому
 * вызывающий код не обязан проверять {@code null} и может работать с единым API.
 */
public final class TopicEnsurer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TopicEnsurer.class);

    private enum Mode { DISABLED, ENABLED, DELEGATE }

    private static final TopicEnsurer DISABLED = new TopicEnsurer();

    private final Mode mode;
    private final EnsureCoordinator coordinator;
    private final TopicEnsureExecutor executor;
    private final EnsureRuntimeState state;
    private final EnsureDelegate ensureDelegate;

    private TopicEnsurer() {
        this.mode = Mode.DISABLED;
        this.coordinator = null;
        this.executor = null;
        this.state = null;
        this.ensureDelegate = null;
    }

    /**
     * Пакетный конструктор для модульных тестов и внутренних сборок.
     * Позволяет напрямую подставлять {@link EnsureCoordinator}, {@link TopicEnsureExecutor}
     * и {@link EnsureRuntimeState} без создания AdminClient.
     */
    TopicEnsurer(EnsureCoordinator coordinator,
                 TopicEnsureExecutor executor,
                 EnsureRuntimeState state) {
        this.mode = Mode.ENABLED;
        this.coordinator = coordinator;
        this.executor = executor;
        this.state = resolveState(state, coordinator);
        this.ensureDelegate = null;
    }

    private TopicEnsurer(EnsureDelegate delegate) {
        this.mode = Mode.DELEGATE;
        this.coordinator = null;
        this.executor = null;
        this.state = null;
        this.ensureDelegate = delegate;
    }

    private TopicEnsurer(EnsureLifecycle lifecycle) {
        this.mode = Mode.ENABLED;
        this.coordinator = lifecycle.coordinator;
        this.executor = lifecycle.executor;
        this.state = lifecycle.state;
        this.ensureDelegate = null;
    }

    /**
     * Фабрика для модульных тестов: позволяет заменить ensure-логику на простую заглушку без reflection.
     */
    public static TopicEnsurer testingDelegate(EnsureDelegate delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate == null");
        }
        return new TopicEnsurer(delegate);
    }

    /**
     * Возвращает активный энсюрер, используя плоские DTO конфигурации.
     * Упрощает тестирование и отделяет ensure-цепочку от громоздкого {@code H2kConfig}.
     */
    public static TopicEnsurer createIfEnabled(EnsureSettings ensureSettings,
                                               TopicNamingSettings topicSettings,
                                               String bootstrap,
                                               Properties adminProps) {
        Objects.requireNonNull(ensureSettings, "EnsureSettings не может быть null");
        Objects.requireNonNull(topicSettings, "TopicNamingSettings не может быть null");
        if (!ensureSettings.isEnsureTopics()) {
            return disabled();
        }
        String trimmedBootstrap = bootstrap == null ? "" : bootstrap.trim();
        if (trimmedBootstrap.isEmpty()) {
            LOG.warn("TopicEnsurer: не задан bootstrap Kafka — ensureTopics будет отключён");
            return disabled();
        }
        Properties props = new Properties();
        if (adminProps != null && !adminProps.isEmpty()) {
            props.putAll(adminProps);
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, trimmedBootstrap);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, ensureSettings.getAdminSpec().getClientId());
        String clientId = prepareClientId(props);
        long timeoutMs = ensureSettings.getAdminSpec().getTimeoutMs();
        int timeout = timeoutMs > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(timeoutMs, 1L);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout);
        AdminClient adminClient = AdminClient.create(props);
        KafkaTopicAdmin admin = new KafkaTopicAdminClient(adminClient);
        TopicEnsureConfig config = TopicEnsureConfig.from(ensureSettings, topicSettings);

        try (EnsureLifecycle lifecycle = EnsureLifecycle.create(admin, config, clientId)) {
            TopicEnsurer ensurer = new TopicEnsurer(lifecycle);
            lifecycle.transferOwnership();
            return ensurer;
        }
    }

    /**
     * Builder для безопасного создания компонентов TopicEnsurer с автоматическим cleanup при ошибках.
     */
    private static final class EnsureLifecycle implements AutoCloseable {
        final EnsureCoordinator coordinator;
        final TopicEnsureExecutor executor;
        final EnsureRuntimeState state;
        private boolean transferred;

        private EnsureLifecycle(EnsureCoordinator coordinator,
                                TopicEnsureExecutor executor,
                                EnsureRuntimeState state) {
            this.coordinator = coordinator;
            this.executor = executor;
            this.state = state;
            this.transferred = false;
        }

        static EnsureLifecycle create(KafkaTopicAdmin admin, TopicEnsureConfig config, String clientId) {
            EnsureRuntimeState state = new EnsureRuntimeState();
            EnsureCoordinator coordinator = new EnsureCoordinator(admin, config, state);
            try (ExecutorHolder holder = new ExecutorHolder(new TopicEnsureExecutor(coordinator, clientId + "-ensure"))) {
                TopicEnsureExecutor executor = holder.detach();
                return new EnsureLifecycle(coordinator, executor, state);
            } catch (RuntimeException creationError) {
                closeQuietly(coordinator);
                throw creationError;
            }
        }

        void transferOwnership() {
            this.transferred = true;
        }

        @Override
        public void close() {
            if (transferred) {
                return;
            }
            closeQuietly(executor);
            closeQuietly(coordinator);
        }

        private static final class ExecutorHolder implements AutoCloseable {
            private TopicEnsureExecutor executor;

            ExecutorHolder(TopicEnsureExecutor executor) {
                this.executor = executor;
            }

            TopicEnsureExecutor detach() {
                TopicEnsureExecutor retained = executor;
                executor = null;
                return retained;
            }

            @Override
            public void close() {
                closeQuietly(executor);
                executor = null;
            }
        }
    }

    /**
     * Возвращает заранее созданный NOOP-экземпляр, который безопасно игнорирует все вызовы.
     */
    public static TopicEnsurer disabled() { return DISABLED; }

    /**
     * Инициирует ensure для одиночной темы; вызов безопасен, даже если ensure отключён (NOOP).
     */
    public void ensureTopic(String topic) {
        if (mode == Mode.DISABLED) {
            return;
        }
        if (mode == Mode.DELEGATE) {
            ensureDelegate.ensureTopic(topic);
            return;
        }
        if (executor == null) {
            coordinator.ensureTopic(topic);
        } else {
            executor.submit(topic);
        }
    }

    /**
     * Проверяет наличие темы и при необходимости инициирует ensure; возвращает true только после подтверждения.
     */
    public boolean ensureTopicOk(String topic) {
        if (mode == Mode.DISABLED) {
            return false;
        }
        if (mode == Mode.DELEGATE) {
            return ensureDelegate.ensureTopicOk(topic);
        }
        return coordinator.ensureTopicOk(topic);
    }

    /**
     * Пакетный ensure для набора тем; NOOP в отключённом режиме.
     */
    public void ensureTopics(Collection<String> topics) {
        if (mode == Mode.DISABLED) {
            return;
        }
        if (mode == Mode.DELEGATE) {
            ensureDelegate.ensureTopics(topics);
            return;
        }
        coordinator.ensureTopics(topics);
    }

    /**
     * Снимок внутренних метрик ensure-процесса; для NOOP-реализации возвращает пустую карту.
     */
    public Map<String, Long> getMetrics() {
        if (mode == Mode.DISABLED) {
            return Collections.emptyMap();
        }
        if (mode == Mode.DELEGATE) {
            return ensureDelegate.metrics();
        }
        Map<String, Long> base = coordinator != null ? coordinator.getMetrics() : Collections.emptyMap();
        Map<String, Long> snapshot = new LinkedHashMap<>(base);
        if (state != null) {
            snapshot.put("state.ensured.count", (long) state.ensuredSize());
        }
        if (executor != null) {
            snapshot.put("queue.pending", (long) executor.queuedTopics());
        }
        return Collections.unmodifiableMap(snapshot);
    }

    public boolean isEnabled() { return mode != Mode.DISABLED; }

    @Override
    /**
     * Закрывает обёрнутый {@link EnsureCoordinator}; в режиме NOOP ничего не делает.
     */
    public void close() {
        if (mode == Mode.DISABLED) {
            return;
        }
        if (mode == Mode.DELEGATE) {
            ensureDelegate.close();
            return;
        }
        closeQuietly(executor);
        closeQuietly(coordinator);
    }

    @Override
    public String toString() {
        if (mode == Mode.DISABLED) {
            return "TopicEnsurer[disabled]";
        }
        if (mode == Mode.DELEGATE) {
            return "TopicEnsurer[test]";
        }
        if (executor == null) {
            return coordinator.toString();
        }
        String base = coordinator.toString();
        int idx = base.lastIndexOf('}');
        if (idx > 0) {
            return base.substring(0, idx) + ", queued=" + executor.queuedTopics() + '}';
        }
        return base + ", queued=" + executor.queuedTopics();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring exception while closing resource", ex);
            }
        }
    }

    /**
     * Вспомогательный метод генерации уникального client.id для AdminClient.
     * Используется для избежания конфликтов MBean AppInfo.
     */
    private static String uniqueClientId(String base) {
        String host = safeHostname();
        String pid = runtimePid();
        long ts = System.currentTimeMillis();
        String prefix = (base == null || base.trim().isEmpty()) ? "h2k-admin" : base.trim();
        return prefix + "-" + host + "-" + pid + "-" + ts;
    }

    private static String prepareClientId(Properties props) {
        String baseId = props.getProperty(AdminClientConfig.CLIENT_ID_CONFIG, "h2k-admin");
        String clientId = uniqueClientId(baseId);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        return clientId;
    }

    private static String safeHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignore) {
            return "host";
        }
    }

    private static String runtimePid() {
        String pid = ManagementFactory.getRuntimeMXBean().getName();
        int at = pid.indexOf('@');
        if (at > 0) {
            return pid.substring(0, at);
        }
        return pid;
    }

    private static EnsureRuntimeState resolveState(EnsureRuntimeState provided, EnsureCoordinator coordinator) {
        if (provided != null) {
            return provided;
        }
        return coordinator != null ? coordinator.state() : null;
    }

    /**
     * Минимальный контракт ensure-логики для модульных тестов.
     * Позволяет управлять поведением {@link TopicEnsurer} без доступа к внутренним финальным полям.
     */
    public interface EnsureDelegate extends AutoCloseable {
        void ensureTopic(String topic);

        default boolean ensureTopicOk(String topic) {
            ensureTopic(topic);
            return false;
        }

        default void ensureTopics(Collection<String> topics) {
            if (topics == null || topics.isEmpty()) {
                return;
            }
            for (String topic : topics) {
                ensureTopic(topic);
            }
        }

        default Map<String, Long> metrics() {
            return Collections.emptyMap();
        }

        @Override
        default void close() {
            // no-op
        }
    }
}
