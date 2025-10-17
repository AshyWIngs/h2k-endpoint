package kz.qazmarka.h2k.kafka.ensure;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
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
 * Инкапсулирует {@link TopicEnsureService}, поставляя безопасный NOOP-экземпляр для сценариев,
 * когда ensure отключён конфигурацией или отсутствуют настройки bootstrap. Благодаря этому
 * вызывающий код не обязан проверять {@code null} и может работать с единым API.
 */
public final class TopicEnsurer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TopicEnsurer.class);

    private static final TopicEnsurer DISABLED = new TopicEnsurer();

    private final TopicEnsureService service;
    private final TopicEnsureExecutor executor;
    private final TopicEnsureState state;
    private final boolean disabledMode;
    private final EnsureDelegate ensureDelegate;

    private TopicEnsurer() {
        this.service = null;
        this.executor = null;
        this.state = null;
        this.disabledMode = true;
        this.ensureDelegate = null;
    }

    /**
     * Пакетный конструктор для модульных тестов и внутренних сборок.
     * Позволяет напрямую подставлять {@link TopicEnsureService}, {@link TopicEnsureExecutor}
     * и {@link TopicEnsureState} без создания AdminClient.
     */
    TopicEnsurer(TopicEnsureService service,
                 TopicEnsureExecutor executor,
                 TopicEnsureState state) {
        this.service = service;
        this.executor = executor;
        this.state = state;
        this.disabledMode = false;
        this.ensureDelegate = null;
    }

    private TopicEnsurer(EnsureDelegate delegate) {
        this.service = null;
        this.executor = null;
        this.state = null;
        this.disabledMode = false;
        this.ensureDelegate = delegate;
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

        return EnsureComponentsBuilder.build(admin, config, clientId);
    }

    /**
     * Builder для безопасного создания компонентов TopicEnsurer с автоматическим cleanup при ошибках.
     */
    private static final class EnsureComponentsBuilder implements AutoCloseable {
        private TopicEnsureService service;
        private TopicEnsureExecutor executor;
        private boolean released;

        private EnsureComponentsBuilder() {
            this.released = false;
        }

        static TopicEnsurer build(KafkaTopicAdmin admin, TopicEnsureConfig config, String clientId) {
            try (EnsureComponentsBuilder builder = new EnsureComponentsBuilder()) {
                TopicEnsureState state = new TopicEnsureState();
                builder.service = new TopicEnsureService(admin, config, state);
                builder.executor = new TopicEnsureExecutor(builder.service, state, clientId + "-ensure");
                builder.executor.start();
                
                // Успешное создание - передаём владение ресурсами TopicEnsurer
                builder.released = true;
                return new TopicEnsurer(builder.service, builder.executor, state);
            }
        }

        @Override
        public void close() {
            if (released) {
                // Ресурсы успешно переданы TopicEnsurer, не закрываем
                return;
            }
            // Ошибка при создании - очищаем временные ресурсы
            closeQuietly(executor);
            closeQuietly(service);
        }
        
        private static void closeQuietly(AutoCloseable closeable) {
            if (closeable == null) {
                return;
            }
            try {
                closeable.close();
            } catch (Exception ignored) {
                // Игнорируем ошибки при закрытии в cleanup-блоке
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
        if (!isEnabled()) {
            return;
        }
        if (ensureDelegate != null) {
            ensureDelegate.ensureTopic(topic);
            return;
        }
        if (executor == null) {
            service.ensureTopic(topic);
            return;
        }
        executor.submit(topic);
    }

    /**
     * Проверяет наличие темы и при необходимости инициирует ensure; возвращает true только после подтверждения.
     */
    public boolean ensureTopicOk(String topic) {
        if (!isEnabled()) {
            return false;
        }
        if (ensureDelegate != null) {
            return ensureDelegate.ensureTopicOk(topic);
        }
        return service.ensureTopicOk(topic);
    }

    /**
     * Пакетный ensure для набора тем; NOOP в отключённом режиме.
     */
    public void ensureTopics(Collection<String> topics) {
        if (!isEnabled()) {
            return;
        }
        if (ensureDelegate != null) {
            ensureDelegate.ensureTopics(topics);
            return;
        }
        service.ensureTopics(topics);
    }

    /**
     * Снимок внутренних метрик ensure-процесса; для NOOP-реализации возвращает пустую карту.
     */
    public Map<String, Long> getMetrics() {
        if (!isEnabled()) {
            return Collections.emptyMap();
        }
        if (ensureDelegate != null) {
            return ensureDelegate.metrics();
        }
        Map<String, Long> base = service.getMetrics();
        if (state == null) {
            return base;
        }
        Map<String, Long> snapshot = new java.util.LinkedHashMap<>(base);
        snapshot.put("state.ensured.count", (long) state.ensured.size());
        return Collections.unmodifiableMap(snapshot);
    }

    public boolean isEnabled() { return ensureDelegate != null || !disabledMode; }

    @Override
    /**
     * Закрывает обёрнутый {@link TopicEnsureService}; в режиме NOOP ничего не делает.
     */
    public void close() {
        if (!isEnabled()) {
            return;
        }
        if (ensureDelegate != null) {
            ensureDelegate.close();
            return;
        }
        if (executor != null) {
            executor.close();
        }
        service.close();
    }

    @Override
    public String toString() {
        if (!isEnabled()) {
            return "TopicEnsurer[disabled]";
        }
        if (ensureDelegate != null) {
            return "TopicEnsurer[test]";
        }
        if (executor == null) {
            return service.toString();
        }
        String base = service.toString();
        int idx = base.lastIndexOf('}');
        if (idx > 0) {
            return base.substring(0, idx) + ", queued=" + executor.queuedTopics() + '}';
        }
        return base + ", queued=" + executor.queuedTopics();
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
