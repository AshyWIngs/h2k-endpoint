package kz.qazmarka.h2k.endpoint.metrics;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import kz.qazmarka.h2k.endpoint.topic.TopicManager;

/**
 * JMX-обёртка для экспонирования метрик H2K через {@link TopicManager#getMetrics()}.
 *
 * Назначение: предоставить стабильный JMX-объект с набором числовых атрибутов, пригодных
 * для считывания Prometheus JMX Exporter без дополнительной логики.
 *
 * Особенности реализации:
 * - Атрибуты формируются из снимка {@code TopicManager.getMetrics()} при регистрации MBean;
 * - Имена атрибутов — это нормализованные ключи метрик: все небуквенно-цифровые символы
 *   заменяются на символ подчёркивания ('_');
 * - Значения читаются динамически при каждом {@code getAttribute(..)} из актуального снимка
 *   {@code TopicManager.getMetrics()}, поэтому данные всегда свежие;
 * - Установка атрибутов не поддерживается (read-only).
 *
 * Потокобезопасность: доступ только на чтение; основу составляет неизменяемый слепок имен атрибутов.
 */
public final class H2kMetricsJmx implements DynamicMBean {
    /** Базовое имя ObjectName для MBean метрик. */
    private static final String OBJECT_NAME_BASE = "kz.qazmarka.h2k:type=Endpoint,name=H2KMetrics";

    /** Поставщик снимка метрик: ключ -> значение. */
    private final Supplier<Map<String, Long>> snapshotSupplier;
    /** Отображение: имя JMX-атрибута -> оригинальный ключ метрики. */
    private final Map<String, String> attrToKey;
    /** Метаданные MBean — фиксированный набор атрибутов. */
    private final MBeanInfo mbeanInfo;

    private H2kMetricsJmx(Supplier<Map<String, Long>> snapshotSupplier) {
        this.snapshotSupplier = Objects.requireNonNull(snapshotSupplier, "snapshotSupplier");
        Map<String, Long> snapshot = safeSnapshot();
        Map<String, String> mapping = new HashMap<>(snapshot.size());
        List<MBeanAttributeInfo> infos = new ArrayList<>(snapshot.size());
        for (String key : snapshot.keySet()) {
            String attr = toAttributeName(key);
            mapping.put(attr, key);
            infos.add(new MBeanAttributeInfo(
                    attr,
                    Long.class.getName(),
                    "Метрика '" + key + "' (чтение)",
                    true,
                    false,
                    false
            ));
        }
        this.attrToKey = Collections.unmodifiableMap(mapping);
        this.mbeanInfo = new MBeanInfo(
                H2kMetricsJmx.class.getName(),
                "Метрики H2K (репликация HBase→Kafka)",
                infos.toArray(new MBeanAttributeInfo[0]),
                null, // конструкторы не экспонируем
                null, // операции не поддерживаются
                null  // уведомления не используются
        );
    }

    /**
     * Регистрирует MBean в платформенном {@link MBeanServer} с уникальным именем.
     * Возвращает зарегистрированное {@link ObjectName} или {@code null} при ошибке регистрации.
     *
     * @param topicManager менеджер топиков; источник метрик
     * @return имя зарегистрированного MBean (или {@code null}, если регистрация не удалась)
     */
    public static ObjectName register(TopicManager topicManager) {
        Objects.requireNonNull(topicManager, "topicManager");
        try {
            H2kMetricsJmx mbean = new H2kMetricsJmx(topicManager::getMetrics);
            return registerInternal(mbean);
        } catch (javax.management.MalformedObjectNameException
                 | MBeanRegistrationException
                 | javax.management.NotCompliantMBeanException
                 | InstanceAlreadyExistsException e) {
            return null;
        }
    }

    private static ObjectName registerInternal(H2kMetricsJmx mbean)
        throws javax.management.MalformedObjectNameException,
           MBeanRegistrationException,
           javax.management.NotCompliantMBeanException,
           InstanceAlreadyExistsException {
        String on = buildObjectName();
        ObjectName name = new ObjectName(on);
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            server.registerMBean(mbean, name);
            return name;
        } catch (InstanceAlreadyExistsException e) {
            // Добавим случайный хвост, чтобы избежать конфликта имен
            ObjectName rnd = new ObjectName(on + ",uid=" + Integer.toHexString(System.identityHashCode(mbean)));
            server.registerMBean(mbean, rnd);
            return rnd;
        }
    }

    /** Пытается снять регистрацию ранее зарегистрированного MBean. */
    public static void unregisterQuietly(ObjectName name) {
        if (name == null) {
            return;
        }
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.unregisterMBean(name);
        } catch (InstanceNotFoundException | MBeanRegistrationException ignored) {
            // без действий
        }
    }

    private static String buildObjectName() { return OBJECT_NAME_BASE; }

    private static String toAttributeName(String key) {
        if (key == null || key.isEmpty()) {
            return "metric";
        }
        String norm = key.toLowerCase(Locale.ROOT);
        // Заменяем всё, кроме [a-z0-9_] на '_'
        norm = norm.replaceAll("[^a-z0-9_]", "_");
        // Сжимаем повторяющиеся подчёркивания
        norm = norm.replaceAll("_+", "_");
        // Убираем крайние подчёркивания
        if (norm.startsWith("_")) norm = norm.substring(1);
        if (norm.endsWith("_")) norm = norm.substring(0, norm.length() - 1);
        return norm.isEmpty() ? "metric" : norm;
    }

    private Map<String, Long> safeSnapshot() {
        try {
            Map<String, Long> snap = snapshotSupplier.get();
            return snap == null ? Collections.<String, Long>emptyMap() : snap;
        } catch (RuntimeException e) {
            return Collections.emptyMap();
        }
    }

    // ==== DynamicMBean ====
    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        String key = attrToKey.get(attribute);
        if (key == null) {
            throw new AttributeNotFoundException(attribute);
        }
        Long v = safeSnapshot().get(key);
        return v == null ? null : v;
    }

    @Override
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("read-only");
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();
        if (attributes == null || attributes.length == 0) {
            return list;
        }
        Map<String, Long> snap = safeSnapshot();
        for (String attr : attributes) {
            String key = attrToKey.get(attr);
            if (key == null) {
                continue;
            }
            Long v = snap.get(key);
            list.add(new Attribute(attr, v));
        }
        return list;
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        return new AttributeList();
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
        return null;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        return mbeanInfo;
    }
}
