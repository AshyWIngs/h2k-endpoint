package kz.qazmarka.h2k.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Плоский DTO с настройками Avro и Schema Registry: каталог схем, список URL и авторизационные параметры.
 * Отдельный класс упрощает тесты и фабрики сериализаторов.
 */
public final class AvroSettings {

    private final String schemaDir;
    private final List<String> registryUrls;
    private final Map<String, String> registryAuth;
    private final Map<String, String> properties;
    private final int fallbackSchemaId;
    private final int maxPendingRetries;

    AvroSettings(String schemaDir,
                 List<String> registryUrls,
                 Map<String, String> registryAuth,
                 Map<String, String> properties) {
        this(schemaDir, registryUrls, registryAuth, properties, -2, 100);
    }

    AvroSettings(String schemaDir,
                 List<String> registryUrls,
                 Map<String, String> registryAuth,
                 Map<String, String> properties,
                 int maxPendingRetries) {
        this(schemaDir, registryUrls, registryAuth, properties, -2, maxPendingRetries);
    }

    AvroSettings(String schemaDir,
                 List<String> registryUrls,
                 Map<String, String> registryAuth,
                 Map<String, String> properties,
                 int fallbackSchemaId,
                 int maxPendingRetries) {
        this.schemaDir = Objects.requireNonNull(schemaDir, "Каталог Avro-схем не может быть null");
        List<String> urls = Objects.requireNonNull(registryUrls, "Список URL Schema Registry не может быть null");
        Map<String, String> auth = Objects.requireNonNull(registryAuth, "Авторизационные параметры Schema Registry не могут быть null");
        Map<String, String> props = Objects.requireNonNull(properties, "Дополнительные свойства Avro не могут быть null");
        this.registryUrls = Collections.unmodifiableList(new ArrayList<>(urls));
        this.registryAuth = Collections.unmodifiableMap(new HashMap<>(auth));
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
        this.fallbackSchemaId = fallbackSchemaId;
        if (maxPendingRetries <= 0) {
            throw new IllegalArgumentException("maxPendingRetries должен быть > 0");
        }
        this.maxPendingRetries = maxPendingRetries;
    }

    public String getSchemaDir() {
        return schemaDir;
    }

    public List<String> getRegistryUrls() {
        return registryUrls;
    }

    public Map<String, String> getRegistryAuth() {
        return registryAuth;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Возвращает ID схемы, используемый как fallback при offline Schema Registry.
     * Значение по умолчанию: -2 (специальный маркер, свидетельствующий о недоступности SR).
     * Может быть переопределено через конфигурацию или для тестирования.
     */
    public int getFallbackSchemaId() {
        return fallbackSchemaId;
    }

    /**
     * Возвращает максимум одновременно ожидающих повторных попыток регистрации схемы в Schema Registry.
     * Ограничение размера очереди предотвращает накопление задач при long-term offline SR.
     * По умолчанию: 100.
     */
    public int getMaxPendingRetries() {
        return maxPendingRetries;
    }
}
