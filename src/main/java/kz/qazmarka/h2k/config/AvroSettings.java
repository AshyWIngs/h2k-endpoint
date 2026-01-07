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

    AvroSettings(String schemaDir,
                 List<String> registryUrls,
                 Map<String, String> registryAuth,
                 Map<String, String> properties) {
        this(schemaDir, registryUrls, registryAuth, properties, -2);
    }

    AvroSettings(String schemaDir,
                 List<String> registryUrls,
                 Map<String, String> registryAuth,
                 Map<String, String> properties,
                 int fallbackSchemaId) {
        this.schemaDir = Objects.requireNonNull(schemaDir, "Каталог Avro-схем не может быть null");
        List<String> urls = Objects.requireNonNull(registryUrls, "Список URL Schema Registry не может быть null");
        Map<String, String> auth = Objects.requireNonNull(registryAuth, "Авторизационные параметры Schema Registry не могут быть null");
        Map<String, String> props = Objects.requireNonNull(properties, "Дополнительные свойства Avro не могут быть null");
        this.registryUrls = Collections.unmodifiableList(new ArrayList<>(urls));
        this.registryAuth = Collections.unmodifiableMap(new HashMap<>(auth));
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
        this.fallbackSchemaId = fallbackSchemaId;
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
}
