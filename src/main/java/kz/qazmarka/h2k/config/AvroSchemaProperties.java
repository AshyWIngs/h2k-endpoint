package kz.qazmarka.h2k.config;

/**
 * Общие имена свойств Avro-схем, используемых в конфигурации и при построении payload.
 */
public final class AvroSchemaProperties {

    /** Свойство Avro-поля, обозначающее пропуск значения при публикации payload. */
    public static final String PAYLOAD_SKIP = "h2k.payload.skip";

    private AvroSchemaProperties() {
        throw new IllegalStateException("Класс-утилита");
    }
}
