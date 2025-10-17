package kz.qazmarka.h2k.config;

/**
 * Источник табличных параметров (соль, подсказки ёмкости, CF-фильтр).
 * Используется для диагностики, чтобы понимать, откуда взято значение.
 */
public enum TableValueSource {
    AVRO("Avro-схема"),
    DEFAULT("значение по умолчанию");

    private final String label;

    TableValueSource(String label) {
        this.label = label;
    }

    /**
     * @return человекочитаемое описание источника для логов.
     */
    public String label() {
        return label;
    }
}
