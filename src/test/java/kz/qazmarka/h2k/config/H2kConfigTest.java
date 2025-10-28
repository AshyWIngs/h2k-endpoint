package kz.qazmarka.h2k.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import kz.qazmarka.h2k.schema.registry.PhoenixTableMetadataProvider;
import kz.qazmarka.h2k.schema.registry.SchemaRegistry;

/**
 * Набор юнит‑тестов для конфигурации {@link H2kConfig}.
 *
 * Что проверяем:
 * - использование Avro‑метаданных для соли/подсказки ёмкости, если отсутствуют явные конфиги;
 * - генерацию имён Kafka‑топиков: плейсхолдеры ({@code ${namespace}}, {@code ${qualifier}}),
 *   санитайзинг недопустимых символов и жёсткое усечение по {@code h2k.topic.max.length};
 * - сборку конфигурации CF‑фильтра из провайдера метаданных;
 * - основные флаги и секции (ensureTopics, параметры BatchSender и т.п.).
 *
 * Нефункциональные требования:
 * - используется только in‑memory {@link org.apache.hadoop.conf.Configuration}; без I/O;
 * - нет дополнительных фреймворков и рантайм‑зависимостей; всё выполняется быстро и без давления на GC.
 *
 * Как читать тесты:
 * - каждый метод оформлен в стиле GIVEN/WHEN/THEN и снабжён кратким описанием;
 * - негативные кейсы проверяют устойчивость к «грязному» вводу (нечисла, пустые токены и т.п.).
 */
class H2kConfigTest {

    /**
     * Хелпер для компактного создания {@link H2kConfig} в тестах.
     *
     * Зачем: изолировать тесты от внешних настроек Kafka; минимально необходимый
     * bootstrap указывается константой, остальное берётся из переданной in‑memory {@link Configuration}.
     *
     * @param cfg конфигурация Hadoop с ключами {@code h2k.*}, специфичными для теста
     * @return сконфигурированный {@link H2kConfig}
     */
    private static H2kConfig fromCfg(Configuration cfg) {
        return H2kConfig.from(cfg, "kafka1:9092");
    }

    private static H2kConfig fromCfg(Configuration cfg, PhoenixTableMetadataProvider provider) {
        return H2kConfig.from(cfg, "kafka1:9092", provider);
    }

    /**
     * GIVEN: подсказки ёмкости заданы и для полного имени ({@code NS:TABLE}), и только для
     *        квалифаера ({@code TABLE}).
     * WHEN:  запрашиваем хинты для разных {@link TableName}.
     * THEN:  разрешение работает и по полному имени, и по квалифаеру; для отсутствующих ключей → 0.
     */
    @Test
    @DisplayName("Avro metadata provider задаёт соль и capacity, если нет конфигурации")
    void metadataProviderProvidesSaltAndCapacity() {
        Configuration c = new Configuration(false);
        TableName table = TableName.valueOf("DEFAULT", "T_META");
        PhoenixTableMetadataProvider provider = PhoenixTableMetadataProvider.builder()
                .table(table)
                .saltBytes(3)
                .capacityHint(42)
                .done()
                .build();

        H2kConfig hc = fromCfg(c, provider);

        assertEquals(3, hc.getSaltBytesFor(table));
        assertEquals(42, hc.getCapacityHintFor(table));
    }

    /**
     * GIVEN: шаблон топика со спецсимволами и маленький лимит длины.
     * WHEN:  генерируем имя топика.
     * THEN:  недопустимые символы заменяются на '_', строка усекётся до лимита,
     *        префикс соответствует ожидаемым плейсхолдерам.
     */
    @Test
    @DisplayName("topicFor: плейсхолдеры, sanitize и truncate по maxLength")
    void topicForSanitizeAndTruncate() {
        Configuration c = new Configuration(false);
        c.set("h2k.topic.pattern", "${namespace}.${qualifier}.raw?/bad");
        c.setInt("h2k.topic.max.length", 10); // намеренно маленький
        H2kConfig hc = fromCfg(c);

        String topic = hc.topicFor(TableName.valueOf("AGG", "INC_DOCS_ACT"));
        // После sanitize все недопустимые символы → '_', затем усечение до 10 символов
        assertEquals(10, topic.length());
        // Начало строки после подстановки и sanitize должно начинаться с "AGG.INC_"
        assertTrue(topic.startsWith("AGG.INC_") || topic.startsWith("AGG_INC_") || topic.startsWith("AGG.INC."));
    }


    @Test
    @DisplayName("cfFilter: список CF берётся из провайдера метаданных")
    void cfFilterFromMetadataProvider() {
        Configuration cfg = new Configuration(false);
        TableName table = TableName.valueOf("ns", "tbl");

        PhoenixTableMetadataProvider provider = PhoenixTableMetadataProvider.builder()
                .table(table)
                .columnFamilies("d", "b")
                .done()
                .build();

        H2kConfig hc = fromCfg(cfg, provider);
        CfFilterSnapshot snapshot = hc.describeCfFilter(table);
        assertTrue(snapshot.enabled());
        assertEquals("d,b", snapshot.csv());
        byte[][] families = snapshot.families();
        assertEquals(2, families.length);
        assertEquals("d", new String(families[0], java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("b", new String(families[1], java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(TableValueSource.AVRO, snapshot.source());
    }

    @Test
    @DisplayName("observers.enabled: по умолчанию отключены")
    void observersDisabledByDefault() {
        Configuration cfg = new Configuration(false);
        H2kConfig hc = fromCfg(cfg);
        assertFalse(hc.isObserversEnabled());
    }

    @Test
    @DisplayName("observers.enabled: включение через конфигурацию")
    void observersEnabledViaConfig() {
        Configuration cfg = new Configuration(false);
        cfg.setBoolean(H2kConfig.Keys.OBSERVERS_ENABLED, true);
        H2kConfig hc = fromCfg(cfg);
        assertTrue(hc.isObserversEnabled());
    }

    @Test
    @DisplayName("cfFilter: отсутствие списка CF отключает фильтр")
    void cfFilterDisabledWhenEmpty() {
        Configuration cfg = new Configuration(false);
        TableName table = TableName.valueOf("ns", "tbl");

        PhoenixTableMetadataProvider provider = PhoenixTableMetadataProvider.builder()
                .table(table)
                .columnFamilies(SchemaRegistry.EMPTY)
                .done()
                .build();

        H2kConfig hc = fromCfg(cfg, provider);
        CfFilterSnapshot snapshot = hc.describeCfFilter(table);
        assertFalse(snapshot.enabled());
        assertEquals("", snapshot.csv());
        assertEquals(0, snapshot.families().length);
        assertEquals(TableValueSource.DEFAULT, snapshot.source());
    }

    @Test
    @DisplayName("Avro: чтение каталога, SR URL и auth")
    void avroConfigTypedParsing() {
        Configuration c = new Configuration(false);
        c.set("h2k.avro.schema.dir", "/opt/avro");
        c.set("h2k.avro.sr.urls", "http://sr1:8081, http://sr2:8081 ");
        c.set("h2k.avro.sr.auth.basic.username", "user");
        c.set("h2k.avro.sr.auth.basic.password", "pass");
        c.set("h2k.avro.extra", "value");

        H2kConfig hc = fromCfg(c);

        assertEquals("/opt/avro", hc.getAvroSchemaDir());
        assertIterableEquals(java.util.Arrays.asList("http://sr1:8081", "http://sr2:8081"), hc.getAvroSchemaRegistryUrls());
        assertEquals("user", hc.getAvroSrAuth().get("basic.username"));
        assertEquals("pass", hc.getAvroSrAuth().get("basic.password"));
        assertFalse(hc.getAvroProps().containsKey("mode"), "Известные ключи не должны попадать в extra map");
        assertEquals("value", hc.getAvroProps().get("extra"));
    }

    @Test
    @DisplayName("Avro: алиасы URL Schema Registry (schema.registry[.url])")
    void avroConfigSchemaRegistryAliases() {
        Configuration c = new Configuration(false);
        c.set("h2k.avro.schema.registry", "http://legacy:8081");
        H2kConfig hc = fromCfg(c);
        assertIterableEquals(java.util.Collections.singletonList("http://legacy:8081"), hc.getAvroSchemaRegistryUrls());

        c = new Configuration(false);
        c.set("h2k.avro.schema.registry.url", "http://single:8081");
        hc = fromCfg(c);
        assertIterableEquals(java.util.Collections.singletonList("http://single:8081"), hc.getAvroSchemaRegistryUrls());
    }

    /**
     * Пограничный кейс: минимальный лимит длины имени топика = 1.
     * Ожидаем: результат строго из одного допустимого символа после санитайза.
     */
    @Test
    @DisplayName("topic.max.length: минимальная граница (1 символ) и жёсткое усечение")
    void topicForMinLengthOneChar() {
        Configuration c = new Configuration(false);
        c.set("h2k.topic.pattern", "${qualifier}");
        c.setInt("h2k.topic.max.length", 1); // минимально допустимый сценарий
        H2kConfig hc = fromCfg(c);

        String topic = hc.topicFor(TableName.valueOf("ANY", "ABC.DEF"));
        assertEquals(1, topic.length(), "Топик должен быть усечён до 1 символа");
        // любой допустимый символ — латиница/цифра/._-
        assertTrue(topic.matches("[a-zA-Z0-9._-]"), "Санитайзер обязан выдать валидный символ");
    }

    /**
     * Нормальный кейс: большой лимит длины.
     * Ожидаем: имя топика не усекётся, останется ≤ лимита и с валидным префиксом namespace.
     */
    @Test
    @DisplayName("topic.max.length: большой лимит — строка не должна усекаться (≤ лимита)")
    void topicForBigLimitNoTruncate() {
        Configuration c = new Configuration(false);
        c.set("h2k.topic.pattern", "${namespace}.${qualifier}.raw");
        c.setInt("h2k.topic.max.length", 255);
        H2kConfig hc = fromCfg(c);

        String topic = hc.topicFor(TableName.valueOf("AGG", "INC_DOCS_ACT"));
        assertTrue(topic.length() <= 255, "Длина не должна превышать лимит");
        assertTrue(topic.startsWith("AGG.") || topic.startsWith("AGG_"),
                "Ожидаем префикс с namespace после санитайза");
    }
}
