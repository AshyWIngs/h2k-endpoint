package kz.qazmarka.h2k.util;

/**
 * Служебные парсеры и нормализация значений конфигурации {@code h2k.*}.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

/**
 * Набор утилит для чтения и нормализации конфигурации {@code h2k.*} и сопутствующих констант.
 * Вынесено из {@code H2kConfig} без изменения поведения.
 *
 * Класс не хранит состояния — все методы статические и thread‑safe при передаче неизменяемых аргументов.
 */
public final class Parsers {

    private static final Pattern TOPIC_SANITIZE = Pattern.compile("[^a-zA-Z0-9._-]");
    /**
     * Значения, которые трактуем как {@code true} при разборе конфигурации.
     */
    private static final Set<String> TRUE_TOKENS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "true", "1", "yes", "on"
    )));
    /**
     * Значения, которые трактуем как {@code false} при разборе конфигурации.
     */
    private static final Set<String> FALSE_TOKENS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "false", "0", "no", "off"
    )));

    private Parsers() {}

    /**
     * Безопасно парсит целое из строки.
     *
     * @param s      исходная строка (может быть {@code null} или содержать пробелы)
     * @param defVal значение по умолчанию, если {@code s} пустая/некорректная
     * @return распарсенное целое либо {@code defVal} при ошибке
     */
    public static int parseIntSafe(String s, int defVal) {
        if (s == null) return defVal;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException ex) {
            return defVal;
        }
    }

    /**
     * Нормализует имя таблицы/ключа для поиска во внутренних картах.
     * Применяет {@code trim()} и переводит в {@link Locale#ROOT ROOT} верхний регистр.
     *
     * @param s исходная строка (может быть {@code null})
     * @return нормализованная строка; для {@code null} — пустая строка
     */
    public static String up(String s) {
        return (s == null) ? "" : s.trim().toUpperCase(Locale.ROOT);
    }

    /**
     * Считывает {@code int} из {@link Configuration} с дефолтом и нижней границей.
     *
     * @param cfg     конфигурация Hadoop
     * @param key     ключ
     * @param defVal  значение по умолчанию
     * @param minVal  минимально допустимое значение (включительно)
     * @return значение из конфигурации, но не меньше {@code minVal}
     */
    public static int readIntMin(Configuration cfg, String key, int defVal, int minVal) {
        int v = cfg.getInt(key, defVal);
        if (v < minVal) return minVal;
        return v;
    }

    /**
     * Считывает {@code long} из {@link Configuration} с мягкой деградацией.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию при {@code null}/некорректном вводе
     * @return валидное {@code long} значение либо {@code defVal}
     */
    public static long readLong(Configuration cfg, String key, long defVal) {
        String v = cfg.get(key);
        if (v == null) return defVal;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException nfe) {
            return defVal;
        }
    }

    /**
     * Читает булево значение из {@link Configuration}, поддерживая распространённые алиасы.
     * Принимаются {@code true/false/1/0/yes/no/on/off} в любом регистре.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию, если ключ отсутствует или формат не распознан
     * @return boolean значение, либо {@code defVal}, если распознать не удалось
     */
    public static boolean readBoolean(Configuration cfg, String key, boolean defVal) {
        String raw = cfg.getTrimmed(key);
        if (raw == null) {
            return defVal;
        }
        return parseBoolean(raw, defVal);
    }

    private static boolean parseBoolean(String value, boolean defVal) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (TRUE_TOKENS.contains(normalized)) {
            return true;
        }
        if (FALSE_TOKENS.contains(normalized)) {
            return false;
        }
        return defVal;
    }

    /**
     * Читает и нормализует шаблон имени топика: применяет {@code getTrimmed} и подставляет дефолт.
     *
     * @param cfg             конфигурация Hadoop
     * @param key             конфигурационный ключ
     * @param defaultPattern  шаблон по умолчанию, если значение пустое или отсутствует
     * @return непустой шаблон
     */
    public static String readTopicPattern(Configuration cfg, String key, String defaultPattern) {
        final String v = cfg.getTrimmed(key, defaultPattern);
        return (v == null || v.isEmpty()) ? defaultPattern : v;
    }

    /**
     * Внутренняя реализация чтения пар ключ/значение по заданному префиксу.
     * Используется публичными методами-обёртками для избежания дублирования логики.
     */
    private static Map<String, String> readByPrefix(Configuration cfg, String prefix) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, String> e : cfg) {
            String k = e.getKey();
            if (k != null && k.startsWith(prefix)) {
                String real = k.substring(prefix.length()).trim();
                if (!real.isEmpty()) {
                    String v = cfg.getTrimmed(k);
                    if (v != null && !v.isEmpty()) {
                        out.put(real, v);
                    }
                }
            }
        }
        return out;
    }

    /**
     * Извлекает topic‑level конфиги по заданному префиксу.
     * Пример: {@code h2k.topic.config.X=Y} → {@code {"X" → "Y"}}.
     *
     * @param cfg    конфигурация Hadoop
     * @param prefix строковый префикс ключей (например, {@code "h2k.topic.config."})
     * @return новая изменяемая {@link java.util.HashMap} с нормализованными ключами без префикса
     */
    public static Map<String, String> readTopicConfigs(Configuration cfg, String prefix) {
        return readByPrefix(cfg, prefix);
    }

    /**
     * Универсальное чтение перечисления (enum) из {@link Configuration} по ключу.
     * Сопоставление значения выполняется без учёта регистра и с предварительным {@code trim()}.
     * Пример использования:
     * {@code PayloadFormat fmt = Parsers.readEnum(cfg, "h2k.payload.format", H2kConfig.PayloadFormat.class, H2kConfig.PayloadFormat.JSON_EACH_ROW);}
     *
     * @param cfg      конфигурация Hadoop
     * @param key      ключ конфигурации
     * @param enumType класс перечисления
     * @param defVal   значение по умолчанию
     * @param <E>      тип перечисления
     * @return распознанное значение перечисления либо {@code defVal}, если ключ отсутствует или значение не распознано
     */
    public static <E extends Enum<E>> E readEnum(Configuration cfg, String key, Class<E> enumType, E defVal) {
        String v = cfg.getTrimmed(key);
        if (v == null || v.isEmpty()) return defVal;
        String normalized = v.trim().toUpperCase(Locale.ROOT);
        try {
            return Enum.valueOf(enumType, normalized);
        } catch (IllegalArgumentException ex) {
            return defVal;
        }
    }

    /**
     * Возвращает строку из {@link Configuration} или значение по умолчанию, если она пустая.
     */
    public static String readStringOrDefault(Configuration cfg, String key, String defVal) {
        String v = cfg.getTrimmed(key);
        return (v == null || v.isEmpty()) ? defVal : v;
    }

    /**
     * Возвращает первый непустой CSV-список среди заданных ключей.
     * Значение разбивается по запятым, элементы триммируются; пустые элементы исключаются.
     */
    public static List<String> readCsvListFirstNonEmpty(Configuration cfg, String... keys) {
        if (keys == null || keys.length == 0) return Collections.emptyList();
        for (String key : keys) {
            List<String> values = readCsvValues(cfg, key);
            if (!values.isEmpty()) {
                return Collections.unmodifiableList(values);
            }
        }
        return Collections.emptyList();
    }

    private static List<String> readCsvValues(Configuration cfg, String key) {
        if (key == null) return Collections.emptyList();
        return splitCsv(cfg.getTrimmed(key));
    }

    private static List<String> splitCsv(String raw) {
        if (raw == null || raw.isEmpty()) return Collections.emptyList();
        String[] parts = raw.split(",");
        List<String> out = new ArrayList<>(parts.length);
        for (String part : parts) {
            String trimmed = part == null ? "" : part.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

    /**
     * Обобщённое чтение пары ключ/значение по заданному префиксу конфигурации.
     * Возвращает карту без префикса в ключах. Пустые ключи/значения игнорируются.
     *
     * Подходит для чтения семейств параметров, например:
     *  - {@code h2k.topic.config.X=Y} → {@code {"X" → "Y"}}
     *  - {@code h2k.avro.schema.registry.url=http://...} → {@code {"schema.registry.url" → "http://..."}}
     *
     * @param cfg    конфигурация Hadoop
     * @param prefix строковый префикс ключей (например, {@code "h2k.avro."})
     * @return новая изменяемая {@link java.util.HashMap} с нормализованными ключами без префикса
     */
    public static Map<String, String> readWithPrefix(Configuration cfg, String prefix) {
        return readByPrefix(cfg, prefix);
    }

    /**
     * Читает число партиций топика с дефолтом и нижней границей.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию
     * @param minVal минимально допустимое значение
     * @return количество партиций не ниже {@code minVal}
     */
    public static int readTopicPartitions(Configuration cfg, String key, int defVal, int minVal) {
        return readIntMin(cfg, key, defVal, minVal);
    }

    /**
     * Читает replication factor как {@code short} с дефолтом и нижней границей.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию
     * @param minVal минимально допустимое значение (включительно)
     * @return валидный replication factor в диапазоне {@code [minVal, Short.MAX_VALUE]}
     */
    public static short readTopicReplication(Configuration cfg, String key, short defVal, short minVal) {
        String raw = cfg.getTrimmed(key);
        if (raw == null || raw.isEmpty()) return defVal;
        try {
            int parsed = Integer.parseInt(raw);
            if (parsed < minVal) return minVal;
            if (parsed > Short.MAX_VALUE) return Short.MAX_VALUE;
            return (short) parsed;
        } catch (NumberFormatException ignore) {
            return defVal;
        }
    }

    /**
     * Читает верхний лимит длины (например, имени/топика) с дефолтом и нижней границей.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию
     * @param minVal минимально допустимое значение
     * @return валидное значение не ниже {@code minVal}
     */
    public static long readTopicMaxLength(Configuration cfg, String key, long defVal, long minVal) {
        long v = readLong(cfg, key, defVal);
        if (v < minVal) return minVal;
        return v;
    }

    /**
     * Читает backoff (мс) для неизвестных ошибок с ограничениями диапазона.
     *
     * @param cfg    конфигурация Hadoop
     * @param key    ключ
     * @param defVal значение по умолчанию
     * @param minVal нижняя граница
     * @param maxVal верхняя граница
     * @return значение в диапазоне {@code [minVal, maxVal]}
     */
    public static long readUnknownBackoffMs(Configuration cfg, String key, long defVal, long minVal, long maxVal) {
        long v = readLong(cfg, key, defVal);
        if (v < minVal) return minVal;
        if (v > maxVal) return maxVal;
        return v;
    }

    /**
     * Собирает подсказки ёмкости (число ключей на таблицу) из CSV и индивидуальных ключей.
     *
     * @param cfg          конфигурация Hadoop
     * @param csvKey       ключ CSV (например, {@code h2k.capacity.hints})
     * @param singlePrefix префикс индивидуальных ключей (например, {@code h2k.capacity.hint.})
     * @return карта { UPPERCASE имя таблицы → положительное число ключей }
     */
    /**
     * Формирует {@code client.id} для {@code AdminClient}.
     * Если значение явно задано — возвращает его; иначе пытается использовать имя хоста.
     *
     * @param cfg         конфигурация Hadoop
     * @param explicitKey ключ явного значения client.id
     * @param defaultId   базовый префикс на случай отсутствия явного значения/ошибки определения хоста
     * @return итоговый {@code client.id}
     */
    public static String buildAdminClientId(Configuration cfg, String explicitKey, String defaultId) {
        String adminClientId = cfg.getTrimmed(explicitKey);
        if (adminClientId != null && !adminClientId.isEmpty()) {
            return adminClientId;
        }
        try {
            return defaultId + "-" + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return defaultId;
        }
    }

    /** Возвращает {@code true}, если символ является разрешённым разделителем Kafka ('.', '_', '-'). */
    private static boolean isKafkaDelimiter(char c) {
        return c == '.' || c == '_' || c == '-';
    }

    /**
     * Удаляет ведущие разделители {@code '.', '_', '-'} для корректной склейки плейсхолдеров.
     * Предполагается, что {@code s != null}.
     *
     * @param s исходная строка
     * @return строка без ведущих разделителей
     */
    public static String topicStripLeadingDelimiters(String s) {
        int i = 0;
        final int len = s.length();
        while (i < len) {
            char c = s.charAt(i);
            if (isKafkaDelimiter(c)) {
                i++;
            } else {
                break;
            }
        }
        return (i > 0) ? s.substring(i) : s;
    }

    /**
     * Схлопывает повторы одного и того же разделителя из набора {@code '.', '_', '-'} до одного символа подряд.
     * Предполагается, что {@code s != null}.
     *
     * @param s исходная строка
     * @return строка с нормализованными разделителями
     */
    public static String topicCollapseRepeatedDelimiters(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        char prev = 0;
        for (int j = 0; j < s.length(); j++) {
            char c = s.charAt(j);
            boolean isDelim = isKafkaDelimiter(c);
            if (!(isDelim && c == prev)) {
                sb.append(c);
                prev = c;
            }
        }
        return sb.toString();
    }

    /**
     * Заменяет недопустимые для Kafka символы на подчёркивание.
     * Предполагается, что {@code s != null}.
     *
     * @param s исходная строка
     * @return строка с заменёнными недопустимыми символами
     */
    public static String topicSanitizeKafkaChars(String s) {
        return TOPIC_SANITIZE.matcher(s).replaceAll("_");
    }

    /**
     * Выполняет полную нормализацию произвольной строки до допустимого имени Kafka‑топика.
     * Шаги: {@code trim} → замена недопустимых символов → удаление ведущих разделителей →
     * схлопывание повторов разделителей.
     *
     * @param raw исходная строка (может быть {@code null})
     * @return нормализованное имя топика; для {@code null}/пустой строки — пустая строка
     */
    public static String sanitizeTopic(String raw) {
        if (raw == null) return "";
        String s = raw.trim();
        if (s.isEmpty()) return "";
        s = topicSanitizeKafkaChars(s);
        s = topicStripLeadingDelimiters(s);
        s = topicCollapseRepeatedDelimiters(s);
        return s;
    }
}
