package kz.qazmarka.h2k.schema.registry.avro.local;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;

/**
 * Минимальный локальный реестр Avro‑схем.
 *
 * Назначение:
 * - Читает схемы из каталога {@code conf/avro/} (по умолчанию).
 * - Имена файлов выводятся из имени таблицы: {@code <table>.avsc}, где {@code <table>} — в нижнем регистре.
 * - Кэширует результат в памяти (ConcurrentHashMap) по ключу {@code TABLE_NAME} в верхнем регистре.
 * - Сообщения об ошибках — на русском, как в остальном проекте.
 *
 * Пример: таблица {@code TBL_JTI_TRACE_CIS_HISTORY} → файл {@code conf/avro/tbl_jti_trace_cis_history.avsc}.
 *
 * SRP/ISP/DIP:
 * - Класс решает единственную задачу — предоставление {@link Schema} по имени таблицы.
 * - Не тянет внешние зависимости (Schema Registry, HTTP и т.п.).
 * - Не хранит глобальное состояние; кэш — локальный, сбрасываемый.
 */
public final class AvroSchemaRegistry {

    private final Path baseDir;
    private final Map<String, Schema> cache = new ConcurrentHashMap<>();

    /**
     * Использует базовый каталог {@code conf/avro}.
     */
    public AvroSchemaRegistry() {
        this(Paths.get("conf", "avro"));
    }

    /**
     * Пользовательский базовый каталог.
     */
    public AvroSchemaRegistry(Path baseDir) {
        if (baseDir == null) {
            throw new IllegalArgumentException("Аргумент 'baseDir' не может быть null");
        }
        this.baseDir = baseDir;
    }

    /**
     * Возвращает Avro-схему по имени таблицы.
     * Имя таблицы нормализуется к ВЕРХНЕМУ регистру для ключа кэша и к нижнему — для имени файла.
     *
     * @param tableName имя таблицы (например, "TBL_JTI_TRACE_CIS_HISTORY")
     * @return распарсенная {@link Schema}
     * @throws IllegalArgumentException если имя пустое
     * @throws IllegalStateException    если файл не найден / не читается / содержит некорректную схему
     */
    public Schema getByTable(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Пустое имя таблицы");
        }
        final String cacheKey = tableName.toUpperCase(Locale.ROOT);
        return cache.computeIfAbsent(cacheKey, k -> load(resolvePath(k)));
    }

    /**
     * Сбросить кэш (например, после деплоя новых схем).
     */
    public void clearCache() {
        cache.clear();
    }

    /**
     * Текущий размер кэша.
     */
    public int cacheSize() {
        return cache.size();
    }

    /**
     * Предварительно загружает все Avro-схемы из базового каталога.
     * Idempotent: повторные вызовы не перечитывают ранее закешированные схемы.
     *
     * @return число новых схем, добавленных в кэш
     */
    public int preloadAll() {
        if (!Files.isDirectory(baseDir)) {
            return 0;
        }
        int added = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir, "*.avsc")) {
            for (Path path : stream) {
                if (Files.isRegularFile(path)) {
                    String fileName = path.getFileName().toString();
                    if (fileName.endsWith(".avsc")) {
                        String tableKey = fileName.substring(0, fileName.length() - 5)
                                .toUpperCase(Locale.ROOT);
                        if (!cache.containsKey(tableKey)) {
                            cache.put(tableKey, load(path));
                            added++;
                        }
                    }
                }
            }
        } catch (NoSuchFileException e) {
            return 0;
        } catch (IOException e) {
            throw new IllegalStateException("Ошибка чтения каталога Avro-схем: " + baseDir + " — " + e.getMessage(), e);
        }
        return added;
    }

    /**
     * @param tableName имя таблицы (например, TBL_FOO)
     * @return путь к Avro-схеме на диске
     */
    public Path schemaPath(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Пустое имя таблицы");
        }
        return resolvePath(tableName.toUpperCase(Locale.ROOT));
    }

    // ---------- внутреннее ----------

    private Path resolvePath(String upperTableName) {
        String fileName = upperTableName.toLowerCase(Locale.ROOT) + ".avsc";
        return baseDir.resolve(fileName);
    }

    private Schema load(Path path) {
        try (BufferedReader r = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            final String json = readAll(r);
            // Каждый вызов использует локальный Parser, чтобы избежать гонок за внутреннее состояние парсера.
            return new Schema.Parser().parse(json);
        } catch (NoSuchFileException e) {
            throw new IllegalStateException("Не найдена Avro‑схема: " + path, e);
        } catch (IOException e) {
            throw new IllegalStateException("Ошибка чтения Avro‑схемы: " + path + " — " + e.getMessage(), e);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Некорректная Avro‑схема: " + path + " — " + e.getMessage(), e);
        }
    }

    private static String readAll(Reader r) throws IOException {
        final char[] buf = new char[8192];
        final StringBuilder sb = new StringBuilder(8192);
        int n;
        while ((n = r.read(buf)) != -1) {
            sb.append(buf, 0, n);
        }
        return sb.toString();
    }
}
