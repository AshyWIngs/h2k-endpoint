

# Диагностика и типовые ошибки

## Общие принципы
- Все логи пишутся через log4j в systemd-journald.
- По умолчанию уровень INFO; DEBUG можно включить для пакета kz.qazmarka.h2k.endpoint.
- Сообщения исключений и диагностики пишутся на русском языке.

## Отладочное логирование ReplicationEndpoint
Для детального анализа H2K на RegionServer добавьте в `/opt/hbase-default-current/conf/log4j.properties`
следующий блок (HBase 1.4.13 использует log4j 1.2.17, синтаксис полностью совместим):

```properties
# === H2K: отдельная консоль в journald (без вмешательства в root) ===
log4j.appender.h2k_stdout=org.apache.log4j.ConsoleAppender
log4j.appender.h2k_stdout.target=System.err
log4j.appender.h2k_stdout.encoding=UTF-8
log4j.appender.h2k_stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.h2k_stdout.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} [%t] %X{table} %X{cf} %X{region} - %m%n
log4j.appender.h2k_stdout.Threshold=DEBUG
log4j.appender.h2k_stdout.ImmediateFlush=true
log4j.throwableRenderer=org.apache.log4j.EnhancedThrowableRenderer

# === Наш пакет целиком — INFO, вывод в отдельный аппендер без дублирования на root ===
log4j.logger.kz.qazmarka.h2k=INFO,h2k_stdout
log4j.additivity.kz.qazmarka.h2k=false

# === DEBUG для всех пакетов проекта (endpoint/kafka/payload/schema/config/utils) ===
log4j.logger.kz.qazmarka.h2k.endpoint=DEBUG
log4j.logger.kz.qazmarka.h2k.kafka.ensure=DEBUG
log4j.logger.kz.qazmarka.h2k.kafka.ensure.admin=DEBUG
log4j.logger.kz.qazmarka.h2k.kafka.ensure.TopicBackoffManager=DEBUG
log4j.logger.kz.qazmarka.h2k.kafka.producer.batch=DEBUG
log4j.logger.kz.qazmarka.h2k.kafka.support=DEBUG
log4j.logger.kz.qazmarka.h2k.payload=DEBUG
log4j.logger.kz.qazmarka.h2k.payload.serializer=DEBUG
log4j.logger.kz.qazmarka.h2k.payload.serializer.avro=DEBUG
log4j.logger.kz.qazmarka.h2k.schema=DEBUG
log4j.logger.kz.qazmarka.h2k.config=DEBUG

# === При необходимости можно включить DEBUG точечно для отдельных классов ===
log4j.logger.kz.qazmarka.h2k.endpoint.processing.WalEntryProcessor=DEBUG
log4j.logger.kz.qazmarka.h2k.kafka.ensure.EnsureCoordinator=DEBUG
log4j.logger.kz.qazmarka.h2k.payload.serializer.avro.ConfluentAvroPayloadSerializer=DEBUG
log4j.logger.kz.qazmarka.h2k.schema.registry.avro.phoenix.AvroPhoenixSchemaRegistry=DEBUG

# === Репликация HBase и шумные подсистемы ===
log4j.logger.org.apache.hadoop.hbase.replication=DEBUG
log4j.logger.org.apache.kafka=WARN
log4j.logger.org.apache.zookeeper=WARN
log4j.logger.org.apache.hadoop=WARN
log4j.logger.org.apache.hadoop.hbase=WARN
log4j.logger.org.apache.phoenix=WARN
```

- `h2k_stdout` — выделенный консольный аппендер с шаблоном, который подхватывает journald; уровень `DEBUG`
  на аппендере позволяет не терять сообщения от точечных логгеров.
- Пакет `kz.qazmarka.h2k` остаётся на `INFO`, поэтому рабочие логи не превращаются в «простыню».
- DEBUG активирован для всех пакетов проекта (`endpoint`, `kafka.ensure`, `kafka.producer.batch`, `payload`,
  `schema`, `config`, `support`). Дополнительные класс-ориентированные логгеры можно включать при необходимости
  для детального анализа конкретного узкого места.
- Репликацию HBase можно вернуть на `INFO`, когда отладка завершена: оставьте строку, заменив `DEBUG → INFO`.
- После изменения файла перезапустите RegionServer (`service hbase-regionserver restart` или
  `bin/hbase-daemon.sh restart regionserver`) и проверяйте вывод `journalctl -fu hbase-regionserver`.

## Проверка конфигурации
- Убедиться, что задан h2k.kafka.bootstrap.servers.
- Проверить корректность h2k.topic.pattern и свойств `"h2k.cf.list"` в Avro-схемах (несуществующие CF игнорируются).
- Все таблицы должны иметь актуальные `.avsc` с `h2k.pk`, `h2k.phoenixType`, `h2k.saltBytes`, `h2k.capacityHint`.

## Частые проблемы

### Нет данных в Kafka
- Проверить, что peer создан (`list_peers`).
- Проверить, что peer включён (`enable_peer`).
- Проверить, что репликация включена для таблиц (alter + REPLICATION_SCOPE).
- Убедиться, что bootstrap.servers указывают на рабочий кластер Kafka.
- Если используется фильтр (свойство `"h2k.cf.list"` в `.avsc`), убедитесь, что список CF соответствует регистру имён в HBase.

### Ошибки декодирования Phoenix
- ValueCodecPhoenix проверяет фиксированные типы (например, UNSIGNED_INT = 4 байта).
- Если длина не совпадает — выбрасывается IllegalStateException с сообщением «ожидалось N байт».
- При неизвестном типе смотрите WARN от PhoenixColumnTypeRegistry (тип будет принят как VARCHAR).
- Для phoenix-avro проверить `.avsc` (атрибуты `h2k.phoenixType`/`h2k.pk`) .
- Для проверки используемых `.avsc` включите DEBUG логгер `kz.qazmarka.h2k.schema.registry.avro.phoenix.AvroPhoenixSchemaRegistry` —
  он выводит путь к файлу, PK, соль и `capacityHint`.

### Ошибки при формировании payload
- Null в обязательных параметрах (TableName/qualifier) → NullPointerException.
- Отсутствует `.avsc` для таблицы → AvroPhoenixSchemaRegistry не сможет инициализироваться (WARN + пропуск данных).
- При некорректном rowkey (например, длина меньше `h2k.saltBytes`) → ошибка парсинга `RowKeySlice`.

### Ошибки Kafka Producer
- Если producer не может подключиться — проверить bootstrap.servers.
- Ошибки acks/idempotence → убедиться, что профиль настроен корректно (см. peer-profiles.md).
- При переполнении буфера — увеличить h2k.producer.buffer.memory или уменьшить batch.size/linger.ms.

### Ошибки ensure.topics
  - Проверить права у Kafka Admin.
  - Проверить h2k.topic.replication и h2k.topic.partitions.
  - Проверить h2k.admin.timeout.ms и h2k.ensure.unknown.backoff.ms.
  - В логах EnsureCoordinator ищите метрики `ensure.*`, `exists.*`, `create.*` — они показываются при DEBUG и
    доступны через `TopicManager.getMetrics()`. Поле `unknown.backoff.size` отражает размер очереди ожидания без
    лишних копий.
  - Для диагностики отказов репликации добавлены метрики `replicate.failures.total` и `replicate.last.failure.epoch.ms`.
    Они регистрируются через `TopicManager.registerMetric(...)` и доступны в снимке `TopicManager.getMetrics()`.
    Первая — общий счётчик неуспешных попыток `replicate()`, вторая — отметка времени (epoch ms) последней ошибки.
  - Для детального анализа backoff включите DEBUG для `kz.qazmarka.h2k.kafka.ensure.TopicBackoffManager` — в логах
    будет указан дедлайн повторной попытки. Дополнительно можно сериализовать `TopicManager.getMetrics()` в JMX и
    следить за `ensure.*`/`create.*`/`unknown.backoff.size` в реальном времени.

### Рост `wal.rowbuffer.*`
  - Метрики `wal.rowbuffer.upsizes` и `wal.rowbuffer.trims` показывают, как часто горячий путь увеличивает
    временный буфер строк. Upsize происходит, если в строке более 32 ячеек; trim — когда строка содержит ≥4096 ячеек и
    буфер принудительно ужимается назад.
  - Быстрый рост счётчиков указывает, что CF-фильтр пропускает неожиданные колонки или таблица содержит «широкие» строки.
    Перепроверьте атрибут `"h2k.cf.list"` в `.avsc`, убедитесь, что ненужные CF исключены, и при необходимости пересмотрите дизайн таблицы.
  - Метрики доступны через `TopicManager.getMetrics()` и Prometheus (см. `docs/prometheus-jmx.md`). Резкий всплеск `trims` часто
    сопровождается повышенным давлением на GC — проверьте GC-паузы и подумайте о сегрегации архивных данных в отдельные таблицы.

## Инструменты быстрой диагностики
- `status 'replication'` в HBase shell — показывает статус пиров.
- Graylog/journald — для поиска ошибок по ключевым словам (IllegalStateException, NullPointerException, KafkaException).
- Логи с префиксом `kz.qazmarka.h2k.endpoint` — основной источник диагностики.
- DEBUG логгер `kz.qazmarka.h2k.payload.serializer.avro.ConfluentAvroPayloadSerializer` показывает сравнение
  локальной схемы с актуальной версией в Confluent SR и фиксирует первую успешную/неуспешную регистрацию.

---

## Связанные документы
- [Конфигурация (все ключи)](config.md)
- [Phoenix метаданные](phoenix.md)
- [Профили peer](peer-profiles.md)
- [HBase shell / операции](hbase.md)
- [Операции эксплуатации](operations.md)
