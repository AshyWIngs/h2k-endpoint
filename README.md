## HBase 1.4.13 → Kafka 2.3.1 ReplicationEndpoint (Avro Confluent only)

**Пакет:** `kz.qazmarka.h2k.endpoint`  
**Endpoint‑класс:** `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`

Лёгкий и быстрый `ReplicationEndpoint` для HBase 1.4.x. Формат полезной нагрузки — строго **Avro (Confluent Schema Registry)**. Минимум аллокаций, стабильный порядок полей, дружелюбные логи на русском.

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Поддерживаемые версии](#поддерживаемые-версии)
- [Установка](#установка)
- [Минимальная конфигурация](#минимальная-конфигурация)
- [Профили peer](#профили-peer)
- [Полная документация](#полная-документация)
- [Поддержка форматов сообщений](#поддержка-форматов-сообщений)
- [Безопасность и ограничения](#безопасность-и-ограничения)
- [Раскатка и мониторинг](#раскатка-и-мониторинг)
- [FAQ](#faq)
- [Структура пакетов](#структура-пакетов)
- [Обновление версии проекта](#обновление-версии-проекта)

---

## Быстрый старт

0) **Требования окружения.** Java 8 на RegionServer, Kafka 2.3.1+, Schema Registry 5.3.8 (при использовании Confluent), доступ к ZK/Kafka без TLS/SASL.

1) **Соберите и разложите JAR** на все RegionServer (JAR уже содержит Avro/Jackson/Confluent 5.3.8):
```bash
mvn -pl endpoint -am -DskipTests clean package
cp endpoint/target/h2k-endpoint-*-shaded.jar /opt/hbase-default-current/lib/
```

2) **Подготовьте Avro‑схемы** с атрибутами `h2k.phoenixType` и массивом `h2k.pk` в каталоге `conf/avro` (см. docs/avro.md).

3) **Включите репликацию CF** в нужных таблицах и глобально:
```xml
<!-- hbase-site.xml (на RS) -->
<property><name>hbase.replication</name><value>true</value></property>
```
```HBase shell
# пример: включить CF 'd'
disable 'TBL_JTI_TRACE_CIS_HISTORY'
alter  'TBL_JTI_TRACE_CIS_HISTORY', { NAME => 'd', REPLICATION_SCOPE => 1 }
enable 'TBL_JTI_TRACE_CIS_HISTORY'
```

4) **Создайте peer** готовым скриптом (рекомендуется BALANCED):
```bash
bin/hbase shell conf/add_peer_shell_balanced.txt
```

   Альтернативы: `add_peer_shell_fast.txt` и `add_peer_shell_reliable.txt` (см. [матрицу профилей](#профили-peer)).

5) **Проверьте доставку**: сообщения появляются в топике `${table}` (формат Avro Confluent).

---

## Поддерживаемые версии

- **Java:** 8 (target 1.8)
- **HBase:** 1.4.13 (совместимо с 1.4.x)
- **Kafka (клиенты):** 2.3.1
- **Phoenix:** 4.14/4.15 (совместимо; используется Avro Phoenix registry)

> RegionServer и Endpoint должны работать на **Java 8**.

---

## Установка

1. После сборки Maven появится два артефакта в `endpoint/target/`: `h2k-endpoint-<version>.jar` (тонкий) и `h2k-endpoint-<version>-shaded.jar`. Для RegionServer используйте shaded-вариант (можно переименовать в `h2k-endpoint.jar` или загрузить как есть).
   Скопируйте `h2k-endpoint-<version>-shaded.jar` в `/opt/hbase-default-current/lib/`.  
2. Убедитесь, что на RS есть базовые клиентские библиотеки из кластера:
   - `kafka-clients-2.3.1.jar`
   - `lz4-java-1.6.0+.jar` (для FAST/BALANCED)
   - `snappy-java-1.1.x+.jar` (для RELIABLE, если `compression.type=snappy`)
   > Avro/Jackson и Confluent Schema Registry 5.3.8 шейдятся внутрь `h2k-endpoint-*.jar`, поэтому дополнительные Confluent JAR не нужны.
3. Перезапустите RegionServer.

Быстрая проверка:
```bash
hbase classpath | tr ':' '\n' | egrep -i 'kafka-clients|lz4|snappy'
```

---

## Минимальная конфигурация

**Откуда читаются ключи:**  
- системный `hbase-site.xml` (дефолты),  
- *и/или* карта `CONFIG` у peer (имеет приоритет).

**Минимально для запуска** (пример для `TBL_JTI_TRACE_CIS_HISTORY`):
```properties
h2k.kafka.bootstrap.servers=10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092
h2k.avro.sr.urls=http://schema-registry-1:8081,http://schema-registry-2:8081
# Каталог локальных .avsc (если не указан — conf/avro)
# h2k.avro.schema.dir=/opt/h2k/conf/avro
# Топик:
h2k.ensure.topics=true
h2k.topic.pattern=${table}
```

**Ключевые опции (коротко):**

| Ключ | Назначение | Примечание |
|---|---|---|
| Ключ | Назначение | Примечание |
|---|---|---|
| `h2k.kafka.bootstrap.servers` | Список брокеров Kafka | `host:port[,host2:port2]` |
| `h2k.avro.sr.urls` | URL Schema Registry | Обязателен; перечисляйте через запятую |
| `h2k.avro.schema.dir` | Каталог локальных `.avsc` | По умолчанию `conf/avro` |
| `h2k.avro.sr.auth.*` | Basic-auth к SR | Используйте при необходимости авторизации |
| `h2k.salt.map` | Карта соли rowkey | Опционально переопределяет `h2k.saltBytes` в `.avsc` |
| `h2k.capacity.hints` | Подсказки ёмкости | Опционально; основной источник — `.avsc` (`h2k.capacityHint`) |
| `h2k.topic.pattern` | Шаблон имени топика | `${table}` по умолчанию |
| `h2k.ensure.topics` | Автосоздание тем | true/false |
| `h2k.topic.config.*` | Доп. параметры Kafka-топика | Передаются в AdminClient при ensure |


> Фильтрация по CF задаётся на уровне Avro-схемы: укажите `"h2k.cf.list": "cf1,cf2"` в `conf/avro/<TABLE>.avsc`.
> Если свойство отсутствует, реплицируются все column family.

> Атрибуты `h2k.phoenixType`, `h2k.pk`, `h2k.saltBytes`, `h2k.capacityHint` должны быть заданы в `.avsc`.
> Файл `schema.json` более не используется.

> Полная справка по ключам и значениям — см. **docs/config.md**.

### Автоадаптация `awaitEvery`

- По умолчанию включена (`h2k.producer.batch.autotune.enabled=true`). Алгоритм анализирует задержку flush и
  аккуратно сужает/расширяет порог `awaitEvery` между партиями WAL.
- Ограничения можно задать ключами `h2k.producer.batch.autotune.min`, `...max`, пороги задержки —
  `...latency.high.ms` и `...latency.low.ms`, интервал между решениями — `...cooldown.ms`.
- Фактические значения и рекомендации видны в метриках TopicManager: `producer.batch.await.recommended`,
  `producer.batch.autotune.decisions.total`, `producer.batch.autotune.last.latency.ms` и других. При необходимости
  автонастройку можно отключить, вернувшись к фиксированному `h2k.producer.await.every`.
- При «тихих» ошибках ожидания подтверждений (`flushFailures`) тюнер автоматически сбрасывает `awaitEvery` к минимуму
  и фиксирует текущее состояние в метриках `producer.batch.fail.streak.current`, `producer.batch.fail.last.ms` и `producer.batch.fail.last.await`.

### Матрица продьюсерских профилей (значения синхронизированы с `conf/`)

| Ключ | Единицы | Дефолт (Endpoint) | FAST (`conf/add_peer_shell_fast.txt`) | BALANCED (`conf/add_peer_shell_balanced.txt`) | RELIABLE (`conf/add_peer_shell_reliable.txt`) |
|---|---|---|---|---|---|
| `h2k.producer.enable.idempotence` | boolean | `true` | `false` | `true` | `true` |
| `h2k.producer.acks` | ack mode | `all` | `1` | `all` | `all` |
| `h2k.producer.max.in.flight` | запросов | `1` | `5` | `5` | `1` |
| `h2k.producer.linger.ms` | миллисекунды | `50` | `100` | `100` | `50` |
| `h2k.producer.batch.size` | байты | `65536` | `524288` | `524288` | `65536` |
| `h2k.producer.compression.type` | алгоритм | `lz4` | `lz4` | `lz4` | `snappy` |
| `h2k.producer.delivery.timeout.ms` | миллисекунды | `180000` | `90000` | `300000` | `300000` |

> Таблица отражает реальные значения из скриптов `conf/add_peer_shell_*.txt`; изменения в профилях необходимо синхронизировать с документацией.

---

## Профили peer

Готовые скрипты (каталог `conf/`):

- **BALANCED** — `conf/add_peer_shell_balanced.txt` (рекомендуется для прод)
- **RELIABLE** — `conf/add_peer_shell_reliable.txt` (строгие гарантии/порядок)
- **FAST** — `conf/add_peer_shell_fast.txt` (максимальная скорость)

Краткая матрица (полные значения и примеры — в [docs/peer-profiles.md](docs/peer-profiles.md)):

| Профиль | Назначение | `acks` | `enable.idempotence` | `max.in.flight` | `linger.ms` | `batch.size` | `compression` |
|---|---|---|---|---|---|---|---|
| FAST | максимальная скорость | `1` | `false` | `5` | `100` | `524288` | `lz4` |
| BALANCED | баланс скорость/надёжность | `all` | `true` | `5` | `100` | `524288` | `lz4` |
| RELIABLE | строгий порядок и гарантии | `all` | `true` | `1` | `50` | `65536` | `snappy` |

> Дополнительные параметры и подсказки по тюнингу см. в **docs/peer-profiles.md** и **docs/hbase.md**.

---

## Полная документация

- [Сводный навигатор по документации](docs/README.md)
- [Конфигурация (все ключи)](docs/config.md)
- [Phoenix и Avro-схемы](docs/phoenix.md)
- [Подсказки ёмкости и метаданные](docs/capacity.md)
- [HBase shell / ZooKeeper / операции](docs/hbase.md) и runbook: [операции](docs/runbook/operations.md), [troubleshooting](docs/runbook/troubleshooting.md)
- [Avro (локальные/Confluent)](docs/avro.md) и [интеграция](docs/integration-avro.md)
- [Roadmap по Avro‑миграции](docs/roadmap-avro.md)
- [Профили peer](docs/peer-profiles.md)

## Обновление версии проекта

- Номер версии задаётся **только** в родительском `pom.xml`; модули `endpoint` и `benchmarks` наследуют его через `<parent>` и `${project.version}`. Ручного редактирования трёх файлов не требуется.
- Для атомарного обновления используйте Maven Versions Plugin:
  ```bash
  mvn versions:set -DnewVersion=0.0.16
  mvn versions:commit   # зафиксировать изменения, или mvn versions:revert при необходимости отката
  ```
- После исполнения команды обновятся все `pom.xml` в дереве. Проверьте diff, запустите `mvn test` и закоммитьте изменения версии вместе с релизными правками.

## Формат сообщений

Endpoint публикует события в формате Avro (Confluent Schema Registry). Ниже приведены основные параметры Avro:

| Ключ | Значение по умолчанию | Комментарий |
|---|---|---|
| `h2k.avro.schema.dir` | `conf/avro` | Каталог локальных схем и fallback‑кэша SR |
| `h2k.avro.sr.urls` | — | CSV вида `http://sr1:8081,http://sr2:8081` |
| `h2k.avro.props.subject.strategy` | `table` | `table` → `namespace:table`; `qualifier` — только имя таблицы |
| `h2k.avro.props.subject.prefix` | пусто | Используйте для разделения окружений |
| `h2k.avro.props.subject.suffix` | пусто | Часто ставят `-value` для совместимости с Kafka Connect |
| `h2k.avro.props.client.cache.capacity` | `1000` | Размер identity-map `CachedSchemaRegistryClient` |
| `h2k.avro.sr.auth.*` | — | Basic-auth (логин/пароль маскируются в логах) |

> Режим сериализации фиксирован: используется только Confluent Schema Registry. Ключ `h2k.avro.mode` удалён; локальные `.avsc` служат источником Phoenix‑метаданных и кешем схем.

При старте endpoint выводит строку уровня INFO вида `Payload: payload.format=AVRO_BINARY, serializer.class=..., schema.registry.urls=...` — по ней видно, что активен Confluent SR и какие URL используются.


## Безопасность и ограничения

- **SASL/SSL отсутствуют.** Endpoint работает в выделенной сети Kafka/HBase; шифрование и аутентификация обеспечиваются внешней инфраструктурой (VPN, ACL на брокерах, firewall).
- **Только задокументированные ключи.** Любые новые параметры добавляются через конфигурацию `h2k.*` и описываются в `docs/`, магии нет.
- **Секреты не храним.** Пароли Schema Registry передаём через `CONFIG` peer или переменные окружения RS; в логах значения маскируются.
- **Совместимость с Java 8.** Код не использует API 9+, коллекции предразмеряются по `h2k.capacity.hints` для экономии GC.
- **Локализация.** Логи, исключения и JavaDoc — на русском языке; структура комментариев отражает бизнес-логику.

## Раскатка и мониторинг

1. **Staged rollout.** Раскатайте JAR на один RegionServer, включите peer только для части таблиц и убедитесь, что Schema Registry зарегистрировал схемы без конфликтов.
2. **Метрики GC/throughput.** Снимите `HRegionServer.GcTimeMillis`, `ReplicationSource.avgReplicationDelay`, а также нагрузку на Kafka продьюсер (`records-sec`, `request-latency`).
3. **Наблюдаемость Kafka.** Проверяйте `UnderReplicatedPartitions`, `RecordErrorRate`, ошибки `org.apache.kafka.clients` в логах RS.
4. **Метрики endpoint.** `TopicManager.getMetrics()` возвращает счётчики ensure и свежие показатели `wal.*`, `schema.registry.*`; INFO-лог `Скорость WAL: ...` появляется примерно каждые 5 секунд и показывает фактическую скорость строк/сек.
5. **Расширение.** После 1–2 часов без аномалий включайте остальные RS и таблицы; держите предыдущую версию JAR в каталоге `lib/backup` до полного завершения миграции.
6. **Откат.** При необходимости отключите peer (`disable_peer`), удалите новый JAR, верните предыдущий и перезапустите RS.

## Бенчмарки производительности (JMH)

- Чтобы собрать отдельный JMH‑джар, установите основной модуль в локальный репозиторий (однократно):
  ```bash
  mvn -pl endpoint -am install -DskipTests
  ```

- Затем соберите модуль `benchmarks`:
  ```bash
  mvn -pl benchmarks -am -DskipTests clean package
  ```
Готовый архив появится в `benchmarks/target/h2k-endpoint-benchmarks-<version>.jar` (параллельно Maven сохранит `original-*.jar`).

Исходники бенчмарков теперь живут в стандартном каталоге `benchmarks/src/main/java`. Это избавляет IDE от попыток анализировать сгенерированные JMH‑классы из `benchmarks/target/generated-sources`. Если VS Code когда-либо восстановит старый путь `src/jmh/java`, выполните «Java: Clean Java Language Server Workspace» и убедитесь, что открыты файлы из нового каталога.

- Запуск всех сценариев:
  ```bash
  java -jar benchmarks/target/h2k-endpoint-benchmarks-<version>.jar
  ```

- Примеры фильтрации:
  ```bash
  java -jar benchmarks/target/h2k-endpoint-benchmarks-<version>.jar BatchSenderBenchmark
  java -jar benchmarks/target/h2k-endpoint-benchmarks-<version>.jar WalEntryProcessorBenchmark.processWideRow
  java -jar benchmarks/target/h2k-endpoint-benchmarks-<version>.jar BatchSenderBenchmark.autotuneScenarios -p scenario=FAIL_THEN_RECOVER
  ```

Бенчмарки моделируют типовые сценарии: Каждый сценарий можно запускать изолированно через `java -jar benchmarks/target/h2k-endpoint-benchmarks-<version>.jar <BenchmarkName>[.<method>]`. небольшие и средние партии для `BatchSender`, а также обработку строк
`WalEntryProcessor` с фильтром CF и без него. Результаты удобно сравнивать до/после изменений горячего пути и логики автоподстройки.

**Интерпретация отчёта JMH (режим AverageTime, микросекунды на операцию):**
- `Score` — среднее время одной операции; `Error` — доверительный интервал 99%.
- Мы фиксируем baseline на чистом запуске и отслеживаем относительные изменения (рост >10–15% считается регрессией).

**Сценарии:**
Avro Confluent — основной формат на проде, поэтому отдельные сценарии измеряют PayloadBuilder в режиме Schema Registry.
- `BatchSenderBenchmark.tryFlushSmall` — фоновые партии ~32 событий, проверяет скорость авто-сброса.
- `BatchSenderBenchmark.strictFlushMedium` — имитация остановки peer: flush 256 записей, ожидаем {@code < 50 мс}.
- `BatchSenderBenchmark.tryFlushLarge` — всплеск нагрузки, оценивает стоимость адаптации awaitEvery.
- `BatchSenderBenchmark.autotuneScenarios` — стресс-тест автотюнера: сценарии `SUCCESS`, `SLOW_HIGH`, `FAIL_THEN_RECOVER` имитируют мгновенные подтверждения, задержки с превышением порога и последовательные таймауты. Искомый результат — финальный `awaitEvery`: после `FAIL_THEN_RECOVER` он должен уйти в минимум и постепенно вернуться после успешных циклов.
- `WalEntryProcessorBenchmark.processSmallRow` — базовый горячий путь без фильтрации.
- `WalEntryProcessorBenchmark.processWideRow` — таблицы с десятками колонок и большим payload.
- `WalEntryProcessorBenchmark.processWithFilterHit` — успешная фильтрация CF.
- `WalEntryProcessorBenchmark.processWithFilterMiss` — отрицательное срабатывание фильтра (должно быть дешёвым).
- `PayloadBuilderConfluentBenchmark.serializeHot` — основной продовый путь: Confluent Avro с прогретым Schema Registry.
- `PayloadBuilderConfluentBenchmark.serializeWithRegistration` — стоимость первичной регистрации Avro-схемы в Schema Registry.
- `TopicManagerBenchmark.resolveCached` — проверка кеша имён топиков.
- `TopicManagerBenchmark.resolveUnique` — нагрузка на кеш при большом числе таблиц.
- `TopicManagerBenchmark.ensureNoop` — оверхед проверок ensure при отключённом режиме.

Совет: при анализе результатов автотюнера фиксируйте значения метрик `producer.batch.fail.streak.*`, `producer.batch.autotune.*` (см. Prometheus/JMX) до и после запуска теста `autotuneScenarios`. Это позволит видеть, как быстро алгоритм реагирует на серии таймаутов и возвращается к базовому `awaitEvery`.

### Разработка (IDE)

- В workspace присутствует `.vscode/settings.json`, исключающий `target/**` из дерева файлов/индексации и указывающий точные пути исходников (`endpoint/src/main/java`, `endpoint/src/test/java`, `benchmarks/src/main/java`). Это устраняет тысячи ложных предупреждений от JMH‑генерации. Если настройки не применились (VS Code кеширует LSP), выполните «Java: Clean Java Language Server Workspace» или перезапустите IDE.
- Для Visual Studio Code/IntelliJ IDEA/модуля Maven дополнительные действия не требуются: структура каталогов уже соответствует стандарту (`src/main/java`).

## FAQ

**Нужно ли класть Confluent JAR на RegionServer?**  
Нет. Версия 0.0.12 шейдит внутрь `io.confluent:kafka-avro-serializer` и `kafka-schema-registry-client` 5.3.8 вместе с зависимостями. Достаточно базовых библиотек кластера (`kafka-clients`, `lz4`, `snappy`).

**Что делать при ошибке 409 от Schema Registry?**  
По умолчанию мы используем `subject.strategy=table`, т.е. `namespace:table` (для `default` остаётся просто имя таблицы). Если ранее использовались сабджекты вида `qualifier`, проверьте `h2k.avro.subject.*`: можно вернуть старую стратегию, либо добавить префикс/суффикс. При конфликте удалите неверную версию в SR и перезапустите peer.

**Поддерживается ли SASL/SSL?**  
Нет. Endpoint рассчитан на доверенную сеть без TLS. Для защиты используйте внешний TLS proxy или переезд на защищённый кластер Kafka; настройки безопасности внутрь endpoint не добавляются.

**Как обновить локальные Avro-схемы?**  
Положите новую `.avsc` в `conf/avro/<table>.avsc`, убедитесь, что она обратимо совместима, и перезапустите peer. Endpoint перечитает схему при первом попадании таблицы в cache.

---

## Структура пакетов

- `kz.qazmarka.h2k.endpoint` — точка входа `KafkaReplicationEndpoint`, инициализация и публичный API.
- `kz.qazmarka.h2k.endpoint.internal` — `TopicManager`, метрики, вспомогательные сервисы endpoint’а.
- `kz.qazmarka.h2k.endpoint.processing` — горячий путь WAL→Kafka: группировка строк, фильтрация CF, построение payload, счётчики.
- `kz.qazmarka.h2k.kafka.ensure` — автоматическое создание/согласование топиков (подпакеты `admin`, `planner`, `state`, `metrics`, `config`).
- `kz.qazmarka.h2k.kafka.producer.batch` — адаптивный `BatchSender`, метрики и тюнер отправок.
- `kz.qazmarka.h2k.kafka.support` — общие Kafka-утилиты (например, `BackoffPolicy`).
- `kz.qazmarka.h2k.payload.builder` — сборка Avro payload’ов и расчёт ёмкости.
- `kz.qazmarka.h2k.payload.serializer` — сериализация Avro и интеграция с Schema Registry.
- `kz.qazmarka.h2k.schema.decoder` — декодирование значений Phoenix; `schema.registry.*` — резолверы Avro-схем (локальные, Phoenix, Confluent).
- `kz.qazmarka.h2k.config` — чтение и валидация конфигурации (`H2kConfig`, секции, builder’ы).
- `kz.qazmarka.h2k.util` — низкоуровневые утилиты (Bytes, JsonWriter, Parsers, RowKeySlice).
- Модуль `endpoint` содержит production‑код и тесты; модуль `benchmarks` — JMH-бенчмарки (`benchmarks/src/...`).
- `docs/` и `conf/` — эксплуатационная документация, профили peer и сопутствующие скрипты.
