## HBase 1.4.13 → Kafka 2.3.1 (клиент 3.3.2) ReplicationEndpoint (Avro Confluent only)

**Пакет:** `kz.qazmarka.h2k.endpoint`  
**Endpoint‑класс:** `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`

Лёгкий и быстрый `ReplicationEndpoint` для HBase 1.4.x. Формат полезной нагрузки — строго **Avro (Confluent Schema Registry)** (только Avro с Confluent Schema Registry). Минимум аллокаций, стабильный порядок полей, дружелюбные логи на русском.

---

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Поддерживаемые версии](#поддерживаемые-версии)
- [Установка](#установка)
- [Минимальная конфигурация](#минимальная-конфигурация)
- [Профили peer](#профили-peer)
- [Полная документация](#полная-документация)
- [Формат сообщений](#формат-сообщений)
- [Безопасность и ограничения](#безопасность-и-ограничения)
- [Раскатка и мониторинг](#раскатка-и-мониторинг)
- [FAQ](#faq)
- [Структура пакетов](#структура-пакетов)
- [Обновление версии проекта](#обновление-версии-проекта)

---

## Быстрый старт

0) **Требования окружения.** Java 8 на RegionServer, брокеры Kafka 2.3.1+ (клиент `kafka-clients` 3.3.2), Schema Registry 5.3.8 при использовании Confluent, доступ к ZK/Kafka без TLS/SASL.

1) **Соберите и разложите JAR** на все RegionServer (JAR уже содержит Avro/Jackson/Confluent 5.3.8):
```bash
# Теперь можно собирать без указания JAVA_HOME
mvn -DskipTests clean package
cp target/h2k-endpoint-*-shaded.jar /opt/hbase-default-current/lib/
```

**Примечание:** Проект настроен на автоматическое использование JDK 1.8. Подробности в [docs/java-setup.md](docs/java-setup.md).

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

4) **Создайте peer** готовым скриптом:
```bash
bin/hbase shell conf/add_peer_shell_balanced.txt
```

5) **Проверьте доставку**: сообщения появляются в топике `${table}` (формат Avro Confluent).

---

## Поддерживаемые версии

- **Java:** 8 (target 1.8)
- **HBase:** 1.4.13 (совместимо с 1.4.x)
- **Kafka (клиенты):** 3.3.2 (поддерживает брокеры 2.3.1+)
- **Phoenix:** 4.14/4.15 (совместимо; используется Avro Phoenix registry)
- **Schema Registry:** Confluent 5.3.8 (совместимо с 5.x)

> RegionServer и Endpoint должны работать на **Java 8**.

---

## Установка

1. После сборки Maven появится два артефакта в `target/`: `h2k-endpoint-<version>.jar` (тонкий) и `h2k-endpoint-<version>-shaded.jar`. Для RegionServer используйте shaded-вариант (можно переименовать в `h2k-endpoint.jar` или загрузить как есть).
   Скопируйте `h2k-endpoint-<version>-shaded.jar` в `/opt/hbase-default-current/lib/`.  
2. Дополнительные Kafka‑клиенты и LZ4 на RS не требуются: `kafka-clients` 3.3.2 и `lz4-java` 1.8.0 уже включены в `h2k-endpoint-*-shaded.jar`.
   > Avro/Jackson и Confluent Schema Registry 5.3.8 шейдятся внутрь `h2k-endpoint-*.jar`, поэтому дополнительные Confluent JAR не нужны.
3. Перезапустите RegionServer.

Если используете тонкий JAR (`h2k-endpoint-<version>.jar`):

- Дополнительно положите на classpath RS следующие библиотеки строгих версий:
  - org.apache.avro:avro:1.11.4
  - com.fasterxml.jackson.core:jackson-core:2.17.2
  - com.fasterxml.jackson.core:jackson-databind:2.17.2
  - com.fasterxml.jackson.core:jackson-annotations:2.17.2
  - org.apache.commons:commons-compress:1.26.0
  - org.apache.commons:commons-lang3:3.18.0
  - io.confluent:kafka-avro-serializer:5.3.8
  - io.confluent:kafka-schema-registry-client:5.3.8
  - io.confluent:common-config:5.3.8
  - io.confluent:common-utils:5.3.8
  - com.101tec:zkclient:0.10
  - (если на RS отсутствуют) org.apache.kafka:kafka-clients:3.3.2 и org.lz4:lz4-java:1.8.0

- Быстрая выгрузка зависимостей через Maven:
  ```bash
  mvn -q -DincludeScope=runtime dependency:copy-dependencies \
    -DoutputDirectory=target/lib-thin
  # скопируйте из target/lib-thin перечисленные выше JAR на RS в HBASE_HOME/lib/
  ```

Примечания для тонкого JAR:
- Не добавляйте `slf4j-simple` на RS — у HBase уже есть свой биндинг SLF4J (log4j).
- Следите, чтобы на classpath не лежали параллельно старые Avro/Jackson: используйте версии, указанные выше.

Быстрая проверка:
```bash
hbase classpath | tr ':' '\n' | egrep -i 'kafka-clients|lz4'
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
| `h2k.kafka.bootstrap.servers` | Список брокеров Kafka | `host:port[,host2:port2]` |
| `h2k.avro.sr.urls` | URL Schema Registry | Обязателен; перечисляйте через запятую |
| `h2k.avro.schema.dir` | Каталог локальных `.avsc` | По умолчанию `conf/avro` |
| `h2k.avro.sr.auth.*` | Basic-auth к SR | Используйте при необходимости авторизации |
| `h2k.topic.pattern` | Шаблон имени топика | `${table}` по умолчанию |
| `h2k.ensure.topics` | Автосоздание тем | true/false |
| `h2k.topic.config.*` | Доп. параметры Kafka-топика | Передаются в AdminClient при ensure |
| `h2k.jmx.enabled` | Регистрация MBean `H2kMetricsJmx` | true/false (дефолт `true`); отключайте только если JMX запрещён |


> Фильтрация по CF задаётся на уровне Avro-схемы: укажите `"h2k.cf.list": "cf1,cf2"` в `conf/avro/<TABLE>.avsc`.
> Если свойство отсутствует, реплицируются все column family.

> Атрибуты `h2k.phoenixType`, `h2k.pk`, `h2k.saltBytes`, `h2k.capacityHint` должны быть заданы в `.avsc`.
> Файл `schema.json` более не используется.

> Полная справка по ключам и значениям — см. **docs/config.md**.
### Наблюдатели и JMX

- Диагностика (`WalDiagnostics`) выключена по умолчанию (`h2k.observers.enabled=false`) и не влияет на горячий путь.
- Включайте флаг точечно для диагностики: появятся рекомендации по `h2k.capacityHint` (из Avro) и предупреждения об неэффективных CF-фильтрах и настройках соли.
- Метрики и WARN-логи помогают уточнить подсказки для таблиц; после тюнинга флаг можно снова отключить.
- JMX-метрики (`H2kMetricsJmx`) публикуются, пока `h2k.jmx.enabled=true` (значение по умолчанию). Отключайте только если на RS запрещён JMX — иначе Prometheus/grafana не увидят метрики.

### Настройки продьюсера (синхронизированы с `conf/add_peer_shell_balanced.txt`)

| Ключ | Единицы | Дефолт (Endpoint) | BALANCED |
|---|---|---|---|
| `h2k.producer.enable.idempotence` | boolean | `true` | `true` |
| `h2k.producer.acks` | ack mode | `all` | `all` |
| `h2k.producer.max.in.flight` | запросов | `1` | `5` |
| `h2k.producer.linger.ms` | миллисекунды | `50` | `100` |
| `h2k.producer.batch.size` | байты | `65536` | `524288` |
| `h2k.producer.delivery.timeout.ms` | миллисекунды | `180000` | `300000` |

> BALANCED — основной профиль, объединяющий скорость и устойчивость. Алгоритм сжатия Kafka Producer жёстко фиксирован на `lz4` и не переопределяется.

---

## Профили peer

Готовый скрипт (каталог `conf/`):

- **BALANCED** — `conf/add_peer_shell_balanced.txt` (единственный поддерживаемый профиль).

Подробные параметры и подсказки по тюнингу см. в [docs/peer-profiles.md](docs/peer-profiles.md) и [docs/hbase.md](docs/hbase.md).

---

## Полная документация

- [Сводный навигатор по документации](docs/README.md)
- [Конфигурация (все ключи)](docs/config.md)
- [Phoenix и Avro-схемы](docs/phoenix.md)
- [Подсказки ёмкости и метаданные](docs/capacity.md)
- [HBase shell / ZooKeeper / операции](docs/hbase.md) и runbook: [операции](docs/runbook/operations.md), [troubleshooting](docs/runbook/troubleshooting.md)
- [Avro (локальные/Confluent)](docs/avro.md)
- [Подробная инструкция по Confluent Schema Registry](docs/schema-registry.md)
- [Профили peer](docs/peer-profiles.md)
- [Руководство по разработке: логирование и диагностика](docs/dev-guidelines.md)
- [Настройка Java 8 для сборки проекта](docs/java-setup.md)
- [Настройка Codacy CLI для WSL](docs/codacy-setup.md)

## Обновление версии проекта

- Номер версии задаётся в `pom.xml`. Ручного редактирования не требуется.
- Для атомарного обновления используйте Maven Versions Plugin:
  ```bash
  mvn versions:set -DnewVersion=0.0.16
  mvn versions:commit   # зафиксировать изменения, или mvn versions:revert при необходимости отката
  ```
- После исполнения команды обновится `pom.xml` в дереве. Проверьте diff, запустите `mvn test` и закоммитьте изменения версии вместе с релизными правками.

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

> Kafka‑продьюсер отправляет ключ сообщения без дополнительных аллокаций: `RowKeySliceSerializer` переиспользует существующий rowkey из WAL, копируя байты только при необходимости (offset/length ≠ всего массива). Это снижает нагрузку на GC при широких строках.

> При кратковременной недоступности Schema Registry Endpoint не блокирует горячий путь: сериализатор берёт
> последний зарегистрированный `schemaId` из локального кеша и ставит повторную регистрацию схемы в фоновый поток
> (экспоненциальный бэкофф, максимум 8 попыток). Готовность SR отслеживайте по метрикам
> `SchemaRegistryMetrics.registrationFailures` и WARN-логам `Schema Registry недоступен...`.

При старте endpoint выводит строку уровня INFO вида `Payload: payload.format=AVRO_BINARY, serializer.class=..., schema.registry.urls=...` — по ней видно, что активен Confluent SR и какие URL используются.

Метрики `sr.регистрация.успехов` и `sr.регистрация.ошибок`, публикуемые через `TopicManager.getMetrics()`, напрямую отражают работу сериализатора Avro Confluent: успехи и ошибки регистрации схем в Schema Registry заметны без дополнительных логов (см. JMX‑алиасы в `docs/prometheus-jmx.md`).

Отдельная метрика `ensure.пропуски.из-за.паузы` показывает, сколько ensure-вызовов сработало в режиме «пропуск по cooldown». Допуски жёстко заданы в коде (1 минута после успешной проверки, 5 секунд после ошибки); рост счётчика означает, что темы часто попадают в окно удержания. Проверьте доступность Kafka/AdminClient и при необходимости инициируйте ensure вручную (например, через `TopicEnsurer#ensureTopicOk` в тестовых утилитах).

## Безопасность и ограничения

- **SASL/SSL отсутствуют.** Endpoint работает в выделенной сети Kafka/HBase; шифрование и аутентификация обеспечиваются внешней инфраструктурой (VPN, ACL на брокерах, firewall).
- **Только задокументированные ключи.** Любые новые параметры добавляются через конфигурацию `h2k.*` и описываются в `docs/`, магии нет.
- **Секреты не храним.** Пароли Schema Registry передаём через `CONFIG` peer или переменные окружения RS; в логах значения маскируются.
- **Совместимость с Java 8.** Код не использует API 9+, коллекции предразмеряются на основе `h2k.capacityHint` из Avro для экономии GC.
- **Локализация.** Логи, исключения и JavaDoc — на русском языке; структура комментариев отражает бизнес-логику.

## Раскатка и мониторинг

1. **Staged rollout.** Раскатайте JAR на один RegionServer, включите peer только для части таблиц и убедитесь, что Schema Registry зарегистрировал схемы без конфликтов.
2. **Метрики GC/throughput.** Снимите `HRegionServer.GcTimeMillis`, `ReplicationSource.avgReplicationDelay`, а также нагрузку на Kafka продьюсер (`records-sec`, `request-latency`).
3. **Наблюдаемость Kafka.** Проверяйте `UnderReplicatedPartitions`, `RecordErrorRate`, ошибки `org.apache.kafka.clients` в логах RS.
4. **Метрики endpoint.** `TopicManager.getMetrics()` возвращает счётчики ensure и свежие показатели `wal.*`, `sr.*`; `wal.rowbuffer.расширения` и `wal.rowbuffer.сжатия` помогают заметить широкие строки (более 32 ячеек) и редкие «гигантские» записи (≥4096 ячеек), а INFO-лог `Скорость WAL: ...` появляется примерно каждые 5 секунд и показывает фактическую скорость строк/сек. Цепочка `TopicEnsurer` (AdminClient + executor) поднимается лениво при первом ensure-вызове, поэтому время старта Endpoint минимально; первый же ensure может занять немного дольше обычного.
5. **Журналы.** Все записи Endpoint теперь начинаются с `h2k-endpoint`, поэтому диагностика сводится к `journalctl -u hbase-regionserver-default.service -o cat --since "2h" | grep h2k-endpoint`. Для таблиц можно дополнительно фильтровать по `table=NAME`.
6. **Расширение.** После 1–2 часов без аномалий включайте остальные RS и таблицы; держите предыдущую версию JAR в каталоге `lib/backup` до полного завершения миграции.
7. **Откат.** При необходимости отключите peer (`disable_peer`), удалите новый JAR, верните предыдущий и перезапустите RS.

### Вспомогательные классы горячего пути

- **`kz.qazmarka.h2k.endpoint.ReplicationResources`** — единая фабрика/контейнер для `PayloadBuilder`, `TopicManager`, `WalEntryProcessor`. Создаёт их атомарно и хранит ссылки, чтобы `KafkaReplicationEndpoint` мог безопасно стартовать и завершаться без дублирования кода.
- **`ReplicationResources.ResourceGuard`** — стековый guard, автоматически закрывающий уже созданные ресурсы, если инициализация оборвётся. Как только все компоненты подняты успешно, guard высвобождается (`releaseAll()`), а ответственность переходит к endpoint’у.
- **`TopicManager`** — тонкий фасад над `TopicEnsurer` и `TopicNamingSettings`, гарантирует ленивое ensure и отдаёт метрики для JMX/логов.
- Эти классы не участвуют в непосредственной обработке WAL, но упрощают жизненный цикл и предотвращают утечки Kafka-продьюсеров/ensure-цепочек. При доработках горячего пути начинайте с `ReplicationResources#create`: там видно, какие зависимости поднимаются в каком порядке.

### Поток обработки WAL

- `ReplicationExecutor` получает батч `ReplicationEndpoint.ReplicateContext`. Пустые списки сразу подтверждаются, чтобы не трогать Kafka.
- Для непустых партий формируется `ReplicationBatch`: из настроек `ProducerAwaitSettings` вычисляются `awaitEvery/awaitTimeoutMs`, о чём пишет DEBUG‑лог. Создаётся `BatchSender`, который сам заботится о пороговом `flush()` и закрытии.
- Каждый `WAL.Entry` прогоняется через `WalEntryProcessor.process(entry, sender)`. Процессор:
  1. Разрешает имя топика и при необходимости запускает Ensure-топик.
  2. Считывает метаданные WAL (sequenceId/writeTime) — дополнительный флаг `includeWalMeta` больше не требуется.
  3. Собирает CF‑фильтр, группирует ячейки по rowkey и отправляет строки в Kafka, накапливая futures в `BatchSender`.
- `ReplicationExecutor` ловит исключения, пишет WARN/ERROR и увеличивает метрики `репликация.ошибок.*`; DEBUG‑лог содержит стек. Метрики доступны через `TopicManager.getMetrics()` и JMX.

### Разработка (IDE)

- В workspace присутствует `.vscode/settings.json`, исключающий `target/**` из дерева файлов/индексации и указывающий точные пути исходников проекта. Если настройки не применились (VS Code кеширует LSP), выполните «Java: Clean Java Language Server Workspace» или перезапустите IDE.
- Для Visual Studio Code/IntelliJ IDEA дополнительной настройки Maven не требуется: структура каталогов уже соответствует стандарту (`src/main/java`).
- После любых правок запускайте полную тестовую сборку в Java 8 окружении:
  ```bash
  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  export PATH="$JAVA_HOME/bin:$PATH"
  mvn -q -DskipTests=false test
  ```
  Команда гарантирует, что проект собирается и что весь набор модульных и интеграционных тестов проходит на целевой версии JDK.

## FAQ

**Нужно ли класть Confluent JAR на RegionServer?**  
Нет. Текущая версия шейдит внутрь `kafka-clients` 3.3.2, `lz4-java` 1.8.0 и `io.confluent:kafka-avro-serializer`/`kafka-schema-registry-client` 5.3.8 — дополнительных JAR на RS не требуется.

**Что делать при ошибке 409 от Schema Registry?**  
По умолчанию мы используем `subject.strategy=table`, т.е. `namespace:table` (для `default` остаётся просто имя таблицы). Если ранее использовались сабджекты вида `qualifier`, проверьте `h2k.avro.subject.*`: можно вернуть старую стратегию, либо добавить префикс/суффикс. При конфликте удалите неверную версию в SR и перезапустите peer.

**Поддерживается ли SASL/SSL?**  
Нет. Endpoint рассчитан на доверенную сеть без TLS. Для защиты используйте внешний TLS proxy или переезд на защищённый кластер Kafka; настройки безопасности внутрь endpoint не добавляются.

## E2E-проверка совместимости с Kafka 2.3.1

1. Установите Docker Desktop / Podman и убедитесь, что демон запущен.
2. Экспортируйте переменную среды, чтобы активировать тест:

   ```bash
   export H2K_E2E_KAFKA=true
   ```

3. Запустите набор e2e-тестов (каждый тест стартует тот же брокер Kafka 2.3.1):

   ```bash
   export H2K_E2E_KAFKA=true
   mvn -Pe2e -Dtest=kz.qazmarka.h2k.kafka.compat.Kafka233CompatibilityE2ETest test
   ```

В тестовом наборе:

- **producerAndAdminAreCompatibleWithKafka23** — базовый happy‑path: ensure → send → consume.
- **adminCanDescribeAndIncreasePartitions** — AdminClient умеет читать конфиги и увеличивать число партиций.
- **producerDeliversBatchInOrder** — батч из 10 сообщений сохраняет порядок и не дублируется.
- **creatingExistingTopicFailsWithTopicExists** — негативный сценарий: повторный create выдаёт `TopicExistsException`.
- **consumerGroupRebalanceDistributesPartitions** — два consumer'а в одной группе корректно делят партиции (rebalance).
- **producerWorksWithRowKeySliceOffset** — посылаем RowKeySlice с неполным массивом (offset/length) и убеждаемся, что десериализация цела.
- **producerFailsWithInvalidBootstrap** — негативный тест: неверный bootstrap приводит к ожидаемому `ExecutionException`.
- **producerRecoversAfterBrokerRestart** — останавливаем Kafka 2.3.1, убеждаемся, что отправка падает, затем брокер перезапускается и приём сообщений восстанавливается.

Запускается фиксированный образ `confluentinc/cp-kafka:5.3.8` (amd64); на Apple Silicon тесты идут через QEMU-эмуляцию, поэтому выполняются медленнее, но дополнительных настроек не требуется.

Команда подтверждает, что клиент `kafka-clients` 3.3.2 корректно работает с брокером Kafka 2.3.1 в позитивных и негативных сценариях.

**Как обновить локальные Avro-схемы?**  
Положите новую `.avsc` в `conf/avro/<table>.avsc`, убедитесь, что она обратимо совместима, и перезапустите peer. Endpoint перечитает схему при первом попадании таблицы в cache.

---

## Структура пакетов

- `kz.qazmarka.h2k.endpoint` — точка входа `KafkaReplicationEndpoint`, вспомогательные классы жизненного цикла (`ReplicationResources`, `ResourceGuard`).
- `kz.qazmarka.h2k.endpoint.topic` — `TopicManager` и обвязка ensure-логики.
- `kz.qazmarka.h2k.endpoint.processing` — горячий путь WAL→Kafka: группировка строк, фильтрация CF, построение payload, счётчики.
- `kz.qazmarka.h2k.kafka.ensure` — автоматическое создание/согласование топиков (ядро `EnsureCoordinator`, фоновой `TopicEnsureExecutor`, управление бэкоффом `TopicBackoffManager`; подпакеты `admin`, `config`).
- `kz.qazmarka.h2k.kafka.producer.batch` — минимальный буфер Kafka Futures с пороговым `flush`.
- `kz.qazmarka.h2k.kafka.support` — общие Kafka-утилиты (например, `BackoffPolicy`).
- `kz.qazmarka.h2k.payload.builder` — сборка Avro payload’ов и расчёт ёмкости.
- `kz.qazmarka.h2k.payload.serializer` — сериализация Avro и интеграция с Schema Registry.
- `kz.qazmarka.h2k.schema.decoder` — декодирование значений Phoenix; `schema.registry.*` — резолверы Avro-схем (локальные, Phoenix, Confluent).
- `kz.qazmarka.h2k.config` — чтение и валидация конфигурации (`H2kConfig`, секции, builder’ы).
- `kz.qazmarka.h2k.util` — низкоуровневые утилиты (Bytes, Parsers, RowKeySlice).
- Репозиторий содержит production‑код и тесты в стандартной структуре Maven (`src/main`, `src/test`).
- `docs/` и `conf/` — эксплуатационная документация, профили peer и сопутствующие скрипты.
