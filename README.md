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
- **Kafka (клиенты):** 2.3.1
- **Phoenix:** 4.14/4.15 (совместимо; используется Avro Phoenix registry)

> RegionServer и Endpoint должны работать на **Java 8**.

---

## Установка

1. После сборки Maven появится два артефакта в `target/`: `h2k-endpoint-<version>.jar` (тонкий) и `h2k-endpoint-<version>-shaded.jar`. Для RegionServer используйте shaded-вариант (можно переименовать в `h2k-endpoint.jar` или загрузить как есть).
   Скопируйте `h2k-endpoint-<version>-shaded.jar` в `/opt/hbase-default-current/lib/`.  
2. Убедитесь, что на RS есть базовые клиентские библиотеки из кластера:
   - `kafka-clients-2.3.1.jar`
   - `lz4-java-1.6.0+.jar`
     *(если измените `compression.type` на `snappy`, добавьте `snappy-java-1.1.x+.jar`)*
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
| `h2k.topic.pattern` | Шаблон имени топика | `${table}` по умолчанию |
| `h2k.ensure.topics` | Автосоздание тем | true/false |
| `h2k.topic.config.*` | Доп. параметры Kafka-топика | Передаются в AdminClient при ensure |


> Фильтрация по CF задаётся на уровне Avro-схемы: укажите `"h2k.cf.list": "cf1,cf2"` в `conf/avro/<TABLE>.avsc`.
> Если свойство отсутствует, реплицируются все column family.

> Атрибуты `h2k.phoenixType`, `h2k.pk`, `h2k.saltBytes`, `h2k.capacityHint` должны быть заданы в `.avsc`.
> Файл `schema.json` более не используется.

> Полная справка по ключам и значениям — см. **docs/config.md**.
### Наблюдатели горячего пути

- Диагностика (`WalDiagnostics`) выключена по умолчанию (`h2k.observers.enabled=false`) и не влияет на горячий путь.
- Включайте флаг точечно для диагностики: появятся рекомендации по `h2k.capacityHint` (из Avro) и предупреждения об неэффективных CF-фильтрах и настройках соли.
- Метрики и WARN-логи помогают уточнить подсказки для таблиц; после тюнинга флаг можно снова отключить.

### Настройки продьюсера (синхронизированы с `conf/add_peer_shell_balanced.txt`)

| Ключ | Единицы | Дефолт (Endpoint) | BALANCED |
|---|---|---|---|
| `h2k.producer.enable.idempotence` | boolean | `true` | `true` |
| `h2k.producer.acks` | ack mode | `all` | `all` |
| `h2k.producer.max.in.flight` | запросов | `1` | `5` |
| `h2k.producer.linger.ms` | миллисекунды | `50` | `100` |
| `h2k.producer.batch.size` | байты | `65536` | `524288` |
| `h2k.producer.compression.type` | алгоритм | `lz4` | `lz4` |
| `h2k.producer.delivery.timeout.ms` | миллисекунды | `180000` | `300000` |

> BALANCED — основной профиль, объединяющий скорость и устойчивость. Если требуется изменить компрессию или поведение, редактируйте `conf/add_peer_shell_balanced.txt` и обновляйте документацию.

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

## Обновление версии проекта

- Номер версии задаётся **только** в корневом `pom.xml`. Ручного редактирования нескольких файлов не требуется.
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

Метрики `schema.registry.register.success` и `schema.registry.register.failures`, публикуемые через `TopicManager.getMetrics()`, напрямую отражают работу сериализатора Avro Confluent: успехи и ошибки регистрации схем в Schema Registry заметны без дополнительных логов.


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
4. **Метрики endpoint.** `TopicManager.getMetrics()` возвращает счётчики ensure и свежие показатели `wal.*`, `schema.registry.*`; `wal.rowbuffer.upsizes` и `wal.rowbuffer.trims` помогают заметить широкие строки (более 32 ячеек) и редкие «гигантские» записи (≥4096 ячеек), а INFO-лог `Скорость WAL: ...` появляется примерно каждые 5 секунд и показывает фактическую скорость строк/сек.
5. **Расширение.** После 1–2 часов без аномалий включайте остальные RS и таблицы; держите предыдущую версию JAR в каталоге `lib/backup` до полного завершения миграции.
6. **Откат.** При необходимости отключите peer (`disable_peer`), удалите новый JAR, верните предыдущий и перезапустите RS.

### Разработка (IDE)

- В workspace присутствует `.vscode/settings.json`, исключающий `target/**` из дерева файлов/индексации и указывающий точные пути исходников проекта. Если настройки не применились (VS Code кеширует LSP), выполните «Java: Clean Java Language Server Workspace» или перезапустите IDE.
- Для Visual Studio Code/IntelliJ IDEA дополнительной настройки Maven не требуется: структура каталогов уже соответствует стандарту (`src/main/java`).

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
- `kz.qazmarka.h2k.kafka.ensure` — автоматическое создание/согласование топиков (ядро `TopicEnsureService`, фоновой `TopicEnsureExecutor`, управление бэкоффом `TopicBackoffManager`; подпакеты `admin`, `config`).
- `kz.qazmarka.h2k.kafka.producer.batch` — минимальный буфер Kafka Futures с пороговым `flush`.
- `kz.qazmarka.h2k.kafka.support` — общие Kafka-утилиты (например, `BackoffPolicy`).
- `kz.qazmarka.h2k.payload.builder` — сборка Avro payload’ов и расчёт ёмкости.
- `kz.qazmarka.h2k.payload.serializer` — сериализация Avro и интеграция с Schema Registry.
- `kz.qazmarka.h2k.schema.decoder` — декодирование значений Phoenix; `schema.registry.*` — резолверы Avro-схем (локальные, Phoenix, Confluent).
- `kz.qazmarka.h2k.config` — чтение и валидация конфигурации (`H2kConfig`, секции, builder’ы).
- `kz.qazmarka.h2k.util` — низкоуровневые утилиты (Bytes, Parsers, RowKeySlice).
- Репозиторий содержит production‑код и тесты в стандартной структуре Maven (`src/main`, `src/test`).
- `docs/` и `conf/` — эксплуатационная документация, профили peer и сопутствующие скрипты.
