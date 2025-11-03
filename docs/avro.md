# Avro: форматы, схемы, эксплуатация

Эндпоинт работает в единственном режиме — Avro (Confluent Schema Registry 5.3.8). Локальные `.avsc` обязательны: они используются как источник Phoenix-метаданных, подсказок по CF/PK и как кеш перед регистрацией схем в SR. JSON/Generic режимы, а также ключи `h2k.payload.format` и `h2k.avro.mode` удалены.

## 1. Основные ключи

| Ключ | Значение по умолчанию | Комментарий |
|---|---|---|
| `h2k.avro.schema.dir` | `conf/avro` | Каталог локальных `.avsc`, читается при старте и служит fallback‑кэшем |
| `h2k.avro.sr.urls` | — | CSV адресов Schema Registry (`http://sr1:8081,http://sr2:8081`) |
| `h2k.avro.subject.strategy` | `table` | `table` → `namespace:table`, `table-upper/table-lower`, `qualifier` |
| `h2k.avro.subject.prefix/suffix` | пусто | Добавляется к subject (часто используют `-value`) |
| `h2k.avro.props.client.cache.capacity` | `1000` | Размер identity-map `CachedSchemaRegistryClient` |
| `h2k.avro.sr.auth.basic.username/password` | — | Basic Auth; значения маскируются в логах |

> Прочие `h2k.avro.props.*` прокидываются в конфигурацию Schema Registry клиента без изменений.

## 2. Требования к Avro-схемам

Схемы описывают **JSON-структуру**, которую собирает `PayloadBuilder`. Для корректной работы добавьте:

- На корневом уровне:
  - `"h2k.pk"` — массив имён PK в порядке Phoenix-таблицы.
  - `"h2k.saltBytes"` — длина соли rowkey (0..8).
  - `"h2k.capacityHint"` — ожидаемое число заполненных полей (для `h2k.capacity.hints`).
- Для каждой колонки:
  - `"h2k.phoenixType"` — точный Phoenix-тип (например, `UNSIGNED_INT`, `TIMESTAMP`).
  - `"h2k.jsonName"` (опц.) — если имя в JSON должно отличаться от колонки.
  - `"h2k.payload.skip"` (опц.) — если колонку нужно полностью исключить из Avro-перегонки (ячейки игнорируются, декодер не вызывается).

> Парсинг метаданных выполняется только через Jackson 2.x (`com.fasterxml.jackson`). Наследие `org.codehaus.jackson` больше не поддерживается и должно быть удалено из схем и окружения.

> Подробности по вычислению capacity и salt — в [`docs/capacity.md`](capacity.md) и [`docs/phoenix.md`](phoenix.md).

## 3. Subject strategy (Confluent)

По умолчанию используется стратегия `table` (`namespace:table`), для `DEFAULT` namespace — только qualifier. Доступны `qualifier`, `table-lower`, `table-upper`; при необходимости добавляйте собственные префикс/суффикс (например, `-value`). Полный перечень параметров см. в [`docs/config.md`](config.md).

## 4. Чеклист включения Avro

1. **Разложить `.avsc`** в `conf/avro/` и убедиться, что заполнены `h2k.phoenixType`, `h2k.pk`, `h2k.saltBytes`, `h2k.capacityHint`.
2. **Настроить ключи Schema Registry** (пример):
   ```properties
   h2k.avro.schema.dir=/opt/hbase/conf/avro
   h2k.avro.sr.urls=http://sr1:8081,http://sr2:8081
   h2k.avro.subject.strategy=table
   h2k.avro.subject.suffix=-value
   h2k.avro.sr.auth.basic.username=svc
   h2k.avro.sr.auth.basic.password=secret
   ```
3. **Перезагрузить peer** (`disable_peer` → `enable_peer`) либо перезапустить RegionServer.
4. **Проверить логи**: при старте `KafkaReplicationEndpoint` выводит строку `Payload: payload.format=AVRO_BINARY, serializer.class=..., schema.registry.urls=...`.
5. **Проверить Kafka/SR**: `curl $SR/subjects` — появился subject; `kafka-avro-console-consumer` (или ClickHouse `FORMAT AvroConfluent`) считывает сообщение с корректным PK.

## 5. Интеграционные сценарии

| Сценарий | Действия | Ожидаемый результат |
|---|---|---|
| Confluent | Включить SR, задать `h2k.avro.sr.urls`. | Subject появляется в SR, ClickHouse `FORMAT AvroConfluent`. |
| Нет `.avsc` | Удалить схему и перезапустить peer. | `IllegalStateException` в логах; peer продолжает работу (fallback невозможен). |
| SR недоступен | Остановить SR → отправить запись. | WARN, повторные попытки; при долгой недоступности backlog растёт. |
| Обновление схемы | Изменить `.avsc`, поднять версию в SR. | Новая версия регистрируется, endpoint предупреждает о несовместимости при расхождениях. |

## 6. Рекомендации по миграции (Roadmap)

1. **Этап 1** — единый горячий путь Avro Confluent, локальный реестр `.avsc` как кеш.
2. **Этап 2** — эксплуатация: мониторинг SR, бэкап `_schemas`, нагрузочные тесты профиля BALANCED (см. `conf/add_peer_shell_balanced.txt`).

Новая инсталляция пропускает этапы generic/JSON: сразу настраивайте Schema Registry и следите за подсказками `h2k.capacity.hints`.

## 7. Эксплуатация Schema Registry 5.3.8

- Развёртывание: tar от Confluent (Java 8). Проверьте конфиги `listeners`, `_schemas`, `compatibility.level=BACKWARD`.
- Запуск под systemd: `schema-registry-start/stop`, `Environment=SCHEMA_REGISTRY_HOME=/opt/confluent-default-current`.
- Создайте `_schemas` с `cleanup.policy=compact`, `RF=3`.
- Мониторинг: `GET /subjects`, `GET /subjects/<subj>/versions`, `GET /schemas/ids/<id>`.
- Настройте алерты на 5xx/latency SR, ошибки совместимости, рост `_schemas`.

Подробный пример конфигураций — см. [`docs/schema-registry.md`](schema-registry.md).

## 8. Мониторинг и отладка

- Включайте DEBUG для пакета `kz.qazmarka.h2k.payload.serializer.avro` и `...schema.registry` (см. runbook). Логи показывают путь к схеме, версию, результат регистрации.
- В логе `TopicManager` фиксируется `payload.format`/`serializer`.
- Для быстрой проверки схем воспользуйтесь `kafka-avro-console-consumer` (Confluent 5.3.8) или любым тестовым потребителем Avro Confluent.

## 9. Прогрев схем и оптимизация cold-start

### 9.1. Автоматический прогрев при инициализации

Эндпоинт выполняет **предварительную загрузку Avro-схем** из локального каталога (`h2k.avro.schema.dir`) в методе `KafkaReplicationEndpoint.init()`. Это снижает задержку первого сообщения и минимизирует I/O на горячем пути репликации:

```java
int loaded = payload.preloadLocalSchemas();
if (loaded > 0) {
    LOG.info("Предварительно загружено {} Avro-схем из каталога {}", loaded, schemaDir);
}
```

**Преимущества:**
- Первая WAL-запись не тормозится чтением `.avsc` с диска.
- Schema Registry клиент уже знает fingerprint локальной схемы → быстрая проверка совместимости.
- Снижается вероятность таймаутов при старте peer на медленных дисках или сетевых FS.

**Рекомендации:**
- Размещайте `conf/avro/` на быстром локальном диске (SSD), избегайте NFS/CIFS для критичных сценариев.
- Если прогрев не требуется (например, в dev-окружении с минимальным набором таблиц), параметр `h2k.avro.schema.dir` можно оставить пустым — эндпоинт выполнит lazy-loading при первой записи.
- Мониторьте лог INFO: строка `"Предварительно загружено N Avro-схем"` подтверждает успешный прогрев.

### 9.2. Влияние на время загрузки

| Этап инициализации | Без прогрева | С прогревом |
|---|---|---|
| Чтение `.avsc` | При первой WAL-записи (I/O блокирует replicate) | В init() (фоново, до старта репликации) |
| Регистрация в SR | Первая запись (может упереться в timeout SR) | Проверка fingerprint уже выполнена |
| First-message latency | +50-200 мс (зависит от диска/SR) | ~0 мс (схемы в памяти) |

Для production окружений прогрев **критичен** — он гарантирует предсказуемую задержку первой репликации и снижает риск таймаутов при старте peer на таблицах с большим числом колонок.

## 10. Размер пакета и минимизация shade

### 10.1. Текущая конфигурация сборки

Проект собирается через Maven Shade Plugin с **явным включением** только необходимых зависимостей в shaded JAR:

```xml
<artifactSet>
  <includes>
    <include>org.apache.avro:*</include>
    <include>com.fasterxml.jackson.core:*</include>
    <include>org.apache.commons:commons-compress</include>
    <include>io.confluent:*</include>
  </includes>
</artifactSet>
```

**Текущий размер артефактов:**
- Тонкий JAR (`h2k-endpoint-0.0.24.jar`): ~257 KB
- Shaded JAR (`h2k-endpoint-0.0.24-shaded.jar`): ~4.3 MB

Тяжёлые зависимости (HBase, Hadoop, Kafka, Phoenix) имеют `scope=provided` и **не входят** в shaded артефакт — их предоставляет окружение RegionServer.

### 10.2. Почему minimizeJar=true опасен

Shade Plugin поддерживает параметр `<minimizeJar>true</minimizeJar>`, который анализирует bytecode и исключает «неиспользуемые» классы. Однако для проектов с **рефлексией и динамической загрузкой** это приводит к runtime-ошибкам:

**Проблемы при включении минимизации:**
1. **Avro GenericDatumWriter/Reader** подгружают кодеки компрессии через рефлексию — минимизация выбросит Snappy/Deflate классы.
2. **Jackson ObjectMapper** создаёт сериализаторы/десериализаторы динамически — minimizeJar удалит модули `jackson-databind` internal classes.
3. **Confluent SchemaRegistryClient** использует ServiceLoader для расширений — минимизация убьёт SPI-провайдеры.
4. **Avro SpecificCompiler** и schema resolvers требуют полного набора классов Avro API.

**Попытка включения минимизации (откачено):**
```bash
mvn -Pslim clean package
# ОШИБКА: package io.confluent.kafka.schemaregistry.client does not exist
```

Компиляция упала из-за отсутствия классов Confluent, которые shade посчитал «лишними» и выбросил на этапе анализа зависимостей.

### 10.3. Рекомендации по снижению размера

Если критичны строгие лимиты на размер JAR (<2 MB), используйте ручной подход:

1. **Профилирование через `dependency:tree`:**
   ```bash
   mvn dependency:tree -Dverbose -Dincludes=io.confluent:*
   ```
   Найдите неиспользуемые модули (например, `kafka-connect-avro`, `kafka-streams-avro-serde`) и явно исключите их в `pom.xml`.

2. **Фильтры shade для классов:**
   ```xml
   <filter>
     <artifact>io.confluent:*</artifact>
     <excludes>
       <exclude>io/confluent/connect/**</exclude>
       <exclude>io/confluent/streams/**</exclude>
     </excludes>
   </filter>
   ```
   Тестируйте **каждое исключение** на всех сценариях с рефлексией.

3. **Strip debug info (безопасно):**
   ```xml
   <plugin>
     <artifactId>maven-compiler-plugin</artifactId>
     <configuration>
       <debug>false</debug>
     </configuration>
   </plugin>
   ```
   Экономия: ~5-10% размера JAR без функциональных рисков.

**Вывод:** Для h2k-endpoint размер 4.3 MB оптимален. Агрессивная минимизация (minimizeJar) **несовместима** с Avro/Confluent и откачена. Дальнейшее снижение требует ручного анализа каждого класса и regression-тестирования на production workload.

## 11. Связанные материалы

- Конфигурация: [`docs/config.md`](config.md)
- Phoenix метаданные: [`docs/phoenix.md`](phoenix.md)
- Подсказки ёмкости: [`docs/capacity.md`](capacity.md)
- Runbook: [`docs/runbook/operations.md`](runbook/operations.md), [`docs/runbook/troubleshooting.md`](runbook/troubleshooting.md)

Следите за актуальностью `.avsc` — добавление колонок в Phoenix требует обновить схему, пересчитать `capacityHint` и перезапустить peer.
