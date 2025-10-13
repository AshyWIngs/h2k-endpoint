# Документация по конфигурации `h2k.*` (точная спецификация)

Ниже — **строго соответствующие коду** (`src/main/java/kz/qazmarka/h2k/config/H2kConfig.java`) и примерам (`conf/add_peer_shell_*.txt`, `README.md`) ключи `h2k.*`. Без домыслов.

> Приоритет источников: **CONFIG в peer** ⟶ `hbase-site.xml`. Любой ключ из `h2k.*`, заданный в peer, перекрывает одноимённый ключ в `hbase-site.xml`.

---

## Содержание
- [Обязательные и базовые ключи](#обязательные-и-базовые-ключи)
- [Топики Kafka](#топики-kafka)
- [CF (Column Families)](#cf-column-families)
- [Avro](#avro)
- [Подсказки ёмкости](#подсказки-ёмкости)
- [Соль rowkey (Phoenix)](#соль-rowkey-phoenix)
- [Автосоздание/администрирование тем](#автосозданиеадминистрирование-тем)
- [Kafka Producer: pass-through и спец-ключи](#kafka-producer-pass-through-и-спец-ключи)
- [Профильные примеры из peer-скриптов](#профильные-примеры-из-peer-скриптов)

---

## Обязательные и базовые ключи

| Ключ | Назначение | Допустимые значения / Дефолт | Пример |
|---|---|---|---|
| **`h2k.kafka.bootstrap.servers`** | Список брокеров Kafka | `host:port[,host2:port2]` | `10.254.3.111:9092,10.254.3.112:9092` |

---

## Топики Kafka

| Ключ | Назначение | Допустимые значения / Дефолт | Пример |
|---|---|---|---|
| **`h2k.topic.pattern`** | Шаблон имени топика | Подставляются плейсхолдеры: `${table}`, `${namespace}`, `${qualifier}`. **Для default‑namespace префикс не пишем.** Дефолт: `${table}` | `${table}` |
| **`h2k.topic.max.length`** | Лимит длины имени топика (символов) | По коду дефолт = `249` | `249` |
| **`h2k.topic.partitions`** | Число разделов при автосоздании | Целое (без дефолта — используется брокерный) | `12` |
| **`h2k.topic.replication`** | Фактор репликации при автосоздании | Целое (без дефолта — используется брокерный) | `3` |
| **`h2k.topic.config.*`** | Доп. конфиги темы | Любые валидные свойства темы Kafka | `h2k.topic.config.cleanup.policy=compact` |

---

## CF (Column Families)

Фильтрация WAL по column family задаётся в Avro-схемах (`conf/avro/<TABLE>.avsc`) через свойство `"h2k.cf.list"`.
Значение — CSV без пробелов, регистр сохраняется (например, `"0,DOCUMENTS,b,d"`). Если свойство отсутствует,
реплицируются все CF. Несуществующие имена игнорируются; при ошибке регистра соответствующие ячейки будут отброшены.

> Поля **PK** из rowkey всегда инъектируются в payload автоматически; перечислять их в `h2k.cf.list` не требуется.

---

## Avro

| Ключ | Назначение | Значения / Дефолт | Примечание |
|---|---|---|---|
| **`h2k.avro.schema.dir`** | Каталог `.avsc` | Строка (дефолт: `conf/avro`) | Используется для загрузки схем и fallback-кэша |
| **`h2k.avro.sr.urls`** | URL Schema Registry (CSV) | `http://host1:8081[,http://hostN:8081]` | Обязательный параметр |
| **`h2k.avro.sr.auth.basic.username/password`** | Basic-auth к Schema Registry | Строки | Значения маскируются в логах |
| **`h2k.avro.props.subject.strategy`** | Стратегия subject | `table` (дефолт) \| `qualifier` \| `table-lower` \| `table-upper` | `table` -> `namespace:table` (если namespace не `default`) |
| **`h2k.avro.props.subject.prefix` / `suffix`** | Префикс/суффикс subject | Строки | Удобно для разделения окружений (`-value`, `dev-` и т.п.) |
| **`h2k.avro.props.client.cache.capacity`** | Размер кэша `CachedSchemaRegistryClient` | Положительное целое (дефолт: `1000`) | Управляет identity-map SR клиента |
| **`h2k.avro.*`** | Доп. свойства Avro | Любые ключи, не перечисленные выше | Сохраняются в `H2kConfig#getAvroProps()` для пользовательских фабрик |

> Aliases `h2k.avro.schema.registry` и `h2k.avro.schema.registry.url` обрабатываются как `h2k.avro.sr.urls`.

**Подробнее:** см. `docs/avro.md` (примеры конфигов и интеграция с Schema Registry).

---

## Подсказки ёмкости

| Ключ | Назначение | Формат | Пример |
|---|---|---|---|
| **`h2k.capacity.hints`** | Оценка «максимума не‑null полей» для таблиц (фолбэк; основной источник — `.avsc` `h2k.capacityHint`) | `TABLE=keys[,NS:TABLE=keys2,...]` (для DEFAULT‑NS имя без `DEFAULT`) | `TBL_JTI_TRACE_CIS_HISTORY=32` |
| **`h2k.capacity.hint.<TABLE>`** | Точечная подсказка для одной таблицы | `<int>` | `h2k.capacity.hint.RECEIPT=18` |

> Подсказки используются для предразмеривания структур в горячем пути (меньше аллокаций/GC). Если включены метаполя, добавляйте к «ключам» соответствующие константы (см. `docs/capacity.md`).

---

## Соль rowkey (Phoenix)

| Ключ | Назначение | Формат | Пример |
|---|---|---|---|
| **`h2k.salt.map`** | Сопоставление таблиц и числа байт соли в начале rowkey (фолбэк; основной источник — `.avsc` `h2k.saltBytes`) | `TABLE=bytes[,NS:TABLE=bytes2][,default=0]` | `TBL_JTI_TRACE_CIS_HISTORY=1,default=0` |

---

## Автосоздание/администрирование тем

| Ключ | Назначение | Значения / Дефолт | Пример |
|---|---|---|---|
| **`h2k.ensure.topics`** | Создавать тему автоматически | `true`/`false` | `true` |
| **`h2k.ensure.increase.partitions`** | Разрешить увеличение партиций | `true`/`false` (дефолт: `false`) | `false` |
| **`h2k.ensure.diff.configs`** | Применять отличающиеся конфиги темы | `true`/`false` (дефолт: `false`) | `false` |
| **`h2k.admin.timeout.ms`** | Таймаут операций админ‑клиента | `int` (ms) | `30000` |
| **`h2k.admin.client.id`** | ClientId для админ‑клиента | Строка | `h2k-admin` |
| **`h2k.ensure.unknown.backoff.ms`** | Бэкофф при «неизвестной теме» | `int` (ms) | `5000` |

---

## Kafka Producer: pass-through и спец-ключи

**Pass‑through:** все ключи с префиксом **`h2k.producer.`** копируются в конфигурацию Kafka Producer **без изменений имени** (например, `h2k.producer.acks`, `h2k.producer.linger.ms`, `h2k.producer.compression.type`, и т.д.).

**Спец‑ключи проекта:**

| Ключ | Что делает | Примечание |
|---|---|---|
| **`h2k.producer.max.in.flight`** | Упрощённая запись для Kafka‑ключа `max.in.flight.requests.per.connection` | Используйте `1` в строгом профиле (RELIABLE); см. README и peer‑скрипты |
| **`h2k.producer.await.every`** | Каждые N записей ждать ACK (бэкпрешер) | Не Kafka‑ключ; внутренняя логика endpoint |
| **`h2k.producer.await.timeout.ms`** | Таймаут ожидания ACK | Не Kafka‑ключ; внутренняя логика endpoint |
| **`h2k.producer.batch.counters.enabled`** | Счётчики батчей | Не Kafka‑ключ |
| **`h2k.producer.batch.debug.on.failure`** | Доп. диагностика при сбоях | Не Kafka‑ключ |
| **`h2k.producer.batch.autotune.enabled`** | Включить автоподстройку `awaitEvery` по задержке и сброс к минимуму при ошибках ожидания | `true`/`false` (дефолт: `true`) |
| **`h2k.producer.batch.autotune.min`** | Нижняя граница `awaitEvery` при автонастройке | `int` (0 — вычислить автоматически) |
| **`h2k.producer.batch.autotune.max`** | Верхняя граница `awaitEvery` при автонастройке | `int` (0 — вычислить автоматически) |
| **`h2k.producer.batch.autotune.latency.high.ms`** | Порог задержки (мс), при превышении которого порог снижается | `int` (0 — 50% от `await.timeout.ms`) |
| **`h2k.producer.batch.autotune.latency.low.ms`** | Порог задержки (мс) для увеличения `awaitEvery` | `int` (0 — ~16% от `await.timeout.ms`) |
| **`h2k.producer.batch.autotune.cooldown.ms`** | Минимальный интервал между решениями автонастройки | `int` (мс; дефолт `30000`) |

> Полную матрицу рекомендованных настроек по профилям см. в README и `docs/peer-profiles.md`.

---

## Профильные примеры из peer-скриптов

**BALANCED (фрагмент из `conf/add_peer_shell_balanced.txt`):**
```ruby
'h2k.kafka.bootstrap.servers'   => '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
'h2k.topic.pattern'             => '${table}',
'h2k.avro.sr.urls'              => 'http://schema-registry-1:8081,http://schema-registry-2:8081',
'h2k.avro.schema.dir'           => '/opt/h2k/conf/avro',
'h2k.salt.map'                 => 'TBL_JTI_TRACE_CIS_HISTORY=1',
'h2k.capacity.hints'           => 'TBL_JTI_TRACE_CIS_HISTORY=32',
'h2k.ensure.topics'            => 'true',
'h2k.topic.partitions'         => '12',
'h2k.topic.replication'        => '3',
'h2k.admin.timeout.ms'         => '30000',
'h2k.ensure.unknown.backoff.ms'=> '5000'
```

**FAST / RELIABLE** — см. соответствующие файлы в `conf/` и README для матрицы acks/idempotence/max.in.flight/компрессии.

---

## Примечания
- Имена таблиц/CF — латиница. Значения могут быть на русском/казахском — без порчи при доставке.
- Все ключи из этого документа — **ровно те**, что используются кодом (см. `H2kConfig`), peer‑скриптами и README.
