# ClickHouse Kafka Ingestion — Runbook

_Версия документа: v1.6 (обновлено: 2025-11-09)_  
_Документ протестирован на ClickHouse 24.8.14.39 (official build)_

## Ключевые изменения в v1.6

- ✅ **Модульная структура для множества таблиц**: раздел 4 теперь содержит подразделы для каждой таблицы
- ✅ Добавлен шаблон для быстрого создания DDL для новых топиков Kafka
- ✅ Раздел 4.1: полный набор DDL для TBL_JTI_TRACE_CIS_HISTORY (7 объектов)
- ✅ Раздел 4.2: заготовка для TBL_JTI_TRACE_DOCUMENT_ERRORS
- ✅ Раздел 4.3: плейсхолдер для следующих таблиц
- ✅ Перенумерованы разделы: управление (5), тюнинг (6), мониторинг (7), деплой (8), DR (9), команды (10)

## Изменения в v1.5

- ✅ Исправлен ZK-путь для `shardless`
- ✅ Заменён `kafka_commit_every_batch` на `kafka_commit_on_select = 0`
- ✅ Оптимизирован ORDER BY для dedupe
- ✅ Добавлен `coalesce` в TTL
- ✅ Убран избыточный ключ шардирования
- ✅ Устранены дубликаты

---

## Содержание

- [0. Проверка окружения](#0-проверка-окружения)
- [1. Архитектура системы](#1-архитектура-системы)
- [2. Настройка кластеров](#2-настройка-кластеров)
- [3. Создание базы данных](#3-создание-базы-данных)
- [4. Таблицы Kafka → ClickHouse](#4-таблицы-kafka--clickhouse)
  - [4.1. TBL_JTI_TRACE_CIS_HISTORY](#41-tbl_jti_trace_cis_history)
  - [4.2. TBL_JTI_TRACE_DOCUMENT_ERRORS](#42-tbl_jti_trace_document_errors)
  - [4.3. [Добавить следующую таблицу]](#43-добавить-следующую-таблицу)
- [5. Управление ingestion](#5-управление-ingestion)
- [6. Тюнинг производительности](#6-тюнинг-производительности)
- [7. Мониторинг и алерты](#7-мониторинг-и-алерты)
- [8. Деплой QA → PROD](#8-деплой-qa--prod)
- [9. DR-сценарии](#9-dr-сценарии)
- [10. Быстрые команды](#10-быстрые-команды)
- [Приложения](#приложения)

---

## 0. Проверка окружения

Перед выполнением инструкций убедитесь:

1. **ClickHouse доступен** по TCP:9000 и HTTP:8123 на всех нодах
2. **Конфигурация идентична** (`clickhouse_remote_servers.xml`, `macros.xml`)
3. **Макросы корректны:**
   - `core`: `{shard}`, `{replica}`
   - `shardless`: `{shardless_repl}` (уникальный на каждой реплике)
   - `per_host_allnodes`: макросы не используются
4. **ZooKeeper доступен:**
   ```sql
   SELECT hostName(), path FROM system.zookeeper WHERE path = '/' LIMIT 1;
   ```
5. **Schema Registry доступен:**
   ```bash
   curl -sf http://schema-registry-host:8081/subjects || echo "SR недоступен"
   ```
6. **Kafka доступна:**
   ```bash
   kcat -L -b 10.254.3.111:9092
   ```
7. **Кластеры согласованы:**
   ```sql
   SELECT cluster, count() FROM system.clusters 
   WHERE cluster IN ('core', 'shardless', 'per_host_allnodes') 
   GROUP BY cluster;
   ```

---

## 1. Архитектура системы

### Поток данных

```
Kafka (TBL_JTI_TRACE_CIS_HISTORY, 12 партиций)
  ↓
[per_host_allnodes] tbl_*_kafka (ENGINE=Kafka + AvroConfluent)
  ↓ mv_*_to_raw
[per_host_allnodes] tbl_*_raw (MergeTree, TTL 60d, локально на каждой ноде)
  ↓ Distributed
[shardless] tbl_*_raw_all (Distributed по per_host_allnodes)
  ↓ mv_*_to_repl
[shardless] tbl_*_repl (ReplicatedReplacingMergeTree, TTL 180d)
  ↓ Distributed
[shardless] tbl_*_repl_all ← ВСЯ АНАЛИТИКА ЧИТАЕТ ЗДЕСЬ
```

### Три логических кластера

| Кластер | Топология | Репликация | Макросы | ZK-пути | Назначение |
|---------|-----------|------------|---------|---------|------------|
| **per_host_allnodes** | N×1 | `false` | — | — | Быстрый приём из Kafka |
| **shardless** | 1×N | `true` | `{shardless_repl}` | `/clickhouse/tables/shardless/...` | Надёжное хранение, аналитика |
| **core** | 2×2 | `true` | `{shard}`, `{replica}` | `/clickhouse/tables/{shard}/...` | Исторические таблицы |

**Ключевые принципы:**
- Ingestion без репликации → максимальная скорость
- Витрины с репликацией → надёжность
- ZK-пути не пересекаются
- Core не трогаем

---

## 2. Настройка кластеров

### 2.1. Конфигурация per_host_allnodes

Добавить в `/etc/clickhouse-server/config.d/clickhouse_remote_servers.xml`:

```xml
<per_host_allnodes>
  <shard>
      <internal_replication>false</internal_replication>
      <replica>
          <host>10.254.3.111</host>
          <port>9000</port>
      </replica>
  </shard>
  <shard>
      <internal_replication>false</internal_replication>
      <replica>
          <host>10.254.3.112</host>
          <port>9000</port>
      </replica>
  </shard>
  <shard>
      <internal_replication>false</internal_replication>
      <replica>
          <host>10.254.3.113</host>
          <port>9000</port>
          </replica>
  </shard>
  <shard>
      <internal_replication>false</internal_replication>
      <replica>
          <host>10.254.3.114</host>
          <port>9000</port>
      </replica>
  </shard>
</per_host_allnodes>
```

**Важно:**
- `internal_replication` указывается внутри каждого `<shard>`
- Для PROD добавлять новые шарды по мере роста

### 2.2. Проверка

```sql
SELECT cluster, shard_num, replica_num, host_name
FROM system.clusters
WHERE cluster IN ('per_host_allnodes', 'shardless', 'core')
ORDER BY cluster, shard_num, replica_num;
```

---

## 3. Создание базы данных

```sql
CREATE DATABASE IF NOT EXISTS kafka ON CLUSTER per_host_allnodes;
CREATE DATABASE IF NOT EXISTS kafka ON CLUSTER shardless;
```

---

## 4. Таблицы Kafka → ClickHouse

Для каждого топика Kafka создаётся набор из 7 объектов:

| Кластер | Объект | Назначение |
|---------|--------|------------|
| `per_host_allnodes` | `tbl_*_raw` | RAW-таблица (MergeTree, TTL 60d) |
| `per_host_allnodes` | `tbl_*_kafka` | Kafka-источник (ENGINE=Kafka) |
| `per_host_allnodes` | `mv_*_to_raw` | MV: Kafka → RAW |
| `shardless` | `tbl_*_raw_all` | Distributed по RAW |
| `shardless` | `tbl_*_repl` | Витрина (ReplicatedReplacingMergeTree, TTL 180d) |
| `shardless` | `tbl_*_repl_all` | Distributed по витрине (для аналитики) |
| `shardless` | `mv_*_to_repl` | MV: RAW → витрина |

### Шаблон для новых таблиц

Скопируйте раздел 4.1 и замените:
- Имя таблицы (`tbl_jti_trace_cis_history` → `tbl_your_table`)
- Топик Kafka (`TBL_JTI_TRACE_CIS_HISTORY` → `YOUR_TOPIC`)
- Consumer group (`ch_tbl_jti_trace_cis_history_qa` → `ch_your_table_qa`)
- Структуру полей

---

### 4.1. TBL_JTI_TRACE_CIS_HISTORY

#### 4.1.1. RAW-таблица (MergeTree + TTL)

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_raw ON CLUSTER per_host_allnodes SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_raw ON CLUSTER per_host_allnodes
(
  -- ключевые поля
  c String,
  t UInt8,
  opd DateTime64(3, 'UTC'),
  _event_ts DateTime64(3, 'UTC'),
  _delete UInt8,
  
  -- бизнес-поля
  id Nullable(String), did Nullable(String), rid Nullable(String), 
  rinn Nullable(String), rn Nullable(String), sid Nullable(String), 
  sinn Nullable(String), sn Nullable(String), gt Nullable(String), 
  prid Nullable(String), st Nullable(UInt8), ste Nullable(UInt8), 
  elr Nullable(UInt8), emd Nullable(DateTime64(3, 'UTC')), 
  apd Nullable(DateTime64(3, 'UTC')), exd Nullable(DateTime64(3, 'UTC')),
  p Nullable(String), pt Nullable(UInt8), o Nullable(String), 
  pn Nullable(String), b Nullable(String), tt Nullable(Int64), 
  tm Nullable(DateTime64(3, 'UTC')), ch Array(String), 
  j Nullable(String), pg Nullable(UInt16), et Nullable(UInt8), 
  pvad Nullable(String), ag Nullable(String),
  
  -- метаданные Kafka
  __kafka_partition Int32,
  __kafka_offset UInt64,
  __kafka_ts DateTime,
  __ingest_ts DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(__ingest_ts)
ORDER BY (c, t, opd, __kafka_partition, __kafka_offset)
TTL __ingest_ts + INTERVAL 60 DAY DELETE
SETTINGS index_granularity = 8192;
```

**Особенности:**
- TTL по `__ingest_ts` (время приёма в CH), не по бизнес-времени
- Партиционирование по месяцам устойчиво к «старым» событиям
- ORDER BY включает Kafka-метаданные для быстрого аудита
- TTL RAW (60d) < TTL витрины (180d)

#### 4.1.2. Kafka-источник (AvroConfluent)

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
(
  c String, t Int32, opd Int64,
  id Nullable(String), did Nullable(String), rid Nullable(String), 
  rinn Nullable(String), rn Nullable(String), sid Nullable(String), 
  sinn Nullable(String), sn Nullable(String), gt Nullable(String), 
  prid Nullable(String), st Nullable(Int32), ste Nullable(Int32), 
  elr Nullable(Int32), emd Nullable(Int64), apd Nullable(Int64), 
  exd Nullable(Int64), p Nullable(String), pt Nullable(Int32), 
  o Nullable(String), pn Nullable(String), b Nullable(String), 
  tt Nullable(Int64), tm Nullable(Int64), ch Array(String), 
  j Nullable(String), pg Nullable(Int32), et Nullable(Int32), 
  pvad Nullable(String), ag Nullable(String),
  _event_ts Nullable(Int64),
  _delete UInt8
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
  kafka_topic_list = 'TBL_JTI_TRACE_CIS_HISTORY',
  kafka_group_name = 'ch_tbl_jti_trace_cis_history_qa',
  kafka_auto_offset_reset = 'earliest',
  kafka_format = 'AvroConfluent',
  format_avro_schema_registry_url = 'http://10.254.3.111:8081',  -- рекомендуется использовать общий LB, не IP ноды
  kafka_commit_on_select = 0,
  kafka_skip_broken_messages = 0,
  input_format_avro_allow_missing_fields = 1,
  input_format_avro_null_as_default = 1,
  kafka_num_consumers = 1,
  kafka_max_block_size = 10000;
```

**Параметры надёжности:**
- `kafka_commit_on_select = 0` — коммит только после MV
- `kafka_skip_broken_messages = 0` — не пропускать битые сообщения
- `kafka_auto_offset_reset = 'earliest'` — читать с начала

#### 4.1.3. Materialized View (Kafka → RAW)

```sql
DROP TABLE IF EXISTS kafka.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER per_host_allnodes SYNC;

CREATE MATERIALIZED VIEW kafka.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER per_host_allnodes
TO kafka.tbl_jti_trace_cis_history_raw
AS
SELECT
  c,
  CAST(t AS UInt8) AS t,
  toDateTime64(opd / 1000.0, 3, 'UTC') AS opd,
  ifNull(toDateTime64(_event_ts / 1000.0, 3, 'UTC'), NULL) AS _event_ts,
  _delete,
  id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
  CAST(st AS Nullable(UInt8)) AS st,
  CAST(ste AS Nullable(UInt8)) AS ste,
  CAST(elr AS Nullable(UInt8)) AS elr,
  ifNull(toDateTime64(emd / 1000.0, 3, 'UTC'), NULL) AS emd,
  ifNull(toDateTime64(apd / 1000.0, 3, 'UTC'), NULL) AS apd,
  ifNull(toDateTime64(exd / 1000.0, 3, 'UTC'), NULL) AS exd,
  p, CAST(pt AS Nullable(UInt8)) AS pt, o, pn, b, tt,
  ifNull(toDateTime64(tm / 1000.0, 3, 'UTC'), NULL) AS tm,
  ch, j, CAST(pg AS Nullable(UInt16)) AS pg, 
  CAST(et AS Nullable(UInt8)) AS et, pvad, ag,
  _partition AS __kafka_partition,
  _offset AS __kafka_offset,
  ifNull(_timestamp, now()) AS __kafka_ts,
  now() AS __ingest_ts
FROM kafka.tbl_jti_trace_cis_history_kafka;
-- если _timestamp отсутствует, используется now()
```

**Конверсии:**
- Миллисекунды → DateTime64(3)
- Виртуальные колонки Kafka → физические поля

#### 4.1.4. Distributed для объединения RAW

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_raw_all ON CLUSTER shardless SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_raw_all ON CLUSTER shardless
AS kafka.tbl_jti_trace_cis_history_raw
ENGINE = Distributed('per_host_allnodes', 'kafka', 'tbl_jti_trace_cis_history_raw', __kafka_partition)
SETTINGS prefer_localhost_replica = 1;
```

#### 4.1.5. Реплицированная витрина

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_repl ON CLUSTER shardless SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_repl ON CLUSTER shardless
(
  c String, t UInt8, opd DateTime64(3, 'UTC'),
  _event_ts DateTime64(3, 'UTC'), _delete UInt8,
  id Nullable(String), did Nullable(String), rid Nullable(String), 
  rinn Nullable(String), rn Nullable(String), sid Nullable(String), 
  sinn Nullable(String), sn Nullable(String), gt Nullable(String), 
  prid Nullable(String), st Nullable(UInt8), ste Nullable(UInt8), 
  elr Nullable(UInt8), emd Nullable(DateTime64(3, 'UTC')), 
  apd Nullable(DateTime64(3, 'UTC')), exd Nullable(DateTime64(3, 'UTC')),
  p Nullable(String), pt Nullable(UInt8), o Nullable(String), 
  pn Nullable(String), b Nullable(String), tt Nullable(Int64), 
  tm Nullable(DateTime64(3, 'UTC')), ch Array(String), 
  j Nullable(String), pg Nullable(UInt16), et Nullable(UInt8), 
  pvad Nullable(String), ag Nullable(String),
  __kafka_partition Int32,
  __kafka_offset UInt64,
  __kafka_ts DateTime,
  ver DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/shardless/kafka.tbl_jti_trace_cis_history_repl',
    '{shardless_repl}',
    ver
)
PARTITION BY toYYYYMM(opd)
ORDER BY (__kafka_partition, __kafka_offset, c, t, opd)
TTL toDateTime(coalesce(_event_ts, __kafka_ts)) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;
```

**Особенности:**
- ReplacingMergeTree — дедупликация по `(__kafka_partition, __kafka_offset)`
- TTL по бизнес-времени с fallback на Kafka timestamp
- ZK-путь `/clickhouse/tables/shardless/...`
- Каждая реплика в кластере shardless должна иметь уникальный макрос {shardless_repl} в macros.xml
- Дедупликация основана на (__kafka_partition, __kafka_offset), повторные заливки не создают дублей

#### 4.1.6. Distributed для аналитики

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_repl_all ON CLUSTER shardless SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_repl_all ON CLUSTER shardless
AS kafka.tbl_jti_trace_cis_history_repl
ENGINE = Distributed('shardless', 'kafka', 'tbl_jti_trace_cis_history_repl');
```

**Важно:** Для кластера с 1 шардом ключ шардирования не нужен.

#### 4.1.7. Materialized View (RAW → витрина)

```sql
DROP TABLE IF EXISTS kafka.mv_tbl_jti_trace_cis_history_to_repl ON CLUSTER shardless SYNC;

CREATE MATERIALIZED VIEW kafka.mv_tbl_jti_trace_cis_history_to_repl ON CLUSTER shardless
TO kafka.tbl_jti_trace_cis_history_repl
AS
SELECT
    c, t, opd, _event_ts, _delete,
    id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
    st, ste, elr, emd, apd, exd, p, pt, o, pn, b, tt, tm,
    ch, j, pg, et, pvad, ag,
    __kafka_partition, __kafka_offset, __kafka_ts,
    now() AS ver
FROM kafka.tbl_jti_trace_cis_history_raw_all;
```

**Примечание:** При перезапусках возможны повторные вставки — ReplacingMergeTree устраняет дубли.

---

### 4.2. TBL_JTI_TRACE_DOCUMENT_ERRORS

_Раздел для второй таблицы — добавить аналогичные DDL по шаблону из 4.1_

#### 4.2.1. RAW-таблица (MergeTree + TTL)
```sql
-- DROP TABLE IF EXISTS kafka.tbl_jti_trace_document_errors_raw ON CLUSTER per_host_allnodes SYNC;
-- CREATE TABLE kafka.tbl_jti_trace_document_errors_raw ...
-- TODO: добавить DDL
```

#### 4.2.2. Kafka-источник
```sql
-- TODO: добавить DDL
```

#### 4.2.3. Materialized View (Kafka → RAW)
```sql
-- TODO: добавить DDL
```

#### 4.2.4. Distributed для объединения RAW
```sql
-- TODO: добавить DDL
```

#### 4.2.5. Реплицированная витрина
```sql
-- TODO: добавить DDL
```

#### 4.2.6. Distributed для аналитики
```sql
-- TODO: добавить DDL
```

#### 4.2.7. Materialized View (RAW → витрина)
```sql
-- TODO: добавить DDL
```

---

### 4.3. [Добавить следующую таблицу]

_Используйте раздел 4.1 как шаблон для новых таблиц_

---

## 5. Управление ingestion

### 5.1. Пауза и возобновление

**Пауза (оффсеты НЕ коммитятся):**
```sql
ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
  MODIFY SETTING kafka_num_consumers = 0;
```

**Возобновление:**
```sql
ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
  MODIFY SETTING kafka_num_consumers = 1;
```

**ВАЖНО:** Не менять `kafka_group_name` без плана миграции!

### 5.2. Проверка здоровья

```sql
-- Активные консьюмеры
SELECT hostName(), consumer_id, is_currently_used
FROM system.kafka_consumers
WHERE database = 'kafka'
  AND table = 'tbl_jti_trace_cis_history_kafka'
ORDER BY hostName();

-- Данные в RAW по партициям
SELECT __kafka_partition, count() AS rows
FROM kafka.tbl_jti_trace_cis_history_raw
GROUP BY __kafka_partition
ORDER BY __kafka_partition;

-- Ошибки DDL
SELECT * FROM system.distributed_ddl_queue WHERE exception_code != 0;

-- Состояние реплик
SELECT database, table, is_readonly, absolute_delay, queue_size
FROM system.replicas
WHERE database = 'kafka' AND table LIKE 'tbl_jti_trace_cis_history_repl%';
```

### 5.3. Аудит оффсетов и целостности

**Быстрая проверка дыр:**
```sql
SELECT __kafka_partition, missing_offsets
FROM (
  SELECT __kafka_partition,
         (max(__kafka_offset) - min(__kafka_offset) + 1) - countDistinct(__kafka_offset) AS missing_offsets
  FROM kafka.tbl_jti_trace_cis_history_raw
  GROUP BY __kafka_partition
) WHERE missing_offsets > 0
ORDER BY missing_offsets DESC;
```

**Полный аудит:**
```sql
SELECT
  __kafka_partition,
  min(__kafka_offset) AS min_offset,
  max(__kafka_offset) AS max_offset,
  countDistinct(__kafka_offset) AS uniq_offsets,
  (max(__kafka_offset) - min(__kafka_offset) + 1) AS expected_span,
  (max(__kafka_offset) - min(__kafka_offset) + 1) - countDistinct(__kafka_offset) AS missing_offsets,
  count() - countDistinct(__kafka_offset) AS duplicate_offsets
FROM kafka.tbl_jti_trace_cis_history_raw
GROUP BY __kafka_partition
ORDER BY __kafka_partition;
```

**Интерпретация:**
- `missing_offsets > 0` — есть дыры (не дочитано или потеряно)
- `duplicate_offsets > 0` — повторные вставки (ReplacingMergeTree устранит)

**Скорость ingestion (15 мин):**
```sql
SELECT __kafka_partition,
       max(__kafka_offset) - min(__kafka_offset) AS delta_offsets
FROM kafka.tbl_jti_trace_cis_history_raw
WHERE __ingest_ts >= now() - INTERVAL 15 MINUTE
GROUP BY __kafka_partition
ORDER BY delta_offsets DESC;
```

---

## 6. Тюнинг производительности

### 6.1. Параметры Kafka

```sql
-- Увеличить throughput (подбирать под SLA):
ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
  MODIFY SETTING kafka_max_block_size = 20000;

-- Увеличить консьюмеров (при наличии CPU):
ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
  MODIFY SETTING kafka_num_consumers = 2;

-- Включить многопоточность (один поток на консьюмера):
ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
  MODIFY SETTING kafka_thread_per_consumer = 1;
```

### 6.2. Оптимизация типов данных

```sql
-- LowCardinality для полей с низкой кардинальностью
ALTER TABLE kafka.tbl_jti_trace_cis_history_raw ON CLUSTER per_host_allnodes
  MODIFY COLUMN c LowCardinality(String),
  MODIFY COLUMN t LowCardinality(UInt8),
  MODIFY COLUMN st LowCardinality(Nullable(UInt8)),
  MODIFY COLUMN pt LowCardinality(Nullable(UInt8));

-- Data skipping index
ALTER TABLE kafka.tbl_jti_trace_cis_history_raw ON CLUSTER per_host_allnodes
  ADD INDEX IF NOT EXISTS idx_delete (_delete) TYPE set(0) GRANULARITY 16;
```

### 6.3. Принудительная дедупликация

```sql
-- ВНИМАНИЕ: высокая нагрузка на диск!
OPTIMIZE TABLE kafka.tbl_jti_trace_cis_history_repl FINAL;
```

### 6.4. Мониторинг нагрузки

```sql
-- Активные парты
SELECT count() AS active_parts
FROM system.parts
WHERE database = 'kafka' 
  AND table = 'tbl_jti_trace_cis_history_raw' 
  AND active;

-- Размер таблиц
SELECT
    database,
    table,
    formatReadableSize(sum(bytes)) AS size,
    sum(rows) AS rows,
    count() AS parts
FROM system.parts
WHERE database = 'kafka' AND active
GROUP BY database, table
ORDER BY sum(bytes) DESC;
```

---

## 7. Мониторинг и алерты

### 7.1. Критичные метрики (Prometheus/Grafana)

| Метрика | Запрос | Alert |
|---------|--------|-------|
| **Нет консьюмеров** | `SELECT count() FROM system.kafka_consumers WHERE database='kafka' AND table='tbl_jti_trace_cis_history_kafka' AND is_currently_used=1` | `== 0` |
| **RAW не растёт** | `SELECT now() - max(__ingest_ts) FROM kafka.tbl_jti_trace_cis_history_raw` | `> 300s` |
| **Дыры в оффсетах** | См. раздел 5.3 | `> 0` |
| **Витрина readonly** | `SELECT count() FROM system.replicas WHERE database='kafka' AND table='tbl_jti_trace_cis_history_repl' AND is_readonly=1` | `> 0` |
| **Lag репликации** | `SELECT max(absolute_delay) FROM system.replicas WHERE database='kafka' AND table='tbl_jti_trace_cis_history_repl'` | `> 300s` |
| **DDL ошибки** | `SELECT count() FROM system.distributed_ddl_queue WHERE exception_code!=0` | `> 0` |

### 7.2. Логи и ошибки (Loki/Promtail)

Фильтровать по:
- `AvroConfluent`
- `schema registry`
- `tbl_jti_trace_cis_history_kafka`
- level: `ERROR`, `EXCEPTION`

---

## 8. Деплой QA → PROD

### 8.1. Чек-лист перед релизом

1. ✅ Зафиксировать текущий `kafka_group_name` (vN)
2. ✅ Проверить отсутствие дыр в RAW (`missing_offsets == 0`)
3. ✅ Снять снапшот метрик (оффсеты, throughput)
4. ✅ Проверить активных консьюмеров
5. ✅ Проверить ZK-доступность и пути
6. ✅ На QA применить новую Avro-схему
7. ✅ Запустить новую группу (vN+1) с `earliest`
8. ✅ Сверить полноту между группами
9. ✅ Перенести на PROD
10. ✅ Обновить runbook (версии, SHA)

### 8.2. Пошаговый сценарий

```bash
# 1. Настроить кластеры (см. раздел 2)
# 2. Создать БД
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS kafka ON CLUSTER per_host_allnodes"
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS kafka ON CLUSTER shardless"

# 3. Создать таблицы ingestion-слоя (см. раздел 4)
# 4. Создать витрины (см. раздел 4.1.5–4.1.7)
# 5. Проверить здоровье (см. раздел 5.2)
# 6. Запустить мониторинг (см. раздел 7)
```

### 8.3. Rollback

```sql
-- Отключить новую группу
ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
  MODIFY SETTING kafka_num_consumers = 0;

-- Вернуть старую группу (если ещё активна)
-- Проверить оффсеты через kafka-consumer-groups.sh
```

---

## 9. DR-сценарии

### 9.1. Падение ingestion-ноды

1. Проверить консьюмеров (см. 6.2)
2. Kafka автоматически перераспределит партиции
3. После восстановления: проверить конфиги, `system.clusters`
4. Оффсеты не трогать руками

### 9.2. Падение нескольких ingestion-нод

1. Временно увеличить `kafka_num_consumers`
2. Проверить диапазоны оффсетов
3. После нормализации вернуть исходное значение

### 9.3. Падение реплики shardless

1. Проверить статус: `SELECT * FROM system.replicas WHERE database='kafka'`
2. Должна быть хотя бы одна реплика с `is_leader=1`
3. После восстановления данные синхронизируются автоматически
4. ZK и таблицы руками не трогать

### 9.4. Недоступен Schema Registry

1. Проверить: `curl -sf http://<sr>:8081/subjects`
2. Ingestion остановится, но оффсеты сохранятся
3. После восстановления SR: ingestion продолжится с последнего коммита
4. **НЕ МЕНЯТЬ** `kafka_group_name` и не удалять таблицы

### 9.5. Ошибочная смена kafka_group_name

1. **НЕ МЕНЯТЬ** без миграционного плана
2. Если случайно сменили:
   - Зафиксировать оффсеты через `kafka-consumer-groups.sh`
   - Оценить разницу
   - Временно дочитать пропущенное в отдельную таблицу

**Правило:** Сначала снимать срезы, потом действовать!

---

## 10. Быстрые команды

```bash
# Состояние консьюмеров
clickhouse-client --query "SELECT __kafka_partition, max(__kafka_offset) last FROM kafka.tbl_jti_trace_cis_history_raw GROUP BY __kafka_partition ORDER BY __kafka_partition" | column -t

# Пауза ingestion
clickhouse-client --query "ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes MODIFY SETTING kafka_num_consumers=0"

# Возобновление
clickhouse-client --query "ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes MODIFY SETTING kafka_num_consumers=1"

# Аудит пропусков
clickhouse-client --query "SELECT __kafka_partition,(max(__kafka_offset)-min(__kafka_offset)+1)-countDistinct(__kafka_offset) missing FROM kafka.tbl_jti_trace_cis_history_raw GROUP BY __kafka_partition HAVING missing>0 ORDER BY missing DESC" | column -t

# Размер таблиц
clickhouse-client --query "SELECT table, formatReadableSize(sum(bytes)) size, sum(rows) rows FROM system.parts WHERE database='kafka' AND active GROUP BY table"
```

---

## Приложения

### Приложение A: Карта кластеров и ZK-путей

| Слой | Кластер | Топология | internal_replication | Макросы | ZK-пути | Движки |
|------|---------|-----------|----------------------|---------|---------|--------|
| Исторический | `core` | 2×2 | `true` | `{shard}`, `{replica}` | `/clickhouse/tables/{shard}/...` | ReplicatedMergeTree |
| Аналитика | `shardless` | 1×N | `true` | `{shardless_repl}` | `/clickhouse/tables/shardless/...` | ReplicatedReplacingMergeTree |
| Ingestion | `per_host_allnodes` | N×1 | `false` | — | — | MergeTree, Kafka |

**Проверки:**
1. `macros.xml` соответствует роли кластера
2. `system.clusters` идентичен на всех нодах
3. ZK-пути не пересекаются
4. Новые таблицы создаются только в согласованных префиксах

### Приложение B: Schema Registry без аутентификации

**Особенности работы с SR в ClickHouse 24.8:**
- Указывать FQDN/VIP балансировщика, не IP одной ноды
- ClickHouse не поддерживает список SR-хостов
- Отказоустойчивость обеспечивается на уровне LB
- При недоступности SR: ingestion останавливается, оффсеты сохраняются
- После восстановления SR: чтение продолжается с последнего коммита
- Длительные простои SR могут вызвать пересоздание consumer-сессий (данные не теряются)

**Операционные проверки:**
```bash
# Доступность SR
curl -sf http://<sr>:8081/subjects

# Активные консьюмеры
clickhouse-client --query "SELECT * FROM system.kafka_consumers WHERE is_currently_used=1"

# Ошибки в логах
grep -i "avroconfluent\|schema registry" /var/log/clickhouse-server/clickhouse-server.err.log
```

---

**Конец документа**
