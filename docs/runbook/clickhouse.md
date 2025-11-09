## Содержание
- [Подготовка ClickHouse](#подготовка-clickhouse)
- [1. Настройка кластера per_host_allnodes](#1-настройка-кластера-per_host_allnodes)
- [2. Общий каталог для всех таблиц из Kafka](#2-общий-каталог-для-всех-таблиц-из-kafka)
- [3. TBL_JTI_TRACE_CIS_HISTORY](#3-tbl_jti_trace_cis_history)
  - [3.1 RAW приёмник (локально на каждом узле) + TTL по __ingest_ts](#31-raw-приёмник-локально-на-каждом-узле--ttl-по-__ingest_ts)
  - [3.2 Источник: Kafka (AvroConfluent + SR), по 1 консьюмеру на узел](#32-источник-kafka-avroconfluent--sr-по-1-консьюмеру-на-узел)
  - [3.3 Материализованное представление (конверсия мс → DateTime64)](#33-материализованное-представление-конверсия-мс--datetime64)
  - [3.4 «Собиратель» для запросов со всех узлов](#34-собиратель-для-запросов-со-всех-узлов)
  - [3.5 Устойчивая (реплицированная) витрина для аналитики](#35-устойчивая-реплицированная-витрина-для-аналитики)
  - [3.6 Контроль целостности и оффсетов](#36-контроль-целостности-и-оффсетов)
  - [3.7 Продвинутый аудит оффсетов и целостности RAW](#37-продвинутый-аудит-оффсетов-и-целостности-raw)
  - [3.8 Тюнинг производительности ingestion](#38-тюнинг-производительности-ingestion)
- [4. Чек-лист релиза и перехода схемы (QA → PROD)](#4-чек-лист-релиза-и-перехода-схемы-qa--prod)
- [5. Быстрые команды для оператора](#5-быстрые-команды-для-оператора)
- [6. Пошаговый боевой сценарий (QA → PROD)](#6-пошаговый-боевой-сценарий-qa--prod)
- [7. High Availability и восстановление после сбоя](#7-high-availability-и-восстановление-после-сбоя)

# Подготовка ClickHouse

Документ описывает рабочие процедуры для ClickHouse 24.8.14.39. Инструкции по настройке кластера с отключенной репликацией для чтения из каждой ноды кластера Kafka и DDL по созданию таблиц для чтения данных из топиков Kafka.

Архитектура состоит из двух логических слоёв:

1. Ingestion-слой (`per_host_allnodes`) — каждый хост отдельный шард, без внутренней репликации. Здесь:
   - Kafka-источники (`ENGINE = Kafka`);
   - локальные RAW-таблицы (`MergeTree`) с TTL;
   - служебные Distributed-таблицы для чтения со всех узлов.
2. Надёжный слой (`shardless`) — 1 шард × N реплик с `internal_replication = true`. Здесь:
   - реплицированные таблицы (`Replicated(Replacing)MergeTree`);
   - финальные витрины для аналитики.

Ingestion отвечает за скорость и распараллеливание чтения из Kafka, надёжный слой — за долговременное хранение и отказоустойчивость.

## 1. Настройка кластера "per_host_allnodes"

- К существующему кластеру *shardless* добавить конфигурацию кластера *per_host_allnodes* по примеру *QA*
/etc/clickhouse-server/config.d/clickhouse_remote_servers.xml:
```
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

**Важно:** `internal_replication` указывается внутри каждого `<shard>`. На уровне всего кластера (`<per_host_allnodes>`) этот элемент недопустим. Для PROD добавляем новые `<shard>` по мере роста, не меняя существующие.

## 2. Общий каталог для всех таблиц из Kafka

```sql
CREATE DATABASE IF NOT EXISTS kafka ON CLUSTER per_host_allnodes;
```

## 3. TBL_JTI_TRACE_CIS_HISTORY

### 3.1 RAW приёмник (локально на каждом узле) + TTL по __ingest_ts

Сохраним служебные поля из Kafka в RAW для диагностики: __kafka_partition, __kafka_offset, __kafka_ts. Для устойчивого хранения TTL считаем по времени приёма строки в ClickHouse: __ingest_ts.

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_raw ON CLUSTER per_host_allnodes SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_raw ON CLUSTER per_host_allnodes
(
  -- ключевые поля
  c String,
  t UInt8,
  opd DateTime64(3, 'UTC'),

  -- бизнес-время события (может использоваться в витринах/аналитике)
  _event_ts DateTime64(3, 'UTC'),

  -- логический флаг удаления
  _delete UInt8,

  -- бизнес-поля
  id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
  sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),
  st Nullable(UInt8), ste Nullable(UInt8), elr Nullable(UInt8),
  emd Nullable(DateTime64(3, 'UTC')), apd Nullable(DateTime64(3, 'UTC')), exd Nullable(DateTime64(3, 'UTC')),
  p Nullable(String), pt Nullable(UInt8), o Nullable(String), pn Nullable(String), b Nullable(String),
  tt Nullable(Int64), tm Nullable(DateTime64(3, 'UTC')),
  ch Array(String), j Nullable(String), pg Nullable(UInt16), et Nullable(UInt8), pvad Nullable(String), ag Nullable(String),

  -- метаданные Kafka (для аудита/сверок)
  __kafka_partition Int32,
  __kafka_offset    UInt64,
  __kafka_ts        DateTime,   -- Kafka timestamp: преобразуем из _timestamp (мс → сек)

  -- техническое время приёма строки ClickHouse (используется для TTL)
  __ingest_ts       DateTime
)
ENGINE = MergeTree
ORDER BY (c, t, opd)
TTL __ingest_ts + INTERVAL 60 DAY DELETE            -- ВАЖНО: TTL принимает Date/DateTime
SETTINGS index_granularity = 8192;
```

TTL в RAW считаем по __ingest_ts (факт приёма строки в ClickHouse). Значение выбирается с запасом относительно максимальной задержки ETL (включая аварийные простои). Рекомендуется согласовать TTL с Kafka retention по топику: минимальное из двух значений определяет горизонт, после которого восстановление пропущенных данных из Kafka или RAW будет невозможно.

### 3.2 Источник: Kafka (AvroConfluent + SR), по 1 консьюмеру на узел

Почему 1? У нас 4 узла → 4 консьюмера в группе. Для 12 партиций Kafka этого достаточно; при росте нагрузки можно увеличить kafka_num_consumers.

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
(
  c String, t Int32, opd Int64,
  id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
  sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),
  st Nullable(Int32), ste Nullable(Int32), elr Nullable(Int32),
  emd Nullable(Int64), apd Nullable(Int64), exd Nullable(Int64),
  p Nullable(String), pt Nullable(Int32), o Nullable(String), pn Nullable(String), b Nullable(String),
  tt Nullable(Int64), tm Nullable(Int64),
  ch Array(String), j Nullable(String), pg Nullable(Int32), et Nullable(Int32), pvad Nullable(String), ag Nullable(String),
  _event_ts Nullable(Int64),
  _delete   UInt8
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
  kafka_topic_list  = 'TBL_JTI_TRACE_CIS_HISTORY',
  kafka_group_name  = 'ch_tbl_jti_trace_cis_history_qa',  -- новое имя группы; стартуем с начала истории вместе с kafka_auto_offset_reset='earliest'
  kafka_auto_offset_reset = 'earliest',
  kafka_format      = 'AvroConfluent',
  format_avro_schema_registry_url = 'http://10.254.3.111:8081',  -- в 24.8 ИМЕННО так называется параметр
  -- надёжность коммита оффсетов
  -- оффсеты коммитятся только после успешной обработки батча
  kafka_commit_every_batch = 1,
  -- не пропускать битые сообщения: при ошибке десериализации ingestion останавливается
  kafka_skip_broken_messages = 0,
  -- эволюция схем Avro (без падения ingest при добавлении nullable полей)
  input_format_avro_allow_missing_fields = 1,
  input_format_avro_null_as_default = 1,
  -- производительность (подбирайте под нагрузку и CPU)
  -- рекомендуется kafka_max_block_size 10k–50k в зависимости от SLA и CPU
  kafka_num_consumers  = 1,
  kafka_max_block_size = 10000;
```

Для надёжности:
- `kafka_auto_offset_reset = 'earliest'` гарантирует чтение всей истории при первом запуске новой группы.
- `kafka_commit_every_batch = 1` обеспечивает at-least-once доставку.
- При ошибках десериализации явно задано `kafka_skip_broken_messages = 0` — ingestion останавливается, не теряя события.

Совместимость и семантика (24.8.14.39):
- `kafka_format='AvroConfluent'` и `format_avro_schema_registry_url` поддерживаются.
- Коммит оффсетов выполняется только при наличии MATERIALIZED VIEW, читающего из Kafka-таблицы (рекомендуемая связка: Kafka → MV → RAW).
- Прямые `SELECT` из Kafka-таблиц в проде запрещены (могут нарушить семантику чтения и коммиты оффсетов).

### 3.3 Материализованное представление (конверсия мс → DateTime64)

```sql
DROP TABLE IF EXISTS kafka.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER per_host_allnodes SYNC;

CREATE MATERIALIZED VIEW kafka.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER per_host_allnodes
TO kafka.tbl_jti_trace_cis_history_raw
AS
SELECT
  c,
  CAST(t AS UInt8)                                   AS t,
  toDateTime64(opd / 1000.0, 3, 'UTC')               AS opd,

  -- важные времена
  ifNull(toDateTime64(_event_ts / 1000.0, 3, 'UTC'), NULL) AS _event_ts,

  _delete,

  id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
  CAST(st  AS Nullable(UInt8))  AS st,
  CAST(ste AS Nullable(UInt8))  AS ste,
  CAST(elr AS Nullable(UInt8))  AS elr,

  ifNull(toDateTime64(emd / 1000.0, 3, 'UTC'), NULL) AS emd,
  ifNull(toDateTime64(apd / 1000.0, 3, 'UTC'), NULL) AS apd,
  ifNull(toDateTime64(exd / 1000.0, 3, 'UTC'), NULL) AS exd,
  p, CAST(pt AS Nullable(UInt8)) AS pt, o, pn, b, tt,
  ifNull(toDateTime64(tm  / 1000.0, 3, 'UTC'), NULL) AS tm,
  ch, j, CAST(pg AS Nullable(UInt16)) AS pg, CAST(et AS Nullable(UInt8)) AS et, pvad, ag,

  -- метаданные Kafka из виртуальных колонок
  _partition AS __kafka_partition,
  _offset    AS __kafka_offset,
  toDateTime(_timestamp / 1000) AS __kafka_ts,  -- _timestamp в Kafka в миллисекундах → секунды
  now() AS __ingest_ts
FROM kafka.tbl_jti_trace_cis_history_kafka;
```

#### 3.2.1 Пауза/возобновление ingestion (без потери данных)

Для безопасной паузы чтения из Kafka (оффсеты не коммитятся, сообщения остаются в брокере):

```sql
ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
  MODIFY SETTING kafka_num_consumers = 0;
```

Для возобновления:

```sql
ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
  MODIFY SETTING kafka_num_consumers = 1;
```

### 3.4 «Собиратель» для запросов со всех узлов

Эта таблица создаётся на надёжном кластере shardless и объединяет данные со всех ingestion-нод (per_host_allnodes). 
Используется для аналитики и для передачи данных в реплицированные витрины.

```sql
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_raw_all ON CLUSTER shardless SYNC;

CREATE TABLE IF NOT EXISTS kafka.tbl_jti_trace_cis_history_raw_all ON CLUSTER shardless
AS kafka.tbl_jti_trace_cis_history_raw
ENGINE = Distributed(
    'per_host_allnodes',
    'kafka',
    'tbl_jti_trace_cis_history_raw',
    __kafka_partition
);
```

### 3.5 Устойчивая (реплицированная) витрина для аналитики

Реплицированная таблица — это основной источник правды (single source of truth) для аналитики.
Сюда попадают данные после успешного ingestion и валидации.

```sql
-- TTL использует _event_ts (бизнес-время события).
-- Важно: _event_ts должен быть корректно заполнен в RAW/MV, иначе строки могут удаляться некорректно.
```

```sql
-- 3.5.1 Реплицированное хранилище на кластере shardless

-- Используем ReplacingMergeTree по полю ver для устранения дубликатов при повторных вставках.

-- ВАЖНО:
-- 1) Этот DDL выполняется ON CLUSTER shardless.
-- 2) Пути в ZooKeeper /clickhouse/tables/{shard}/... должны соответствовать macros.xml.
-- 3) TTL использует toDateTime(_event_ts), т.к. _event_ts хранится в DateTime64(3).

DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_repl ON CLUSTER shardless SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_repl ON CLUSTER shardless
(
  -- бизнес-данные (та же структура, что в RAW)
  c String,
  t UInt8,
  opd DateTime64(3, 'UTC'),
  _event_ts DateTime64(3, 'UTC'),
  _delete UInt8,
  id Nullable(String), did Nullable(String), rid Nullable(String), rinn Nullable(String), rn Nullable(String),
  sid Nullable(String), sinn Nullable(String), sn Nullable(String), gt Nullable(String), prid Nullable(String),
  st Nullable(UInt8), ste Nullable(UInt8), elr Nullable(UInt8),
  emd Nullable(DateTime64(3, 'UTC')), apd Nullable(DateTime64(3, 'UTC')), exd Nullable(DateTime64(3, 'UTC')),
  p Nullable(String), pt Nullable(UInt8), o Nullable(String), pn Nullable(String), b Nullable(String),
  tt Nullable(Int64), tm Nullable(DateTime64(3, 'UTC')),
  ch Array(String), j Nullable(String), pg Nullable(UInt16), et Nullable(UInt8), pvad Nullable(String), ag Nullable(String),

  -- служебные поля из Kafka
  __kafka_partition Int32,
  __kafka_offset    UInt64,
  __kafka_ts        DateTime,

  -- версия для ReplacingMergeTree (устранение дубликатов)
  ver DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/kafka.tbl_jti_trace_cis_history_repl',
    '{replica}',
    ver
)
PARTITION BY toYYYYMM(opd)
ORDER BY (c, t, opd, __kafka_partition, __kafka_offset)
TTL toDateTime(_event_ts) + INTERVAL 180 DAY DELETE;

-- 3.5.2 Distributed-таблица для аналитики

DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_repl_all ON CLUSTER shardless SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_repl_all ON CLUSTER shardless
AS kafka.tbl_jti_trace_cis_history_repl
ENGINE = Distributed(
    'shardless',
    'kafka',
    'tbl_jti_trace_cis_history_repl',
    cityHash64(c)
);
```

Выбор ключа `(c, t, opd, __kafka_partition, __kafka_offset)` и ReplacingMergeTree устраняет дубли по оффсетам: при повторной вставке с тем же `(__kafka_partition, __kafka_offset)` останется запись с наибольшей `ver`. Для строгих проверок используйте `FINAL` или агрегаты по ключу.

-- 3.5.3 Материализованное представление, переносящее данные из ingestion-слоя

-- Читает из распределённого RAW (per_host_allnodes) и пишет в реплицированное хранилище.
-- Выполняется ON CLUSTER shardless с использованием Distributed-таблицы сверху ingestion-слоя.
-- Предполагается, что kafka.tbl_jti_trace_cis_history_raw_all уже создан на кластере shardless как Distributed-таблица поверх per_host_allnodes (см. раздел 3.4).

```sql
DROP TABLE IF EXISTS kafka.mv_tbl_jti_trace_cis_history_to_repl ON CLUSTER shardless SYNC;

CREATE MATERIALIZED VIEW kafka.mv_tbl_jti_trace_cis_history_to_repl
ON CLUSTER shardless
TO kafka.tbl_jti_trace_cis_history_repl
AS
SELECT
    c, t, opd, _event_ts, _delete,
    id, did, rid, rinn, rn,
    sid, sinn, sn, gt, prid,
    st, ste, elr,
    emd, apd, exd,
    p, pt, o, pn, b,
    tt, tm,
    ch, j, pg, et, pvad, ag,
    __kafka_partition,
    __kafka_offset,
    __kafka_ts,
    now() AS ver
FROM kafka.tbl_jti_trace_cis_history_raw_all;
```

Этот материализованный вид обеспечивает автоматический перенос данных из ingestion-слоя в надёжный слой.
Благодаря ReplacingMergeTree обеспечивается идемпотентность вставок: повторные данные по одинаковым оффсетам заменяются без дубликатов.

Так как MV на `shardless` читает из Distributed-таблицы ingestion-слоя, возможны повторные вставки одних и тех же строк при перебоях сети. Это допустимо: витрина на ReplacingMergeTree устраняет дубликаты по `(__kafka_partition, __kafka_offset)`.

### 3.6 Контроль целостности и оффсетов

- Состояние консьюмеров (на ingestion-слое):

```sql
SELECT *
FROM system.kafka_consumers
WHERE database = 'kafka' AND table = 'tbl_jti_trace_cis_history_kafka' AND is_currently_used = 1;
```

Пример:
```sql
SELECT hostName() AS host, consumer_id, is_currently_used
FROM system.kafka_consumers
WHERE database = 'kafka'
  AND table = 'tbl_jti_trace_cis_history_kafka'
ORDER BY host;
```

- Дубликаты и «дырки» по оффсетам (RAW, по партициям):

```sql
SELECT __kafka_partition,
       count() AS rows,
       countDistinct(__kafka_offset) AS uniq
FROM kafka.tbl_jti_trace_cis_history_raw
GROUP BY __kafka_partition
ORDER BY __kafka_partition;
```

- Очередь распределённых DDL:

```sql
SELECT *
FROM system.distributed_ddl_queue
WHERE exception_code != 0;
```

### 3.7 Продвинутый аудит оффсетов и целостности RAW

Периодический (например, раз в час) аудит полноты и отсутствия пропусков (__kafka_offset) по каждой партиции. Формулы опираются на то, что оффсеты возрастают на 1 без дыр при отсутствии потерь.

1. Сводка диапазонов и пропусков:

```sql
SELECT __kafka_partition,
     min(__kafka_offset) AS min_offset,
     max(__kafka_offset) AS max_offset,
     countDistinct(__kafka_offset) AS uniq_offsets,
     (max_offset - min_offset + 1) AS expected_span,
     expected_span - uniq_offsets AS missing_offsets,
     uniq_offsets - count() AS duplicate_offsets
FROM kafka.tbl_jti_trace_cis_history_raw
GROUP BY __kafka_partition
ORDER BY __kafka_partition;
```

При обнаружении missing_offsets сверяемся с `kafka-consumer-groups.sh --describe` для той же группы; если Kafka уже удалил эти оффсеты, восстановление возможно только из бэкапов.

Интерпретация:
- missing_offsets > 0 ⇒ есть «дыры» (сообщения не попали или ещё не дочитаны).
- duplicate_offsets > 0 ⇒ повторные вставки (атакуем через ReplacingMergeTree, не критично, но нужно наблюдать динамику).

2. Быстрый список партиций с дырками:

```sql
SELECT __kafka_partition, missing_offsets
FROM (
  SELECT __kafka_partition,
       (max(__kafka_offset)-min(__kafka_offset)+1) - countDistinct(__kafka_offset) AS missing_offsets
  FROM kafka.tbl_jti_trace_cis_history_raw
  GROUP BY __kafka_partition
) t
WHERE missing_offsets > 0
ORDER BY missing_offsets DESC;
```

3. Диапазоны оффсетов для проблемной партиции (подставьте партицию X):

```sql
SELECT __kafka_offset
FROM kafka.tbl_jti_trace_cis_history_raw
WHERE __kafka_partition = X
ORDER BY __kafka_offset
LIMIT 1000;  -- сузить для первичного анализа
```

4. Агрегированная задержка (lag) относительно последнего прочитанного оффсета в каждой партиции (если нужен Kafka CLI — сверить вручную):

```sql
SELECT __kafka_partition,
     max(__kafka_offset) AS last_seen_offset
FROM kafka.tbl_jti_trace_cis_history_raw
GROUP BY __kafka_partition
ORDER BY __kafka_partition;
```

Сравниваем с `kafka-consumer-groups.sh --describe` для той же группы.

5. Оперативная оценка «скорости» поступления (Δ оффсетов) за интервал (пример: последние 15 минут):

```sql
SELECT __kafka_partition,
     (max(__kafka_offset) - min(__kafka_offset)) AS delta_offsets
FROM kafka.tbl_jti_trace_cis_history_raw
WHERE __kafka_ts >= now() - INTERVAL 15 MINUTE
GROUP BY __kafka_partition
ORDER BY delta_offsets DESC;
```

### 3.8 Тюнинг производительности ingestion

- Увеличение `kafka_max_block_size` даёт прирост пропускной способности, но растит задержку первой записи в блоке. Подбирать под SLA (обычно диапазон 10k–50k).
- Если партиций существенно больше, чем консьюмеров × узлов, постепенно увеличивать `kafka_num_consumers` (по одному за релиз) при наличии свободного CPU.
- Следить за средним размером блока через метрику `system.query_log` (поле `read_rows`).
- При всплесках задержек уменьшить блок до прежнего значения (оперативный rollback).

Не меняем `kafka_group_name` у существующей таблицы при перезапуске. Для паузы ingestion используем только `ALTER TABLE ... MODIFY SETTING kafka_num_consumers=0`.

### 4. Чек-лист релиза и перехода схемы (QA → PROD)

Перед запуском новой версии / сменой Avro-схемы:

1. Зафиксировать действующее `kafka_group_name` (vN) и подготовить новое (vN+1) — только при несовместимых изменениях.
2. Проверить, что все оффсеты дочитаны (нет missing_offsets) в текущей группе.
3. Снять снапшот метрик: объём за 15 мин (delta_offsets), максимальные оффсеты.
4. Проверить `system.kafka_consumers` (is_currently_used=1, нет залипших партиций).
5. Проверить отказоустойчивость ZooKeeper путей перед созданием/обновлением реплицированных таблиц.
6. На QA применить новую схему Avro; убедиться, что ingest не падает (нет ошибок десериализации).
7. Запустить новую consumer group (vN+1) в QA: старт с earliest, сверить полноту против старой группы.
8. Перенести на PROD: создать новую группу, старая продолжает до подтверждения полноты.
9. После подтверждения: заархивировать мониторинг старой группы, оставить только новую.
10. Обновить runbook и схемы (хранить версионные метки: commit SHA + версию Avro). 

Отмена (rollback):
- Отключить новую группу (`kafka_num_consumers=0`), вернуть старую (если она ещё активна).
- Проверить, что оффсеты старой группы не продвинулись нелинейно (отсутствие скачков или пропусков).

### 5. Быстрые команды для оператора

```bash
# Состояние консьюмеров ClickHouse
clickhouse-client --query "SELECT __kafka_partition, max(__kafka_offset) last FROM kafka.tbl_jti_trace_cis_history_raw GROUP BY __kafka_partition ORDER BY __kafka_partition" | column -t

# Пауза ingest (QA)
clickhouse-client --query "ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes MODIFY SETTING kafka_num_consumers=0"

# Возобновление ingest (QA)
clickhouse-client --query "ALTER TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes MODIFY SETTING kafka_num_consumers=1"

# Быстрый аудит пропусков
clickhouse-client --query "SELECT __kafka_partition,(max(__kafka_offset)-min(__kafka_offset)+1)-countDistinct(__kafka_offset) missing FROM kafka.tbl_jti_trace_cis_history_raw GROUP BY __kafka_partition HAVING missing>0 ORDER BY missing DESC" | column -t
 
 # Для остановки ingestion **не** удаляем Kafka-таблицу и **не** переименовываем `kafka_group_name` — только меняем `kafka_num_consumers`.
```

## 6. Пошаговый боевой сценарий (QA → PROD)

1. **Настроить кластеры:**
   - Убедиться, что `shardless` (1 шард × N реплик, `internal_replication=true`) уже используется и не трогать существующие боевые таблицы.
   - Добавить `per_host_allnodes` с одним репликой на хост, `internal_replication=false`.

2. **Создать DB для Kafka:**
   - `CREATE DATABASE IF NOT EXISTS kafka ON CLUSTER per_host_allnodes;`
   - `CREATE DATABASE IF NOT EXISTS kafka ON CLUSTER shardless;`

3. **На ingestion-слое (`per_host_allnodes`):**
   - Создать `kafka.tbl_jti_trace_cis_history_raw` (RAW + TTL + __kafka_*).
   - Создать `kafka.tbl_jti_trace_cis_history_kafka` (ENGINE=Kafka, AvroConfluent, корректный `format_avro_schema_registry_url`, новый `kafka_group_name`).
   - Создать `kafka.mv_tbl_jti_trace_cis_history_to_raw` (Kafka → RAW).
   - Создать `kafka.tbl_jti_trace_cis_history_raw_all` (Distributed по `per_host_allnodes`).

4. **На надёжном слое (`shardless`):**
   - Создать `kafka.tbl_jti_trace_cis_history_repl` (ReplicatedReplacingMergeTree).
   - Создать `kafka.tbl_jti_trace_cis_history_repl_all` (Distributed).
   - Создать `kafka.mv_tbl_jti_trace_cis_history_to_repl` (из RAW_all в repl).

5. **Вся аналитика и витрины читают только из:**
   - `kafka.tbl_jti_trace_cis_history_repl_all`.

6. **Kafka consumer group:**
   - Для PROD завести стабильный `kafka_group_name` вида `ch_tbl_jti_trace_cis_history_ingest_v1`.
   - Новый релиз схемы → новый `kafka_group_name` (v2, v3), старую группу не удалять, пока не подтверждена полнота.

7. **Проверки здоровья:**
   - `system.kafka_consumers` по `per_host_allnodes`: есть `is_currently_used = 1` на нужных нодах.
   - Сверка оффсетов через `kafka-consumer-groups.sh` и `__kafka_*` поля.
   - Отсутствие дыр и дубликатов:
     - `SELECT __kafka_partition, countDistinct(__kafka_offset) ...`
   - Очередь DDL:
     - `SELECT * FROM system.distributed_ddl_queue WHERE exception_code != 0`.

Этот сценарий:
- даёт at-least-once доставку из Kafka без потерь;
- изолирует ingestion от боевых витрин;
- обеспечивает сохранность данных при падении одной ноды ClickHouse;
- остаётся совместимым с ClickHouse 24.8.14.39 (TTL по DateTime, корректный AvroConfluent SR-параметр, настройки Avro input и commit оффсетов).

## 7. High Availability и восстановление после сбоя

- Ingestion-слой (`per_host_allnodes`) работает без репликации, но устойчив к сбоям отдельных нод: Kafka автоматически перераспределяет партиции между активными консьюмерами.
- Надёжный слой (`shardless`) реплицирован (`internal_replication=true`), обеспечивает отказоустойчивость и консистентность данных.
- При потере ingestion-ноды:
  - Kafka переназначает партиции другим консьюмерам в группе.
  - После восстановления ноды ClickHouse возобновит чтение своих партиций с сохранённых оффсетов (в Kafka).
- При временной недоступности Schema Registry ingestion-процесс автоматически приостанавливается, но не теряет оффсеты. После восстановления SR чтение продолжается с последнего коммитнутого оффсета.
- При потере реплики слоя `shardless`:
  - Данные восстанавливаются из других реплик по путям ZooKeeper `/clickhouse/tables/{shard}/...`.
  - После возвращения реплики ClickHouse автоматически синхронизирует данные.
- Для предотвращения потерь:
  - `kafka_commit_every_batch = 1` — коммит оффсетов только после успешной записи в RAW/MV.
  - `ReplicatedReplacingMergeTree` на надёжном слое устраняет дубликаты при повторном чтении тех же оффсетов.
  - TTL в RAW должен превышать Kafka retention, чтобы можно было восстановить данные при задержке ETL.

При критических сбоях рекомендуется:
1. Проверить system.distributed_ddl_queue на ошибки.
2. Проверить целостность данных через аудит оффсетов (см. 3.7).
3. Выполнить ручной replay из Kafka при необходимости: задать новый kafka_group_name и перезапустить ingestion.
