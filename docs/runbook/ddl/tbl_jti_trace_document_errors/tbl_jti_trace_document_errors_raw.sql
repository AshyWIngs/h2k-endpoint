-- RAW-таблица для TBL_JTI_TRACE_DOCUMENT_ERRORS (MergeTree)
-- Назначение: буфер приёма из Kafka через MV с минимальными преобразованиями.
-- TTL по __ingest_ts для контроля жизненного цикла «сырых» данных.

CREATE TABLE IF NOT EXISTS {db}.tbl_jti_trace_document_errors_raw
(
    -- Бизнес-колонки из Avro (с приведением типов)
    id String,                                 -- PK (NOT NULL)
    did Nullable(String),                      -- идентификатор документа
    dt Nullable(DateTime64(3, 'UTC')),         -- событие (из epoch ms)
    oid Nullable(String),
    en Nullable(String),
    emsg Nullable(String),
    inn Nullable(String),
    dnum Nullable(String),
    _event_ts Nullable(DateTime64(3, 'UTC')),  -- версия события (из epoch ms)
    _delete UInt8,

    -- Служебные колонки для трассировки и TTL:
    __kafka_partition Int32,
    __kafka_offset UInt64,
    __kafka_ts DateTime,
    __ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(__ingest_ts)
ORDER BY (__kafka_partition, __kafka_offset, id)
TTL __ingest_ts + INTERVAL 60 DAY DELETE
SETTINGS index_granularity = 8192;
