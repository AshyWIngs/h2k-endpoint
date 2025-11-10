-- Реплицируемая таблица аналитики для TBL_JTI_TRACE_DOCUMENT_ERRORS
-- Шаблон: ReplicatedReplacingMergeTree с дедупликацией по (__kafka_partition, __kafka_offset)
-- TTL по coalesce(event_ts, __kafka_ts) для гарантированного истечения, если бизнес-время отсутствует.

CREATE TABLE IF NOT EXISTS {db}.tbl_jti_trace_document_errors_repl
(
    id String,                                 -- PK (NOT NULL)
    did Nullable(String),
    dt Nullable(DateTime64(3, 'UTC')),
    oid Nullable(String),
    en Nullable(String),
    emsg Nullable(String),
    inn Nullable(String),
    dnum Nullable(String),
    _event_ts Nullable(DateTime64(3, 'UTC')),
    _delete UInt8,

    __kafka_partition Int32,
    __kafka_offset UInt64,
    __kafka_ts DateTime,
    ver DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/shardless/kafka.tbl_jti_trace_document_errors_repl', '{shardless_repl}', ver)
PARTITION BY toYYYYMM(coalesce(dt, __kafka_ts))
ORDER BY (__kafka_partition, __kafka_offset, id, dt)
TTL coalesce(dt, __kafka_ts) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;
