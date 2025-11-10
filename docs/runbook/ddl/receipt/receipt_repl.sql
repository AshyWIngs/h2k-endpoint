-- Реплицируемая витрина для RECEIPT (ReplacingMergeTree по версии ver)
CREATE TABLE IF NOT EXISTS kafka.receipt_repl
(
    did String,
    id Nullable(String),
    dt Nullable(DateTime64(3,'UTC')),
    t  Nullable(Int16),
    st Nullable(UInt8),
    sid Nullable(String),
    sinn Nullable(String),
    sn Nullable(String),
    sa Nullable(String),
    said Nullable(String),
    tm Nullable(DateTime64(3,'UTC')),
    tt Nullable(Int64),
    vat Nullable(Int64),
    pgm Nullable(String),
    j   Nullable(String),
    b   Nullable(String),
    _event_ts Nullable(DateTime64(3,'UTC')),
    _delete UInt8,
    __kafka_partition Int32,
    __kafka_offset UInt64,
    __kafka_ts DateTime,
    ver DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/kafka/receipt_repl', '{replica}', ver)
PARTITION BY toYYYYMM(coalesce(dt, __kafka_ts))
ORDER BY (__kafka_partition, __kafka_offset, did, dt)
TTL coalesce(dt, __kafka_ts) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;