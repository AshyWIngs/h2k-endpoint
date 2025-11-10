-- RAW MergeTree для DOCUMENT (TTL по __ingest_ts)
CREATE TABLE IF NOT EXISTS kafka.document_raw
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
    rid Nullable(String),
    rinn Nullable(String),
    rn Nullable(String),
    ra Nullable(String),
    raid Nullable(String),
    invi Nullable(String),
    invd Nullable(DateTime64(3,'UTC')),
    cinvi Nullable(String),
    cinvd Nullable(DateTime64(3,'UTC')),
    fn Nullable(UInt32),
    fd Nullable(DateTime64(3,'UTC')),
    tm Nullable(DateTime64(3,'UTC')),
    tt Nullable(Int64),
    vat Nullable(Int64),
    exc Nullable(Int64),
    pgm Nullable(String),
    j   Nullable(String),
    b   Nullable(String),
    _event_ts Nullable(DateTime64(3,'UTC')),
    _delete UInt8,
    __kafka_partition Int32,
    __kafka_offset UInt64,
    __kafka_ts DateTime,
    __ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(__ingest_ts)
ORDER BY (__kafka_partition, __kafka_offset, did)
TTL __ingest_ts + INTERVAL 60 DAY DELETE
SETTINGS index_granularity = 8192;