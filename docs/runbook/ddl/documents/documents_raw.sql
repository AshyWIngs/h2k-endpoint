-- RAW таблица для DOCUMENTS (MergeTree)
CREATE TABLE IF NOT EXISTS kafka.documents_raw
(
    OWNER_ID String,
    DIRECTION Int8,
    DOCUMENT_ID String,
    IS_DELETED Nullable(UInt8),
    owner_extra_id Nullable(String),
    group_id Nullable(String),
    partner_id Nullable(String),
    partner_extra_id Nullable(String),
    created_at DateTime64(3,'UTC'),
    processed_at Nullable(DateTime64(3,'UTC')),
    type Nullable(String),
    status Nullable(UInt8),
    folder Nullable(UInt8),
    date Nullable(DateTime64(3,'UTC')),
    number Nullable(String),
    filename Nullable(String),
    data Nullable(String),
    content Nullable(String),
    signature Nullable(String),
    outer_partner_id Nullable(String),
    created_by Nullable(String),
    changed_by Nullable(String),
    count_at Nullable(String),
    OUTER_ID Nullable(String),
    _event_ts DateTime64(3,'UTC'),
    _delete Nullable(UInt8),
    __kafka_partition Int32,
    __kafka_offset Int64,
    __kafka_ts DateTime DEFAULT now(),
    __ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
PRIMARY KEY (OWNER_ID, DOCUMENT_ID, __kafka_partition, __kafka_offset)
ORDER BY (OWNER_ID, DOCUMENT_ID, __kafka_partition, __kafka_offset)
TTL __ingest_ts + INTERVAL 14 DAY
SETTINGS index_granularity = 8192;