-- RAW MergeTree для <NAME>
CREATE TABLE IF NOT EXISTS {db}.tbl_<name>_raw
(
    -- бизнес-колонки с целевыми типами CH (DateTime64 для времени и т.д.)
    -- служебные поля:
    __kafka_partition Int32,
    __kafka_offset UInt64,
    __kafka_ts DateTime,
    __ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(__ingest_ts)
ORDER BY (__kafka_partition, __kafka_offset)
TTL __ingest_ts + INTERVAL 60 DAY DELETE
SETTINGS index_granularity = 8192;