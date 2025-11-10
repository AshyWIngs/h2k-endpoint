-- Реплицируемая витрина для <NAME>
CREATE TABLE IF NOT EXISTS {db}.tbl_<name>_repl
(
    -- бизнес-колонки (как в RAW)
    __kafka_partition Int32,
    __kafka_offset UInt64,
    __kafka_ts DateTime,
    ver DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/shardless/{db}.tbl_<name>_repl', '{shardless_repl}', ver)
PARTITION BY toYYYYMM(coalesce(/* business_ts */, __kafka_ts))
ORDER BY (__kafka_partition, __kafka_offset)
TTL coalesce(/* business_ts */, __kafka_ts) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;