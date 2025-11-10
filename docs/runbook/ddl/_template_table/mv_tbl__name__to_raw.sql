-- MV Kafka → RAW для <NAME>
CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.mv_tbl_<name>_to_raw
TO {db}.tbl_<name>_raw
AS
SELECT
    -- Преобразуйте long (epoch ms) в DateTime64 где нужно
    -- id,
    -- toDateTime64(dt/1000.0,3,'UTC') AS dt,
    -- _event_ts преобразование при необходимости,
    -- _delete,
    _partition AS __kafka_partition,
    _offset AS __kafka_offset,
    ifNull(_timestamp, now()) AS __kafka_ts,
    now() AS __ingest_ts
FROM {db}.tbl_<name>_kafka;