-- MV Kafka → RAW для RECEIPT (без префикса tbl_, БД kafka)
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka.mv_receipt_to_raw
TO kafka.receipt_raw
AS
SELECT
    did,
    id,
    toDateTime64(dt/1000.0,3,'UTC') AS dt,
    CAST(t AS Nullable(Int16)) AS t,
    CAST(st AS Nullable(UInt8)) AS st,
    sid, sinn, sn, sa, said,
    toDateTime64(tm/1000.0,3,'UTC') AS tm,
    tt, vat, pgm, j, b,
    toDateTime64(_event_ts/1000.0,3,'UTC') AS _event_ts,
    _delete,
    _partition AS __kafka_partition,
    _offset AS __kafka_offset,
    ifNull(_timestamp, now()) AS __kafka_ts,
    now() AS __ingest_ts
FROM kafka.receipt_kafka;