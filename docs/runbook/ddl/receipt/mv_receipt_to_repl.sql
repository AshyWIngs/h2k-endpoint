-- MV RAW → витрина для RECEIPT (без префикса tbl_, БД kafka)
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka.mv_receipt_to_repl
TO kafka.receipt_repl
AS
SELECT
    did, id, dt, t, st, sid, sinn, sn, sa, said,
    tm, tt, vat, pgm, j, b,
    _event_ts, _delete,
    __kafka_partition, __kafka_offset, __kafka_ts,
    now() AS ver
FROM kafka.receipt_raw;