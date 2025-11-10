-- MV RAW → витрина для DOCUMENT (без префикса tbl_, БД kafka)
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka.mv_document_to_repl
TO kafka.document_repl
AS
SELECT
    did, id, dt, t, st, sid, sinn, sn, sa, said, rid, rinn, rn, ra, raid,
    invi, invd, cinvi, cinvd, fn, fd, tm, tt, vat, exc, pgm, j, b,
    _event_ts, _delete,
    __kafka_partition, __kafka_offset, __kafka_ts,
    now() AS ver
FROM kafka.document_raw;