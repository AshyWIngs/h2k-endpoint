-- MV RAW → витрина для <NAME>
CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.mv_tbl_<name>_to_repl
TO {db}.tbl_<name>_repl
AS
SELECT
    -- перечислите поля 1:1 из RAW
    __kafka_partition, __kafka_offset, __kafka_ts,
    now() AS ver
FROM {db}.tbl_<name>_raw;