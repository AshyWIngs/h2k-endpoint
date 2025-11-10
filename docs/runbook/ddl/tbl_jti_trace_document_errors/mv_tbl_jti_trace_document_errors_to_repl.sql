-- MATERIALIZED VIEW: RAW → REPL для TBL_JTI_TRACE_DOCUMENT_ERRORS
-- Назначение: перенос данных из RAW в реплицируемую таблицу с минимальными преобразованиями.

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.mv_tbl_jti_trace_document_errors_to_repl
TO {db}.tbl_jti_trace_document_errors_repl
AS
SELECT
    id, did, dt, oid, en, emsg, inn, dnum, _event_ts, _delete,
    __kafka_partition, __kafka_offset, __kafka_ts,
    now() AS ver
FROM {db}.tbl_jti_trace_document_errors_raw;
