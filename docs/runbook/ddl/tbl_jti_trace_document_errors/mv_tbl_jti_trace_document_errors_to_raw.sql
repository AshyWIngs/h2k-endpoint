-- MATERIALIZED VIEW: Kafka → RAW для TBL_JTI_TRACE_DOCUMENT_ERRORS
-- Назначение: преобразование типов/полей на лету и добавление служебных атрибутов.
-- Примечание: виртуальные поля движка Kafka — _topic, _key, _timestamp, _offset, _partition.

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.mv_tbl_jti_trace_document_errors_to_raw
TO {db}.tbl_jti_trace_document_errors_raw
AS
SELECT
    -- Прямое сопоставление полей Avro → RAW с конвертацией времени из ms
    id,
    did,
    toDateTime64(dt / 1000.0, 3, 'UTC') AS dt,
    oid,
    en,
    emsg,
    inn,
    dnum,
    toDateTime64(_event_ts / 1000.0, 3, 'UTC') AS _event_ts,
    _delete,

    _partition AS __kafka_partition,
    _offset AS __kafka_offset,
    ifNull(_timestamp, now()) AS __kafka_ts,
    now() AS __ingest_ts
FROM {db}.tbl_jti_trace_document_errors_kafka;
