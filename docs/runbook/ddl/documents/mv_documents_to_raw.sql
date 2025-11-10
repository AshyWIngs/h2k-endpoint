-- MV Kafka → RAW для DOCUMENTS (без префикса tbl_, БД kafka)
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka.mv_documents_to_raw
TO kafka.documents_raw
AS
SELECT
    OWNER_ID,
    CAST(DIRECTION AS Int8) AS DIRECTION,
    DOCUMENT_ID,
    CAST(IS_DELETED AS Nullable(UInt8)) AS IS_DELETED,
    owner_extra_id,
    group_id,
    partner_id,
    partner_extra_id,
    toDateTime64(created_at/1000.0,3,'UTC') AS created_at,
    toDateTime64(processed_at/1000.0,3,'UTC') AS processed_at,
    type,
    CAST(status AS Nullable(UInt8)) AS status,
    CAST(folder AS Nullable(UInt8)) AS folder,
    toDateTime64(date/1000.0,3,'UTC') AS date,
    number,
    filename,
    data,
    content,
    signature,
    outer_partner_id,
    created_by,
    changed_by,
    count_at,
    OUTER_ID,
    toDateTime64(_event_ts/1000.0,3,'UTC') AS _event_ts,
    _delete,
    _partition AS __kafka_partition,
    _offset AS __kafka_offset,
    ifNull(_timestamp, now()) AS __kafka_ts,
    now() AS __ingest_ts
FROM kafka.documents_kafka;