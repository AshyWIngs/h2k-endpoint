-- MV RAW → REPL для DOCUMENTS
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka.mv_documents_to_repl
TO kafka.documents_repl
AS
SELECT
    OWNER_ID,
    DIRECTION,
    DOCUMENT_ID,
    IS_DELETED,
    owner_extra_id,
    group_id,
    partner_id,
    partner_extra_id,
    created_at,
    processed_at,
    type,
    status,
    folder,
    date,
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
    _event_ts,
    _delete,
    __kafka_partition,
    __kafka_offset,
    __kafka_ts,
    __ingest_ts
FROM kafka.documents_raw;