-- Distributed над RAW для TBL_JTI_TRACE_DOCUMENT_ERRORS (кластер per_host_allnodes)
-- Назначение: объединённый просмотр RAW на всех узлах без репликации.

CREATE TABLE IF NOT EXISTS {db}.tbl_jti_trace_document_errors_raw_all AS {db}.tbl_jti_trace_document_errors_raw
ENGINE = Distributed per_host_allnodes, {db}, tbl_jti_trace_document_errors_raw, cityHash64(__kafka_partition, __kafka_offset);
