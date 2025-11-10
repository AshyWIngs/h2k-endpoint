-- Distributed над реплицируемой таблицей для TBL_JTI_TRACE_DOCUMENT_ERRORS (кластер shardless)
-- Назначение: единая точка чтения аналитических данных.

CREATE TABLE IF NOT EXISTS {db}.tbl_jti_trace_document_errors_repl_all AS {db}.tbl_jti_trace_document_errors_repl
ENGINE = Distributed shardless, {db}, tbl_jti_trace_document_errors_repl, rand();
