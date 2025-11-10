-- Distributed over vitrine for TBL_JTI_TRACE_CIS_HISTORY
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_repl_all ON CLUSTER shardless SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_repl_all ON CLUSTER shardless
AS kafka.tbl_jti_trace_cis_history_repl
ENGINE = Distributed('shardless', 'kafka', 'tbl_jti_trace_cis_history_repl');