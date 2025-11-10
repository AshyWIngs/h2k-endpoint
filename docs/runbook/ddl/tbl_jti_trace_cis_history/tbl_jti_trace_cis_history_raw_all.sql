-- Distributed over RAW for TBL_JTI_TRACE_CIS_HISTORY
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_raw_all ON CLUSTER shardless SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_raw_all ON CLUSTER shardless
AS kafka.tbl_jti_trace_cis_history_raw
ENGINE = Distributed('per_host_allnodes', 'kafka', 'tbl_jti_trace_cis_history_raw', __kafka_partition)
SETTINGS prefer_localhost_replica = 1;