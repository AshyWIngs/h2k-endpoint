-- MV: RAW -> vitrine for TBL_JTI_TRACE_CIS_HISTORY
DROP TABLE IF EXISTS kafka.mv_tbl_jti_trace_cis_history_to_repl ON CLUSTER shardless SYNC;

CREATE MATERIALIZED VIEW kafka.mv_tbl_jti_trace_cis_history_to_repl ON CLUSTER shardless
TO kafka.tbl_jti_trace_cis_history_repl
AS
SELECT
    c, t, opd, _event_ts, _delete,
    id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
    st, ste, elr, emd, apd, exd, p, pt, o, pn, b, tt, tm,
    ch, j, pg, et, pvad, ag,
    __kafka_partition, __kafka_offset, __kafka_ts,
    now() AS ver
FROM kafka.tbl_jti_trace_cis_history_raw_all;