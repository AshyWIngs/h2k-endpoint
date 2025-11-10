-- MV: Kafka -> RAW for TBL_JTI_TRACE_CIS_HISTORY
DROP TABLE IF EXISTS kafka.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER per_host_allnodes SYNC;

CREATE MATERIALIZED VIEW kafka.mv_tbl_jti_trace_cis_history_to_raw ON CLUSTER per_host_allnodes
TO kafka.tbl_jti_trace_cis_history_raw
AS
SELECT
  c,
  CAST(t AS UInt8) AS t,
  toDateTime64(opd / 1000.0, 3, 'UTC') AS opd,
  ifNull(toDateTime64(_event_ts / 1000.0, 3, 'UTC'), NULL) AS _event_ts,
  _delete,
  id, did, rid, rinn, rn, sid, sinn, sn, gt, prid,
  CAST(st AS Nullable(UInt8)) AS st,
  CAST(ste AS Nullable(UInt8)) AS ste,
  CAST(elr AS Nullable(UInt8)) AS elr,
  ifNull(toDateTime64(emd / 1000.0, 3, 'UTC'), NULL) AS emd,
  ifNull(toDateTime64(apd / 1000.0, 3, 'UTC'), NULL) AS apd,
  ifNull(toDateTime64(exd / 1000.0, 3, 'UTC'), NULL) AS exd,
  p, CAST(pt AS Nullable(UInt8)) AS pt, o, pn, b, tt,
  ifNull(toDateTime64(tm / 1000.0, 3, 'UTC'), NULL) AS tm,
  ch, j, CAST(pg AS Nullable(UInt16)) AS pg, 
  CAST(et AS Nullable(UInt8)) AS et, pvad, ag,
  _partition AS __kafka_partition,
  _offset AS __kafka_offset,
  ifNull(_timestamp, now()) AS __kafka_ts,
  now() AS __ingest_ts
FROM kafka.tbl_jti_trace_cis_history_kafka;