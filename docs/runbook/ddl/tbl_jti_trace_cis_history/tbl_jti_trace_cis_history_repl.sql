-- ReplicatedReplacingMergeTree vitrine for TBL_JTI_TRACE_CIS_HISTORY
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_repl ON CLUSTER shardless SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_repl ON CLUSTER shardless
(
  c String, t UInt8, opd DateTime64(3, 'UTC'),
  _event_ts DateTime64(3, 'UTC'), _delete UInt8,
  id Nullable(String), did Nullable(String), rid Nullable(String), 
  rinn Nullable(String), rn Nullable(String), sid Nullable(String), 
  sinn Nullable(String), sn Nullable(String), gt Nullable(String), 
  prid Nullable(String), st Nullable(UInt8), ste Nullable(UInt8), 
  elr Nullable(UInt8), emd Nullable(DateTime64(3, 'UTC')), 
  apd Nullable(DateTime64(3, 'UTC')), exd Nullable(DateTime64(3, 'UTC')),
  p Nullable(String), pt Nullable(UInt8), o Nullable(String), 
  pn Nullable(String), b Nullable(String), tt Nullable(Int64), 
  tm Nullable(DateTime64(3, 'UTC')), ch Array(String), 
  j Nullable(String), pg Nullable(UInt16), et Nullable(UInt8), 
  pvad Nullable(String), ag Nullable(String),
  __kafka_partition Int32,
  __kafka_offset UInt64,
  __kafka_ts DateTime,
  ver DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/shardless/kafka.tbl_jti_trace_cis_history_repl',
    '{shardless_repl}',
    ver
)
PARTITION BY toYYYYMM(opd)
ORDER BY (__kafka_partition, __kafka_offset, c, t, opd)
TTL toDateTime(coalesce(_event_ts, __kafka_ts)) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192;