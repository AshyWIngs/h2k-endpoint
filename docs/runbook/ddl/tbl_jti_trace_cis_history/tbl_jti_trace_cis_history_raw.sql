-- RAW table for TBL_JTI_TRACE_CIS_HISTORY (MergeTree, TTL 60d)
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_raw ON CLUSTER per_host_allnodes SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_raw ON CLUSTER per_host_allnodes
(
  -- ключевые поля
  c String,
  t UInt8,
  opd DateTime64(3, 'UTC'),
  _event_ts DateTime64(3, 'UTC'),
  _delete UInt8,
  
  -- бизнес-поля
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
  
  -- метаданные Kafka
  __kafka_partition Int32,
  __kafka_offset UInt64,
  __kafka_ts DateTime,
  __ingest_ts DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(__ingest_ts)
ORDER BY (c, t, opd, __kafka_partition, __kafka_offset)
TTL __ingest_ts + INTERVAL 60 DAY DELETE
SETTINGS index_granularity = 8192;