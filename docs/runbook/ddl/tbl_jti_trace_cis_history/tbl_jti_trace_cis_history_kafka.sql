-- Kafka source table for TBL_JTI_TRACE_CIS_HISTORY (ENGINE=Kafka, AvroConfluent)
DROP TABLE IF EXISTS kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes SYNC;

CREATE TABLE kafka.tbl_jti_trace_cis_history_kafka ON CLUSTER per_host_allnodes
(
  c String, t Int32, opd Int64,
  id Nullable(String), did Nullable(String), rid Nullable(String), 
  rinn Nullable(String), rn Nullable(String), sid Nullable(String), 
  sinn Nullable(String), sn Nullable(String), gt Nullable(String), 
  prid Nullable(String), st Nullable(Int32), ste Nullable(Int32), 
  elr Nullable(Int32), emd Nullable(Int64), apd Nullable(Int64), 
  exd Nullable(Int64), p Nullable(String), pt Nullable(Int32), 
  o Nullable(String), pn Nullable(String), b Nullable(String), 
  tt Nullable(Int64), tm Nullable(Int64), ch Array(String), 
  j Nullable(String), pg Nullable(Int32), et Nullable(Int32), 
  pvad Nullable(String), ag Nullable(String),
  _event_ts Nullable(Int64),
  _delete UInt8
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
  kafka_topic_list = 'TBL_JTI_TRACE_CIS_HISTORY',
  kafka_group_name = 'ch_tbl_jti_trace_cis_history_qa',
  kafka_auto_offset_reset = 'earliest',
  kafka_format = 'AvroConfluent',
  format_avro_schema_registry_url = 'http://10.254.3.111:8081',
  kafka_commit_on_select = 0,
  kafka_skip_broken_messages = 0,
  input_format_avro_allow_missing_fields = 1,
  input_format_avro_null_as_default = 1,
  kafka_num_consumers = 1,
  kafka_max_block_size = 10000;