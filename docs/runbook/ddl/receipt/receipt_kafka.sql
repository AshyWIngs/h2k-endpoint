-- Kafka-источник для RECEIPT (без префикса tbl_, БД kafka)
CREATE TABLE IF NOT EXISTS kafka.receipt_kafka
(
    did String,
    id Nullable(String),
    dt Nullable(Int64),
    t  Nullable(Int16),
    st Nullable(Int32),
    sid Nullable(String),
    sinn Nullable(String),
    sn Nullable(String),
    sa Nullable(String),
    said Nullable(String),
    tm Nullable(Int64),
    tt Nullable(Int64),
    vat Nullable(Int64),
    pgm Nullable(String),
    j   Nullable(String),
    b   Nullable(String),
    _event_ts Nullable(Int64),
    _delete UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
    kafka_topic_list = 'RECEIPT',
    kafka_group_name = 'ch_receipt_qa',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://10.254.3.111:8081',
    kafka_commit_on_select = 0,
    kafka_skip_broken_messages = 0,
    kafka_auto_offset_reset = 'earliest',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size = 10000;