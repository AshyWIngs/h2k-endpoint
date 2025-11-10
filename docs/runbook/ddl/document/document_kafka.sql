-- Kafka-источник для DOCUMENT (без префикса tbl_, БД kafka)
CREATE TABLE IF NOT EXISTS kafka.document_kafka
(
    did String,
    id Nullable(String),
    dt Nullable(Int64),
    t  Nullable(Int16),           -- SMALLINT
    st Nullable(Int32),           -- UNSIGNED_TINYINT в Avro, конвертируется далее
    sid Nullable(String),
    sinn Nullable(String),
    sn Nullable(String),
    sa Nullable(String),
    said Nullable(String),
    rid Nullable(String),
    rinn Nullable(String),
    rn Nullable(String),
    ra Nullable(String),
    raid Nullable(String),
    invi Nullable(String),
    invd Nullable(Int64),
    cinvi Nullable(String),
    cinvd Nullable(Int64),
    fn Nullable(Int64),           -- UNSIGNED_INT
    fd Nullable(Int64),
    tm Nullable(Int64),
    tt Nullable(Int64),
    vat Nullable(Int64),
    exc Nullable(Int64),
    pgm Nullable(String),         -- VARBINARY → String
    j   Nullable(String),
    b   Nullable(String),
    _event_ts Nullable(Int64),
    _delete UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
    kafka_topic_list = 'DOCUMENT',
    kafka_group_name = 'ch_document_qa',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://10.254.3.111:8081',
    kafka_commit_on_select = 0,
    kafka_skip_broken_messages = 0,
    kafka_auto_offset_reset = 'earliest',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size = 10000;