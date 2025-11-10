-- Kafka-источник для DOCUMENTS (без префикса tbl_, БД kafka)
CREATE TABLE IF NOT EXISTS kafka.documents_kafka
(
    OWNER_ID   Int64,
    DIRECTION  Int32,
    DOCUMENT_ID String,
    IS_DELETED        Nullable(Int32),
    owner_extra_id    Nullable(String),
    group_id          Nullable(String),
    partner_id        Nullable(Int64),
    partner_extra_id  Nullable(String),
    created_at        Nullable(Int64),
    processed_at      Nullable(Int64),
    type              Nullable(Int32),
    status            Nullable(Int32),
    folder            Nullable(Int32),
    date              Nullable(Int64),
    number            Nullable(String),
    filename          Nullable(String),
    data              Nullable(String),
    content           Nullable(String),
    signature         Nullable(String),
    outer_partner_id  Nullable(String),
    created_by        Nullable(Int32),
    changed_by        Nullable(Int32),
    count_at          Nullable(Int64),
    OUTER_ID          Nullable(String),
    _event_ts Nullable(Int64),
    _delete   UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
    kafka_topic_list = 'TBL_DOCUMENTS',
    kafka_group_name = 'ch_documents_qa',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://10.254.3.111:8081',
    kafka_commit_on_select = 0,
    kafka_skip_broken_messages = 0,
    kafka_auto_offset_reset = 'earliest',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size = 10000;