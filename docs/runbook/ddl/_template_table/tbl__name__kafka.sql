-- Kafka-источник для <NAME>
-- Все поля из Avro описываются исходными типами; время в миллисекундах (long) конвертируется в MV.

CREATE TABLE IF NOT EXISTS {db}.tbl_<name>_kafka
(
    -- пример: id String, dt Nullable(Int64), payload Nullable(String), _event_ts Nullable(Int64), _delete UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '<brokers_csv>',
    kafka_topic_list = '<TOPIC_NAME>',
    kafka_group_name = 'ch_tbl_<name>_qa',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry-lb:8081',
    kafka_commit_on_select = 0,
    kafka_skip_broken_messages = 0,
    kafka_auto_offset_reset = 'earliest',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size = 10000;
