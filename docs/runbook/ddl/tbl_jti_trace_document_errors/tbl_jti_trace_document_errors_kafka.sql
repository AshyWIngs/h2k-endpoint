-- Kafka-источник для TBL_JTI_TRACE_DOCUMENT_ERRORS
-- Назначение: движок Kafka читает AvroConfluent и отдаёт строки в MV для загрузки в RAW.
-- Семантика потребления «at-least-once»: фиксация оффсетов происходит после выборки MV.
-- Все значения времени (dt, _event_ts) приходят как long (epoch millis) и будут преобразованы в MV.

CREATE TABLE IF NOT EXISTS {db}.tbl_jti_trace_document_errors_kafka
(
    id String,                 -- PK (NOT NULL)
    did Nullable(String),      -- идентификатор документа (опционально)
    dt Nullable(Int64),        -- время события (epoch ms)
    oid Nullable(String),      -- внешний объект
    en Nullable(String),       -- код ошибки
    emsg Nullable(String),     -- текст ошибки
    inn Nullable(String),      -- ИНН (опционально)
    dnum Nullable(String),     -- номер документа
    _event_ts Nullable(Int64), -- версия события (epoch ms)
    _delete UInt8              -- логическое удаление (Avro boolean → UInt8)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
    kafka_topic_list = 'TBL_JTI_TRACE_DOCUMENT_ERRORS',
    kafka_group_name = 'ch_tbl_jti_trace_document_errors_qa',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://10.254.3.111:8081', -- рекомендуется использовать LB/FQDN
    kafka_commit_on_select = 0,
    kafka_skip_broken_messages = 0,
    kafka_auto_offset_reset = 'earliest',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size = 10000;
