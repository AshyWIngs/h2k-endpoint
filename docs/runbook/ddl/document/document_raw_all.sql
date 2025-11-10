-- Distributed RAW для DOCUMENT
CREATE TABLE IF NOT EXISTS kafka.document_raw_all AS kafka.document_raw
ENGINE = Distributed(shardless, kafka, document_raw, rand());