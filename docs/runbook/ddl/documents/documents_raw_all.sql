-- Distributed таблица для DOCUMENTS RAW
CREATE TABLE IF NOT EXISTS kafka.documents_raw_all AS kafka.documents_raw
ENGINE = Distributed(per_host_allnodes, kafka, documents_raw, rand());