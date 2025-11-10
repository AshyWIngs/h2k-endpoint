-- Distributed RAW для RECEIPT
CREATE TABLE IF NOT EXISTS kafka.receipt_raw_all AS kafka.receipt_raw
ENGINE = Distributed(per_host_allnodes, kafka, receipt_raw, rand());