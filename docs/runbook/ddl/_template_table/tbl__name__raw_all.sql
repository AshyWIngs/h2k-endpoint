-- Distributed RAW для <NAME>
CREATE TABLE IF NOT EXISTS {db}.tbl_<name>_raw_all AS {db}.tbl_<name>_raw
ENGINE = Distributed shardless, {db}, tbl_<name>_raw, rand();