-- Distributed витрина для RECEIPT
CREATE TABLE IF NOT EXISTS kafka.receipt_repl_all AS kafka.receipt_repl
ENGINE = Distributed(shardless, kafka, receipt_repl, rand());