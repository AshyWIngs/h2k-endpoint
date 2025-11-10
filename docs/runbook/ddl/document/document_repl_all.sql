-- Distributed витрина для DOCUMENT
CREATE TABLE IF NOT EXISTS kafka.document_repl_all AS kafka.document_repl
ENGINE = Distributed(shardless, kafka, document_repl, rand());