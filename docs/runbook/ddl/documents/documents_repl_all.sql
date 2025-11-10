-- Distributed витрина для DOCUMENTS
CREATE TABLE IF NOT EXISTS kafka.documents_repl_all AS kafka.documents_repl
ENGINE = Distributed(shardless, kafka, documents_repl, rand());