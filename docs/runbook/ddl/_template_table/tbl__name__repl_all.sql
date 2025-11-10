-- Distributed витрина для <NAME>
CREATE TABLE IF NOT EXISTS {db}.tbl_<name>_repl_all AS {db}.tbl_<name>_repl
ENGINE = Distributed shardless, {db}, tbl_<name>_repl, rand();