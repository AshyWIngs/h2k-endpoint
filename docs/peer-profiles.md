# Профиль peer (экспорт из `conf/add_peer_shell_balanced.txt`)

Документ фиксирует актуальные параметры единого поддерживаемого профиля BALANCED.

---

## BALANCED: Avro по умолчанию

- Формат: Avro binary через Confluent Schema Registry (`h2k.avro.sr.urls`, стратегия subject `table`).
- Топик: `${table}`; метаданные таблиц (PK, соль, `capacityHint`) берутся из `.avsc`.
- Kafka Producer: `acks=all`, `enable.idempotence=true`, `max.in.flight=5`, `linger.ms=100`, `batch.size=524288`, `compression.type=lz4`, `delivery.timeout.ms=300000`, `await.every=500`, `await.timeout.ms=300000`.
- TopicEnsurer: `h2k.ensure.topics=true`, `h2k.topic.partitions=12`, `h2k.topic.replication=3`, `h2k.admin.timeout.ms=30000`.
- Мониторинг: `h2k.jmx.enabled=true` (регистрируем `H2kMetricsJmx`), `h2k.observers.enabled=false` (WalDiagnostics отключены в проде).
- Payload метаполя выключены (`include.meta`, `include.meta.wal`, `include.rowkey` = `false`).
- Буферы/лимиты: `h2k.producer.buffer.memory=268435456`, `h2k.producer.max.request.size=2097152`.

Полный скрипт с комментариями — `conf/add_peer_shell_balanced.txt`. Разбор `h2k.*` ключей — в [docs/config.md](docs/config.md); порядок действий по настройке peer — в [docs/hbase.md](docs/hbase.md).
