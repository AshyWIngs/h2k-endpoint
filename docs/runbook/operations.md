# Операции эксплуатации

Документ описывает рабочие процедуры для HBase 1.4.13 + Kafka 2.3.1. Подробные справочники и скрипты вынесены в отдельные файлы; здесь — быстрый план действий с перекрёстными ссылками.

## 1. Требования окружения

- Java 8 на RegionServer и Kafka CLI.
- На каждом RS лежит **толстый JAR** `h2k-endpoint-<версія>.jar` (внутри шейдятся Avro/Jackson/Gson/Confluent 5.3.8).
- Kafka clients 2.3.1, без SASL/SSL (безопасность обеспечивается инфраструктурой).
- Все новые ключи `h2k.*` документируются в [`docs/config.md`](../config.md).

## 2. Подготовка таблиц

1. Убедитесь, что нужные CF реплицируются (`REPLICATION_SCOPE=1`).
2. Если нужно, скорректируйте свойства `"h2k.cf.list"` в `.avsc` (эффективность фильтра контролируется автоматически, см. [`troubleshooting.md`](troubleshooting.md)).
3. Проверка: `describe 'TABLE'` и `status 'replication'`.

Подробные команды HBase shell — в [`docs/hbase.md`](../hbase.md).

## 3. Peer workflow

- **Добавление / профили** — используйте скрипты из `conf/` и раздел «Добавление peer» в [`docs/hbase.md`](../hbase.md#добавление-peer).
- **Редактирование CONFIG** — `update_peer_config` или `disable_peer → add_peer → enable_peer` (см. то же место).
- **Проверка конфигурации** — `cfg = get_peer_config 'peerId'; cfg['CONFIG'].select { |k,_| k.start_with?('h2k.') }`.
- **Управление** — `list_peers`, `show_peer_tableCFs`, `enable_peer`, `disable_peer`, `remove_peer` (подробности в `docs/hbase.md`).

**Быстрые команды (QA-профиль):**

```ruby
# Обновить параметры Avro + Confluent SR 5.3.8
update_peer_config 'h2k_balanced',
  { 'CONFIG' => {
      'h2k.avro.schema.dir'        => '/opt/hbase/conf/avro',
      'h2k.avro.sr.urls'           => 'http://10.254.3.111:8081,http://10.254.3.112:8081,http://10.254.3.113:8081',
      'h2k.avro.subject.strategy'  => 'table',
      'h2k.avro.subject.suffix'    => '-value'
    }
  }

# Применить настройки гарантированно
disable_peer 'h2k_balanced'
enable_peer  'h2k_balanced'

# Посмотреть итоговые h2k.*
cfg = get_peer_config 'h2k_balanced'
cfg['CONFIG'].select { |k,_| k.start_with?('h2k.') }
```

## 4. Раскатка и обновления

1. **Подготовка** — скопируйте новый JAR на один RS, отключите peer на остальных.
2. **Smoke-тест** — включите peer только на тестовых таблицах, убедитесь, что в Kafka появляются сообщения, а Schema Registry (если используется) регистрирует схему.
3. **Наблюдение** — следите за метриками (`status 'replication'`, `WalEntryProcessor.metrics()`, метрики BatchSender/TopicEnsurer, SR).
4. **Массовый rollout** — разложите JAR на остальные RS и включайте peer поочерёдно.
5. **Откат** — `disable_peer`, вернуть предыдущий JAR, перезапустить RegionServer.

## 5. Мониторинг

- `status 'replication'` — `SizeOfLogQueue`, `AgeOfLastShippedOp`.
- Метрики `TopicEnsurer` и `BatchSender` (через JMX или логи).
- Schema Registry: `curl $SR/subjects`, latency, ошибки 4xx/5xx.
- Лог `KafkaReplicationEndpoint` выводит throughput каждые 5 с (`Скорость WAL…`).

Добавление Prometheus JMX exporter — см. раздел «Сбор метрик» в [`docs/runbook/troubleshooting.md`](troubleshooting.md#сбор-метрик-через-prometheus).

## 6. Быстрая диагностика

- Логи: `journalctl -u hbase-regionserver -n 200` или выделенный аппендер `h2k_stdout` (см. troubleshooting).
- DEBUG для точечных пакетов (`kz.qazmarka.h2k.endpoint`, `...payload.serializer.avro`, `...schema.registry`) включается через log4j (инструкция — в [`docs/runbook/troubleshooting.md`](troubleshooting.md#отладочное-логирование-replicationendpoint)).
- Типичные проблемы и решения — в том же файле.

## 7. Полезные ссылки

- Конфигурация ключей: [`docs/config.md`](../config.md)
- Профили peer: [`docs/peer-profiles.md`](../peer-profiles.md)
- Работа с HBase shell: [`docs/hbase.md`](../hbase.md)
- Phoenix/Avro/Capacity: [`docs/phoenix.md`](../phoenix.md), [`docs/avro.md`](../avro.md), [`docs/capacity.md`](../capacity.md)
- Диагностика: [`docs/runbook/troubleshooting.md`](troubleshooting.md)

Документ держим коротким: если добавляется новая процедура — опишите её здесь одним абзацем и дайте ссылку на подробный гайд.
