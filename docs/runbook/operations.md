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
3. **Наблюдение** — следите за метриками (`status 'replication'`, `WalEntryProcessor.metrics()`, метрики TopicEnsurer, SR).
4. **Массовый rollout** — разложите JAR на остальные RS и включайте peer поочерёдно.
5. **Откат** — `disable_peer`, вернуть предыдущий JAR, перезапустить RegionServer.

### 4.1 Стендовый прогон после обновлений Jackson/Guava

- **Контроль версий** — убедитесь, что на стенде разложен JAR, собранный с `jackson-*-2.17.2` и `guava-24.1.1-jre` (`java -jar h2k-endpoint-*.jar --version` выводит список шейдженных библиотек в логах при старте RS).
- **Schema Registry** — выполните повторную регистрацию схемы по тестовой таблице (`KafkaReplicationEndpointIntegrationTest` сценарий на стенде: `produce int_test_table`). Логи `ConfluentAvroPayloadSerializer` не должны содержать `No schema registered under subject!` после первой успешной регистрации.
- **WAL hot-path** — прогоните нагрузочный сценарий (5–10 минут WAL репликации) и сравните метрики `WalCounterService` и `EnsureCoordinator` с предыдущими запусками; всплесков ошибок сериализации быть не должно.
- **Kafka потребители** — на тестовом consumer убедитесь, что гуавовские структуры (например, `ImmutableMap` в payload) корректно сериализуются и читаются. При необходимости запустите smoke-консьюмер `kafka-console-consumer` и проверьте, что сообщения десериализуются avro-tools без ошибок.
- **Регресс при ensure** — проверьте, что ensure-топики создаются с прежними параметрами, задержек `slowCreate`/таймаутов не прибавилось.
- **Фиксация результатов** — если все проверки прошли, отметьте выполнение пункта 13 в [`refactor-progress.instructions.md`](../../.github/instructions/refactor-progress.instructions.md) и сохраните ссылку на Grafana/логи в вики команды.

#### 4.1.1 Подготовка перед прогоном

- Снимите baseline метрики (`WalCounterService.entriesTotal`, `TopicEnsurer.ensure.success`) за последний успешный релиз.
- Обновите стенд до артефакта, собранного с `mvn -DskipTests clean package` после локальных `mvn test`.
- Проверьте доступность всех узлов Schema Registry (`curl -sf $URL/config`).
- Очистите тестовые топики (`kafka-topics --bootstrap-server ... --delete --topic <test-topic>`) при необходимости, чтобы исключить старые сообщения.
- Зафиксируйте commit hash и версию JAR в журнале сопровождения.

#### 4.1.2 Пошаговый сценарий

1. **Smoke до старта** — на одном RS отключите peer, очистите кэш Confluent (`schema-registry-cli subjects delete --hard default:INT_TEST_TABLE`) и выполните тестовую публикацию `bin/h2k-smoke.sh int_test_table`.
2. **Регистрация схемы** — убедитесь по логам `ConfluentAvroPayloadSerializer` в качестве INFO-сообщения, что идентификатор схемы получен и fingerprint совпадает (`fingerprint` WARN отсутствует).
3. **Нагрузочный прогон** — включите peer на стенде, подайте WAL-нагрузку `h2k-wal-driver` минимум на 10 минут. Следите, что `schema.registry.register.failures` не растёт.
4. **Валидация потребителей** — запустите `kafka-console-consumer` (или боевого consumer в dry-run) и `avro-tools frombinary` для нескольких сообщений.
5. **Ensure контроль** — запросите `TopicEnsurer` метрики (`/metrics` или JMX), проверьте отсутствие повторных попыток создания топиков.
6. **Завершение** — верните peer в исходное состояние, задокументируйте результаты в журнале.

#### 4.1.3 Шаблон фиксации результатов

```
Дата: 2025-10-__
Commit/JAR: <hash> / h2k-endpoint-<version>.jar
SR URLs: http://...
Smoke тест: ok (schema id=___, fingerprint match=yes/no)
Нагрузка 10 мин: ok (entries/s=___, ошибок=___)
Consumer проверка: ok (avro-tools / console)
Ensure: ok (create retries=0)
Примечания: <ссылки на Grafana/логи>
Ответственный: <ФИО>
```

#### 4.1.4 Локальная репетиция перед стендом

- Выполните локально `mvn test` и `mvn -DskipTests clean package` — убедитесь, что `KafkaReplicationEndpointIntegrationTest` проходит.
- Прогоните `./codacy-analyze.sh` и убедитесь в отсутствии замечаний.
- При необходимости перегенерируйте тестовые схемы командой `./scripts/refresh-test-avro.sh` (если схема менялась).

### 4.2 Горячий путь replicate()

1. **`ReplicationExecutor`** — точка входа из `ReplicationEndpoint.replicate()`. Пустые батчи (`entries.isEmpty()`) завершаются немедленно без обращения к Kafka.
2. **`ReplicationBatch`** — для непустых списков формирует `BatchSender` на основе `ProducerAwaitSettings` (`awaitEvery`, `awaitTimeoutMs`). Значения логируются на уровне DEBUG, что помогает подтвердить актуальность конфигурации.
3. **`WalEntryProcessor.process(entry, sender)`**:
   - Разрешает имя топика, при необходимости вызывает ensure-топик.
   - Всегда читает метаданные WAL (`sequenceId`, `writeTime`) и передаёт их дальше (флаг `includeWalMeta` больше не используется).
   - Собирает CF-фильтр, группирует ячейки по rowkey и добавляет futures в `BatchSender`.
4. **`BatchSender`** — накапливает Futures и блокирующе вызывает `flush()` при достижении порога `awaitEvery`. При закрытии (`try-with-resources`) гарантирует обработку оставшихся записей.
5. **Обработка ошибок** — любые исключения ловит `ReplicationExecutor.failReplicate(...)`: пишет WARN/ERROR (stacktrace в DEBUG) и обновляет метрики `replicate.failures.total` и `replicate.last.failure.epoch.ms`. Эти показатели видны через `TopicManager.getMetrics()` и JMX (`H2kMetricsJmx`).

> Админам: при анализе инцидентов фиксируйте DEBUG‑лог `"Репликация: записей=..., awaitEvery=..., awaitTimeoutMs=..."` и текущее значение `replicate.failures.total` — это ускоряет поиск узкого места.

## 5. Мониторинг

- `status 'replication'` — `SizeOfLogQueue`, `AgeOfLastShippedOp`.
- Метрики `TopicEnsurer` (через JMX или логи).
- `TopicManager.getMetrics()`/JMX: отслеживайте `wal.rowbuffer.upsizes` (широкие строки требуют >32 ячеек) и `wal.rowbuffer.trims` (гигантские строки ≥4096 ячеек). Эти счётчики должны расти медленно; резкий рост указывает, что CF-фильтр пропускает слишком много данных.
- Schema Registry: `curl $SR/subjects`, latency, ошибки 4xx/5xx.
- При падении SR Endpoint продолжает отправку с последним зарегистрированным `schemaId` и запускает фоновые
  попытки повторной регистрации (экспоненциальный бэкофф до 8 шагов). Следите за счётчиком
  `SchemaRegistryMetrics.registrationFailures` и логами `Schema Registry недоступен, использую ранее зарегистрированный id`.
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
