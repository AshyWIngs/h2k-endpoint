# Avro: форматы, схемы, эксплуатация

Эндпоинт работает в единственном режиме — Avro (Confluent Schema Registry 5.3.8). Локальные `.avsc` обязательны: они используются как источник Phoenix-метаданных, подсказок по CF/PK и как кеш перед регистрацией схем в SR. JSON/Generic режимы, а также ключи `h2k.payload.format` и `h2k.avro.mode` удалены.

## 1. Основные ключи

| Ключ | Значение по умолчанию | Комментарий |
|---|---|---|
| `h2k.avro.schema.dir` | `conf/avro` | Каталог локальных `.avsc`, читается при старте и служит fallback‑кэшем |
| `h2k.avro.sr.urls` | — | CSV адресов Schema Registry (`http://sr1:8081,http://sr2:8081`) |
| `h2k.avro.subject.strategy` | `table` | `table` → `namespace:table`, `table-upper/table-lower`, `qualifier` |
| `h2k.avro.subject.prefix/suffix` | пусто | Добавляется к subject (часто используют `-value`) |
| `h2k.avro.props.client.cache.capacity` | `1000` | Размер identity-map `CachedSchemaRegistryClient` |
| `h2k.avro.sr.auth.basic.username/password` | — | Basic Auth; значения маскируются в логах |

> Прочие `h2k.avro.props.*` прокидываются в конфигурацию Schema Registry клиента без изменений.

## 2. Требования к Avro-схемам

Схемы описывают **JSON-структуру**, которую собирает `PayloadBuilder`. Для корректной работы добавьте:

- На корневом уровне:
  - `"h2k.pk"` — массив имён PK в порядке Phoenix-таблицы.
  - `"h2k.saltBytes"` — длина соли rowkey (0..8).
  - `"h2k.capacityHint"` — ожидаемое число заполненных полей (для `h2k.capacity.hints`).
- Для каждой колонки:
  - `"h2k.phoenixType"` — точный Phoenix-тип (например, `UNSIGNED_INT`, `TIMESTAMP`).
  - `"h2k.jsonName"` (опц.) — если имя в JSON должно отличаться от колонки.

> Подробности по вычислению capacity и salt — в [`docs/capacity.md`](capacity.md) и [`docs/phoenix.md`](phoenix.md).

## 3. Subject strategy (Confluent)

По умолчанию используется стратегия `table` (`namespace:table`), для `DEFAULT` namespace — только qualifier. Доступны `qualifier`, `table-lower`, `table-upper`; при необходимости добавляйте собственные префикс/суффикс (например, `-value`). Полный перечень параметров см. в [`docs/config.md`](config.md).

## 4. Чеклист включения Avro

1. **Разложить `.avsc`** в `conf/avro/` и убедиться, что заполнены `h2k.phoenixType`, `h2k.pk`, `h2k.saltBytes`, `h2k.capacityHint`.
2. **Настроить ключи Schema Registry** (пример):
   ```properties
   h2k.avro.schema.dir=/opt/hbase/conf/avro
   h2k.avro.sr.urls=http://sr1:8081,http://sr2:8081
   h2k.avro.subject.strategy=table
   h2k.avro.subject.suffix=-value
   h2k.avro.sr.auth.basic.username=svc
   h2k.avro.sr.auth.basic.password=secret
   ```
3. **Перезагрузить peer** (`disable_peer` → `enable_peer`) либо перезапустить RegionServer.
4. **Проверить логи**: при старте `KafkaReplicationEndpoint` выводит строку `Payload: payload.format=AVRO_BINARY, serializer.class=..., schema.registry.urls=...`.
5. **Проверить Kafka/SR**: `curl $SR/subjects` — появился subject; `kafka-avro-console-consumer` (или ClickHouse `FORMAT AvroConfluent`) считывает сообщение с корректным PK.

## 5. Интеграционные сценарии

| Сценарий | Действия | Ожидаемый результат |
|---|---|---|
| Confluent | Включить SR, задать `h2k.avro.sr.urls`. | Subject появляется в SR, ClickHouse `FORMAT AvroConfluent`. |
| Нет `.avsc` | Удалить схему и перезапустить peer. | `IllegalStateException` в логах; peer продолжает работу (fallback невозможен). |
| SR недоступен | Остановить SR → отправить запись. | WARN, повторные попытки; при долгой недоступности backlog растёт. |
| Обновление схемы | Изменить `.avsc`, поднять версию в SR. | Новая версия регистрируется, endpoint предупреждает о несовместимости при расхождениях. |

## 6. Рекомендации по миграции (Roadmap)

1. **Этап 0 (архив)** — legacy режимы (`payload.format`, `avro.mode`) выключены.
2. **Этап 1** — единый горячий путь Avro Confluent, локальный реестр `.avsc` как кеш.
3. **Этап 2** — эксплуатация: мониторинг SR, бэкап `_schemas`, нагрузочные тесты профиля BALANCED (см. `conf/add_peer_shell_balanced.txt`).

Новая инсталляция пропускает этапы generic/JSON: сразу настраивайте Schema Registry и следите за подсказками `h2k.capacity.hints`.

## 7. Эксплуатация Schema Registry 5.3.8

- Развёртывание: tar от Confluent (Java 8). Проверьте конфиги `listeners`, `_schemas`, `compatibility.level=BACKWARD`.
- Запуск под systemd: `schema-registry-start/stop`, `Environment=SCHEMA_REGISTRY_HOME=/opt/confluent-default-current`.
- Создайте `_schemas` с `cleanup.policy=compact`, `RF=3`.
- Мониторинг: `GET /subjects`, `GET /subjects/<subj>/versions`, `GET /schemas/ids/<id>`.
- Настройте алерты на 5xx/latency SR, ошибки совместимости, рост `_schemas`.

Подробный пример конфигураций — см. [`docs/roadmap-avro.md`](roadmap-avro.md) (оставлен как архивный раздел, см. ниже).

## 8. Мониторинг и отладка

- Включайте DEBUG для пакета `kz.qazmarka.h2k.payload.serializer.avro` и `...schema.registry` (см. runbook). Логи показывают путь к схеме, версию, результат регистрации.
- Метрики `BatchSender`: `producer.batch.await.recommended`, `producer.batch.flush.latency.*` — следите за изменением задержки.
- В логе `TopicManager` фиксируется `payload.format`/`serializer`.
- Для ClickHouse используйте таск `SELECT * FROM kafka_table SETTINGS input_format_skip_unknown_fields=1` при тестах.

## 9. Архивные документы

Исторические заметки сохранены для совместимости ссылок:

- [`docs/integration-avro.md`](integration-avro.md) — теперь содержит краткую ссылку на чеклист (этот раздел).
- [`docs/roadmap-avro.md`](roadmap-avro.md) — сокращён до основных этапов (этот раздел).

## 10. Связанные материалы

- Конфигурация: [`docs/config.md`](config.md)
- Phoenix метаданные: [`docs/phoenix.md`](phoenix.md)
- Подсказки ёмкости: [`docs/capacity.md`](capacity.md)
- ClickHouse ingest: [`docs/clickhouse.md`](clickhouse.md)
- Runbook: [`docs/runbook/operations.md`](runbook/operations.md), [`docs/runbook/troubleshooting.md`](runbook/troubleshooting.md)

Следите за актуальностью `.avsc` — добавление колонок в Phoenix требует обновить схему, пересчитать `capacityHint` и перезапустить peer.
