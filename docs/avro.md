# Avro: форматы, схемы, эксплуатация

Эндпоинт поддерживает два режима Avro-пейлоада:

- **Generic** — читаем локальные `.avsc` (подход для QA/отладки и как fallback).
- **Confluent** — регистрируем схемы в Confluent Schema Registry 5.3.8 (целевой продовый вариант).

JSONEachRow остаётся доступным для отладки и интеграций без Avro.

## 1. Выбор режима

| Ключ | Generic (`local .avsc`) | Confluent Schema Registry |
|---|---|---|
| `h2k.payload.format` | `avro-binary` или `avro-json` | **только** `avro-binary` |
| `h2k.avro.mode` | `generic` | `confluent` |
| `h2k.avro.schema.dir` | каталог с `.avsc` (по умолчанию `conf/avro`) | такой же каталог; нужен для валидации при старте |
| Доп. ключи | — | `h2k.avro.sr.urls`, `h2k.avro.subject.*`, `h2k.avro.sr.auth.*`, `h2k.avro.props.*` |

`h2k.payload.format=json-eachrow` возвращает систему в legacy-режим без Avro.

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

## 3. Конфигурация (основные ключи)

| Ключ | Значение по умолчанию | Описание |
|---|---|---|
| `h2k.payload.format` | `json-eachrow` | Формат пейлоада. Для Avro задайте `avro-binary` или `avro-json` (generic). |
| `h2k.serializerFactoryClass` | auto | При необходимости можно указать пользовательскую фабрику. |
| `h2k.avro.mode` | `generic` | Режим Avro. |
| `h2k.avro.schema.dir` | `conf/avro` | Каталог локальных `.avsc`. |
| `h2k.avro.sr.urls` | — | CSV адресов Schema Registry (Confluent). |
| `h2k.avro.subject.strategy` | `table` | Стратегия именования subject (см. ниже). |
| `h2k.avro.subject.prefix/suffix` | пусто | Дополнительный префикс/суффикс для subject. |
| `h2k.avro.props.client.cache.capacity` | `1000` | Размер кэша `CachedSchemaRegistryClient`. |
| `h2k.avro.sr.auth.basic.username/password` | — | Basic Auth при необходимости. |

Полный перечень параметров — в [`docs/config.md`](config.md).

### Subject strategy (Confluent)

По умолчанию используется стратегия `table` (`namespace:table`), для `DEFAULT` namespace — только qualifier. Доступны `qualifier`, `table-lower`, `table-upper`, а также `table`, `table-upper` и собственные префикс/суффикс (например, `-value`).

## 4. Чеклист включения Avro

1. **Разложить `.avsc`** в `conf/avro/` и убедиться, что заполнены `h2k.phoenixType`, `h2k.pk`, `h2k.saltBytes`, `h2k.capacityHint`.
2. **Изменить конфиг**:
   ```properties
   h2k.payload.format=avro-binary
   h2k.avro.mode=confluent            # или generic
   h2k.avro.schema.dir=/opt/hbase/conf/avro
   h2k.avro.sr.urls=http://sr1:8081,http://sr2:8081
   h2k.avro.subject.strategy=table-upper
   h2k.avro.subject.suffix=-value
   ```
3. **Перезагрузить peer** (`disable_peer` → `enable_peer`) либо перезапустить RegionServer.
4. **Проверить логи**: при старте `KafkaReplicationEndpoint` выводит строку `Payload: format=avro-binary, mode=...` и информацию об Avro-схеме.
5. **Проверить Kafka**:
   - generic: `kafka-console-consumer` + декодирование Avro (через `avro-tools`).
   - confluent: `curl $SR/subjects` — появился subject; consumer (ClickHouse) читает `FORMAT AvroConfluent`.

## 5. Интеграционные сценарии

| Сценарий | Действия | Ожидаемый результат |
|---|---|---|
| Generic (локальные схемы) | Положить `.avsc`, включить `avro-binary`. | Сообщения читаются через `avro-tools`, ClickHouse `FORMAT Avro`. |
| Confluent | Включить SR, задать `h2k.avro.sr.urls`. | Subject появляется в SR, ClickHouse `FORMAT AvroConfluent`. |
| Нет `.avsc` | Удалить схему и перезапустить peer. | `IllegalStateException` в логах; peer продолжает работу (fallback на JSON невозможен). |
| SR недоступен | Остановить SR → отправить событие. | WARN, повторные попытки; при долгой недоступности backlog растёт. |
| Обновление схемы | Изменить `.avsc`, поднять версию в SR. | Новая версия регистрируется, endpoint логирует предупреждение о несовместимости, если типы не совпали. |

## 6. Рекомендации по миграции (Roadmap)

1. **Этап 0** — фича-флаг (выполнено): ключи `h2k.payload.format`, `h2k.avro.mode`.
2. **Этап 1** — фабрика `PayloadSerializer` (выполнено).
3. **Этап 2** — поддержка Avro (generic) + маппинг Phoenix-типов (выполнено).
4. **Этап 3** — локальный реестр `.avsc` (выполнено, используется как fallback).
5. **Этап 4** — Confluent SR (в проде).
6. **Этап 5** — нагрузочные тесты под профилями FAST/BALANCED/RELIABLE.
7. **Этап 6** — эксплуатация: мониторинг SR, ClickHouse ingest.

Применяйте Avro пошагово: сначала generic на тестовом стенде, затем SR, затем производственный rollout (с записью схем в `h2k.capacity.hints`).

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
- Phoenix и schema.json: [`docs/phoenix.md`](phoenix.md)
- Подсказки ёмкости: [`docs/capacity.md`](capacity.md)
- ClickHouse ingest: [`docs/clickhouse.md`](clickhouse.md)
- Runbook: [`docs/runbook/operations.md`](runbook/operations.md), [`docs/runbook/troubleshooting.md`](runbook/troubleshooting.md)

Следите за актуальностью `.avsc` — добавление колонок в Phoenix требует обновить схему, пересчитать `capacityHint` и перезапустить peer.
