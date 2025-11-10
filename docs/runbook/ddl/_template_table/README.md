# Шаблон DDL для нового Kafka топика

Используйте этот каталог для быстрого создания полного набора из 7 объектов:

| № | Объект | Назначение |
|---|--------|-----------|
| 1 | <name>_kafka.sql | Источник (ENGINE=Kafka + AvroConfluent) |
| 2 | mv_<name>_to_raw.sql | Materialized View Kafka → RAW |
| 3 | <name>_raw.sql | RAW таблица (MergeTree + TTL ingest) |
| 4 | <name>_raw_all.sql | Distributed по RAW (шлюз в репликацию) |
| 5 | <name>_repl.sql | Реплицируемая витрина (ReplacingMergeTree + TTL business) |
| 6 | <name>_repl_all.sql | Distributed по витрине (точка аналитики) |
| 7 | mv_<name>_to_repl.sql | Materialized View RAW → витрина |

## Общие принципы
- Время в Avro (epoch millis) хранится как Int64 в Kafka источнике и преобразуется в DateTime64(3,'UTC') в RAW/витрине.
- Логическое удаление boolean → UInt8 для единообразия.
- Сортировка включает метаданные Kafka (`__kafka_partition`, `__kafka_offset`) и ключ бизнес-слоя.
- TTL RAW < TTL витрины для экономии места.
- ReplacingMergeTree использует версию `ver` (DateTime now()) для дедупликации повторных вставок.

## Порядок действий при добавлении топика
1. Получить Avro-схему и определить PK.
2. Сформировать Kafka-таблицу: все поля как исходные типы Avro (long, string, boolean) + метаданные `_event_ts`, `_delete`.
3. Создать MV Kafka→RAW с маппингом типов и преобразованием времени.
4. Создать RAW таблицу: добавить служебные поля `__kafka_*` и `__ingest_ts`.
5. Создать Distributed RAW на кластере shardless.
6. Создать реплицируемую витрину с расширенным ORDER BY и TTL.
7. Создать Distributed витрину для конечного доступа.
8. Создать MV RAW→витрина с прямым копированием и вычислением `ver`.
9. Проверить оффсеты и отсутствие дыр (аудит). Запустить мониторинг.
10. Добавить ссылки в runbook (раздел 4.x).

## Плейсхолдеры
Замените `<name>` на системное имя (используйте единообразие в нейминге).

## Быстрый аудит после деплоя
```sql
SELECT __kafka_partition, (max(__kafka_offset)-min(__kafka_offset)+1)-countDistinct(__kafka_offset) AS missing
FROM kafka.<name>_raw GROUP BY __kafka_partition HAVING missing>0 ORDER BY missing DESC;
```

## Уведомление
Любые изменения Avro-схемы требуют: (1) обновления Kafka-источника (если добавлены публичные поля), (2) перегенерации MV-конверсий, (3) валидации TTL и ORDER BY.
