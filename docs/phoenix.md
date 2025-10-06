# Phoenix и источники метаданных

Phoenix остаётся основным источником схемы для декодирования WAL. Эндпоинт использует два взаимодополняющих набора метаданных:

1. **Avro-схемы (`conf/avro/*.avsc`)** — основной источник в режимах `h2k.payload.format=avro-*`.
2. **`schema.json`** — исторический JSON-файл, который служит fallback-источником и используется в JSONEachRow.

## 1. Что должно быть описано

| Компонент | Назначение |
|---|---|
| PK (`h2k.pk` или `primaryKey:true`) | Позволяет `PhoenixPkParser` восстановить ключ из rowkey и добавить PK в payload. |
| Тип (`h2k.phoenixType` или `type`) | Нужен `PhoenixColumnTypeRegistry` и `ValueCodecPhoenix` для корректного декодирования. |
| Соль (`h2k.saltBytes`) | Позволяет правильно разбить rowkey на соль+PK (Phoenix salt). У таблиц без соли ставьте `0`. |
| capacity (`h2k.capacityHint`) | Используется `TableCapacityObserver` и `h2k.capacity.hints`. |

### Как проверить соль в Phoenix

```sql
SELECT SALT_BUCKETS
FROM SYSTEM.CATALOG
WHERE TABLE_SCHEM IS NULL            -- или = '<namespace>'
  AND TABLE_NAME = 'DOCUMENTS'
  AND COLUMN_NAME IS NULL;           -- строка уровня таблицы
```

- Пустая колонка `SALT_BUCKETS` (или значение `NULL/0`) означает, что соль не используется — указывайте `"h2k.saltBytes": 0` в `.avsc`.
- Любое положительное значение (например, `10`) означает, что таблица солится одним байтом (Phoenix всегда добавляет ровно 1 байт для SALT_BUCKETS > 0) — в схеме пишем `"h2k.saltBytes": 1`.

Храните это значение в git вместе со схемой, чтобы расчёт PK и разбор rowkey оставались корректными.

## 2. Приоритеты источников

1. **Avro** (если `h2k.payload.format=avro-*`) — единственный источник, `schema.json` игнорируется.
2. **schema.json** (если Avro отключён) — загружается через `JsonSchemaRegistry`.
3. При отсутствии описания таблицы → WARN в логах и пропуск данных.

## 3. Обновление схемы

1. Изменили таблицу в Phoenix → обновите `.avsc` или `schema.json`.
2. Пересчитайте `h2k.capacity.hints` (см. [`docs/capacity.md`](capacity.md)).
3. Выполните `disable_peer / enable_peer` для подхвата новых метаданных.

## 4. Внутренние компоненты

- **`ValueCodecPhoenix`** — orchestrator декодирования:
  - `PhoenixColumnTypeRegistry` нормализует типы и кеширует `PDataType`.
  - `PhoenixValueNormalizer` приводит времени к epoch millis, массивы → `List`.
  - `PhoenixPkParser` извлекает PK из rowkey (учёт соли и Phoenix escape).
- **`RowKeySlice`** — обёртка для rowkey с доступом к offset/length (без копий).
- **`Decoder`** — интерфейс для модульного декодирования значения/rowkey.

## 5. Практика

- Храните `.avsc` и `schema.json` в git, синхронно с изменениями Phoenix.
- Проверяйте соответствие типов: fixed-size (например, `UNSIGNED_INT`) должны иметь корректную длину, иначе `ValueCodecPhoenix` выбросит `IllegalStateException`.
- PK автоматически добавляются в JSON/Avro payload; не надо повторно перечислять их в `h2k.cf.list`.
- При отладке включайте DEBUG для `kz.qazmarka.h2k.schema` (см. runbook).

## 6. Связанные документы

- Avro и схема: [`docs/avro.md`](avro.md)
- Подсказки ёмкости: [`docs/capacity.md`](capacity.md)
- Конфигурация ключей: [`docs/config.md`](config.md)

Содержите источники метаданных в актуальном состоянии — всё декодирование строится на этих описаниях.
