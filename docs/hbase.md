# HBase shell / ZooKeeper / операции

Документ описывает, как работать с peer-репликацией для **HBase 1.4.13** и нашим эндпоинтом **`kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`**. Все команды и формат конфигов синхронизированы с README и примерами в `conf/`.

> Важное: приоритет источников конфигурации — **CONFIG в peer > `hbase-site.xml`**. Любой ключ `h2k.*`, заданный в peer, перекрывает одноимённый ключ из файла.

---

## Требования и окружение

- HBase **1.4.13** (ветка 1.4.x).
- ZK и RegionServers — штатная конфигурация HBase.
- Kafka clients **2.3.1** (на стороне эндпоинта), без SASL/SSL (по проектной договорённости).
- Файлы примеров для удобства:
  - `conf/add_peer_shell_fast.txt`
  - `conf/add_peer_shell_balanced.txt`
  - `conf/add_peer_shell_reliable.txt`
  - `conf/schema.json`
  - `conf/hbase-site.xml` (опциональный источник `h2k.*`)

---

## Базовые команды HBase shell

Поддерживаются стандартные команды репликации HBase 1.4.x:

- `add_peer 'peerId', { 'ENDPOINT_CLASSNAME' => '...', 'CONFIG' => { 'h2k.*' => '...' } }`
- `list_peers`
- `show_peer_tableCFs 'peerId'`
- `enable_peer 'peerId'`
- `disable_peer 'peerId'`
- `remove_peer 'peerId'`

Где:
- `peerId` — произвольный идентификатор (например, `h2k_fast`, `h2k_balanced`, `h2k_reliable`).
- `ENDPOINT_CLASSNAME` — ровно `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`.
- `CONFIG` — карта с ключами **`h2k.*`** (см. подробности в `docs/config.md`).

---

## Добавление peer (add_peer)

Рекомендуется использовать готовые профили:

- **FAST**: см. `conf/add_peer_shell_fast.txt`
- **BALANCED**: см. `conf/add_peer_shell_balanced.txt`
- **RELIABLE**: см. `conf/add_peer_shell_reliable.txt`

Пример общего вида (сокращённо):

```ruby
add_peer 'h2k_balanced',
  { 'ENDPOINT_CLASSNAME' => 'kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint',
    'CONFIG' => {
      'h2k.kafka.bootstrap.servers'   => '10.254.3.111:9092,10.254.3.112:9092,10.254.3.113:9092',
      'h2k.topic.pattern'             => '${table}',
      'h2k.cf.list'                   => '0,DOCUMENTS,b,d',
      'h2k.decode.mode'               => 'phoenix-avro',
      'h2k.schema.path'               => '/opt/hbase-default-current/conf/schema.json',
      'h2k.json.serialize.nulls'      => 'false',
      'h2k.payload.include.meta'      => 'false',
      'h2k.payload.include.meta.wal'  => 'false',
      'h2k.payload.include.rowkey'    => 'false',
      'h2k.rowkey.encoding'           => 'BASE64',
      # Соль и capacity берутся из conf/avro/*.avsc (h2k.saltBytes / h2k.capacityHint)
      'h2k.ensure.topics'             => 'true',
      'h2k.topic.partitions'          => '12',
      'h2k.topic.replication'         => '3',
      'h2k.admin.timeout.ms'          => '30000',
      'h2k.ensure.unknown.backoff.ms' => '5000',
      # Kafka Producer (pass-through + спец-ключи проекта)
      'h2k.producer.enable.idempotence'     => 'true',
      'h2k.producer.acks'                   => 'all',
      'h2k.producer.max.in.flight'          => '5',
      'h2k.producer.retries'                => '2147483647',
      'h2k.producer.request.timeout.ms'     => '30000',
      'h2k.producer.delivery.timeout.ms'    => '120000',
      'h2k.producer.linger.ms'              => '100',
      'h2k.producer.batch.size'             => '524288',
      'h2k.producer.compression.type'       => 'lz4',
      'h2k.producer.buffer.memory'          => '67108864'
    }
  }
```

> Ключи `h2k.*` описаны в `docs/config.md`. Матрица профилей — в `docs/peer-profiles.md`.

### Column Family и фильтрация

- Ключ `h2k.cf.list` задаёт список column family, которые endpoint будет реплицировать. Пустое значение → реплицируются все CF.
- Имена указываем через запятую без пробелов, в том же регистре, что возвращает Phoenix/HBase.
- Чтобы посмотреть фактические CF таблицы, используйте запрос к Phoenix `SYSTEM.CATALOG`:
  ```sql
  SELECT COALESCE(COLUMN_FAMILY,'0') AS CF,
         COLUMN_NAME,
         DATA_TYPE,
         ORDINAL_POSITION
  FROM SYSTEM.CATALOG
  WHERE TABLE_SCHEM IS NULL              -- или = '<namespace>'
    AND TABLE_NAME  = 'DOCUMENTS'
    AND COLUMN_NAME IS NOT NULL
  ORDER BY CF, ORDINAL_POSITION;
  ```
  Значение `CF='0'` соответствует колонкам, лежащим в «нулевом» family (часто совпадает с PK). Остальные строки — реальные CF, их и перечисляем в `h2k.cf.list` (например, для DOCUMENTS получаем `0,DOCUMENTS,b,d`).
- Эффективность фильтра endpoint контролирует автоматически: при доле отфильтрованных строк <1 % появится WARNING с рекомендацией пересмотреть список CF (см. `docs/runbook/troubleshooting.md`).

### Редактирование существующего peer

**Онлайн через `update_peer_config`:**

```ruby
# Обновляем только вложенную карту CONFIG (наши h2k.*)
update_peer_config 'h2k_balanced',
  { 'CONFIG' => {
      'h2k.payload.format'  => 'avro-binary',
      'h2k.avro.mode'       => 'generic',
      'h2k.avro.schema.dir' => '/opt/hbase/conf/avro'
    }
  }
```

Быстрые сценарии:
- Вернуть JSONEachRow:
  ```ruby
  update_peer_config 'h2k_balanced', { 'CONFIG' => { 'h2k.payload.format' => 'json-eachrow' } }
  ```
- Включить Confluent Schema Registry 5.3.x:
  ```ruby
  update_peer_config 'h2k_balanced',
    { 'CONFIG' => {
        'h2k.payload.format'         => 'avro-binary',
        'h2k.avro.mode'              => 'confluent',
        'h2k.avro.schema.dir'        => '/opt/hbase/conf/avro',
        'h2k.avro.sr.urls'           => 'http://sr1:8081,http://sr2:8081',
        'h2k.avro.sr.auth.basic.username' => 'svc-hbase',
        'h2k.avro.sr.auth.basic.password' => '***'
      }
    }
  ```
  Схема перед регистрацией в SR автоматически экранируется: управляющие символы (`\n`, `\u0001`, кавычки) переводятся в JSON-escape. Дополнительные варианты subject/авторизации описаны в [`docs/avro.md`](avro.md).

**Перечитать конфигурацию принудительно:**

```ruby
disable_peer 'h2k_balanced'
# внести правки (update_peer_config или add_peer)
enable_peer  'h2k_balanced'
```

Endpoint кэширует часть настроек; такой цикл гарантирует применение изменений.

**Проверить текущие ключи `h2k.*`:**

```ruby
cfg = get_peer_config 'h2k_balanced'
cfg['CONFIG'].select { |k,_| k.start_with?('h2k.') }
```

---

## Просмотр и управление peer

Список всех пиров:

```ruby
list_peers
```

Просмотр таблиц/CF, подключённых к конкретному пиру:

```ruby
show_peer_tableCFs 'h2k_balanced'
```

Включение/выключение репликации:

```ruby
enable_peer 'h2k_balanced'
disable_peer 'h2k_balanced'
```

Удаление пира:

```ruby
remove_peer 'h2k_balanced'
```

---

## Что попадает в Kafka и когда

- Репликация обрабатывает **только новые события**, появляющиеся **после создания peer** и его включения (`enable_peer`). Исторические данные из таблиц автоматически не подхватываются.
- Топики создаются/проверяются автоматом (если включён `h2k.ensure.topics=true`), имя строится по `h2k.topic.pattern` (для default‑namespace префикс не добавляется).
- Поля PK из `rowkey` **всегда** инъектируются в JSON (`ValueCodecPhoenix` → `PhoenixPkParser`). Добавлять их в `cf.list` не требуется.
- Формат сообщения — **JSONEachRow**; метаполя и `_rowkey` включаются флагами `h2k.payload.include.*` (см. `docs/config.md`).

---

## ZooKeeper: что важно знать

- Конфигурация peer хранится в ZooKeeper как часть состояния HBase (стандартный механизм репликации HBase 1.4.x).
- Для надёжного обновления настроек используйте цикл `disable_peer → update/add_peer → enable_peer` — так endpoint перечитает кэшированные значения.
- Прямые правки ZK **не требуются**.
- Идентификатор `peerId` должен быть уникален в кластере. При удалении/создании можно переиспользовать значение.

---

## Связанные документы

- [Конфигурация (все ключи)](config.md)
- [Профили peer (полная матрица)](peer-profiles.md)
- [Phoenix и `schema.json`](phoenix.md)
- [Подсказки ёмкости и метаданные](capacity.md)
- [Диагностика и типовые ошибки](runbook/troubleshooting.md)
