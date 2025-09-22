# Операции эксплуатации

## Подготовка таблиц

Перед настройкой репликации убедитесь, что нужные Column Family (CF) включены в репликацию (параметр `REPLICATION_SCOPE=1`).

- Для каждой таблицы, которую требуется реплицировать (например, `HISTORY`, `RECEIPT`, `DOCUMENTS`), выполните:
  1. **Отключите таблицу**:
     ```ruby
     disable 'HISTORY'
     ```
  2. **Включите репликацию для нужного CF** (замените `cf_name` на реальное имя семейства колонок):
     ```ruby
     alter 'HISTORY', { NAME => 'cf_name', REPLICATION_SCOPE => 1 }
     ```
     Если CF несколько — повторите пункт для каждого.
  3. **Включите таблицу**:
     ```ruby
     enable 'HISTORY'
     ```
  4. Повторите для других таблиц (`RECEIPT`, `DOCUMENTS` и т.д.).

**Примечание:** проверить текущее значение можно командой `describe 'HISTORY'` (ищите `REPLICATION_SCOPE => 1` у нужных CF).

## Добавление peer

Для подключения нового peer используйте команду:
```ruby
add_peer 'peer1',
  { 'ENDPOINT_CLASSNAME' => 'kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint',
    'CONFIG' => {
      'h2k.kafka.bootstrap.servers' => 'host1:9092,host2:9092',
      'h2k.topic.pattern'           => '${table}',
      'h2k.cf.list'                 => 'd',
      'h2k.decode.mode'             => 'json-phoenix',
      'h2k.schema.path'             => '/opt/hbase-default-current/conf/schema.json'
    }
  }
```
- `<peer_id>` — уникальный идентификатор peer.
- `ENDPOINT_CLASSNAME` — всегда: `kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint`
- `CONFIG` — карта с ключами `h2k.*` (см. примеры профилей).

**Примеры профилей:**
- `conf/add_peer_shell_fast.txt` — профиль FAST (низкая задержка, минимальные гарантии)
- `conf/add_peer_shell_balanced.txt` — профиль BALANCED (компромисс между скоростью и надёжностью)
- `conf/add_peer_shell_reliable.txt` — профиль RELIABLE (максимальная надёжность, выше задержки)

## Управление peer

- **Список всех peers:**
    ```
    list_peers
    ```
- **Показать таблицы и CF, реплицируемые через peer:**
    ```
    show_peer_tableCFs '<peer_id>'
    ```
- **Включить peer:**
    ```
    enable_peer '<peer_id>'
    ```
- **Отключить peer:**
    ```
    disable_peer '<peer_id>'
    ```
- **Удалить peer:**
    ```
    remove_peer '<peer_id>'
    ```

### Редактирование существующего peer

```ruby
# Обновляем только вложенную карту CONFIG (наши h2k.*)
update_peer_config 'h2k_balanced',
  { 'CONFIG' => {
      'h2k.payload.format'  => 'avro',   # дефолт: json-eachrow
      'h2k.avro.schema.dir' => '/opt/hbase-default-current/conf/avro'
    }
  }
```

**Примеры быстрых правок:**
- Вернуть JSONEachRow:
  ```ruby
  update_peer_config 'h2k_balanced', { 'CONFIG' => { 'h2k.payload.format' => 'json-eachrow' } }
  ```
- Включить Avro (generic, локальные *.avsc):
  ```ruby
  update_peer_config 'h2k_balanced',
    { 'CONFIG' => {
        'h2k.payload.format'  => 'avro',
        'h2k.avro.schema.dir' => '/opt/hbase-default-current/conf/avro'
      }
    }
  ```
- (На будущее) Confluent‑режим с open‑source Schema Registry 5.3.8:
  ```ruby
  update_peer_config 'h2k_balanced',
    { 'CONFIG' => {
        'h2k.payload.format'        => 'avro',
        'h2k.avro.mode'             => 'confluent',
        'h2k.avro.schema.registry'  => 'http://sr1:8081,http://sr2:8081'
      }
    }
  ```
  Эти ключи начнут работать после включения соответствующего кода в endpoint.

Изменения конфигурации применяются онлайн, однако сам ReplicationEndpoint может кэшировать значения. Надёжный способ «принудить» перечитать конфиг:
```ruby
disable_peer 'h2k_balanced'
enable_peer  'h2k_balanced'
```

### Получение списка параметров peer

Для HBase 1.4.13 используйте команду:
```
get_peer_config 'h2k_balanced'
```
Команда вернёт всю карту параметров peer, включая вложенный раздел `CONFIG`. Чтобы в HBase Shell отфильтровать только наши ключи `h2k.*`, можно использовать небольшую JRuby‑выражение:
```ruby
cfg = get_peer_config 'h2k_balanced'
cfg['CONFIG'].select { |k,_| k.start_with?('h2k.') }
```

## Проверка статуса

- **Проверить статус репликации:**
    ```
    status 'replication'
    ```
    Вывод покажет состояние каждого peer, очереди, задержки, ошибки.

- **Интерпретация статуса:**
    - `PeerState=ENABLED` — репликация активна.
    - `SizeOfLogQueue` — размер очереди; если растёт, значит есть задержки.
    - Ошибки и stacktrace — ищите строки с ERROR.

## Проверка Avro/схем

- **Локальные *.avsc (generic Avro):**
  1) Убедитесь, что файл существует и читаем для пользователя HBase:
     ```bash
     ls -l /opt/hbase-default-current/conf/avro/tbl_jti_trace_cis_history.avsc
     ```
  2) Включите ключи:
     ```ruby
     update_peer_config 'h2k_balanced',
       { 'CONFIG' => {
           'h2k.payload.format'  => 'avro',
           'h2k.avro.schema.dir' => '/opt/hbase-default-current/conf/avro'
         }
       }
     ```
  3) Перезапустите peer (disable/enable) и смотрите логи RegionServer:
     ```bash
     journalctl -u h2k-endpoint.service -n 200 | grep -i avro
     ```

- **Confluent Schema Registry (open‑source 5.3.8):**
  1) Проверьте доступность SR:
     ```bash
     curl -s http://sr1:8081/subjects
     ```
  2) Включите ключи (после реализации поддержки в коде):
     ```ruby
     update_peer_config 'h2k_balanced',
       { 'CONFIG' => {
           'h2k.payload.format'        => 'avro',
           'h2k.avro.mode'             => 'confluent',
           'h2k.avro.schema.registry'  => 'http://sr1:8081,http://sr2:8081'
         }
       }
     ```

## Быстрая диагностика

- Ошибка чтения *.avsc (`NoSuchFileException` / "Файл не найден"): проверьте `h2k.avro.schema.dir`, права на каталог/файл и регистр имени таблицы (ожидается `<table>.avsc` в нижнем регистре).
- Все логи пишутся в `journald` (systemd).
    - Для просмотра последних сообщений:
        ```
        journalctl -u h2k-endpoint.service -n 200
        ```
- Для расширенной отладки включите DEBUG для пакета:
    - В конфиге log4j или через переменную окружения установите уровень DEBUG для `kz.qazmarka.h2k.endpoint`.
    - Пример для log4j:
        ```
        log4j.logger.kz.qazmarka.h2k.endpoint=DEBUG
        ```

## Подсказки по тюнингу

- Для снижения задержек:
    - Уменьшите `batch.size` (например, до 16384 или ниже).
    - Уменьшите `linger.ms` (например, до 5-10 мс).
    - Увеличьте `acks` до 1 или 0 для ускорения, но потеряете надёжность.
- Для повышения надёжности:
    - Установите `acks=all`.
    - Увеличьте `retries` (например, до 10).
    - Используйте профиль RELIABLE.
- Для сбалансированной работы:
    - Используйте профиль BALANCED.
    - Проверьте значения параметров в профиле и при необходимости скорректируйте под свою нагрузку.

**Важно:** Для детальной настройки параметров используйте примеры профилей в `conf/add_peer_shell_*.txt`.

## Шпаргалка команд HBase 1.4.13 (replication)

```ruby
list_peers
add_peer '<id>', { 'ENDPOINT_CLASSNAME' => '...', 'CONFIG' => { ... } }
get_peer_config '<id>'
update_peer_config '<id>', { 'CONFIG' => { ... } }
show_peer_tableCFs '<id>'
enable_peer  '<id>'
disable_peer '<id>'
remove_peer  '<id>'

# REPLICATION_SCOPE для CF:
disable 'TABLE'
alter   'TABLE', { NAME => 'cf_name', REPLICATION_SCOPE => 1 }
enable  'TABLE'
```

---

## Связанные документы

- [config.md](config.md) — параметры конфигурации endpoint
- [peer-profiles.md](peer-profiles.md) — описание профилей репликации
- [hbase.md](hbase.md) — подробности по работе с HBase
- [troubleshooting.md](troubleshooting.md) — диагностика и устранение проблем