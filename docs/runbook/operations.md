# Операции эксплуатации

## Требования окружения

- RegionServer'ы и Kafka-CLI работают на **Java 8** (ниже или выше не допускаются).
- На каждый RS раскладывается **толстый** `h2k-endpoint-*.jar` (внутри уже есть Avro/Jackson/Gson и Confluent Schema Registry 5.3.8).
- Kafka clients строго **2.3.1**, без SASL/SSL (по проектной договорённости безопасность обеспечивается внешней инфраструктурой).
- Конфигурация ReplicationEndpoint задаётся только ключами `h2k.*`; новые параметры документируем в `docs/` перед использованием.

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

### Фильтр по Column Family

- Ключ `h2k.cf.list` включает фильтрацию на уровне Endpoint: в Kafka попадают только ячейки из перечисленных
  CF. Если ключ не задан, фильтр выключен и реплицируются все семейства.
- Значения перечисляются через запятую. Пробелы по краям обрезаются, но регистр сохраняется — имя должно
  совпадать с тем, что возвращает `describe 'TABLE'`.
- Если указать несуществующее имя CF, записи из этого семейства будут отброшены (автоматика не понижает регистр).

## Добавление peer

Для подключения нового peer используйте команду:
```ruby
add_peer 'peer1',
  { 'ENDPOINT_CLASSNAME' => 'kz.qazmarka.h2k.endpoint.KafkaReplicationEndpoint',
    'CONFIG' => {
      'h2k.kafka.bootstrap.servers' => 'host1:9092,host2:9092',
      'h2k.topic.pattern'           => '${table}',
      'h2k.cf.list'                 => 'd',
      'h2k.decode.mode'             => 'phoenix-avro',
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
        'h2k.payload.format'  => 'avro-binary',
        'h2k.avro.mode'       => 'generic',
        'h2k.avro.schema.dir' => '/opt/hbase-default-current/conf/avro'
      }
    }
  ```
- Confluent Schema Registry 5.3.x:
  ```ruby
  update_peer_config 'h2k_balanced',
    { 'CONFIG' => {
        'h2k.payload.format'         => 'avro-binary',
        'h2k.avro.mode'              => 'confluent',
        'h2k.avro.schema.dir'        => '/opt/hbase-default-current/conf/avro',
        'h2k.avro.sr.urls'           => 'http://sr1:8081,http://sr2:8081',
        'h2k.avro.sr.auth.basic.username' => 'svc-hbase',
        'h2k.avro.sr.auth.basic.password' => '***'
      }
    }
  ```
  Схема перед отправкой в SR автоматически экранируется: спецсимволы (`\n`, `\u0001`, кавычки) кодируются
  по JSON-правилам, так что doc/description с управляющими символами не ломают регистрацию.
  Дополнительные варианты subject/авторизации см. в `docs/avro.md`.

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

## Сбор метрик через Prometheus

Ниже приведён проверенный способ снять метрики RegionServer’а вместе с показателями нашего эндпоинта. Все инструменты — бесплатные и open-source.

### 1. Подготовить JMX Exporter

1. Скачайте jar из официального репозитория Prometheus: `https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/` (для Java 8 подходит актуальная версия 0.17.x).
2. Скопируйте jar на каждую машину с RegionServer’ом, например в `/opt/jmx-exporter/jmx_prometheus_javaagent.jar`.
3. Создайте рядом файл конфигурации `hbase.yml` со списком метрик. Минимальный вариант:
   ```yaml
   startDelaySeconds: 0
   lowercaseOutputName: true
   lowercaseOutputLabelNames: true
   rules:
     - pattern: 'Hadoop<service=HBase,name=RegionServer,sub=Replication><>(.*)'
   ```
   При необходимости добавьте фильтры или переименования — формат описан в README проекта JMX Exporter.

### 2. Запустить RegionServer с javaagent

1. Откройте `hbase-env.sh` (находится в `${HBASE_HOME}/conf`).
2. Добавьте параметр к переменной `HBASE_REGIONSERVER_OPTS`:
   ```bash
   export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx-exporter/hbase.yml"
   ```
   *7071* — порт HTTP-эндпоинта. Выберите свободный порт и, при необходимости, пробросьте его в firewall.
3. Перезапустите RegionServer (rolling-режим разрешён, зоопарк/peer не страдает).

### 3. Проверка на месте

1. На сервере выполните:
   ```bash
   curl http://localhost:7071/metrics
   ```
2. В выводе должны появиться строки вида:
   - `wal_rowbuffer_upsizes` и `wal_rowbuffer_trims`
   - `producer_batch_flush_latency_max_ms`
   - стандартные метрики RegionServer’а (`hbase_regionserver_Server_activeHandlerCount` и т.д.).

Если доступ идёт через балансировщик, аналогично проверяем с рабочей станции, заменив `localhost` на реальное имя узла.

### 4. Подключить Prometheus

1. Добавьте job в `prometheus.yml`:
   ```yaml
   scrape_configs:
     - job_name: 'hbase-regionservers'
       scrape_interval: 15s
       metrics_path: /
       static_configs:
         - targets:
             - rs1.example.com:7071
             - rs2.example.com:7071
   ```
2. После перезагрузки Prometheus метрики будут доступны в UI и Alertmanager’е. Можно построить графики по латентности батчей, количеству усадок буфера и пр.

### 5. Замечания

- Наши счётчики регистрируются через `TopicManager`, поэтому автоматически попадают в JMX-поток RegionServer’а. Никаких дополнительных адаптеров внутри эндпоинта не требуется.
- При изменении набора метрик достаточно обновить `hbase.yml` и перезапустить RegionServer (либо использовать hot-reload, если он настроен средствами orchestration).
- Если нужно ограничить доступ, повесьте на порт 7071 firewall или сделайте reverse-proxy с basic auth — сам exporter работает в режиме read-only.

После подключения Prometheus рекомендуется завести дашборд: latency BatchSender’а, подтверждённые отправки, число расширений/усадок буфера строк, задержки Schema Registry. Это поможет оперативно реагировать на отклонения в нагрузке.

### Какие именно метрики мы публикуем

Метрики появляются в JMX (а через exporter — в Prometheus) под `Hadoop<service=HBase,name=RegionServer,sub=Replication>`. Все значения — `Long`. Ниже список ключей, которые добавляет наш код:

| Имя метрики | Что показывает | Комментарий |
|-------------|----------------|-------------|
| `wal.entries.total` | Общее число обработанных записей WAL | Сумма всех вызовов `WalEntryProcessor.process`. |
| `wal.rows.total` | Количество строк, отправленных в Kafka | Учитывает строки, прошедшие фильтр CF. |
| `wal.cells.total` | Число ячеек, обработанных и отправленных | Полезно для оценки среднего размера строки. |
| `wal.rows.filtered` | Строки, отброшенные фильтром CF | Если значение растёт, фильтр настроен слишком агрессивно. |
| `wal.rowbuffer.upsizes` | Сколько раз внутренний буфер строк расширялся сверх базовой ёмкости | Рост говорит о длинных WAL-записях; можно скорректировать `ROW_BUFFER_BASE_CAPACITY`. |
| `wal.rowbuffer.trims` | Сколько раз буфер принудительно усаживался после крупных строк | Высокое значение = много «жирных» строк, стоит проверить конфигурацию HBase. |
| `producer.batch.await.configured` | Текущее базовое значение `awaitEvery` из конфигурации | Полезно сверять с ожиданиями. |
| `producer.batch.await.current` | Адаптивный порог, к которому пришёл `BatchSender` | Если сильно отходит от базового значения, стоит задуматься о ручной настройке. |
| `producer.batch.flush.success.total` | Сколько раз `tryFlush`/`flush` завершались успешно | Служит для расчёта средней latency. |
| `producer.batch.flush.failures.total` | Сколько раз «тихий» сброс фиксировал ошибку | Если растёт — проверить логи продьюсера. |
| `producer.batch.records.confirmed.total` | Количество сообщений, которые Kafka подтвердила | Полезно для приблизительной оценки throughput. |
| `producer.batch.flush.latency.last.ms` | Длительность последнего успешного flush (мс) | Добавить на дашборд как instant-величину. |
| `producer.batch.flush.latency.max.ms` | Максимальная задержка flush со времени последнего reset | Контролируем хвост задержки. |
| `producer.batch.flush.latency.avg.ms` | Экспоненциальное среднее задержки flush (мс) | Плавный показатель для алертинга. |
| `schema.registry.register.success` | Сколько раз регистрация схемы прошла успешно | Метрика PayloadBuilder’а, полезна при массовых развертываниях. |
| `schema.registry.register.failures` | Ошибки регистрации схемы | При росте проверяйте доступность Schema Registry. |

Все значения доступны через JMX (например, в `jconsole`) и автоматически попадают в Prometheus после подключения exporter’а.

### Диагностика ensure.topics

- `TopicEnsurer#getMetrics()` возвращает неизменяемую карту с метриками ensure: счётчики `ensure.*`, `exists.*`,
  `create.*` и размер очереди backoff (`unknown.backoff.size`). Размер вычисляется без дополнительных снапшотов,
  поэтому можно опрашивать метод в цикле мониторинга.
- `TopicEnsurer#getBackoffSnapshot()` помогает увидеть, какие топики ждут повторного ensure. Карта неизменяема, а
  отрицательные значения автоматически обрезаются до `0` для удобства отображения.
- После прерывания `Thread.sleep` внутри ensure повторный create не выполняется: если в логах есть WARN о
  прерывании, инициируйте повторный ensure извне (перевызов API или `ensureTopics` для набора имён).

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
        'h2k.payload.format'  => 'avro-binary',
        'h2k.avro.schema.dir' => '/opt/hbase-default-current/conf/avro'
      }
    }
     ```
  3) Перезапустите peer (disable/enable) и смотрите логи RegionServer:
     ```bash
     journalctl -u h2k-endpoint.service -n 200 | grep -i avro
     ```

Дополнительно см. `docs/avro.md` для расширенного чек-листа по Avro.

- **Confluent Schema Registry (open‑source 5.3.8):**
  1) Проверьте доступность SR:
     ```bash
     curl -s http://10.254.3.111:8081/subjects
     ```
  2) Включите ключи (после реализации поддержки в коде):
     ```ruby
     update_peer_config 'h2k_balanced',
       { 'CONFIG' => {
           'h2k.payload.format'  => 'avro-binary',
           'h2k.avro.mode'       => 'confluent',
           'h2k.avro.schema.dir' => '/opt/hbase-default-current/conf/avro',
           'h2k.avro.sr.urls'    => 'http://10.254.3.111:8081,http://10.254.3.112:8081,http://10.254.3.113:8081'
         }
       }
     ```
  3) По умолчанию subject формируется как `namespace:table` (стратегия `table`). При необходимости старого поведения укажите `h2k.avro.props.subject.strategy=qualifier`.
  4) Размер кеша SR регулируется через `h2k.avro.props.client.cache.capacity`; рекомендуется держать ≥ `1024`, чтобы избежать повторных запросов при горячем профиле.

## Раскатка и мониторинг

1. **Staged rollout.** Скопируйте новый `h2k-endpoint-*.jar` только на один RegionServer, отключите peer на остальных (`disable_peer`) и включите репликацию на тестовых таблицах. Убедитесь, что SR выдал id и в логах нет ошибок сериализации.
2. **Метрики во время обкатки.**
   - `status 'replication'` → контролируем `SizeOfLogQueue` и `AgeOfLastShippedOp`.
   - `HRegionServer.GcTimeMillis` и размер heap (`jstat -gcutil <pid> 5s`) — не должно быть резких скачков.
   - Kafka-продьюсер: `ProducerMetrics` (`records-send-rate`, `request-latency-avg`, `record-error-rate`).
   - Schema Registry: `curl $SR/subjects` и `journalctl -u schema-registry` — исключаем 409/500 ошибки, следим за latency.
   - `TopicManager.getMetrics()` → `ensure.*`, `unknown.backoff.size`, `wal.entries.total`, `wal.rows.total`, `wal.rows.filtered`, `schema.registry.register.success/failures` — удобно отдавать в JMX.
   - `WalEntryProcessor.metrics()` для локального контроля и INFO-лог `Скорость WAL: ... строк/с=...` каждые ~5 секунд.
3. **Расширение.** Если в течение 1–2 часов производительность и задержки стабильны, разложите JAR на остальные RS, поочерёдно включайте peer и проверяйте метрики.
4. **Откат.** При проблемах:
   - `disable_peer 'h2k_balanced'` на всех RS.
   - Верните предыдущий JAR из `lib/backup`, перезапустите `hbase-regionserver`.
   - После стабилизации включите peer обратно.

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
    - Точечные логгеры:
        - `kz.qazmarka.h2k.schema.registry.avro.phoenix.AvroPhoenixSchemaRegistry` — путь к `.avsc`, PK, соль, capacity.
        - `kz.qazmarka.h2k.payload.serializer.avro.ConfluentAvroPayloadSerializer` — сравнение локальной схемы с SR и
          фиксация первой успешной/неуспешной регистрации.

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
