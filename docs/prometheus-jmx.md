## Экспорт метрик H2K в Prometheus через JMX

Этот документ описывает, как экспортировать метрики H2K (репликация HBase→Kafka) в Prometheus
с использованием JMX и официального JMX Exporter от Prometheus.

Поддерживаются два источника метрик:
- Метрики ensure и дополнительные счётчики из `TopicManager.getMetrics()`;
- Новые метрики репликации на уровне эндпоинта: `репликация.ошибок.всего` и `репликация.последняя.ошибка.epoch.ms`.

Начиная с этой версии, метрики H2K экспонируются через JMX в виде DynamicMBean:
- ObjectName: `kz.qazmarka.h2k:type=Endpoint,name=H2KMetrics`
- Атрибуты (read‑only): нормализованные имена метрик, значения — `Long`
- Регистрация MBean управляется флагом `h2k.jmx.enabled` (дефолт `true`). Не выключайте его на тех peers, где нужны метрики.

Примеры атрибутов:
- ensure.invocations.total → ensure_invocations_total
- ensure.invocations.accepted → ensure_invocations_accepted
- ensure.invocations.rejected → ensure_invocations_rejected
- ensure.cache.hit → ensure_cache_hit
- ensure.batch.count → ensure_batch_count
- создание.успех → алиас create.ok → create_ok
- ensure.бэкофф.размер → алиас unknown.backoff.size → unknown_backoff_size
- репликация.ошибок.всего → алиас replicate.failures.total → replicate_failures_total
- репликация.последняя.ошибка.epoch.ms → алиас replicate.last.failure.epoch.ms → replicate_last_failure_epoch_ms

#### Таблица JMX‑алиасов (ключ → алиас → атрибут)

- wal.записей.всего → wal.entries.total → wal_entries_total
- wal.строк.всего → wal.rows.total → wal_rows_total
- wal.ячеек.всего → wal.cells.total → wal_cells_total
- wal.строк.отфильтровано → wal.rows.filtered → wal_rows_filtered
- wal.rowbuffer.расширения → wal.rowbuffer.upsizes → wal_rowbuffer_upsizes
- wal.rowbuffer.сжатия → wal.rowbuffer.trims → wal_rowbuffer_trims
- ensure.вызовов.всего → ensure.invocations.total → ensure_invocations_total
- ensure.вызовов.принято → ensure.invocations.accepted → ensure_invocations_accepted
- ensure.вызовов.отклонено → ensure.invocations.rejected → ensure_invocations_rejected
- существует.да → exists.true → exists_true
- существует.нет → exists_false → exists_false
- существует.неизвестно → exists.unknown → exists_unknown
- создание.успех → create.ok → create_ok
- создание.гонка → create.race → create_race
- создание.ошибка → create.fail → create_fail
- ensure.бэкофф.размер → unknown.backoff.size → unknown_backoff_size
- ensure.подтверждено.тем → state.ensured.count → state_ensured_count
- ensure.очередь.ожидает → queue.pending → queue_pending
- ensure.пропуски.из-за.паузы → ensure.cooldown.skipped → ensure_cooldown_skipped
- sr.регистрация.успехов → schema.registry.register.success → schema_registry_register_success
- sr.регистрация.ошибок → schema.registry.register.failures → schema_registry_register_failures
- sr.повторные.попытки.очередь → schema.registry.pending.retries → schema_registry_pending_retries
- репликация.ошибок.всего → replicate.failures.total → replicate_failures_total
- репликация.последняя.ошибка.epoch.ms → replicate.last.failure.epoch.ms → replicate_last_failure_epoch_ms

Обратите внимание: в JMX имена атрибутов нормализованы (все небуквенно‑цифровые символы заменяются на `_`).

### Почему НЕ встраиваем HTTP‑экспортёр в библиотеку

H2K — это библиотека/плагин для HBase RegionServer. Чтобы не влиять на стабильность HBase 1.4.13 и упростить сопровождение,
мы сознательно НЕ поднимаем внутри H2K ни отдельный HTTP‑сервер, ни «агента» экспорта метрик. Вместо этого:

- Библиотека публикует метрики только через JMX (пассивно, без потоков и сетевых сокетов).
- Экспорт в Prometheus выполняет уже используемый на RegionServer JMX Exporter (как javaagent) или внешний standalone процесс.

Плюсы такого подхода для эксплуатации:
- Нулевое влияние на lifecycle RegionServer: нет дополнительных портов, потоков и зависимостей внутри RS.
- Минимум конфигурации: достаточно добавить правило для нашего ObjectName в существующий jmx_exporter.
- Прозрачный rollback: удаление правила/перезапуск агента полностью возвращает прежнее состояние.

Когда может понадобиться «встроенный» экспортёр? Практически никогда в HBase 1.4.13. Это добавит риски (портовые конфликты,
обновления зависимостей, безопасность) и усложнит поддержку. Рекомендуем всегда использовать JMX → jmx_exporter.

### 1. Включение JMX на RegionServer/процессе с H2K

Убедитесь, что Java‑процесс, в котором работает H2K (обычно HBase RegionServer), запущен с включённым JMX.
Минимальный набор параметров JVM:

```
-Dcom.sun.management.jmxremote
-Dcom.sun.management.jmxremote.port=9010
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false
```

Для HBase 1.4.13 добавьте параметры в переменную окружения `HBASE_OPTS` в скриптах запуска или в соответствующем сервис‑юните systemd.

Безопасность: в бою рекомендуется включать аутентификацию JMX и ограничивать доступ по файрволу/VPN. Приведённый пример — для стенда/внутренней сети.

### 2. JMX Exporter (процесс‑агент)

Самый простой способ — запустить JMX Exporter в режиме standalone (Java‑процесс), который будет подключаться по RMI к вашему процессу с H2K.

Пример конфигурации `jmx_exporter_h2k.yml`:

```
startDelaySeconds: 0
hostPort: 127.0.0.1:9010
ssl: false
lowercaseOutputName: true
lowercaseOutputLabelNames: true
whitelistObjectNames:
  - 'kz.qazmarka.h2k:type=Endpoint,name=H2KMetrics*'
rules:
  # Общие правила для всех числовых атрибутов нашего MBean
  - pattern: 'kz.qazmarka.h2k<type=Endpoint, name=H2KMetrics(?:, uid=[^>]*)?>:(.*)'
    name: h2k_$1
    type: GAUGE
    labels: {}
    help: 'H2K metric $1 exported from JMX'
```

Запуск JMX Exporter:

```
java -jar jmx_prometheus_httpserver.jar 9404 jmx_exporter_h2k.yml
```

- 9404 — порт HTTP‑эндпойнта для метрик Prometheus (подберите свободный порт);
- jmx_exporter_h2k.yml — файл конфигурации из примера выше.

Проверка: откройте `http://localhost:9404/metrics` и убедитесь, что появились метрики `h2k_*`.

Примеры ожидаемых метрик:
- h2k_ensure_invocations_total
- h2k_ensure_invocations_accepted
- h2k_ensure_invocations_rejected
- h2k_ensure_cache_hit
- h2k_ensure_batch_count
- h2k_exists_true / h2k_exists_false / h2k_exists_unknown
- h2k_create_ok / h2k_create_race / h2k_create_fail
- h2k_unknown_backoff_size
- h2k_replicate_failures_total
- h2k_replicate_last_failure_epoch_ms

Если на RegionServer уже установлен jmx_prometheus_javaagent, просто добавьте приведённое выше правило
в текущий конфигурационный файл агента и перезапустите RS (или агент). Никаких изменений в коде/либе H2K не требуется.

Быстрый старт (standalone httpserver):

1) Включите JMX на RS (порт по умолчанию в примере — 9010).
2) Скопируйте `conf/jmx/h2k_jmx_standalone.yml` на хост RS и запустите экспортёр:

```
java -jar jmx_prometheus_httpserver.jar 9404 conf/jmx/h2k_jmx_standalone.yml
```

3) Проверьте метрики по адресу `http://localhost:9404/metrics`.

### 3. JMX Exporter (JavaAgent) — альтернативный вариант

Можно запускать JMX Exporter как JavaAgent внутри того же процесса, где работает H2K (RegionServer).
Пример параметров JVM:

```
-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=9404:/opt/jmx/h2k_jmx.yml
```

Пример `h2k_jmx.yml` аналогичен `jmx_exporter_h2k.yml` выше, но без `hostPort`.

Примечание: для HBase 1.4.x это самый распространённый и безопасный способ. Встроенные Prometheus‑sink для Hadoop Metrics2
актуальны в новых линейках, но для H2K достаточно JMX.

Быстрый старт (RS javaagent):

1) Скопируйте на хост RS файл `conf/jmx/h2k_jmx_javaagent.yml` из репозитория.
2) Добавьте в `HBASE_OPTS` (или unit‑файл) параметр:

```
-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=9404:/opt/jmx/h2k_jmx_javaagent.yml
```

3) Перезапустите RegionServer и проверьте `http://<host>:9404/metrics`.

### 4. Конфигурация Prometheus

Добавьте job в `prometheus.yml`:

```
scrape_configs:
  - job_name: 'h2k-endpoint'
    scrape_interval: 15s
    static_configs:
      - targets: ['h2k-host-1:9404', 'h2k-host-2:9404']
```

Замените `h2k-host-*` на реальные хосты RegionServer или место, где запущен standalone JMX Exporter.

### 5. Карта всех метрик H2K

Метрики ensure (Kafka топики):
- ensure.invocations.total — все обращения к ensure (включая отклонённые)
- ensure.invocations.accepted — число реально запущенных ensure (совпадает со старым ensure.invocations)
- ensure.invocations.rejected — фильтры кандидатов (пустые имена, невалидные, backoff)
- ensure.cache.hit — попадания кеша ensure
- ensure.batch.count — количество топиков, обработанных через batch ensure
- существует.да / существует.нет / существует.неизвестно — результаты проверки существования топика
- создание.успех / создание.гонка / создание.ошибка — исходы создания топика
- ensure.бэкофф.размер — размер очереди отложенных ensure
- ensure.подтверждено.тем — число тем в локальном кеше «подтверждённых» ensure
- ensure.очередь.ожидает — размер очереди фонового EnsureExecutor

Метрики WAL/построения payload:
- wal.записей.всего — обработанные записи WAL
- wal.строк.всего — обработанные строки
- wal.ячеек.всего — обработанные ячейки
- wal.строк.отфильтровано — отброшенные строки CF‑фильтром
- wal.rowbuffer.расширения — увеличения буфера строк
- wal.rowbuffer.сжатия — усадки буфера строк

`wal.rowbuffer.расширения` инкрементируется, когда для новой строки требуется вместить больше базовых 32 ячеек: это сигнал, что встречаются широкие строки и горячий путь временно увеличил `ArrayList`. `wal.rowbuffer.сжатия` растёт при обработке строк из ≥4096 ячеек, после чего буфер принудительно ужимается обратно до 32 элементов. Если значения быстро накапливаются, проверьте схемы и фильтры CF: возможно, из репликации не исключены архивные или «широкие» таблицы, что способно увеличить давление на GC.

Метрики Schema Registry:
- sr.регистрация.успехов — успешные регистрации схем
- sr.регистрация.ошибок — ошибки регистрации схем
- sr.повторные.попытки.очередь — текущее число ожидающих повторных попыток регистрации (gauge)

**Про метрику `sr.повторные.попытки.очередь`:**
Отслеживает количество задач повторной регистрации схем, ожидающих в очереди. При стабильной работе значение должно быть 0.
Если регулярно превышает, например, 50 из максимума 100, это указывает на длительный offline Schema Registry.
Максимум можно отрегулировать параметром `h2k.avro.max.pending.retries` (см. `docs/config.md`).
Когда очередь переполнена, новые попытки отклоняются логированием `Очередь переполнена`, но основная репликация продолжается.

Новые метрики отказов репликации:
- репликация.ошибок.всего — суммарное число отказов `replicate()`
- репликация.последняя.ошибка.epoch.ms — отметка времени последней ошибки (epoch ms)

Все эти метрики доступны как атрибуты MBean `H2KMetrics` и автоматически экспортируются
JMX Exporter по правилу из примера.

### 6. Частые вопросы (FAQ)

- Могу ли я увидеть метрики через `jconsole`/`jmc`?
  Да. Подключитесь к JVM процесса и распахните домен `kz.qazmarka.h2k` → `Endpoint` → `H2KMetrics`.

- Почему имена атрибутов отличаются от ключей в коде?
  В JMX используются нормализованные имена (строчные, только `[a-z0-9_]`). Это облегчает правила в JMX Exporter.

- Что если MBean не зарегистрировался?
  В логах будет предупреждение `JMX-метрики H2K не были зарегистрированы`. Это не влияет на работу репликации.
  Проверьте права на JMX, отсутствие конфликтов имён и наличие платформенного MBeanServer.

- Можно ли вшить Prometheus‑экспортёр прямо в H2K?
  Не рекомендуем. Это усложняет lifecycle RegionServer (порты, потоки, зависимости, безопасность). Текущая JMX‑модель даёт
  полный набор метрик без риска для стабильности: их подхватывает уже используемый jmx_exporter.

### 7. Отладка

- Включите DEBUG для `kz.qazmarka.h2k.endpoint` — при старте будет INFO‑строка с именем зарегистрированного MBean.
- Проверьте доступность JMX порта и правила Firewalld/iptables.
- Запустите JMX Exporter с `--config.reload` (если используете форки с поддержкой hot reload) или перезапускайте процесс при правках правил.

---

См. также:
- `docs/runbook/troubleshooting.md` — диагностика и новые метрики отказов репликации
- `docs/peer-profiles.md` — рекомендации по настройке Kafka Producer/Admin

### 8. Альтернативы и соображения стабильности

- Hadoop Metrics2 sink для Prometheus.
  Плюсы: нативная интеграция с Hadoop‑экосистемой. Минусы: для HBase 1.4.13 потребуются бэкпорты/форки, риск несовместимости.
  Для цели экспорта метрик H2K из RegionServer избыточно — JMX уже доступен «из коробки».

- Встроенный HTTP‑экспортёр в H2K.
  Плюсы: «всё в одном». Минусы: добавляет порты/потоки в процесс RS, требует зависимостей и политики безопасности,
  повышает поверхность отказа. Мы избегаем этого по соображениям простоты и стабильности.

- Sidecar‑процесс jmx_exporter рядом с RS.
  Плюсы: изоляция от RS, гибкое обновление экспортёра. Минусы: нужно следить за управлением процессом/юнитом.
  Это рекомендуемый вариант, если javaagent недоступен по организационным причинам.

### 9. Быстрая проверка и откат

Проверка:
- Убедитесь, что ObjectName `kz.qazmarka.h2k:type=Endpoint,name=H2KMetrics` виден через `jconsole/jmc`.
- Откройте `http://<host>:9404/metrics` и проверьте наличие `h2k_*` метрик.

Откат:
- Удалите правило из конфигурации jmx_exporter и перезапустите агент/RS. H2K продолжит работать как прежде — метрики просто перестанут экспортироваться.

Замечание по стабильности:
- Регистрация MBean — операция без побочных потоков/портов; при любой ошибке H2K продолжает репликацию, ошибка лишь логируется.
