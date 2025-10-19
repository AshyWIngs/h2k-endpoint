# Навигатор по документации

Документация разбита на тематические блоки. Этот файл помогает быстро найти нужный материал.

## Конфигурация и общая архитектура
- `README.md` (в корне) — обзор, быстрый старт, FAQ.
- [`docs/config.md`](config.md) — полный справочник по ключам `h2k.*`.
- [`docs/peer-profiles.md`](peer-profiles.md) — готовые профили peer и матрица параметров.
- [`docs/hbase.md`](hbase.md) — команды HBase shell, управление таблицами и ZooKeeper.
	В разделе *Автосоздание/администрирование тем* описаны актуальные параметры ensure (`h2k.ensure.*`, `h2k.topic.config.*`).

## Эксплуатация и диагностика
- [`docs/runbook/operations.md`](runbook/operations.md) — ежедневные операции: требования окружения, добавление/редактирование peers, сбор метрик.
- [`docs/runbook/troubleshooting.md`](runbook/troubleshooting.md) — включение DEBUG, разбор типовых ошибок, подсказки по логам.

## Схемы, payload и интеграции
- [`docs/avro.md`](avro.md) — работа с Avro (локальные `.avsc`, Confluent Schema Registry).
- [`docs/schema-registry.md`](schema-registry.md) — практические сценарии работы с Confluent Schema Registry.
- [`docs/capacity.md`](capacity.md) — подсказки ёмкости JSON и рекомендации по `h2k.capacity.hints`.
- [`docs/phoenix.md`](phoenix.md) — особенности Phoenix (PK, salt).

## Дополнительно
- [`docs/runbook/`](runbook/) — папка с эксплуатационным runbook.
- [`conf/`](../conf/) — готовые скрипты добавления peer и примеры конфигураций.

## Разработка
- [`docs/dev-guidelines.md`](dev-guidelines.md) — правила инкапсуляции диагностических структур и работы с логированием.

Если в коде добавляется новый раздел или изменяются интерфейсы — дополняйте соответствующий документ и обновляйте данный навигатор.
