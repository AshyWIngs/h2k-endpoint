# Avro: локальные схемы и Confluent Schema Registry

Поддержка Avro включается ключом `h2k.payload.format` и работает в двух режимах:

- `h2k.avro.mode=generic` — читаем локальные `.avsc` из каталога.
- `h2k.avro.mode=confluent` — регистрируем/используем схемы в Confluent Schema Registry 5.3.x.

## Локальные `.avsc` (`generic`)

1. **Структура каталога** — по умолчанию `conf/avro`. Для каждой таблицы создаём файл
   `conf/avro/<table>.avsc`. Имя таблицы берётся без namespace (если namespace `DEFAULT`) либо
   `ns:table` (нижний регистр). Путь можно переопределить ключом `h2k.avro.schema.dir`.

   ```
   conf/avro/
     t_receipt.avsc
     default:t_receipt_items.avsc
   ```

2. **Конфигурация**: минимальный набор ключей.

   ```properties
   h2k.payload.format=avro-binary        # или avro-json
   h2k.avro.mode=generic
   h2k.avro.schema.dir=/opt/hbase/conf/avro
   ```

3. **Проверка** — используйте `bin/hbase classpath` для проверки наличия Avro-зависимостей и
   команду `mvn -q -DskipITs -Dtest=GenericAvroPayloadSerializerTest test` для локальной валидации.

## Confluent Schema Registry (`confluent`)

1. **Версии** — SR 5.3.x (совместимый с Kafka clients 2.3.1, Java 8). Wire-format:
   `MAGIC_BYTE (0x0)` + `int32 schemaId` + `avroBinary`.

2. **Конфигурация** (минимальный пример):

   ```properties
   h2k.payload.format=avro-binary
   h2k.avro.mode=confluent
   h2k.avro.schema.dir=/opt/hbase/conf/avro       # локальная схема всё равно нужна
   h2k.avro.sr.urls=http://sr1:8081,http://sr2:8081
   h2k.avro.sr.auth.basic.username=svc-hbase
   h2k.avro.sr.auth.basic.password=...           # опционально
   h2k.avro.subject.strategy=table               # qualifier | table | table-lower | table-upper
 h2k.avro.subject.prefix=hbase-
  h2k.avro.subject.suffix=-value
  ```

   > В режиме `confluent` поддерживается только `h2k.payload.format=avro-binary`.

3. **Subject strategy** — по умолчанию используется `qualifier`. Укажите `table`, если нужно
   включать namespace; `table-lower/upper` — для совместимости с существующими subject-именами.

4. **Расширенная авторизация** — пока поддерживается только Basic. Для TLS/OAuth добавьте ключи
   в секцию `h2k.avro.sr.auth.*` и реализуйте обработку при необходимости.

5. **Экранирование JSON** — перед отправкой в Schema Registry локальная схема проходит безопасное
   экранирование: кавычки, управляющие символы (`\n`, `\r`, `\t`, `\u0000`…`\u001F`, `\u2028/\u2029`)
   кодируются по правилам JSON. Это устраняет проблемы с doc/description, где встречаются переносы и
   управляющие символы, и гарантирует валидный payload для SR 5.3.x.

6. **Диагностика** — при ошибках регистрации в логах появляются WARN с адресом SR и телом ответа.
   Помните о бэкоффах и кешировании schemaId: повторные сообщения не бьют SR повторно.

## Негативные сценарии и рекомендованные тесты

- Отсутствие `.avsc` или несовместимая схема → `IllegalStateException` (см. тесты
  `GenericAvroPayloadSerializerTest`).
- Ошибка регистрации в SR (500/timeout) → `IllegalStateException` (тест
  `ConfluentAvroPayloadSerializerTest`).
- Еволюция схемы: протестируйте добавление опциональных полей и смену типов в собственных
  интеграционных тестах (потребитель должен уметь обрабатывать новые версии).

## Интеграционный поток (рекомендации)

1. **HBase → Kafka** — в бою построить стенд с тестовой таблицей, включить replication scope и
   проверить появление событий в Kafka (JSONEachRow и Avro).
2. **Kafka → ClickHouse** — из Kafka Engine читать `Avro`/`AvroConfluent` и проверить соответствие
   полей. Для SR укажите `kafka_schema_registry_url`.
3. **Мониторинг** — добавить метрики успешных/неуспешных регистраций, время ожидания SR и размер
   очереди schemaId. Реализация пока не входит в проект, но рекомендуется для эксплуатации.

---

Дополнительные вопросы и рецепты см. в `docs/operations.md` (раздел *Avro и Schema Registry*).
