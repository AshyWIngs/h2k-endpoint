# План декомпозиции `TopicEnsurer` / `EnsureCoordinator`

Документ фиксирует целевую архитектуру ensure-пайплайна Kafka-тем и шаги перехода.
Приоритеты: **стабильность → простота → скорость разработки**.

## Текущее состояние (2025-10-28)
- Исторический сервис ensure содержал и оркестрацию, и низкоуровневую работу с AdminClient, и сбор метрик (≈150 NLOC, CCN ≈ 12).
- `TopicEnsurer` смешивает фасад, lifecycle `TopicEnsureExecutor` и fallback-делегаты.
- Валидация `TopicCandidateChecker` интегрирована, но не протестирована отдельно.
- Метрики `ensure.*` считаются асимметрично: batch-путь (`ensureTopics(Collection)`) не увеличивает `ensure.invocations`, `ensureTopic()` инкрементирует счётчик даже при отклонении кандидата.

## Целевая архитектура

```
TopicEnsurer (фасад)
 ├── EnsureCoordinator       // оркестрация describe/create, выполнение ensureTopic/ensureTopics
 │    ├── TopicCandidateChecker (уже есть)
 │    ├── TopicDescribeSupport (без изменений)
 │    ├── TopicCreationSupport (без изменений)
 │    └── EnsureMetricsSink   // новая прослойка для атомарной записи метрик/кеша
 ├── EnsureMetrics           // value-object + exporters (JMX/Prometheus)
 ├── EnsureExecutor          // сохранённый TopicEnsureExecutor
 ├── EnsureState             // сохранённый runtime-state ensure (переименовать поля, сделать package-private)
 └── EnsureLifecycle         // создание/закрытие AdminClient, safe cleanup
```

### Новые компоненты
- **`EnsureCoordinator`** — инкапсулирует текущее тело `ensureTopic*`, управляет последовательностью шагов и обновлением состояния через `EnsureMetricsSink`.
- **`EnsureMetricsSink`** — типизированный интерфейс (`onInvocation()`, `onCandidateRejected(reason)`, `onCacheHit()`, `onDescribeResult(result)`, `onCreateOutcome(outcome)`). Реализация пишет в EnsureRuntimeState и возвращает снимки.
- **`EnsureLifecycle`** — переносит из `TopicEnsurer.createIfEnabled` логику конфигурирования AdminClient, закрытие при ошибках (нынешний `EnsureComponentsBuilder` станет приватным классом в lifecycle).
- **`EnsureMetrics`** — иммутабельный DTO (используется `TopicEnsurer#getMetrics()`/`getBackoffSnapshot()`), упрощает экспорт в `TopicManager`.

### Изменения существующих классов
- Исторический `TopicEnsureService` заменён `EnsureCoordinator`; публичный API ограничен `ensureTopic`, `ensureTopicOk`, `ensureTopics`, `metrics()`, `backoffSnapshot()`.
- `TopicEnsurer` держит только фасад: ленивое создание координатора, делегированные вызовы, отключение ensure (NOOP) и `EnsureExecutor`.
- Переименование runtime-state → `EnsureRuntimeState`: сделать поля package-private, снапшоты отдавать через `EnsureMetricsSink`.
- `TopicEnsureContext` — заменить прямой доступ к `state`/`backoffManager` на методы sink’а (например, `markEnsured`, `scheduleRetry`).

## План внедрения

### Этап 1. Подготовка
- [x] Добавить unit-тесты для `TopicCandidateChecker` (валидные/невалидные имена, кеш-хиты, backoff, sanitizer=null).
- [x] Ввести `EnsureMetricsSink` поверх текущего `TopicEnsureState`, но без переименования классов.
- [x] Перенести учёт метрик в `TopicCandidateChecker` и `TopicCreationSupport` через sink; исправить асимметрию `ensure.invocations`.

### Этап 2. Выделение координатора
- [x] Создать класс `EnsureCoordinator`, перенести в него `ensureTopic*`, `getMetrics`, `getBackoffSnapshot`.
- [x] Обновить `TopicEnsureService` → thin-wrapper/alias (временно делегирующий на `EnsureCoordinator`) для плавной миграции.
- [x] Переназначить `TopicEnsureExecutor` на работу с `EnsureCoordinator`.

### Этап 3. Упрощение фасада
- [x] Переработать `TopicEnsurer` (выделить `EnsureLifecycle`, ввести статусы `ENABLED`/`DISABLED`/`DELEGATE`).
- [x] Расширить метрики ensure/бэкоффа: `invocations.total`, `invocations.accepted`, `invocations.rejected`, `cache.hits`, `describe.true/false/unknown`, `create.ok/race/fail`, `batch.count`.
- [x] Обновить экспорты метрик (`TopicManager`, Prometheus/JMX документацию).

### Этап 4. Рефакторинг состояния
- [x] Переименовать `TopicEnsureState` → `EnsureRuntimeState`, скрыть поля за геттерами.
- [x] Перевести `TopicEnsureContext` на работу с `EnsureMetricsSink` (без прямого доступа к sate/backoff).
- [x] Очистить deprecated-классы/методы (старый `TopicEnsureService` удалить после миграции).

## Риски и контроль качества
- **Совместимость с ensureMetrics** — добавить интеграционный тест, проверяющий, что `TopicEnsurer#getMetrics()` выдаёт те же ключи/значения, что и текущая реализация.
- **Гонки внутри backoff** — сохранить `TopicBackoffManager` как есть, но протестировать с новым sink’ом.
- **Тестовое покрытие** — дополнить `EnsureCoordinatorTest` и `TopicEnsureExecutorTest` сценариями для новых счётчиков, добавить snapshot-тест для `EnsureCoordinator`.

## Быстрые выгоды
- Чистое разделение ответственности упрощает дальнейшие улучшения (динамические политики backoff, метрики Prometheus).
- Появится возможность изолированно тестировать `EnsureCoordinator` без AdminClient.
- Снижение когнитивной сложности (каждый класс ≤ 200 строк / CCN < 8).
