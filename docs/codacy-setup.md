# Codacy CLI v2 Setup Guide

## Установка

Codacy CLI v2 установлен через Homebrew:

```bash
brew tap codacy/codacy-cli-v2
brew install codacy/codacy-cli-v2/codacy-cli-v2
```

Версия: `1.0.0-main.358.sha.7cb05d0` (darwin/arm64)

## Конфигурация

Проект инициализирован с помощью:

```bash
codacy-cli init
```

Конфигурационные файлы находятся в директории `.codacy/`:

- `.codacy/codacy.yaml` - основная конфигурация
- `.codacy/tools-configs/` - настройки отдельных инструментов:
  - `semgrep.yaml` - Semgrep OSS (код-анализ)
  - `revive.toml` - Revive (Go linter)
  - `trivy.yaml` - Trivy (vulnerability scanner)
  - `lizard.yaml` - Lizard (complexity analyzer)
  - `eslint.config.js` - ESLint (JavaScript/TypeScript)
  - `pmd.xml` - PMD7 (Java code analyzer)
  - `pylint.toml` - Pylint (Python linter)
  - `dartanalyzer.yaml` - Dart analyzer

## Использование

### Запуск анализа

Используйте обёртку-скрипт (аргументы прокидываются напрямую в `codacy-cli analyze`):

```bash
./codacy-analyze.sh
```

Или напрямую:

```bash
codacy-cli analyze --format sarif --output codacy-results.sarif
```

> Команда `codacy_cli_analyze` **удалена** и более не используется. Всегда запускайте анализ через `codacy-cli` или скрипт `./codacy-analyze.sh`.

Codacy CLI v2 выполняет анализ целиком — выбор отдельных файлов пока не поддерживается. Для сфокусированных проверок можно комбинировать инструменты (например, `--tool pmd`) или запускать Codacy из IDE.

### Запуск отдельного инструмента

```bash
./codacy-analyze.sh --tool pmd
./codacy-analyze.sh --tool trivy
./codacy-analyze.sh --tool semgrep
```

### Форматы вывода

- **SARIF** (рекомендуется для GitHub Code Scanning):
  ```bash
  ./codacy-analyze.sh --format sarif --output results.sarif
  ```

- **JSON**:
  ```bash
  ./codacy-analyze.sh --format json --output results.json
  ```

- **Text** (консольный вывод):
  ```bash
  ./codacy-analyze.sh --format text
  ```

## Результаты последнего анализа

**Дата:** 17 октября 2025 (последний прогон 14:40 по ALMT)

**Инструменты:**
- ✅ Semgrep OSS — 0 findings (639 правил, 85 файлов)
- ✅ Trivy — уязвимостей не найдено
- ✅ PMD7 — 0 code quality issues
- ✅ Revive — успешно завершается (заменён бинарь для darwin/arm64)
- ✅ ESLint — completed
- ✅ Pylint — completed
- ✅ Dartanalyzer — completed

**Всего найдено:** 0 проблем качества кода + 0 уязвимостей

### Основные уязвимости в зависимостях

- Guava обновлена до `24.1.1-jre`, CVE-2018-10237 закрыта.
- Jackson — `2.17.2`, уязвимостей нет.
- Avro — `1.11.4`, устранены CVE-2023-39410/2024-47561.
- commons-compress — `1.26.0`, устранены CVE-2024-25710/26308.

## Интеграция с CI/CD

### GitHub Actions

```yaml
name: Code Quality

on: [push, pull_request]

jobs:
  codacy:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Codacy CLI
        run: |
          brew tap codacy/codacy-cli-v2
          brew install codacy/codacy-cli-v2/codacy-cli-v2
      
      - name: Run Analysis
        run: codacy-cli analyze --format sarif --output codacy-results.sarif
      
      - name: Upload to GitHub Code Scanning
        uses: github/codeql-action/upload-sarif@v2
        with:
          sarif_file: codacy-results.sarif
```

### Локальная pre-commit hook

Создайте файл `.git/hooks/pre-commit`:

```bash
#!/bin/bash
codacy-cli analyze --tool pmd --format text
```

## Игнорирование файлов

Добавлено в `.gitignore`:

```
# === Codacy / Code Quality ===
codacy-results.json
codacy-results.sarif
.codacy/
```

## Команды для работы

```bash
# Обновление Codacy CLI
codacy-cli update

# Установка инструментов
codacy-cli install

# Справка
codacy-cli --help
codacy-cli analyze --help

# Версия
codacy-cli version

# Настройка конфигурации
codacy-cli config --help
```

## Настройка конкретных правил

Отредактируйте соответствующие файлы в `.codacy/tools-configs/`:

- PMD правила: `.codacy/tools-configs/pmd.xml`
- Trivy: `.codacy/tools-configs/trivy.yaml`
- Semgrep: `.codacy/tools-configs/semgrep.yaml`

Или отредактируйте главный файл: `.codacy/codacy.yaml`

## Полезные ссылки

- [Codacy CLI v2 GitHub](https://github.com/codacy/codacy-cli-v2)
- [Документация](https://docs.codacy.com/getting-started/codacy-cli/)
- [Supported Tools](https://docs.codacy.com/getting-started/supported-languages-and-tools/)
