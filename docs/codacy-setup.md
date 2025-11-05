# Codacy CLI v2 в среде Windows Subsystem for Linux (WSL)

> Инструкция переписана под повседневную разработку на Windows + WSL2 (Ubuntu/Debian). После выполнения шагов Codacy MCP сможет автоматически запускать анализ.

## Что уже есть в репозитории

- `.codacy/cli.sh` — обёртка, скачивающая и запускающая свежую версию Codacy CLI v2.
- `.codacy/codacy.yaml` и `.codacy/tools-configs/` — готовые настройки для PMD, Trivy, Semgrep и других инструментов.
   (для WSL требуется лёгкий shim `wsl`, см. ниже «Установка shim wsl»)

## Подготовка окружения WSL

1. **curl или wget.** На Ubuntu/Debian выполните `sudo apt update && sudo apt install curl`.
2. **Каталог для пользовательских бинарей.** Используем `~/.local/bin`; при отсутствии директория будет создана автоматически.
3. **PATH.** Убедитесь, что `~/.local/bin` присутствует в PATH. Добавьте в `~/.bashrc` (или `~/.zshrc`):
   ```bash
   export PATH="$HOME/.local/bin:$PATH"
   ```
   Затем выполните `source ~/.bashrc` или откройте новое окно терминала.

## Установка shim `wsl`

AutoRemediation вызывает CLI командой `wsl .codacy/cli.sh …`. Внутри Linux-подсистемы утилиты `wsl` нет, поэтому создаём лёгкую прокладку вручную:

```bash
mkdir -p "$HOME/.local/bin"
cat >"$HOME/.local/bin/wsl" <<'EOF'
#!/usr/bin/env bash
if [ "$#" -eq 0 ]; then
   echo "WSL shim: missing command" >&2
   exit 1
fi
exec "$@"
EOF
chmod +x "$HOME/.local/bin/wsl"
```

Проверьте, что shim в PATH:
```bash
wsl echo "WSL shim is ready"
```

## Первый запуск Codacy CLI

Быстрая проверка (только PMD, чтобы убедиться, что связка работает):
```bash
wsl .codacy/cli.sh analyze --tool pmd --format text
```

Полный анализ всех инструментов:
```bash
wsl .codacy/cli.sh analyze --format sarif --output codacy-results.sarif
```

CLI принимает флаг `--tool` для выборочного запуска (pmd, trivy, semgrep, pylint, eslint, dartanalyzer и др.).

## Связка с MCP

После появления shim MCP автоматически вызывает `codacy_cli_analyze` для изменённых файлов. Если снова увидите `Command failed: wsl …`, проверьте:

1. Существует ли `~/.local/bin/wsl` и имеет ли он права на исполнение (`chmod +x`).
2. Возвращает ли `which wsl` путь к shim.
3. Выполняется ли вручную `wsl .codacy/cli.sh analyze --tool pmd --format text`.

## Полезные команды

```bash
# Обновление CLI до свежей версии
wsl .codacy/cli.sh update

# Проверка установленной версии
wsl .codacy/cli.sh version

# Установка дополнительных инструментов (обычно не требуется)


# Анализ зависимостей только Trivy
wsl .codacy/cli.sh analyze --tool trivy --format text

# Полный анализ с выгрузкой SARIF
wsl .codacy/cli.sh analyze --format sarif --output codacy-results.sarif
```

## Если вы не в WSL

- **Чистый Linux / macOS:** можно напрямую запускать `.codacy/cli.sh analyze …`, shim не нужен.
- **Windows без WSL:** используйте официальный установщик Codacy CLI или задействуйте GitHub Actions.

## Последние результаты (октябрь 2025)

- Semgrep OSS — 0 findings (639 правил / 85 файлов)
- Trivy — критических уязвимостей не найдено
- PMD7, Revive, ESLint, Pylint, Dartanalyzer завершаются без ошибок
- Обновления зависимостей: Guava 24.1.1-jre, Jackson 2.17.2, Avro 1.11.4, commons-compress 1.26.0

После настройки запустите анализ, чтобы убедиться в отсутствии регрессий.
  ./codacy-analyze.sh --format json --output results.json
