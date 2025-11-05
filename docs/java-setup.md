# Настройка Java для сборки проекта

## Автоматическое определение JDK 1.8

Проект настроен на автоматическое использование Java 8 (JDK 1.8) для компиляции. Maven автоматически определит подходящий путь к JDK на основе системного окружения.

## Поддерживаемые пути JDK

При сборке Maven проверяет следующие расположения JDK 1.8 (в порядке приоритета):

1. **Переменная окружения `JAVA_HOME`** — если установлена, Maven использует её значение.
2. **Системные пути (Linux/WSL):**
   - `/usr/lib/jvm/java-8-openjdk-amd64`
   - `/usr/lib/jvm/java-1.8.0-openjdk-amd64`
   - `/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64`
   - `/usr/lib/jvm/temurin-8-jdk-amd64`

Если ни один из этих путей не найден, Maven использует текущую версию Java из `PATH`.

## Ручная настройка JAVA_HOME

Если требуется явно указать путь к JDK:

### Linux/WSL
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Добавьте эти строки в `~/.bashrc` или `~/.zshrc` для постоянного применения.

### Windows (PowerShell)
```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk1.8.0_XXX"
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
```

### Windows (CMD)
```cmd
set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_XXX
set PATH=%JAVA_HOME%\bin;%PATH%
```

## Проверка версии Java

Убедитесь, что используется правильная версия:

```bash
java -version
```

Ожидаемый вывод:
```
openjdk version "1.8.0_XXX"
OpenJDK Runtime Environment (build 1.8.0_XXX-XXX)
OpenJDK 64-Bit Server VM (build 25.XXX-XXX, mixed mode)
```

## Сборка проекта

После настройки JDK выполните сборку:

```bash
# Полная пересборка с тестами
mvn clean package

# Быстрая сборка без тестов
mvn -DskipTests clean package
```

## Требования к Runtime-окружению

- **RegionServer:** Обязательно Java 8. HBase 1.4.13 не поддерживает Java 9+.
- **Сборка:** Рекомендуется Java 8, но совместимо с Java 11+ при условии, что `maven.compiler.source=1.8` и `maven.compiler.target=1.8`.

## Устранение проблем

### Ошибка: `JAVA_HOME is not defined correctly`

**Решение:**
1. Проверьте, что `JAVA_HOME` указывает на корень JDK (не JRE).
2. Убедитесь, что путь существует: `ls -l $JAVA_HOME` (Linux) или `dir %JAVA_HOME%` (Windows).

### Ошибка: `Unsupported class file major version XX`

**Причина:** Используется Java версии выше 8 для запуска HBase RegionServer.

**Решение:** На RegionServer должна быть установлена исключительно Java 8.

### Maven использует неправильную версию Java

**Проверка:**
```bash
mvn -version
```

Если выводится Java 11+, явно укажите `JAVA_HOME` перед запуском Maven:
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
mvn clean package
```

## См. также

- [Установка](../README.md#установка) — общие шаги развёртывания проекта.
- [Поддерживаемые версии](../README.md#поддерживаемые-версии) — совместимость с HBase, Kafka, Phoenix.
