#!/usr/bin/env python3
"""
Простой линтер для сообщений логирования и текстов исключений.

Цели:
- Подсветить англоязычные сообщения, в которых нет кириллических символов и
  которые не входят в белый список допустимых терминов (Kafka, Avro, payload и т.п.).
- Помочь на code review не пропускать расхождения стиля.

Использование:
  python3 scripts/log_lint.py [--path src/main/java]

Код выхода:
  0 — нарушений не найдено
  1 — найдены потенциальные нарушения

Ограничения:
- Парсинг упрощённый: анализируются строки вида LOG.info("...") и throw new X("...").
- Конкатенации/многострочные литералы могут быть пропущены.
"""

import argparse
import pathlib
import re
import sys


LOG_RE = re.compile(r"\b(?:LOG|log)\.(?:trace|debug|info|warn|error)\s*\(\s*\"([^\"]*)\"")
THROW_RE = re.compile(r"throw\s+new\s+[A-Za-z0-9_\.]+Exception\s*\(\s*\"([^\"]+)\"")

# Юникодный диапазон кириллицы
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")

# Разрешённые термины/токены (нижний регистр)
WHITELIST = {
    # платформенные термины
    'kafka', 'topic', 'topics', 'producer', 'consumer', 'payload', 'endpoint', 'replication',
    'wal', 'phoenix', 'avro', 'jmx', 'mbean', 'metrics', 'schema', 'registry', 'schema registry',
    'subject', 'fingerprint', 'batch', 'idempotence', 'backoff', 'timeout', 'acks', 'bootstrap',
    'adminclient', 'executor', 'retry', 'cache', 'key', 'value', 'partition', 'namespace',
    'qualifier', 'table', 'record', 'genericrecord',
    # имена конфигов/свойств
    'client.id', 'linger.ms', 'batch.size', 'retention.ms', 'min.insync.replicas',
    'lz4', 'subject.strategy', 'subject.suffix', 'subject.prefix',
    # наши ключи
    'h2k', 'ensure', 'ensuretopics', 'unknown', 'schemaid',
}

# Полные сообщения, которые допустимы в английском виде (совместимость с тестами
# и контрактами)
EXACT_WHITELIST = {
    'normalized name is unavailable for status',
}


def is_violation(text: str) -> bool:
    """Возвращает True, если строка выглядит англоязычной и не содержит кириллицы
    и при этом не состоит только из разрешённых терминов.
    """
    if not text:
        return False
    # Если есть кириллица — считаем ОК
    if CYRILLIC_RE.search(text):
        return False
    # Оставляем только буквы/цифры/._- и пробелы, приводим к нижнему регистру
    norm = re.sub(r"[^A-Za-z0-9._\- ]+", " ", text).lower()
    tokens = [t for t in norm.split() if t]
    if not tokens:
        return False
    # Если полное сообщение находится в точном белом списке — считаем ОК
    if text.strip().lower() in EXACT_WHITELIST:
        return False
    # Если все токены из белого списка — считаем ОК
    all_whitelisted = all(t in WHITELIST for t in tokens)
    return not all_whitelisted


def scan_file(path: pathlib.Path):
    findings = []
    try:
        content = path.read_text(encoding='utf-8', errors='ignore').splitlines()
    except Exception:
        return findings
    for idx, line in enumerate(content, start=1):
        for rx in (LOG_RE, THROW_RE):
            m = rx.search(line)
            if m:
                msg = m.group(1)
                if is_violation(msg):
                    findings.append((idx, msg.strip()))
    return findings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--path', default='src/main/java', help='Каталог для сканирования (по умолчанию src/main/java)')
    args = ap.parse_args()

    root = pathlib.Path(args.path)
    if not root.exists():
        print(f"Путь не найден: {root}", file=sys.stderr)
        return 2

    violations = 0
    for java in root.rglob('*.java'):
        findings = scan_file(java)
        if findings:
            violations += len(findings)
            for line_no, msg in findings:
                print(f"{java}:{line_no}: подозрительное сообщение (без кириллицы): {msg}")

    if violations:
        print(f"\nНайдено потенциальных нарушений: {violations}")
        return 1
    print("Логи: нарушений не обнаружено")
    return 0


if __name__ == '__main__':
    sys.exit(main())
