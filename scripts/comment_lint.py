#!/usr/bin/env python3
"""
Линтер для обычных комментариев в Java‑коде: подсвечивает комментарии без кириллицы.

Цель: поощрять русскоязычные комментарии (// и /* ... */), сохраняя исключения
для TODO/FIXME, URL, и общеупотребимых IT‑терминов.

Правила (консервативно, чтобы не зашуметь):
  - Анализируются только //‑комментарии и /* ... */ блоки; JavaDoc (/** ... */) не проверяется.
  - Комментарий игнорируется, если:
      * начинается с TODO/FIXME/noinspection;
      * содержит URL (http/https), e.g. ссылки;
      * очень короткий (< 6 символов) или выглядит как код (содержит ';' или '()');
      * состоит только из разрешённых английских терминов (Kafka, Avro, WAL, JMX, SR и т.п.).
  - Если после нормализации в комментарии есть буквы, но нет кириллицы — предупреждение.

Использование:
  python3 scripts/comment_lint.py [--path src]

Код выхода:
  0 — нарушений не найдено
  1 — найдены потенциальные нарушения
  2 — ошибка запуска
"""

import argparse
import pathlib
import re
import sys


CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")
ASCII_LETTER_RE = re.compile(r"[A-Za-z]")
URL_RE = re.compile(r"https?://")

# Разрешённые английские термины (в нижнем регистре)
WHITELIST = {
    # платформенные термины/аббревиатуры
    'kafka','topic','topics','wal','avro','sr','schema','registry','jmx','jvm','jar','jdk','jre',
    'phoenix','hbase','hdfs','zookeeper','zk','cf','pk','ttl','ns','ms','sec','min','epoch',
    'offset','partition','producer','consumer','acks','ack','retry','backoff','cache','payload',
    'serializer','deserializer','subject','fingerprint','batch','idempotence','admin','client',
    'executor','thread','pool','nio','netty','url','http','https','json','xml','utf','crc','md5','sha',
    'key','value','table','row','cell','namespace','qualifier','record','genericrecord',
    # общие служебные
    'todo','fixme','nb','wip','stub','noop','id','uid','uid2','guid',
}

def _skip_string_literal(text: str, start: int, quote: str) -> int:
    i = start
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == '\\':
            i += 2
            continue
        if ch == quote:
            return i + 1
        i += 1
    return n


def _consume_line_comment(text: str, start: int):
    end = text.find('\n', start)
    if end == -1:
        end = len(text)
    return end, (start, end)


def _consume_block_comment(text: str, start: int):
    if start + 2 < len(text) and text[start + 2] == '*':
        end = text.find('*/', start + 3)
        end = len(text) if end == -1 else end + 2
        return end, None
    end = text.find('*/', start + 2)
    if end == -1:
        return len(text), (start, len(text))
    return end + 2, (start, end + 2)


def _try_consume_comment(text: str, start: int):
    if text[start] != '/' or start + 1 >= len(text):
        return start, None
    nxt = text[start + 1]
    if nxt == '/':
        return _consume_line_comment(text, start)
    if nxt == '*':
        return _consume_block_comment(text, start)
    return start, None


def scan_java_comments(text: str):
    """Грубый сканер комментариев: вытаскивает // и /*...*/, исключая /**...*/ (JavaDoc)."""
    i = 0
    n = len(text)
    comments = []  # (start, end)
    while i < n:
        ch = text[i]
        if ch in ('"', '\''):
            i = _skip_string_literal(text, i + 1, ch)
            continue
        new_i, rng = _try_consume_comment(text, i)
        if new_i != i:
            i = new_i
            if rng:
                comments.append(rng)
            continue
        i += 1
    return comments


STAR_LEADING_RE = re.compile(r"^[ \t]*\* ?")

def normalize_comment(raw: str) -> str:
    # убрать /* */, //, ведущие * у строк, обрезать пробелы
    s = raw
    if s.startswith('//'):
        s = s[2:]
    elif s.startswith('/*'):
        s = s[2:-2] if s.endswith('*/') else s[2:]
    # построчная очистка
    lines = []
    for ln in s.splitlines():
        ln = STAR_LEADING_RE.sub('', ln).strip()
        if not ln:
            continue
        lines.append(ln)
    return '\n'.join(lines)


def is_violation(text: str) -> bool:
    if not text:
        return False
    low = text.lower()
    # явные исключения
    if low.startswith(('todo', 'fixme', 'noinspection')):
        return False
    if URL_RE.search(text):
        return False
    if len(text) < 6:
        return False
    if ';' in text or '()' in text or '=' in text:
        # с большой вероятностью внутри — код
        return False
    # есть буквы?
    if not (CYRILLIC_RE.search(text) or ASCII_LETTER_RE.search(text)):
        return False
    # Если есть кириллица — ок
    if CYRILLIC_RE.search(text):
        return False
    # Нормализуем в токены и проверим белый список
    norm = re.sub(r"[^A-Za-z0-9._\- ]+", " ", low)
    tokens = [t for t in norm.split() if t]
    if tokens and all(t in WHITELIST for t in tokens):
        return False
    return True


def scan_file(path: pathlib.Path):
    try:
        content = path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return []
    comment_ranges = scan_java_comments(content)
    if not comment_ranges:
        return []
    # подготовим смещения строк
    line_starts = [0]
    for m in re.finditer(r"\n", content):
        line_starts.append(m.end())
    def offset_to_line(off: int) -> int:
        lo, hi = 0, len(line_starts) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if line_starts[mid] <= off:
                lo = mid + 1
            else:
                hi = mid - 1
        return hi + 1
    findings = []
    for (start, end) in comment_ranges:
        raw = content[start:end]
        text = normalize_comment(raw)
        if is_violation(text):
            findings.append((offset_to_line(start), text.strip()))
    return findings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--path', default='src/main/java', help='Корневой путь сканирования (по умолчанию src/main/java)')
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
            for line, msg in findings:
                short = msg if len(msg) <= 120 else msg[:117] + '...'
                print(f"{java}:{line}: комментарий без кириллицы: {short}")
    if violations:
        print(f"\nНайдено комментариев без кириллицы: {violations}")
        return 1
    print("Комментарии: нарушений не обнаружено")
    return 0


if __name__ == '__main__':
    sys.exit(main())
