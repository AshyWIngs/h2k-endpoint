#!/usr/bin/env python3
"""
Линтер для JavaDoc: подсвечивает наличие HTML‑тегов в блоках JavaDoc (/** ... */)
и проверяет, что в содержательной части комментария есть кириллица (JavaDoc на русском).

Цель: закрепить правило «без HTML‑тегов в JavaDoc» и поймать случайные вставки
<p>, <br>, <code>, <pre>, <ul>, <li> и т.п. в комментариях.

Как работает:
  - Ищет только внутри JavaDoc‑блоков (начинаются с '/**' и заканчиваются '*/').
  - Сопоставляет теги из ограниченного списка html‑тегов, чтобы не цеплять generic‑параметры
    (например, <T>, <K,V>) и имена типов вроде <String, Long>.

Использование:
  python3 scripts/javadoc_html_lint.py [--path src]

Код выхода:
  0 — нарушений не найдено
  1 — найдены нарушения
  2 — ошибка запуска (не найден путь)
"""

import argparse
import pathlib
import re
import sys


# Минимальный набор часто встречающихся HTML‑тегов в JavaDoc, которые мы запрещаем
HTML_TAGS = {
    'p', 'br', 'code', 'pre', 'tt', 'em', 'strong', 'b', 'i', 'u',
    'ul', 'ol', 'li',
    'a', 'img', 'hr',
    'table', 'thead', 'tbody', 'tr', 'td', 'th',
    'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
    # часто попадается <center> в легаси‑примерах
    'center',
}

# Сопоставляет <tag ...> или </tag> внутри комментария
TAG_RE = re.compile(r"<\/?([a-zA-Z][a-zA-Z0-9]*)\b")

# Юникодный диапазон кириллицы
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")


def find_javadoc_blocks(text: str):
    blocks = []
    i = 0
    n = len(text)
    while i < n:
        start = text.find('/**', i)
        if start == -1:
            break
        end = text.find('*/', start + 3)
        if end == -1:
            break
        # исключить /**/ пустые и /*** (неформатные) — но для простоты принимаем как есть
        blocks.append((start, end + 2))
        i = end + 2
    return blocks


def _build_line_index(text: str):
    starts = [0]
    for m in re.finditer(r"\n", text):
        starts.append(m.end())
    return starts

def _offset_to_line(line_starts, off: int) -> int:
    lo, hi = 0, len(line_starts) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if line_starts[mid] <= off:
            lo = mid + 1
        else:
            hi = mid - 1
    return hi + 1


INLINE_TAG_RE = re.compile(r"\{@[a-zA-Z]+[^}]*\}", re.DOTALL)
AT_TAG_LINE_RE = re.compile(r"\s*\*?\s*@(?:param|return|throws|see|deprecated|since|author|version)\b")
STAR_LEADING_RE = re.compile(r"^[ \t]*\* ?")

def _javadoc_core_text(raw_block: str) -> str:
    inner = raw_block
    if inner.startswith("/**"):
        inner = inner[3:]
    if inner.endswith("*/"):
        inner = inner[:-2]
    inner = INLINE_TAG_RE.sub("", inner)
    lines = inner.splitlines()
    kept = []
    for ln in lines:
        ln2 = STAR_LEADING_RE.sub("", ln).strip()
        if not ln2 or AT_TAG_LINE_RE.match(ln2):
            continue
        kept.append(ln2)
    return "\n".join(kept)


def _check_html_tags(clean_block: str, start: int, offset_to_line) -> list:
    issues = []
    for m in TAG_RE.finditer(clean_block):
        tag = m.group(1).lower()
        if tag in HTML_TAGS:
            line = offset_to_line(start + m.start(0))
            issues.append((line, m.group(1)))
    return issues


def _check_russian_text(raw_block: str, start: int, offset_to_line) -> list:
    core = _javadoc_core_text(raw_block)
    if not core:
        return []
    has_letters = re.search(r"[A-Za-z\u0400-\u04FF]", core) is not None
    if has_letters and not CYRILLIC_RE.search(core):
        return [(offset_to_line(start), 'NO_CYRILLIC')]
    return []


def scan_file(path: pathlib.Path):
    content = path.read_text(encoding='utf-8', errors='ignore')
    blocks = find_javadoc_blocks(content)
    if not blocks:
        return []
    line_index = _build_line_index(content)
    offset_to_line = lambda off: _offset_to_line(line_index, off)

    findings = []
    for (start, end) in blocks:
        raw_block = content[start:end]
        clean_block = INLINE_TAG_RE.sub("", raw_block)
        findings.extend(_check_html_tags(clean_block, start, offset_to_line))
        findings.extend(_check_russian_text(raw_block, start, offset_to_line))
    return findings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--path', default='src', help='Корневой путь сканирования (по умолчанию src)')
    args = ap.parse_args()

    root = pathlib.Path(args.path)
    if not root.exists():
        print(f"Путь не найден: {root}", file=sys.stderr)
        return 2

    violations = 0
    for java in root.rglob('*.java'):
        findings = scan_file(java)
        if findings:
            for line, tag in findings:
                if tag == 'NO_CYRILLIC':
                    print(f"{java}:{line}: JavaDoc без кириллицы — добавьте описание на русском")
                else:
                    print(f"{java}:{line}: недопустимый HTML‑тег в JavaDoc: <{tag}>")
            violations += len(findings)

    if violations:
        print(f"\nНайдено нарушений JavaDoc: {violations}")
        return 1
    print("JavaDoc: нарушений не обнаружено")
    return 0


if __name__ == '__main__':
    sys.exit(main())
