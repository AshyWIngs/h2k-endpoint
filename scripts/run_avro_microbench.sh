#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

JAVA_HOME_8=""
if command -v /usr/libexec/java_home >/dev/null 2>&1; then
  JAVA_HOME_8=$(/usr/libexec/java_home -v 1.8 2>/dev/null || true)
fi

if [ -n "$JAVA_HOME_8" ]; then
  JAVA_HOME="$JAVA_HOME_8"
fi

if [ -z "${JAVA_HOME:-}" ]; then
  echo "JDK 1.8 не найден. Укажите JAVA_HOME." >&2
  exit 1
fi

JAVA_BIN="$JAVA_HOME/bin/java"
if [ ! -x "$JAVA_BIN" ]; then
  echo "JAVA_HOME не содержит bin/java: $JAVA_HOME" >&2
  exit 1
fi

if ! "$JAVA_BIN" -version 2>&1 | grep -q "1.8"; then
  echo "JAVA_HOME не указывает на JDK 1.8: $JAVA_HOME" >&2
  exit 1
fi

export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

WARMUP="${1:-10000}"
ITERATIONS="${2:-100000}"

mvn -q -DskipTests test-compile dependency:build-classpath -Dmdep.outputFile=target/test-classpath.txt

CLASSPATH="$(cat target/test-classpath.txt):target/test-classes:target/classes"
java -cp "$CLASSPATH" kz.qazmarka.h2k.payload.serializer.avro.ConfluentAvroPayloadSerializerMicrobench "$WARMUP" "$ITERATIONS"
