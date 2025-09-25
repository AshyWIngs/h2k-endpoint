package kz.qazmarka.h2k.kafka.ensure;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Потокобезопасное состояние ensure-процесса: кеш подтверждённых тем, дедлайны повторных
 * попыток и счётчики диагностики. Используется только внутри пакета ensure.
 */
final class TopicEnsureState {
    final Set<String> ensured = ConcurrentHashMap.newKeySet();
    final ConcurrentMap<String, Long> unknownUntil = new ConcurrentHashMap<>();

    final LongAdder ensureInvocations = new LongAdder();
    final LongAdder ensureHitCache   = new LongAdder();
    final LongAdder existsTrue       = new LongAdder();
    final LongAdder existsFalse      = new LongAdder();
    final LongAdder existsUnknown    = new LongAdder();
    final LongAdder createOk         = new LongAdder();
    final LongAdder createRace       = new LongAdder();
    final LongAdder createFail       = new LongAdder();

    /** Удаляет дедлайн повторной проверки для темы. */
    void resetUnknownUntil(String topic) {
        unknownUntil.remove(topic);
    }

    /** Сохраняет дедлайн повторной проверки (System.nanoTime-based). */
    void scheduleUnknown(String topic, long deadlineNs) {
        unknownUntil.put(topic, deadlineNs);
    }

    /** @return дедлайн backoff или {@code null}, если не запланирован. */
    Long getUnknownDeadline(String topic) {
        return unknownUntil.get(topic);
    }

    /** Немодифицируемая копия карты дедлайнов (для диагностики). */
    Map<String, Long> snapshotUnknown() {
        return java.util.Collections.unmodifiableMap(new java.util.HashMap<>(unknownUntil));
    }

    /**
     * Возвращает текущий размер backoff-очереди «неизвестных» топиков.
     * Константная сложность {@code O(1)}. Метод потокобезопасен, так как
     * {@link #unknownUntil} — это {@code ConcurrentMap}, размер которого
     * запрашивается без внешней синхронизации.
     *
     * @return количество элементов в очереди backoff
     */
    int unknownSize() {
        return unknownUntil.size();
    }
}
