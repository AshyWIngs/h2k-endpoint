package kz.qazmarka.h2k.kafka;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

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

    void resetUnknownUntil(String topic) {
        unknownUntil.remove(topic);
    }

    void scheduleUnknown(String topic, long deadlineNs) {
        unknownUntil.put(topic, deadlineNs);
    }

    Long getUnknownDeadline(String topic) {
        return unknownUntil.get(topic);
    }

    Map<String, Long> snapshotUnknown() {
        return unknownUntil;
    }
}
