package kz.qazmarka.h2k.kafka.ensure;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Потокобезопасное состояние ensure-процесса: кеш подтверждённых тем, дедлайны повторных
 * попыток и счётчики диагностики. Ранее поля были публичными; теперь доступ предоставляется
 * через методы, чтобы централизовать изменения и упростить последующие refactor'ы.
 */
public final class EnsureRuntimeState {

    private final Set<String> ensured = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, Long> unknownUntil = new ConcurrentHashMap<>();

    private final LongAdder ensureEvaluations = new LongAdder();
    private final LongAdder ensureInvocations = new LongAdder();
    private final LongAdder ensureRejected   = new LongAdder();
    private final LongAdder ensureHitCache   = new LongAdder();
    private final LongAdder ensureBatchCount = new LongAdder();
    private final LongAdder existsTrue       = new LongAdder();
    private final LongAdder existsFalse      = new LongAdder();
    private final LongAdder existsUnknown    = new LongAdder();
    private final LongAdder createOk         = new LongAdder();
    private final LongAdder createRace       = new LongAdder();
    private final LongAdder createFail       = new LongAdder();

    // ------- Операции кеша подтверждённых ensure -------

    public boolean markEnsured(String topic) {
        return ensured.add(topic);
    }

    public boolean isEnsured(String topic) {
        return ensured.contains(topic);
    }

    public int ensuredSize() {
        return ensured.size();
    }

    public Set<String> ensuredSnapshot() {
        return Collections.unmodifiableSet(ensured);
    }

    public void clearEnsured(String topic) {
        ensured.remove(topic);
    }

    // ------- Операции бэкоффа переиспользуются как есть -------

    public void resetUnknownUntil(String topic) {
        unknownUntil.remove(topic);
    }

    public void scheduleUnknown(String topic, long deadlineNs) {
        unknownUntil.put(topic, deadlineNs);
    }

    public Long getUnknownDeadline(String topic) {
        return unknownUntil.get(topic);
    }

    public Map<String, Long> snapshotUnknown() {
        return Collections.unmodifiableMap(new java.util.HashMap<>(unknownUntil));
    }

    public int unknownSize() {
        return unknownUntil.size();
    }

    // ------- Методы доступа к метрикам -------

    public LongAdder ensureEvaluations() {
        return ensureEvaluations;
    }

    public LongAdder ensureInvocations() {
        return ensureInvocations;
    }

    public LongAdder ensureRejected() {
        return ensureRejected;
    }

    public LongAdder ensureHitCache() {
        return ensureHitCache;
    }

    public LongAdder ensureBatchCount() {
        return ensureBatchCount;
    }

    public LongAdder existsTrue() {
        return existsTrue;
    }

    public LongAdder existsFalse() {
        return existsFalse;
    }

    public LongAdder existsUnknown() {
        return existsUnknown;
    }

    public LongAdder createOk() {
        return createOk;
    }

    public LongAdder createRace() {
        return createRace;
    }

    public LongAdder createFail() {
        return createFail;
    }
}
