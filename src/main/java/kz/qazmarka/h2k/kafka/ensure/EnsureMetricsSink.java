package kz.qazmarka.h2k.kafka.ensure;

/**
 * Концентратор для обновления метрик ensure-процесса.
 * Инкапсулирует доступ к {@link EnsureRuntimeState}, чтобы все счётчики обновлялись единообразно.
 */
final class EnsureMetricsSink {

    private final EnsureRuntimeState state;

    EnsureMetricsSink(EnsureRuntimeState state) {
        this.state = state;
    }

    void recordEvaluation() {
        state.ensureEvaluations().increment();
    }

    void recordRejected() {
        state.ensureRejected().increment();
    }

    void recordInvocationAccepted() {
        state.ensureInvocations().increment();
    }

    void recordCacheHit() {
        state.ensureHitCache().increment();
    }

    void recordBatchAccepted(int topics) {
        if (topics > 0) {
            state.ensureBatchCount().add(topics);
        }
    }

    void recordExistsTrue() {
        state.existsTrue().increment();
    }

    void recordExistsFalse() {
        state.existsFalse().increment();
    }

    void recordExistsUnknown() {
        state.existsUnknown().increment();
    }

    void recordCreateSuccess() {
        state.createOk().increment();
    }

    void recordCreateRace() {
        state.createRace().increment();
    }

    void recordCreateFailure() {
        state.createFail().increment();
    }
}
