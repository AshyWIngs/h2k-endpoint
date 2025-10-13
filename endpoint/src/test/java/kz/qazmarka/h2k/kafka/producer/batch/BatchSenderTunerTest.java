package kz.qazmarka.h2k.kafka.producer.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BatchSenderTunerTest {

    @Test
    @DisplayName("Отключённый тюнер возвращает исходное значение и не накапливает решения")
    void disabled_returnsConfigured() {
        BatchSenderTuner tuner = new BatchSenderTuner(false, 100, 500, 200, 50, 1_000);
        BatchSenderMetrics metrics = new BatchSenderMetrics();
        metrics.updateConfiguredAwaitEvery(300);

        assertEquals(300, tuner.selectAwaitEvery(300, metrics));
        tuner.afterBatch(metrics, true);
        assertEquals(0, tuner.recommendedAwaitEvery());
        assertEquals(0, tuner.decisionsTotal());
    }

    @Test
    @DisplayName("Высокая задержка уменьшает awaitEvery, решение фиксируется один раз")
    void highLatency_decreasesAwait() {
        BatchSenderTuner tuner = new BatchSenderTuner(true, 100, 1000, 200, 80, 1);
        BatchSenderMetrics metrics = new BatchSenderMetrics();
        metrics.updateConfiguredAwaitEvery(200);

        assertEquals(200, tuner.selectAwaitEvery(200, metrics));

        metrics.onFlushSuccess(10, 250, 200);
        tuner.afterBatch(metrics, true);

        assertEquals(150, tuner.recommendedAwaitEvery());
        assertEquals(150, tuner.selectAwaitEvery(200, metrics));
        assertEquals(1, tuner.decisionsTotal());
        assertFalse(tuner.lastDecisionWasIncrease());
        assertEquals(250, tuner.lastObservedLatencyMs());
    }

    @Test
    @DisplayName("Низкая задержка увеличивает awaitEvery в пределах max")
    void lowLatency_increasesAwait() {
        BatchSenderTuner tuner = new BatchSenderTuner(true, 100, 400, 300, 90, 1);
        BatchSenderMetrics metrics = new BatchSenderMetrics();
        metrics.updateConfiguredAwaitEvery(120);

        assertEquals(120, tuner.selectAwaitEvery(120, metrics));

        metrics.onFlushSuccess(8, 70, 120);
        tuner.afterBatch(metrics, true);

        assertEquals(144, tuner.recommendedAwaitEvery());
        assertTrue(tuner.lastDecisionWasIncrease());
        assertEquals(1, tuner.decisionsTotal());
    }

    @Test
    @DisplayName("Cooldown блокирует повторное решение, пока не истекло время")
    void cooldown_preventsRapidChanges() {
        BatchSenderTuner tuner = new BatchSenderTuner(true, 100, 500, 200, 80, 1_000);
        BatchSenderMetrics metrics = new BatchSenderMetrics();
        metrics.updateConfiguredAwaitEvery(200);

        tuner.selectAwaitEvery(200, metrics);
        metrics.onFlushSuccess(5, 260, 200);
        tuner.afterBatch(metrics, true);
        assertEquals(150, tuner.recommendedAwaitEvery());
        assertEquals(1, tuner.decisionsTotal());

        metrics.onFlushSuccess(5, 60, 150);
        tuner.afterBatch(metrics, true);

        assertEquals(150, tuner.recommendedAwaitEvery());
        assertEquals(1, tuner.decisionsTotal());
    }

    @Test
    @DisplayName("Неуспешный сброс сбрасывает awaitEvery к минимуму и считает решение")
    void failure_forcesMinimum() {
        BatchSenderTuner tuner = new BatchSenderTuner(true, 80, 400, 250, 100, 1);
        BatchSenderMetrics metrics = new BatchSenderMetrics();
        metrics.updateConfiguredAwaitEvery(200);

        tuner.selectAwaitEvery(200, metrics);
        metrics.onFlushSuccess(5, 120, 200);
        metrics.onFlushFailure(200);

        tuner.afterBatch(metrics, false);

        assertEquals(80, tuner.recommendedAwaitEvery());
        assertEquals(1, tuner.decisionsTotal());
        assertFalse(tuner.lastDecisionWasIncrease());
    }
}
