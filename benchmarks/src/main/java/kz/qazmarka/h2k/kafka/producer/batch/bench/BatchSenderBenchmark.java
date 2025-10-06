package kz.qazmarka.h2k.kafka.producer.batch.bench;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import kz.qazmarka.h2k.kafka.producer.batch.BatchSender;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSenderMetrics;
import kz.qazmarka.h2k.kafka.producer.batch.BatchSenderTuner;

/**
 * Бенчмарки горячих операций {@link BatchSender}: измеряем среднее время выполнения ключевых операций
 * tryFlush/flush на партиях различного размера, чтобы снимать регрессию по задержке и адаптивному порогу.
 *
 * Формат отчёта JMH:
 *  - колонка {@code Score} показывает среднюю продолжительность одной операции в микросекундах;
 *  - {@code Error} — доверительный интервал (99%), по нему видно разброс;
 *  - чем ниже {@code Score}, тем быстрее сценарий.
 *
 * Запускайте бенчмарк точечно (см. README) и фиксируйте baseline — значения стоит сравнивать до/после
 * изменения горячего пути или логики адаптивного awaitEvery.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BatchSenderBenchmark {

private static final int BASE_AWAIT = 256;
private static final int TIMEOUT_MS = 1_000;
private static final int AUTOTUNE_BASE_AWAIT = 64;
private static final int AUTOTUNE_TIMEOUT_MS = 120;

    /**
     * Состояние одного потока: переиспользуем {@link BatchSender}, как это происходит
     * внутри {@code WalEntryProcessor} на реальном RegionServer.
     */
    @State(Scope.Thread)
    public static class SenderState {
        BatchSender sender;

        @Setup(Level.Iteration)
        public void setUp() {
            sender = new BatchSender(BASE_AWAIT, TIMEOUT_MS, true, false);
        }

        void fill(int count) {
            for (int i = 0; i < count; i++) {
                sender.add(ok());
            }
        }

        private static CompletableFuture<RecordMetadata> ok() {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Авто-сброс небольшой партии (типичный фон нагрузки). См. Score — это среднее время tryFlush()
     * на 32 futures. Рост более чем на 10% от базовой выборки сигнализирует о регрессии в проверке
     * порога или работе адаптивного awaitEvery.
     */
    @Benchmark
    public boolean tryFlushSmall(SenderState state) {
        state.fill(32);
        boolean ok = state.sender.tryFlush();
        state.sender.resetCounters();
        return ok;
    }

    /**
     * Строгий {@link BatchSender#flush()} на средней партии: моделируем финальный хвост перед stop().
     * По Score видно, как долго длится полное ожидание подтверждений; увеличения нужно сравнивать
     * с целевым SLA остановки (обычно {@code < 50 мс} на 256 записей).
     */
    @Benchmark
    public void strictFlushMedium(SenderState state, Blackhole bh)
            throws InterruptedException, ExecutionException, TimeoutException {
        state.fill(256);
        state.sender.flush();
        bh.consume(state.sender.getConfirmedCount());
        state.sender.resetCounters();
    }

    /**
     * Большая партия, которая провоцирует адаптивное снижение порога ожидания. Score иллюстрирует
     * расходы адаптации после всплесков нагрузки; рост означает, что trim/resize или расчёт backoff
     * стали дороже.
     */
    @Benchmark
    public boolean tryFlushLarge(SenderState state) {
        state.fill(600);
        boolean ok = state.sender.tryFlush();
        state.sender.resetCounters();
        return ok;
    }

    /**
     * Сценарии для стресс-теста автотюнера: имитируем задержки и таймауты, чтобы увидеть, как
     * {@link BatchSenderTuner} реагирует на них. Возвращаемое значение — итоговое рекомендованное
     * значение awaitEvery (потребляется {@link Blackhole}, чтобы не мешать JIT).
     */
    @Benchmark
    public long autotuneScenarios(AutotuneState state, Blackhole bh) {
        long recommended = state.executeScenario();
        bh.consume(recommended);
        return recommended;
    }

    @State(Scope.Thread)
    public static class AutotuneState {

        @org.openjdk.jmh.annotations.Param({"SUCCESS", "SLOW_HIGH", "FAIL_THEN_RECOVER"})
        public String scenario;

        BatchSenderMetrics metrics;
        BatchSenderTuner tuner;

        @Setup(Level.Invocation)
        public void setUp() {
            metrics = new BatchSenderMetrics();
            tuner = new BatchSenderTuner(true,
                    Math.max(8, AUTOTUNE_BASE_AWAIT / 4),
                    AUTOTUNE_BASE_AWAIT * 4,
                    70L,
                    20L,
                    10_000L);
        }

        long executeScenario() {
            ScenarioMode mode = ScenarioMode.valueOf(scenario);
            return mode.execute(this);
        }

        BatchSender newSender() {
            BatchSender sender = new BatchSender(AUTOTUNE_BASE_AWAIT, AUTOTUNE_TIMEOUT_MS, true, false, metrics);
            metrics.updateConfiguredAwaitEvery(AUTOTUNE_BASE_AWAIT);
            return sender;
        }

        void fill(BatchSender sender, int count, SimulatedFutureTemplate template) {
            for (int i = 0; i < count; i++) {
                sender.add(template.create());
            }
        }
    }

    private enum ScenarioMode {
        /** Все завершения мгновенные — тюнер удерживает конфигурацию. */
        SUCCESS {
            @Override
            long execute(AutotuneState state) {
                BatchSender sender = state.newSender();
                state.fill(sender, AUTOTUNE_BASE_AWAIT, SimulatedFutureTemplate.SUCCESS);
                boolean ok = sender.tryFlush();
                state.tuner.afterBatch(state.metrics, ok);
                return state.tuner.recommendedAwaitEvery();
            }
        },
        /** Подтверждения приходят с задержкой &gt; threshold — тюнер должен понизить awaitEvery. */
        SLOW_HIGH {
            @Override
            long execute(AutotuneState state) {
                BatchSender sender = state.newSender();
                state.fill(sender, AUTOTUNE_BASE_AWAIT, SimulatedFutureTemplate.SLOW_HIGH);
                boolean ok = sender.tryFlush();
                state.tuner.afterBatch(state.metrics, ok);
                return state.tuner.recommendedAwaitEvery();
            }
        },
        /** Пара таймаутов, затем восстановление: проверяем возврат к исходному awaitEvery. */
        FAIL_THEN_RECOVER {
            @Override
            long execute(AutotuneState state) {
                // две последовательные ошибки
                for (int i = 0; i < 2; i++) {
                    BatchSender sender = state.newSender();
                    state.fill(sender, AUTOTUNE_BASE_AWAIT / 2, SimulatedFutureTemplate.FAIL_TIMEOUT);
                    boolean ok = sender.tryFlush();
                    state.tuner.afterBatch(state.metrics, ok);
                }
                // успешные циклы с небольшой задержкой
                for (int i = 0; i < 3; i++) {
                    BatchSender sender = state.newSender();
                    state.fill(sender, AUTOTUNE_BASE_AWAIT, SimulatedFutureTemplate.SLOW_HIGH_SUCCESS);
                    boolean ok = sender.tryFlush();
                    state.tuner.afterBatch(state.metrics, ok);
                }
                return state.tuner.recommendedAwaitEvery();
            }
        };

        abstract long execute(AutotuneState state);
    }

    private static final class SimulatedFutureTemplate {
        static final SimulatedFutureTemplate SUCCESS = new SimulatedFutureTemplate(0L, false);
        static final SimulatedFutureTemplate SLOW_HIGH = new SimulatedFutureTemplate(80L, false);
        static final SimulatedFutureTemplate SLOW_HIGH_SUCCESS = new SimulatedFutureTemplate(30L, false);
        static final SimulatedFutureTemplate FAIL_TIMEOUT = new SimulatedFutureTemplate(0L, true);

        private final long sleepMillis;
        private final boolean fail;

        private SimulatedFutureTemplate(long sleepMillis, boolean fail) {
            this.sleepMillis = sleepMillis;
            this.fail = fail;
        }

        SimulatedFuture create() {
            return new SimulatedFuture(sleepMillis, fail);
        }
    }

    private static final class SimulatedFuture implements Future<RecordMetadata> {
        private final long sleepMillis;
        private final boolean fail;

        private SimulatedFuture(long sleepMillis, boolean fail) {
            this.sleepMillis = sleepMillis;
            this.fail = fail;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public RecordMetadata get() throws InterruptedException {
            try {
                return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new UnexpectedSimulatedTimeoutException("неожиданный таймаут в смоделированном Future", e);
            }
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            long timeoutMs = unit.toMillis(timeout);
            boolean bounded = timeoutMs > 0L && timeout < Long.MAX_VALUE;
            if (sleepMillis > 0L) {
                long actual = sleepMillis;
                if (bounded && timeoutMs < actual) {
                    Thread.sleep(timeoutMs);
                    throw new TimeoutException("смоделированный таймаут");
                }
                Thread.sleep(actual);
            }
            if (fail) {
                throw new SimulatedTimeoutException("смоделированная ошибка");
            }
            return null;
        }
    }
}
