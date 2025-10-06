package kz.qazmarka.h2k.kafka.producer.batch.bench;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

    /**
     * Состояние одного потока: переиспользуем {@link BatchSender}, как это происходит
     * внутри {@code WalEntryProcessor} на реальном RegionServer.
     */
    @State(Scope.Thread)
    public static class SenderState {
        private static final int BASE_AWAIT = 256;
        private static final int TIMEOUT_MS = 1_000;

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
}
