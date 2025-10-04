package kz.qazmarka.h2k.kafka.producer.bench;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import kz.qazmarka.h2k.kafka.producer.BatchSender;

/**
 * Бенчмарки горячих операций {@link BatchSender}: измеряем расходы tryFlush/flush на партиях
 * различного размера, чтобы отслеживать влияние изменений на задержку и адаптивный порог.
 */
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
     * Авто-сброс небольшой партии (типичный фон нагрузки).
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
     * Большая партия, которая провоцирует адаптивное снижение порога ожидания.
     */
    @Benchmark
    public boolean tryFlushLarge(SenderState state) {
        state.fill(600);
        boolean ok = state.sender.tryFlush();
        state.sender.resetCounters();
        return ok;
    }
}
